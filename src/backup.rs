use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use zstd::Encoder;

use crate::conf::{Custom, ScrollMode};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BackupIndexCatalog {
    pub indices: Vec<BackupIndexEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupIndexEntry {
    pub name: String,
    pub dir: String,
    pub created_at: String,
    pub docs_total: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupMetadata {
    pub created_at: String,
    pub from_endpoint: String,
    pub to_endpoint: String,
    pub index_name: String,
    pub name_of_copy: Option<String>,
    pub alias_name: Option<String>,
    pub alias_remove_if_exists: bool,
    pub routing_field: Option<String>,
    pub pre_create_doc_ids: bool,
    pub pre_create_doc_source: String,
    pub scroll_mode: ScrollMode,
    pub buffer_size: u64,
    pub copy_mapping: bool,
    pub copy_content: bool,
    pub delete_if_exists: bool,
    pub custom: Option<Custom>,
    pub original_number_of_shards: Option<u64>,
    pub original_number_of_replicas: Option<u64>,
    pub docs_total: Option<u64>,
    pub quantile_field: Option<String>,
    pub quantile_digest: Option<Vec<QuantileCentroid>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupDoc {
    pub id: String,
    pub doc_type: String,
    pub source: Value,
}

pub struct BackupChunkWriter {
    dir: PathBuf,
    max_docs: u64,
    current_docs: u64,
    counter: u64,
    writer: Option<Encoder<'static, File>>,
}

impl BackupChunkWriter {
    pub fn new(dir: PathBuf, max_docs: u64) -> Self {
        Self {
            dir,
            max_docs,
            current_docs: 0,
            counter: 0,
            writer: None,
        }
    }

    pub fn write_doc(&mut self, doc: &BackupDoc) -> std::io::Result<()> {
        let line = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
        if self.current_docs >= self.max_docs {
            self.finish()?;
        }
        if self.writer.is_none() {
            self.start_new()?;
        }
        if let Some(writer) = &mut self.writer {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            self.current_docs += 1;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.take() {
            let _ = writer.finish()?;
        }
        self.current_docs = 0;
        Ok(())
    }

    fn start_new(&mut self) -> std::io::Result<()> {
        let timestamp = Utc::now().format("%Y%m%d-%H%M%S-%6f");
        let pid = std::process::id();
        let base = format!("{:06}-{}-{}", self.counter, timestamp, pid);
        let mut attempt = 0u32;
        let file = loop {
            let suffix = if attempt == 0 {
                String::new()
            } else {
                format!("-{:03}", attempt)
            };
            let file_name = format!("{}{}.jsonl.zst", base, suffix);
            let file_path = self.dir.join(file_name);
            match OpenOptions::new().write(true).create_new(true).open(file_path) {
                Ok(file) => break file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(err) => return Err(err),
            }
        };
        let encoder = Encoder::new(file, 3)?;
        self.writer = Some(encoder);
        self.counter += 1;
        self.current_docs = 0;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuantileCentroid {
    pub mean: f64,
    pub count: f64,
}

#[derive(Debug, Clone)]
pub struct QuantileDigest {
    centroids: Vec<QuantileCentroid>,
    max_centroids: usize,
    pending: usize,
}

impl QuantileDigest {
    pub fn new(max_centroids: usize) -> Self {
        Self {
            centroids: Vec::new(),
            max_centroids: max_centroids.max(20),
            pending: 0,
        }
    }

    pub fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.add_centroid(value, 1.0);
    }

    pub fn add_centroid(&mut self, mean: f64, count: f64) {
        if !mean.is_finite() || !count.is_finite() || count <= 0.0 {
            return;
        }
        self.centroids.push(QuantileCentroid { mean, count });
        self.pending += 1;
        if self.pending >= self.max_centroids * 10 {
            self.compress();
            self.pending = 0;
        }
    }

    pub fn into_centroids(mut self) -> Vec<QuantileCentroid> {
        self.compress();
        self.centroids
    }

    fn compress(&mut self) {
        if self.centroids.len() <= self.max_centroids {
            return;
        }
        self.centroids
            .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));
        let total: f64 = self.centroids.iter().map(|c| c.count).sum();
        if total <= 0.0 {
            return;
        }
        let max_weight = (total / self.max_centroids as f64).max(1.0);
        let mut merged: Vec<QuantileCentroid> = Vec::with_capacity(self.max_centroids);
        let mut current = self.centroids[0].clone();
        for centroid in self.centroids.iter().skip(1) {
            if current.count + centroid.count <= max_weight {
                let combined = current.count + centroid.count;
                let mean = (current.mean * current.count + centroid.mean * centroid.count) / combined;
                current = QuantileCentroid {
                    mean,
                    count: combined,
                };
            } else {
                merged.push(current);
                current = centroid.clone();
            }
        }
        merged.push(current);
        self.centroids = merged;
    }
}

pub fn merge_quantile_centroids(
    existing: Option<Vec<QuantileCentroid>>,
    incoming: Option<Vec<QuantileCentroid>>,
    max_centroids: usize,
) -> Option<Vec<QuantileCentroid>> {
    let mut digest = QuantileDigest::new(max_centroids);
    let mut added = false;
    if let Some(list) = existing {
        for centroid in list {
            digest.add_centroid(centroid.mean, centroid.count);
            added = true;
        }
    }
    if let Some(list) = incoming {
        for centroid in list {
            digest.add_centroid(centroid.mean, centroid.count);
            added = true;
        }
    }
    if added {
        Some(digest.into_centroids())
    } else {
        None
    }
}

pub fn with_file_lock<T, F>(lock_path: &Path, action: F) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T>,
{
    let start = SystemTime::now();
    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
        {
            Ok(mut file) => {
                let pid = std::process::id();
                let _ = writeln!(file, "pid={}", pid);
                let result = action();
                let _ = fs::remove_file(lock_path);
                return result;
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Ok(metadata) = fs::metadata(lock_path) {
                    if let Ok(modified) = metadata.modified() {
                        if SystemTime::now()
                            .duration_since(modified)
                            .unwrap_or(Duration::ZERO)
                            > Duration::from_secs(600)
                        {
                            let _ = fs::remove_file(lock_path);
                            continue;
                        }
                    }
                }
                if SystemTime::now()
                    .duration_since(start)
                    .unwrap_or(Duration::ZERO)
                    > Duration::from_secs(20)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout waiting for lock",
                    ));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(err) => return Err(err),
        }
    }
}

pub fn merge_metadata(existing: BackupMetadata, incoming: BackupMetadata) -> BackupMetadata {
    let docs_total = match (existing.docs_total, incoming.docs_total) {
        (Some(a), Some(b)) => Some(a.saturating_add(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    let quantile_field = existing
        .quantile_field
        .clone()
        .or(incoming.quantile_field.clone());
    let quantile_digest = merge_quantile_centroids(
        existing.quantile_digest.clone(),
        incoming.quantile_digest.clone(),
        200,
    );

    BackupMetadata {
        created_at: existing.created_at,
        from_endpoint: existing.from_endpoint,
        to_endpoint: existing.to_endpoint,
        index_name: existing.index_name,
        name_of_copy: existing.name_of_copy.or(incoming.name_of_copy),
        alias_name: existing.alias_name.or(incoming.alias_name),
        alias_remove_if_exists: existing.alias_remove_if_exists,
        routing_field: existing.routing_field.or(incoming.routing_field),
        pre_create_doc_ids: existing.pre_create_doc_ids,
        pre_create_doc_source: existing.pre_create_doc_source,
        scroll_mode: existing.scroll_mode,
        buffer_size: existing.buffer_size,
        copy_mapping: existing.copy_mapping,
        copy_content: existing.copy_content,
        delete_if_exists: existing.delete_if_exists,
        custom: existing.custom.or(incoming.custom),
        original_number_of_shards: existing
            .original_number_of_shards
            .or(incoming.original_number_of_shards),
        original_number_of_replicas: existing
            .original_number_of_replicas
            .or(incoming.original_number_of_replicas),
        docs_total,
        quantile_field,
        quantile_digest,
    }
}

pub fn write_metadata_with_lock(
    path: &Path,
    incoming: BackupMetadata,
) -> std::io::Result<BackupMetadata> {
    let lock_path = path.with_extension("lock");
    with_file_lock(&lock_path, || {
        let merged = if path.exists() {
            match read_json_file::<BackupMetadata>(path) {
                Ok(existing) => merge_metadata(existing, incoming),
                Err(_) => incoming,
            }
        } else {
            incoming
        };
        write_json_file(path, &merged)?;
        Ok(merged)
    })
}

pub fn update_catalog_entry_with_lock(
    path: &Path,
    entry: BackupIndexEntry,
) -> std::io::Result<()> {
    let lock_path = path.with_extension("lock");
    with_file_lock(&lock_path, || {
        let mut catalog = if path.exists() {
            load_catalog(path)
        } else {
            BackupIndexCatalog::default()
        };
        if let Some(existing) = catalog.indices.iter_mut().find(|item| item.name == entry.name) {
            if let Some(total) = entry.docs_total {
                existing.docs_total = Some(total);
            }
            if existing.created_at.is_empty() {
                existing.created_at = entry.created_at;
            }
        } else {
            catalog.indices.push(entry);
        }
        save_catalog(path, &catalog)
    })
}

pub fn extract_quantile_value(source: &Value, field: &str) -> Option<f64> {
    let value = if field.starts_with('/') {
        source.pointer(field)
    } else if field.contains('.') {
        let mut current = source;
        for part in field.split('.') {
            current = current.get(part)?;
        }
        Some(current)
    } else {
        source.get(field)
    }?;

    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => {
            if let Ok(parsed) = f64::from_str(text) {
                Some(parsed)
            } else if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
                Some(parsed.timestamp_millis() as f64)
            } else if let Ok(parsed) = chrono::DateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f%z") {
                Some(parsed.timestamp_millis() as f64)
            } else if let Ok(parsed) = chrono::DateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%z") {
                Some(parsed.timestamp_millis() as f64)
            } else {
                None
            }
        }
        _ => None,
    }
}

pub fn ensure_dir(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

pub fn load_catalog(path: &Path) -> BackupIndexCatalog {
    if let Ok(content) = fs::read_to_string(path) {
        if let Ok(value) = serde_json::from_str::<BackupIndexCatalog>(&content) {
            return value;
        }
    }
    BackupIndexCatalog::default()
}

pub fn save_catalog(path: &Path, catalog: &BackupIndexCatalog) -> std::io::Result<()> {
    let content = serde_json::to_string_pretty(catalog).unwrap_or_else(|_| "{}".to_string());
    fs::write(path, content)
}

pub fn write_json_file<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let content = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string());
    fs::write(path, content)
}

pub fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> std::io::Result<T> {
    let content = fs::read_to_string(path)?;
    let value = serde_json::from_str(&content)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(value)
}

pub fn list_data_files(path: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = vec![];
    if path.exists() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".jsonl.zst"))
                .unwrap_or(false)
            {
                files.push(entry_path);
            }
        }
    }
    files.sort();
    Ok(files)
}

pub fn read_backup_docs(path: &Path) -> std::io::Result<Vec<BackupDoc>> {
    let file = File::open(path)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::new(decoder);
    let mut docs = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let doc: BackupDoc = serde_json::from_str(&line)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        docs.push(doc);
    }
    Ok(docs)
}

pub fn extract_routing_ids(
    docs: &[BackupDoc],
    routing_field: &Option<String>,
) -> HashSet<String> {
    let mut ids = HashSet::new();
    if let Some(pointer) = routing_field {
        for doc in docs {
            if let Some(value) = doc.source.pointer(pointer) {
                if let Some(id) = value.as_str() {
                    ids.insert(id.to_string());
                }
            }
        }
    }
    ids
}
