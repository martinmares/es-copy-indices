use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupDoc {
    pub id: String,
    pub doc_type: String,
    pub source: Value,
}

pub struct BackupChunkWriter {
    dir: PathBuf,
    max_bytes: u64,
    current_bytes: u64,
    counter: u64,
    writer: Option<Encoder<'static, File>>,
}

impl BackupChunkWriter {
    pub fn new(dir: PathBuf, max_bytes: u64) -> Self {
        Self {
            dir,
            max_bytes,
            current_bytes: 0,
            counter: 0,
            writer: None,
        }
    }

    pub fn write_doc(&mut self, doc: &BackupDoc) -> std::io::Result<()> {
        let line = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
        let line_len = (line.len() + 1) as u64;
        if self.current_bytes > 0 && self.current_bytes + line_len > self.max_bytes {
            self.finish()?;
        }
        if self.writer.is_none() {
            self.start_new()?;
        }
        if let Some(writer) = &mut self.writer {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            self.current_bytes += line_len;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.take() {
            let _ = writer.finish()?;
        }
        self.current_bytes = 0;
        Ok(())
    }

    fn start_new(&mut self) -> std::io::Result<()> {
        let timestamp = Utc::now().format("%Y%m%d-%H%M%S-%6f");
        let file_name = format!("{:06}-{}.jsonl.zst", self.counter, timestamp);
        let file_path = self.dir.join(file_name);
        let file = File::create(file_path)?;
        let encoder = Encoder::new(file, 3)?;
        self.writer = Some(encoder);
        self.counter += 1;
        self.current_bytes = 0;
        Ok(())
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
