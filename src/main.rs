#![allow(dead_code)]

mod audit_builder;
mod backup;
mod conf;
mod es_client;
mod models;
mod utils;

// use std::thread;
// use std::time::Duration;

use audit_builder::AuditBuilder;
use clap::{command, value_parser, Arg};
use tracing::{error, info, warn};
use tracing_subscriber;
use tracing_subscriber::EnvFilter;
// use env_logger::Env;
// use log::{error, info, warn};
use std::path::{Path, PathBuf};
use chrono::Utc;
use twelf::Layer;
use crate::backup::{BackupChunkWriter, BackupDoc, BackupIndexEntry, BackupMetadata};
use std::collections::{HashMap, HashSet};

async fn flush_restore_batch(
    docs: &mut Vec<BackupDoc>,
    dest_name: &str,
    index: &conf::Index,
    destination_es_client: &mut es_client::EsClient,
    server_major_version: u64,
    pre_created_ids: &mut HashSet<String>,
) {
    if docs.is_empty() {
        return;
    }
    let is_routing_field = index.is_routing_field();
    let routing_field = index.get_routing_field().clone();
    let mut bulk_body_pre_create = String::new();
    let mut bulk_body = String::new();

    for doc in docs.drain(..) {
        let mut add_routing_to_bulk: Option<String> = None;
        if is_routing_field {
            if let Some(pointer) = &routing_field {
                if let Some(value) = doc.source.pointer(pointer) {
                    if let Some(id) = value.as_str() {
                        add_routing_to_bulk = Some(id.to_string());
                        if index.is_pre_create_doc_ids() && !pre_created_ids.contains(id) {
                            pre_created_ids.insert(id.to_string());
                            if server_major_version <= 7 {
                                bulk_body_pre_create.push_str(&format!(
                                    "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\" }} }}",
                                    "create", dest_name, "_doc", id));
                            } else {
                                bulk_body_pre_create.push_str(&format!(
                                    "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_id\" : \"{}\" }} }}",
                                    "create", dest_name, id
                                ));
                            }
                            bulk_body_pre_create.push_str("\n");
                            bulk_body_pre_create.push_str(index.get_pre_create_doc_source());
                            bulk_body_pre_create.push_str("\n");
                        }
                    }
                }
            }
        }
        let id = doc.id;
        let doc_type = doc.doc_type;
        if server_major_version <= 7 {
            if let Some(id_routing) = add_routing_to_bulk {
                bulk_body.push_str(&format!(
                    "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\", \"routing\": \"{}\" }} }}",
                    "index", dest_name, doc_type, id, id_routing));
            } else {
                bulk_body.push_str(&format!(
                    "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\" }} }}",
                    "index", dest_name, doc_type, id));
            }
        } else {
            if let Some(id_routing) = add_routing_to_bulk {
                bulk_body.push_str(&format!(
                    "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_id\" : \"{}\", \"routing\": \"{}\" }} }}",
                    "index", dest_name, id, id_routing
                ));
            } else {
                bulk_body.push_str(&format!(
                    "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_id\" : \"{}\" }} }}",
                    "index", dest_name, id
                ));
            }
        }
        bulk_body.push_str("\n");
        bulk_body.push_str(&serde_json::to_string(&doc.source).unwrap());
        bulk_body.push_str("\n");
    }

    if !bulk_body_pre_create.is_empty() {
        let _ = destination_es_client.post_bulk(dest_name, &bulk_body_pre_create).await;
    }

    let _ = destination_es_client.post_bulk(dest_name, &bulk_body).await;
}

fn resolve_backup_dir(config_path: &Path, dir: &str) -> PathBuf {
    let dir_path = PathBuf::from(dir);
    if dir_path.is_absolute() {
        dir_path
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(dir)
    }
}

fn is_run_id_component(value: &str) -> bool {
    if value.len() != 20 {
        return false;
    }
    let bytes = value.as_bytes();
    if bytes[8] != b'-' || bytes[15] != b'-' {
        return false;
    }
    for (idx, byte) in bytes.iter().enumerate() {
        if idx == 8 || idx == 15 {
            continue;
        }
        if !byte.is_ascii_digit() {
            return false;
        }
    }
    true
}

fn validate_endpoint_for_backup(endpoint: &conf::Endpoint) -> Result<(), String> {
    if endpoint.has_backup_dir() {
        if !endpoint.get_url().is_empty() {
            return Err(format!(
                "endpoint '{}' has backup_dir and url set; remove url for backup endpoints",
                endpoint.get_name()
            ));
        }
        if endpoint.is_basic_auth() {
            return Err(format!(
                "endpoint '{}' has backup_dir and basic_auth set; remove auth for backup endpoints",
                endpoint.get_name()
            ));
        }
    } else if endpoint.get_url().is_empty() {
        return Err(format!(
            "endpoint '{}' missing url (required for ES endpoints)",
            endpoint.get_name()
        ));
    }
    Ok(())
}

fn get_endpoint<'a>(endpoints: &'a Vec<conf::Endpoint>, name: &String) -> Option<&'a conf::Endpoint> {
    endpoints.iter().find(|endpoint| endpoint.get_name() == name)
}

fn apply_settings_overrides(settings_value: &mut serde_json::Value, index: &conf::Index) {
    if let Some(settings_val) = settings_value.get_mut("settings") {
        if let Some(index_val) = settings_val.get_mut("index") {
            index_val.as_object_mut().unwrap().insert(
                "number_of_shards".to_string(),
                index.get_number_of_shards().into(),
            );
            index_val.as_object_mut().unwrap().insert(
                "number_of_replicas".to_string(),
                index.get_number_of_replicas().into(),
            );
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_file(true)
        .with_line_number(true)
        .init();

    let matches = command!()
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .help("Sets a config file")
                .value_parser(value_parser!(PathBuf))
                .required(true),
        )
        .get_matches();

    info!("Application started!");

    let config_path = if let Some(value) = matches.get_one::<PathBuf>("config") {
        value.to_owned()
    } else {
        panic!("config path must be set!")
    };

    info!("Args config_path={:?}", config_path);

    let config: conf::Config;

    match conf::Config::with_layers(&[Layer::Toml(config_path.clone())]) {
        Ok(value) => {
            config = value;
        }
        Err(e) => {
            error!("Error loading config {:?}, error: {:?}", config_path, e);
            panic!("Failed to load config file with name {:?}!", config_path)
        }
    }

    for endpoint in config.get_endpoints() {
        if let Err(message) = validate_endpoint_for_backup(endpoint) {
            error!("Invalid endpoint configuration: {}", message);
            panic!("Invalid endpoint configuration!");
        }
    }

    let backup_run_tag = format!(
        "{}-{:06}",
        Utc::now().format("%Y%m%d-%H%M%S-%6f"),
        std::process::id()
    );
    let mut backup_run_roots: HashMap<String, PathBuf> = HashMap::new();

    for index in config.get_indices() {
        let mut indices_names: Vec<String> = vec![];
        // if index.is_multiple() {
        //     index_names.push(index.get_name().clone());
        //     info!("Copy multiple indices! ({:?})", index_names);
        // } else {
        //     index_names.push(index.get_name().clone());
        // }
        // for index_name in &index_names {
        // let index_name = index.get_name();
        let from = index.get_from();
        let to = index.get_to();

        let from_endpoint = get_endpoint(config.get_endpoints(), from)
            .unwrap_or_else(|| panic!("Missing endpoint '{}'", from));
        let to_endpoint = get_endpoint(config.get_endpoints(), to)
            .unwrap_or_else(|| panic!("Missing endpoint '{}'", to));

        let from_backup = from_endpoint.has_backup_dir();
        let to_backup = to_endpoint.has_backup_dir();

        if from_backup && to_backup {
            panic!(
                "Both source and destination endpoints have backup_dir for index '{}'",
                index.get_name()
            );
        }

        let mut source_es_client = if !from_backup {
            let mut client = utils::create_es_client(config.get_endpoints(), from)
                .await
                .expect("Create source elastic client failed!");
            client.print_server_info(from);
            Some(client)
        } else {
            None
        };

        let mut destination_es_client = if !to_backup {
            let mut client = utils::create_es_client(config.get_endpoints(), to)
                .await
                .expect("Create destination elastic client failed!");
            client.print_server_info(to);
            Some(client)
        } else {
            None
        };

        let mut index_name_of_copy = match index.get_name_of_copy() {
            Some(name) => name,
            None => index.get_name(),
        };

        if index.is_multiple() {
            if from_backup {
                panic!("Multiple index backup patterns are not supported from backup_dir");
            }
            indices_names = source_es_client
                .as_mut()
                .unwrap()
                .get_indices_names(&index.get_name())
                .await;
            info!(
                "Copying multiple indices ... {:?} ...",
                indices_names.iter().take(5).collect::<Vec<_>>()
            );
            // thread::sleep(Duration::from_secs(120));
        } else {
            indices_names.push(index.get_name().clone());
        }

        for index_name in &indices_names {
            if index.is_multiple() {
                index_name_of_copy = index_name;
            }

            if to_backup {
                let backup_dir = if let Some(existing) = backup_run_roots.get(to) {
                    existing.clone()
                } else {
                    let base = resolve_backup_dir(
                        config_path.as_path(),
                        to_endpoint.get_backup_dir().as_ref().unwrap(),
                    );
                    let run_root = match base.file_name().and_then(|name| name.to_str()) {
                        Some(name) if is_run_id_component(name) => base.clone(),
                        _ => base.join(&backup_run_tag),
                    };
                    backup_run_roots.insert(to.to_string(), run_root.clone());
                    run_root
                };
                let index_dir = backup_dir.join(index_name);
                let data_dir = index_dir.join("data");
                if let Err(err) = backup::ensure_dir(&data_dir) {
                    panic!("Failed to create backup directories: {:?}", err);
                }

                let backup_catalog_path = backup_dir.join("indices.json");

                let mapping = source_es_client
                    .as_mut()
                    .unwrap()
                    .fetch_mappings(index_name)
                    .await
                    .expect("Failed to fetch mappings");
                let (settings, original_shards, original_replicas) = source_es_client
                    .as_mut()
                    .unwrap()
                    .fetch_settings(index_name)
                    .await
                    .expect("Failed to fetch settings");

                let mut metadata = BackupMetadata {
                    created_at: Utc::now().to_rfc3339(),
                    from_endpoint: from.to_string(),
                    to_endpoint: to.to_string(),
                    index_name: index_name.to_string(),
                    name_of_copy: index.get_name_of_copy().clone(),
                    alias_name: index.get_alias_name(),
                    alias_remove_if_exists: index.is_alias_remove_if_exists(),
                    routing_field: index.get_routing_field().clone(),
                    pre_create_doc_ids: index.is_pre_create_doc_ids(),
                    pre_create_doc_source: index.get_pre_create_doc_source().to_string(),
                    scroll_mode: index.get_scroll_mode().clone(),
                    buffer_size: index.get_buffer_size(),
                    copy_mapping: index.is_copy_mapping(),
                    copy_content: index.is_copy_content(),
                    delete_if_exists: index.is_delete_if_exists(),
                    custom: index.get_custom().clone(),
                    original_number_of_shards: original_shards,
                    original_number_of_replicas: original_replicas,
                    docs_total: None,
                    quantile_field: None,
                    quantile_digest: None,
                };

                if let Err(err) = backup::write_json_file(&index_dir.join("mappings.json"), &mapping) {
                    panic!("Failed to write mappings.json: {:?}", err);
                }
                if let Err(err) = backup::write_json_file(&index_dir.join("settings.json"), &settings) {
                    panic!("Failed to write settings.json: {:?}", err);
                }

                if index.is_copy_content() {
                    let max_docs = index.get_buffer_size().max(1);
                    let mut writer = BackupChunkWriter::new(data_dir.clone(), max_docs);
                    let mut quantile_digest = index
                        .get_backup_quantile_field()
                        .as_ref()
                        .map(|field| (field.clone(), backup::QuantileDigest::new(200)));
                    source_es_client
                        .as_mut()
                        .unwrap()
                        .scroll_start(index, index_name)
                        .await;
                    while source_es_client.as_mut().unwrap().has_docs() {
                        let total = source_es_client.as_mut().unwrap().get_total_size();
                        let counter = source_es_client.as_mut().unwrap().get_docs_counter();
                        let percent = if total > 0 {
                            (counter as f64 / total as f64) * 100.00
                        } else {
                            0.00
                        };
                        info!(
                            "Iterate {} - docs {}/{} ({:.2} %)",
                            index_name, counter, total, percent
                        );
                        if let Some(docs) = source_es_client.as_mut().unwrap().get_docs() {
                            for doc in docs {
                                let source_value: serde_json::Value =
                                    serde_json::from_str(doc.get_source()).unwrap_or_default();
                                if let Some((field, digest)) = quantile_digest.as_mut() {
                                    if let Some(value) =
                                        backup::extract_quantile_value(&source_value, field)
                                    {
                                        digest.add(value);
                                    }
                                }
                                let backup_doc = BackupDoc {
                                    id: doc.get_id().to_string(),
                                    doc_type: doc.get_doc_type().to_string(),
                                    source: source_value,
                                };
                                if let Err(err) = writer.write_doc(&backup_doc) {
                                    panic!("Failed to write backup chunk: {:?}", err);
                                }
                            }
                        }
                        source_es_client
                            .as_mut()
                            .unwrap()
                            .scroll_next(index)
                            .await;
                    }
                    metadata.docs_total = Some(source_es_client.as_mut().unwrap().get_total_size());
                    source_es_client.as_mut().unwrap().scroll_stop().await;
                    if let Err(err) = writer.finish() {
                        panic!("Failed to finish backup chunk: {:?}", err);
                    }
                    if let Some((field, digest)) = quantile_digest {
                        let centroids = digest.into_centroids();
                        if !centroids.is_empty() {
                            metadata.quantile_field = Some(field);
                            metadata.quantile_digest = Some(centroids);
                        }
                    }
                }

                let metadata = match backup::write_metadata_with_lock(
                    &index_dir.join("metadata.json"),
                    metadata,
                ) {
                    Ok(value) => value,
                    Err(err) => panic!("Failed to write metadata.json: {:?}", err),
                };

                let entry = BackupIndexEntry {
                    name: index_name.to_string(),
                    dir: index_name.to_string(),
                    created_at: Utc::now().to_rfc3339(),
                    docs_total: metadata.docs_total,
                };
                if let Err(err) =
                    backup::update_catalog_entry_with_lock(&backup_catalog_path, entry)
                {
                    warn!("Failed to update indices.json: {:?}", err);
                }

                info!("Backup completed for {}", index_name);
                continue;
            }

            if from_backup {
                let backup_dir = resolve_backup_dir(
                    config_path.as_path(),
                    from_endpoint.get_backup_dir().as_ref().unwrap(),
                );
                let index_dir = backup_dir.join(index_name);
                let data_dir = index_dir.join("data");

                let mapping: serde_json::Value =
                    backup::read_json_file(&index_dir.join("mappings.json"))
                        .expect("Failed to read mappings.json");
                let mut settings: serde_json::Value =
                    backup::read_json_file(&index_dir.join("settings.json"))
                        .expect("Failed to read settings.json");

                apply_settings_overrides(&mut settings, index);

                let mappings_value = mapping
                    .get("mappings")
                    .cloned()
                    .unwrap_or_else(|| mapping.clone());
                let settings_value = settings
                    .get("settings")
                    .cloned()
                    .unwrap_or_else(|| settings.clone());

                let dest_name = match index.get_name_of_copy() {
                    Some(name) => name,
                    None => index.get_name(),
                };

                if index.is_copy_mapping() {
                    let resp = destination_es_client
                        .as_mut()
                        .unwrap()
                        .create_index_with_mapping_settings(
                            dest_name,
                            &settings_value,
                            &mappings_value,
                        )
                        .await;
                    if let Some(value) = resp {
                        info!("Index mappings and settings (response: {})", value);
                    }
                } else if index.is_custom_mapping() {
                    let _ = destination_es_client
                        .as_mut()
                        .unwrap()
                        .apply_custom_mappings(index, &dest_name.to_string())
                        .await;
                }

                if index.is_copy_content() {
                    let files = backup::list_data_files(&data_dir)
                        .expect("Failed to list backup data files");
                    let max_docs = index.get_buffer_size().max(1) as usize;
                    let server_major_version = destination_es_client
                        .as_ref()
                        .unwrap()
                        .get_server_major_version();
                    let mut batch: Vec<backup::BackupDoc> = Vec::new();
                    let mut pre_created_ids: HashSet<String> = HashSet::new();

                    for file in files {
                        let docs = backup::read_backup_docs(&file)
                            .expect("Failed to read backup data file");
                        for doc in docs {
                            batch.push(doc);
                            if batch.len() >= max_docs {
                                flush_restore_batch(
                                    &mut batch,
                                    dest_name,
                                    index,
                                    destination_es_client.as_mut().unwrap(),
                                    server_major_version,
                                    &mut pre_created_ids,
                                )
                                .await;
                            }
                        }
                    }
                    flush_restore_batch(
                        &mut batch,
                        dest_name,
                        index,
                        destination_es_client.as_mut().unwrap(),
                        server_major_version,
                        &mut pre_created_ids,
                    )
                    .await;
                }

                destination_es_client
                    .as_mut()
                    .unwrap()
                    .create_alias(index)
                    .await;

                info!("Restore completed for {}", index_name);
                continue;
            }

            if !index.is_multiple() {
                if index.is_copy_mapping() {
                    info!(
                        "Copying mapping for {} (from: {}, to: {})",
                        index_name, from, to
                    );
                    source_es_client
                        .as_mut()
                        .unwrap()
                        .copy_mappings_to(
                            destination_es_client.as_mut().unwrap(),
                            &index,
                            index_name,
                            index_name_of_copy,
                        )
                        .await;
                } else if index.is_custom_mapping() {
                    info!(
                        "Copying custom mapping for {} (from: {}, to: {})",
                        index_name, from, to
                    );
                    source_es_client
                        .as_mut()
                        .unwrap()
                        .copy_custom_mappings_to(
                            destination_es_client.as_mut().unwrap(),
                            &index,
                            index_name_of_copy,
                        )
                        .await;
                }
            }

            if index.is_copy_content() {
                info!(
                    "Copying index content {} (from: {}, to: {})",
                    index_name, from, to
                );

                let mut audit_builder: Option<AuditBuilder> = None;
                if let Some(conf_audit) = config.get_audit() {
                    let audit_file = conf_audit.get_file_name();
                    audit_builder = Some(AuditBuilder::new(audit_file).await);
                }

                source_es_client
                    .as_mut()
                    .unwrap()
                    .scroll_start(index, index_name)
                    .await;

                memory_stats!();

                while source_es_client.as_mut().unwrap().has_docs() {
                    let total = source_es_client.as_mut().unwrap().get_total_size();
                    let counter = source_es_client.as_mut().unwrap().get_docs_counter();

                    info!(
                        "Iterate {} - docs {}/{} ({:.2} %)",
                        index_name,
                        counter,
                        total,
                        (counter as f64 / total as f64) * 100.00
                    );

                    // pre create parent->child docs and post bulk insert docs
                    source_es_client
                        .as_mut()
                        .unwrap()
                        .copy_content_to(
                            destination_es_client.as_mut().unwrap(),
                            &index,
                            index_name_of_copy,
                            &mut audit_builder,
                        )
                        .await;

                    // next docs?
                    source_es_client
                        .as_mut()
                        .unwrap()
                        .scroll_next(index)
                        .await;

                    memory_stats!();
                }
                source_es_client.as_mut().unwrap().scroll_stop().await;
            } else {
                warn!(
                    "Copying index content for {} is disabled by config!",
                    index_name
                );
            }

            destination_es_client
                .as_mut()
                .unwrap()
                .create_alias(index)
                .await;

            memory_stats!();

            info!(
                "Copying index {} => {} done!",
                index_name, index_name_of_copy
            );
        }
    }

    info!("Application completed!");

    // Copy indices
}
