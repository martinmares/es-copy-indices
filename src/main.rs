#![allow(dead_code)]

mod audit_builder;
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
use std::path::PathBuf;
use twelf::Layer;

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

        // Initialize SOURCE client
        let mut source_es_client = utils::create_es_client(config.get_endpoints(), from)
            .await
            .expect("Create source elastic client failed!");
        source_es_client.print_server_info(from);

        // Initialize DESTINATION client
        let mut destination_es_client = utils::create_es_client(config.get_endpoints(), to)
            .await
            .expect("Create destination elastic client failed!");
        destination_es_client.print_server_info(to);

        let mut index_name_of_copy = match index.get_name_of_copy() {
            Some(name) => name,
            None => index.get_name(),
        };

        if index.is_multiple() {
            indices_names = source_es_client.get_indices_names(&index.get_name()).await;
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

            if !index.is_multiple() {
                if index.is_copy_mapping() {
                    info!(
                        "Copying mapping for {} (from: {}, to: {})",
                        index_name, from, to
                    );
                    source_es_client
                        .copy_mappings_to(
                            &mut destination_es_client,
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
                        .copy_custom_mappings_to(
                            &mut destination_es_client,
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

                source_es_client.scroll_start(index, index_name).await;

                memory_stats!();

                while source_es_client.has_docs() {
                    let total = source_es_client.get_total_size();
                    let counter = source_es_client.get_docs_counter();

                    info!(
                        "Iterate {} - docs {}/{} ({:.2} %)",
                        index_name,
                        counter,
                        total,
                        (counter as f64 / total as f64) * 100.00
                    );

                    // pre create parent->child docs and post bulk insert docs
                    source_es_client
                        .copy_content_to(
                            &mut destination_es_client,
                            &index,
                            index_name_of_copy,
                            &mut audit_builder,
                        )
                        .await;

                    // next docs?
                    source_es_client.scroll_next(index).await;

                    memory_stats!();
                }
                source_es_client.scroll_stop().await;
            } else {
                warn!(
                    "Copying index content for {} is disabled by config!",
                    index_name
                );
            }

            destination_es_client.create_alias(index).await;

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
