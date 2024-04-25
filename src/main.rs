#![allow(dead_code)]

mod conf;
mod es_client;
mod models;
mod utils;

use log::{info, warn};
use std::path::PathBuf;

use clap::{command, value_parser, Arg, ArgAction};
use env_logger::{Builder, Target};
use twelf::Layer;

#[tokio::main]
async fn main() {
    // default logging is to Target::Stderr, switch to Target::Stdout
    // env_logger::init();
    let mut builder = Builder::from_default_env();
    builder.target(Target::Stdout);
    builder.init();

    let matches = command!()
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .help("Sets a config file")
                .value_parser(value_parser!(PathBuf))
                .required(true),
        )
        .arg(
            Arg::new("no-dry-run")
                .short('n')
                .long("no-dry-run")
                .help("Disable dry run only")
                .action(ArgAction::SetTrue),
        )
        .get_matches();

    info!("Application started!");

    let no_dry_run = matches
        .get_one::<bool>("no-dry-run")
        .unwrap_or_else(|| &false)
        .to_owned();

    let config_path = if let Some(value) = matches.get_one::<PathBuf>("config") {
        value.to_owned()
    } else {
        panic!("config path must be set!")
    };

    info!(
        "Args no-dry-run={:?}, config_path={:?}",
        no_dry_run, config_path
    );

    let config = if let Ok(value) = conf::Config::with_layers(&[Layer::Yaml(config_path.clone())]) {
        value
    } else {
        panic!("Failed to load config file with name {:?}!", config_path)
    };

    for index in config.get_indices() {
        let index_name = index.get_name();
        let index_name_of_copy = index.get_name_of_copy();
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

        if index.is_copy_mapping() {
            info!(
                "Copying mapping for {} (from: {}, to: {})",
                index_name, from, to
            );
            source_es_client
                .copy_mappings_to(&mut destination_es_client, index)
                .await;
        }

        if index.is_copy_content() {
            info!(
                "Copying index content {} (from: {}, to: {})",
                index_name, from, to
            );

            source_es_client.scroll_start(index).await;

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
                    .copy_content_to(&mut destination_es_client, &index)
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

    info!("Application completed!");

    // Copy indices
}
