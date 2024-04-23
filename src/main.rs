#![allow(dead_code)]

mod conf;
mod es_client;
mod models;
mod utils;

use log::info;
use std::path::PathBuf;

use clap::{command, value_parser, Arg, ArgAction};
use twelf::Layer;

#[tokio::main]
async fn main() {
    env_logger::init();

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

    info!("application started!");

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
        "args no-dry-run={:?}, config_path={:?}",
        no_dry_run, config_path
    );

    let config = if let Ok(value) = conf::Config::with_layers(&[Layer::Yaml(config_path.clone())]) {
        value
    } else {
        panic!("Failed to load config file with name {:?}!", config_path)
    };

    for index in config.get_indices() {
        let index_name = index.get_name();
        let from = index.get_from();
        let to = index.get_to();
        info!("copy index {}, from: {}, to: {}", index_name, from, to);

        let mut source_es_client = utils::create_es_client(config.get_endpoints(), from)
            .await
            .expect("create source ES client failed!");
        source_es_client.print_server_info(from).await;

        let mut destination_es_client = utils::create_es_client(config.get_endpoints(), to)
            .await
            .expect("create destination ES client failed!");
        destination_es_client.print_server_info(to).await;

        memory_stats!();

        source_es_client.scroll_start(index).await;

        while source_es_client.has_docs() {
            let total = source_es_client.get_total_size();
            let counter = source_es_client.get_docs_counter();

            info!(
                "docs {}/{} ({:.2} %)",
                counter,
                total,
                (counter as f64 / total as f64) * 100.00
            );

            source_es_client.scroll_next(index).await;

            memory_stats!();
        }

        memory_stats!();

        source_es_client.scroll_stop().await;

        info!("copying index {} done!", index_name);
    }

    info!("completed!");

    // Copy indices
}
