#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod conf;
mod es_client;
mod models;
mod utils;

use es_client::EsClient;
use std::collections::HashMap;
use std::path::PathBuf;

use log::{debug, error, info, warn};
use reqwest::{Certificate, Client, ClientBuilder};
use tokio::fs::File;
use tokio::io::AsyncReadExt; // for read_to_end()

use clap::{arg, command, value_parser, Arg, ArgAction, Command};
use twelf::reexports::serde::{Deserialize, Serialize};
use twelf::{config, Layer};

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = command!() // requires `cargo` feature
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
        panic!("Config path must be set!")
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
        let from = index.get_from();
        let to = index.get_to();
        info!("Copy index {}, from: {}, to: {}", index_name, from, to);

        let source_es_client = utils::create_es_client(config.get_endpoints(), from)
            .await
            .expect("Create source ES client failed!");
        source_es_client.clone().print_server_info(from).await;

        let destination_es_client = utils::create_es_client(config.get_endpoints(), to)
            .await
            .expect("Create destination ES client failed!");
        destination_es_client.print_server_info(to).await;

        let scroll_response = source_es_client.scroll_start(index_name, "5m", 10).await;
        if let Some(response) = scroll_response {
            info!("Scroll id = {}", response.get_scroll_id());
            info!("Has docs = {}", response.has_docs());
            info!("Current size = {}", response.get_current_size());
            info!("Total size = {}", response.get_total_size());
        }
    }

    // Copy indices
}
