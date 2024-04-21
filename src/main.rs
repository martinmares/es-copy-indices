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
        let from = index.get_from();
        let to = index.get_to();
        info!(
            "Copy index {}, from: {}, to: {}",
            index.get_name(),
            from,
            to
        );

        let source_es_client = utils::create_es_client(config.get_endpoints(), from)
            .await
            .expect("Create source ES client failed!");
        if let Some(server_info) = source_es_client.server_info().await {
            info!(
                "From ES: hostname={}, name={}, uuid={}, version={}, lucene={}",
                server_info.get_hostname(),
                server_info.get_name(),
                server_info.get_uuid(),
                server_info.get_version(),
                server_info.get_lucene_version()
            );
        }

        let source_es_client = utils::create_es_client(config.get_endpoints(), to)
            .await
            .expect("Create source ES client failed!");
        if let Some(server_info) = source_es_client.server_info().await {
            info!(
                "To ES: hostname={}, name={}, uuid={}, version={}, lucene={}",
                server_info.get_hostname(),
                server_info.get_name(),
                server_info.get_uuid(),
                server_info.get_version(),
                server_info.get_lucene_version()
            );
        }
    }

    // Copy indices
}
