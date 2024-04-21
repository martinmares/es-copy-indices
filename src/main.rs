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

    for endpoint in config.get_endpoints() {
        let name = endpoint.get_name();
        let url = endpoint.get_url();

        let http_client =
            utils::create_http_client(endpoint, endpoint.get_root_certificates()).await;

        if let Ok(http_client) = http_client {
            let es_client = EsClient::new(endpoint.clone(), http_client);
            let server_info = es_client.server_info().await;
            if let Some(server_info) = server_info {
                info!(
                    "Server info: hostname={}, name={}, uuid={}, version={}",
                    server_info.get_hostname(),
                    server_info.get_name(),
                    server_info.get_uuid(),
                    server_info.get_version()
                );
            }
        }
    }

    for index in config.get_indices() {
        debug!("Copy index {:?}", index.get_name());
    }

    // Copy indices
}
