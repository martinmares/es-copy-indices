#![allow(dead_code)]

mod conf;
mod es_client;
mod models;
mod utils;

use log::info;
use std::path::PathBuf;

use clap::{command, value_parser, Arg, ArgAction};
use twelf::Layer;

fn main() {
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

        let source_es_client = utils::build_es_client(config.get_endpoints(), from)
            .expect("Create source ES client failed!");
        source_es_client.clone().print_server_info(from);

        let destination_es_client = utils::build_es_client(config.get_endpoints(), to)
            .expect("Create destination ES client failed!");
        destination_es_client.print_server_info(to);

        memory_stats!();

        let mut scroll_response = source_es_client.clone().scroll_start(index).unwrap();
        let mut docs_counter: u64 = 0;
        while scroll_response.has_docs() {
            docs_counter += scroll_response.get_current_size();
            scroll_response = source_es_client
                .clone()
                .scroll_next(index, scroll_response.get_scroll_id())
                .unwrap();
            info!(
                "Iter docs {}/{}",
                docs_counter,
                scroll_response.get_total_size()
            );

            memory_stats!();
        }

        source_es_client
            .clone()
            .scroll_stop(scroll_response.get_scroll_id());

        memory_stats!();
    }

    // Copy indices
}
