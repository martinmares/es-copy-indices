#![allow(dead_code)]

mod conf;
mod es_client;
mod models;
mod utils;

use std::path::PathBuf;
// use std::thread;
// use std::time::Duration;

use log::info;

// use indicatif::{ProgressBar, ProgressStyle};

use clap::{command, value_parser, Arg, ArgAction};
use twelf::Layer;

// use crate::models::scroll_response::ScrollResponse;

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

        let mut scroll_response = source_es_client.clone().scroll_start(index).await.unwrap();
        let mut docs_counter: u64 = 0;
        while scroll_response.has_docs() {
            docs_counter += scroll_response.get_current_size();
            scroll_response = source_es_client
                .clone()
                .scroll_next(index, scroll_response.get_scroll_id())
                .await
                .unwrap();
            info!(
                "Iter docs {}/{}",
                docs_counter,
                scroll_response.get_total_size()
            );
        }
        source_es_client
            .clone()
            .scroll_stop(scroll_response.get_scroll_id())
            .await;

        //if let Some(response) = scroll_response {
        //     info!("Scroll id = {}", response.get_scroll_id());
        //     info!("Has docs = {}", response.has_docs());
        //     info!("Docs.len() = {}", response.get_docs().len());
        //     info!("Current size = {}", response.get_current_size());
        //     info!("Total size = {}", response.get_total_size());
        //}

        // let total_size = 1000;

        // let pb = ProgressBar::new(total_size);
        // let pb_style = ProgressStyle::with_template(
        //     "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        // )
        // .unwrap()
        // .progress_chars("##-");
        // pb.set_style(pb_style);
        // pb.set_message("documents");

        // for _ in 0..(total_size / 10) {
        //     thread::sleep(Duration::from_millis(50));
        //     pb.inc(10);
        // }
        // pb.finish_with_message("copying done");
    }

    // Copy indices
}
