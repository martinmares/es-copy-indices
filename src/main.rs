#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod conf;
mod utils;

use std::collections::HashMap;
use std::path::PathBuf;

use reqwest::{Certificate, ClientBuilder};
use tokio::fs::File;
use tokio::io::AsyncReadExt; // for read_to_end()

use slog::{debug, error, info, o, warn, Drain, Logger};
use slog_async;
use slog_term;

use clap::{arg, command, value_parser, Arg, ArgAction, Command};
use twelf::reexports::serde::{Deserialize, Serialize};
use twelf::{config, Layer};

#[tokio::main]
async fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!());
    // env_logger::init();

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
            Arg::new("debug")
                .short('d')
                .long("debug")
                .help("Enable debug mode")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("no-dry-run")
                .short('n')
                .long("no-dry-run")
                .help("Disable dry run only")
                .action(ArgAction::SetTrue),
        )
        .get_matches();

    info!(log, "Application started!");

    let debug = matches
        .get_one::<bool>("debug")
        .unwrap_or_else(|| &false)
        .to_owned();
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
        log,
        "Args debug={:?}, no-dry-run={:?}, config_path={:?}", debug, no_dry_run, config_path
    );

    let config = if let Ok(value) = conf::Config::with_layers(&[Layer::Yaml(config_path.clone())]) {
        value
    } else {
        panic!("Failed to load config file with name {:?}!", config_path)
    };

    debug_if!(debug, log, "Config file loaded correctly ... {:#?}", config)
}
