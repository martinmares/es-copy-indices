#![allow(dead_code)]
#![allow(unused_imports)]

use slog::{info, o, Drain, Logger};
use slog_async;
use slog_term;

use std::collections::HashMap;
use std::path::PathBuf;

use twelf::reexports::serde::{Deserialize, Serialize};
use twelf::{config, Layer};

use clap::{command, value_parser, Arg, ArgAction};

#[config]
#[derive(Debug, Default)]
struct Config {
    #[serde(flatten)]
    endpoints: Vec<Endpoint>,
    indices: Vec<Indice>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Endpoint {
    name: String,
    url: String,
    auth_basic: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct Indice {
    from: String,
    to: String,
    name: String,
    append_timestamp: bool,
    transfer_mapping: bool,
    delete_if_exists: bool,
}

fn main() {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let log = slog::Logger::root(drain, o!());

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

    if let Some(config_path) = matches.get_one::<PathBuf>("config") {
        println!("Config/path: {}", config_path.display());
    }

    if let Some(debug) = matches.get_one::<bool>("debug") {
        println!("Config/debug: {}", debug);
    }

    if let Some(no_dry_run) = matches.get_one::<bool>("no-dry-run") {
        println!("Config/no-dry-run: {}", no_dry_run);
    }

    info!(log, "Application ready!");
}
