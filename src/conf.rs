use twelf::reexports::serde::{Deserialize, Serialize};
use twelf::{config, Layer};

#[config]
#[derive(Debug, Default)]
pub struct Config {
    //#[serde(flatten)]
    endpoints: Vec<Endpoint>,
    root_certificates: Vec<String>,
    indices: Vec<Indice>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Endpoint {
    name: String,
    url: String,
    auth_basic: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Indice {
    from: String,
    to: String,
    name: String,
    add_timestamp: bool,
    mapping: bool,
    content: bool,
    delete_if_exists: bool,
}
