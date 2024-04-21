use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Serialize, Deserialize)]
pub struct ServerInfo {
    #[serde(rename = "name")]
    hostname: String,
    #[serde(rename = "cluster_name")]
    name: String,
    #[serde(rename = "cluster_uuid")]
    uuid: String,
    version: Version,
}

#[derive(Serialize, Deserialize)]
pub struct Version {
    number: String,
}

impl ServerInfo {
    pub fn get_hostname(&self) -> &String {
        &self.hostname
    }
    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_uuid(&self) -> &String {
        &self.uuid
    }
    pub fn get_version(&self) -> &String {
        &self.version.number
    }
}
