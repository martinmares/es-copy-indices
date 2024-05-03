use semver::Version as Semver;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    #[serde(rename = "name")]
    hostname: String,
    #[serde(rename = "cluster_name")]
    name: String,
    #[serde(rename = "cluster_uuid")]
    uuid: Option<String>,
    version: Version,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Version {
    number: String,
    lucene_version: String,
}

impl ServerInfo {
    pub fn get_hostname(&self) -> &String {
        &self.hostname
    }
    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_uuid(&self) -> &Option<String> {
        &self.uuid
    }
    pub fn get_version(&self) -> &String {
        &self.version.number
    }
    pub fn get_lucene_version(&self) -> &String {
        &self.version.lucene_version
    }

    pub fn get_version_major(&self) -> u64 {
        let version = Semver::parse(&self.version.number).unwrap();
        version.major
    }
}
