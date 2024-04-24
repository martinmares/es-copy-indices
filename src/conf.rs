use twelf::config;
use twelf::reexports::serde::{Deserialize, Serialize};

#[config]
#[derive(Debug, Default)]
pub struct Config {
    endpoints: Vec<Endpoint>,
    indices: Vec<Index>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    name: String,
    url: String,
    #[serde(default)]
    basic_auth: Option<BasicAuth>,
    root_certificates: Vec<String>,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct BasicAuth {
    username: String,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Index {
    from: String,
    to: String,
    buffer_size: u64,
    keep_alive: String,
    name: String,
    name_of_copy: String,
    #[serde(default)]
    create_alias: Option<String>,
    number_of_shards: u64,
    number_of_replicas: u64,
    copy_mapping: bool,
    copy_content: bool,
    delete_if_exists: bool,
}

impl Config {
    pub fn get_indices(&self) -> &Vec<Index> {
        &self.indices
    }
    pub fn get_endpoints(&self) -> &Vec<Endpoint> {
        &self.endpoints
    }
}

impl BasicAuth {
    pub fn get_username(&self) -> &String {
        &self.username
    }
    pub fn get_password(&self) -> &Option<String> {
        &self.password
    }
}

impl Endpoint {
    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_url(&self) -> &String {
        &self.url
    }
    pub fn get_root_certificates(&self) -> &Vec<String> {
        &self.root_certificates
    }
    pub fn has_basic_auth(&self) -> bool {
        let mut result = false;
        if let Some(basic_auth) = &self.basic_auth {
            if let Some(_) = basic_auth.get_password() {
                result = true;
            }
        }
        result
    }
    pub fn get_username(&self) -> String {
        if let Some(basic_auth) = &self.basic_auth {
            let username = basic_auth.get_username();
            username.clone()
        } else {
            String::default()
        }
    }
    pub fn get_password(&self) -> Option<String> {
        if let Some(basic_auth) = &self.basic_auth {
            let password = basic_auth.get_password();
            return password.clone();
        }
        None
    }
}

impl Index {
    pub fn get_from(&self) -> &String {
        &self.from
    }
    pub fn get_to(&self) -> &String {
        &self.to
    }
    pub fn get_name(&self) -> &String {
        &self.name
    }
    pub fn get_name_of_copy(&self) -> &String {
        &self.name_of_copy
    }
    pub fn get_create_alias(&self) -> &Option<String> {
        &self.create_alias
    }
    pub fn is_create_alias(&self) -> bool {
        match &self.create_alias {
            Some(_) => true,
            _ => false,
        }
    }
    pub fn get_number_of_shards(&self) -> u64 {
        *&self.number_of_shards
    }
    pub fn get_number_of_replicas(&self) -> u64 {
        *&self.number_of_replicas
    }
    pub fn get_buffer_size(&self) -> u64 {
        *&self.buffer_size
    }
    pub fn get_keep_alive(&self) -> &String {
        &self.keep_alive
    }
    pub fn is_copy_mapping(&self) -> bool {
        *&self.copy_mapping
    }
    pub fn is_copy_content(&self) -> bool {
        *&self.copy_content
    }
}
