use reqwest::Certificate;
use twelf::reexports::serde::{Deserialize, Serialize};
use twelf::{config, Layer};

#[config]
#[derive(Debug, Default)]
pub struct Config {
    endpoints: Vec<Endpoint>,
    indices: Vec<Indice>,
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
pub struct Indice {
    from: String,
    to: String,
    name: String,
    add_timestamp: bool,
    mapping: bool,
    content: bool,
    delete_if_exists: bool,
}

impl Config {
    pub fn get_indices(&self) -> &Vec<Indice> {
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
            if let Some(password) = basic_auth.get_password() {
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

impl Indice {
    pub fn get_name(&self) -> &String {
        &self.name
    }
}
