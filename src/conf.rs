use twelf::config;
use twelf::reexports::serde::{Deserialize, Serialize};

#[config]
#[derive(Debug, Default)]
pub struct Config {
    endpoints: Vec<Endpoint>,
    indices: Vec<Index>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    name: String,
    url: String,
    #[serde(default)]
    basic_auth: Option<BasicAuth>,
    #[serde(default)]
    root_certificates: Option<String>,
    #[serde(default)]
    timeout: Option<Timeout>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BasicAuth {
    username: String,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Timeout {
    connect: Option<u64>,
    read: Option<u64>,
}

fn default_shards() -> u64 {
    1
}
fn default_replicas() -> u64 {
    1
}
fn default_keep_alive() -> String {
    "5m".to_string()
}

// * Custom deserializer:
// * https://stackoverflow.com/a/66961340/362880
// * https://stackoverflow.com/questions/37870428/convert-two-types-into-a-single-type-with-serde
// * https://stackoverflow.com/questions/46753955/how-to-transform-fields-during-deserialization-using-serde
fn parse_buffer_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_any(BufferSizeVisitor)
}

struct BufferSizeVisitor;

impl<'de> serde::de::Visitor<'de> for BufferSizeVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an integer or a string representing an integer")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .parse::<u64>()
            .map_err(|e| serde::de::Error::custom(format!("failed to parse string as u64: {}", e)))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Index {
    from: String,
    to: String,
    #[serde(deserialize_with = "parse_buffer_size")]
    buffer_size: u64,
    #[serde(default = "default_keep_alive")]
    keep_alive: String,
    #[serde(default)]
    routing_field: Option<String>,
    #[serde(default)]
    custom: Option<Custom>,
    name: String,
    #[serde(default)]
    multiple: bool, // tohle zn. že chci víc indexů, typicky "tsm-log-*"
    name_of_copy: Option<String>,
    #[serde(default)]
    delete_if_exists: bool,
    #[serde(default)]
    alias: Option<Alias>,
    #[serde(default = "default_shards")]
    number_of_shards: u64,
    #[serde(default = "default_replicas")]
    number_of_replicas: u64,
    copy_mapping: bool,
    copy_content: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Alias {
    name: String,
    #[serde(default)]
    remove_if_exists: bool,
}

impl Default for Alias {
    fn default() -> Self {
        Self {
            name: String::default(),
            remove_if_exists: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Custom {
    query: Option<String>,
    sort: Option<String>,
    doc_type: Option<String>,
    mapping: Option<String>,
}

impl Default for Custom {
    fn default() -> Self {
        Self {
            query: None,
            sort: None,
            doc_type: None,
            mapping: None,
        }
    }
}

impl Default for Timeout {
    fn default() -> Self {
        Self {
            connect: Some(10),
            read: Some(30),
        }
    }
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
    pub fn get_root_certificates(&self) -> &Option<String> {
        &self.root_certificates
    }
    pub fn is_basic_auth(&self) -> bool {
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
    pub fn get_timeout_connect(&self) -> u64 {
        (&self.timeout)
            .clone()
            .unwrap_or_default()
            .connect
            .unwrap_or_default()
    }
    pub fn get_timeout_read(self) -> u64 {
        (&self.timeout)
            .clone()
            .unwrap_or_default()
            .read
            .unwrap_or_default()
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
    pub fn get_name_of_copy(&self) -> &Option<String> {
        &self.name_of_copy
    }
    pub fn get_alias_name(&self) -> Option<String> {
        if let Some(alias) = &self.alias {
            Some(alias.name.clone())
        } else {
            None
        }
    }
    pub fn is_alias(&self) -> bool {
        if let Some(alias) = &self.alias {
            !alias.name.is_empty()
        } else {
            false
        }
    }
    pub fn is_alias_remove_if_exists(&self) -> bool {
        if let Some(alias) = &self.alias {
            alias.remove_if_exists
        } else {
            false
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
    pub fn is_custom(&self) -> bool {
        match &self.custom {
            Some(_) => true,
            _ => false,
        }
    }
    pub fn is_custom_doc_type(&self) -> bool {
        match &self.custom {
            Some(custom) => match custom.doc_type {
                Some(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
    pub fn is_custom_mapping(&self) -> bool {
        match &self.custom {
            Some(custom) => match custom.mapping {
                Some(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
    pub fn get_custom(&self) -> &Option<Custom> {
        &self.custom
    }
    pub fn is_routing_field(&self) -> bool {
        match &self.routing_field {
            Some(_) => true,
            _ => false,
        }
    }
    pub fn get_custom_doc_type(&self) -> &Option<String> {
        match &self.custom {
            Some(custom) => &custom.doc_type,
            _ => &None,
        }
    }
    pub fn get_custom_mapping(&self) -> &Option<String> {
        match &self.custom {
            Some(custom) => &custom.mapping,
            _ => &None,
        }
    }
    pub fn get_routing_field(&self) -> &Option<String> {
        &self.routing_field
    }
    pub fn is_copy_mapping(&self) -> bool {
        *&self.copy_mapping
    }
    pub fn is_copy_content(&self) -> bool {
        *&self.copy_content
    }
    pub fn is_delete_if_exists(&self) -> bool {
        *&self.delete_if_exists
    }
    pub fn is_multiple(&self) -> bool {
        *&self.multiple
    }
}

impl Custom {
    pub fn get_query(&self) -> &Option<String> {
        &self.query
    }
    pub fn get_sort(&self) -> &Option<String> {
        &self.sort
    }
    pub fn get_doc_type(&self) -> &Option<String> {
        &self.doc_type
    }
    pub fn get_mapping(&self) -> &Option<String> {
        &self.mapping
    }
}
