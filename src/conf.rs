use serde_with::{serde_as, DisplayFromStr, PickFirst};
use twelf::config;
use twelf::reexports::serde::{Deserialize, Serialize};

#[config]
#[derive(Debug, Default)]
pub struct Config {
    endpoints: Vec<Endpoint>,
    indices: Vec<Index>,
    audit: Option<Audit>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    name: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    basic_auth: Option<BasicAuth>,
    #[serde(default)]
    root_certificates: Option<String>,
    #[serde(default)]
    insecure: bool,
    #[serde(default = "default_timeout")]
    timeout: u64,
    #[serde(default)]
    backup_dir: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Audit {
    file_name: String,
    #[serde(default)]
    enabled: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BasicAuth {
    username: String,
    #[serde(default)]
    password: Option<String>,
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
fn default_timeout() -> u64 {
    90
}
fn default_pre_create_doc_ids() -> bool {
    true
}

fn default_scroll_mode() -> ScrollMode {
    ScrollMode::ScrollApi
}

fn default_pre_create_doc_source() -> String {
    "{}".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScrollMode {
    ScrollApi,
    ScrollingSearch,
    Auto,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct Index {
    from: String,
    to: String,
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    buffer_size: u64,
    #[serde(default = "default_keep_alive")]
    keep_alive: String,
    #[serde(default)]
    routing_field: Option<String>,
    #[serde(default)]
    backup_quantile_field: Option<String>,
    #[serde(default = "default_pre_create_doc_ids")]
    pre_create_doc_ids: bool,
    #[serde(default = "default_pre_create_doc_source")]
    pre_create_doc_source: String,
    #[serde(default)]
    custom: Option<Custom>,
    #[serde(default = "default_scroll_mode")]
    scroll_mode: ScrollMode,
    name: String,
    #[serde(default)]
    multiple: bool, // tohle zn. že chci víc indexů, typicky "tsm-log-*"
    name_of_copy: Option<String>,
    #[serde(default)]
    delete_if_exists: bool,
    #[serde(default)]
    alias: Option<Alias>,
    #[serde(default = "default_shards")]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    number_of_shards: u64,
    #[serde(default = "default_replicas")]
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
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

impl Config {
    pub fn get_indices(&self) -> &Vec<Index> {
        &self.indices
    }
    pub fn get_endpoints(&self) -> &Vec<Endpoint> {
        &self.endpoints
    }
    pub fn get_audit(&self) -> &Option<Audit> {
        &self.audit
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
    pub fn get_insecure(&self) -> bool {
        self.insecure
    }
    pub fn get_backup_dir(&self) -> &Option<String> {
        &self.backup_dir
    }
    pub fn has_backup_dir(&self) -> bool {
        self.backup_dir.is_some()
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
    pub fn get_timeout(&self) -> &u64 {
        &self.timeout
    }
}

impl Audit {
    pub fn get_file_name(&self) -> &String {
        &self.file_name
    }
    pub fn is_enabled(&self) -> bool {
        *&self.enabled
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
    pub fn get_scroll_mode(&self) -> &ScrollMode {
        &self.scroll_mode
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
    pub fn is_pre_create_doc_ids(&self) -> bool {
        *&self.pre_create_doc_ids
    }
    pub fn get_pre_create_doc_source(&self) -> &String {
        &self.pre_create_doc_source
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
    pub fn get_backup_quantile_field(&self) -> &Option<String> {
        &self.backup_quantile_field
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
