use logging_timer::time;
use serde_json::Value;

use crate::conf::{Endpoint, Index};
use crate::models::scroll_response::{Document, ScrollResponse};
use crate::models::server_info::ServerInfo;
use log::{debug, info};

use reqwest::{Client, RequestBuilder};

const BULK_OPER_INDEX: &str = "index";
const DEFAULT_DOC_TYPE: &str = "_doc"; // ! from elastic version_major=7 is "_doc" default type!

#[derive(Debug, Clone)]
pub struct EsClient {
    endpoint: Endpoint,
    http_client: Client,
    server_info: Option<ServerInfo>,
    scroll_response: Option<ScrollResponse>,
    scroll_id: Option<String>,
    current_size: u64,
    total_size: u64,
    docs_counter: u64,
}

fn inject_auth(request_builder: RequestBuilder, endpoint: Endpoint) -> RequestBuilder {
    if endpoint.has_basic_auth() {
        request_builder.basic_auth(endpoint.get_username(), endpoint.get_password())
    } else {
        request_builder
    }
}

impl EsClient {
    // pub async fn get_info(&self) -> Option<String> {}

    pub fn new(endpoint: Endpoint, http_client: Client) -> Self {
        Self {
            endpoint,
            http_client,
            server_info: None,
            scroll_response: None,
            scroll_id: None,
            current_size: 0,
            total_size: 0,
            docs_counter: 0,
        }
    }

    async fn call_get(
        &mut self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Getting url: {}", url);
        let mut request_builder = self.http_client.get(url).query(&query);

        for (key, val) in headers {
            request_builder = request_builder.header(key, val);
        }

        let endpoint = self.endpoint.clone();
        request_builder = inject_auth(request_builder, endpoint);

        let call = request_builder.send().await;
        if let Ok(call) = call {
            let text = call.text().await;
            if let Ok(text) = text {
                return Some(text);
            }
        }

        todo!("Implement empty response!")
    }

    async fn call_post(
        &mut self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Posting url: {}", url);
        let mut request_builder = self.http_client.post(url).query(&query).body(body.clone());

        for (key, val) in headers {
            request_builder = request_builder.header(key, val);
        }

        let endpoint = self.endpoint.clone();
        request_builder = inject_auth(request_builder, endpoint);

        let call = request_builder.send().await;
        if let Ok(call) = call {
            let text = call.text().await;
            if let Ok(text) = text {
                debug!("Post response text: {}", text);
                return Some(text);
            }
        }

        todo!("Implement empty response!")
    }

    async fn call_put(
        &mut self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Posting url: {}", url);
        let mut request_builder = self.http_client.put(url).query(&query).body(body.clone());

        for (key, val) in headers {
            request_builder = request_builder.header(key, val);
        }

        let endpoint = self.endpoint.clone();
        request_builder = inject_auth(request_builder, endpoint);

        let call = request_builder.send().await;
        if let Ok(call) = call {
            let text = call.text().await;
            if let Ok(text) = text {
                debug!("Put response text: {}", text);
                return Some(text);
            }
        }

        todo!("Implement empty response!")
    }

    async fn call_delete(
        &mut self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Deleting url: {}", url);
        let mut request_builder = self
            .http_client
            .delete(url)
            .query(&query)
            .body(body.clone());

        for (key, val) in headers {
            request_builder = request_builder.header(key, val);
        }

        let endpoint = self.endpoint.clone();
        request_builder = inject_auth(request_builder, endpoint);

        let call = request_builder.send().await;
        if let Ok(call) = call {
            let text = call.text().await;
            if let Ok(text) = text {
                debug!("Post response text: {}", text);
                return Some(text);
            }
        }

        todo!("implement empty response!")
    }

    async fn server_info(&mut self) -> Option<ServerInfo> {
        let resp = self.call_get("/", &vec![], &vec![]).await;
        if let Some(value) = resp {
            let json: ServerInfo =
                serde_json::from_str(&value).expect("incorrect response for ServerInfo struct");
            return Some(json);
        }

        None
    }

    pub async fn detect_server(&mut self) {
        self.server_info = self.server_info().await;
    }

    pub fn print_server_info(&mut self, prefix: &str) {
        if let Some(server_info) = &self.server_info {
            info!(
                "Server details about \"{}\" (hostname={}, name={}, uuid={}, version_major={}, version={}, lucene={})",
                prefix,
                server_info.get_hostname(),
                server_info.get_name(),
                server_info.get_uuid(),
                server_info.get_version_major(),
                server_info.get_version(),
                server_info.get_lucene_version()
            );
        }
    }

    #[time("debug")]
    pub async fn scroll_start(&mut self, index: &Index) -> &mut Self {
        let index_name = index.get_name();
        let keep_alive = index.get_keep_alive();
        let buffer_size = index.get_buffer_size();

        let body = format!(
            "{{ \"size\": {}, \"query\": {{ \"match_all\": {{}} }} }}",
            buffer_size
        );
        debug!("Querying: {}", body);
        let resp = self
            .call_post(
                &format!("/{}/_search", index_name),
                &vec![(String::from("scroll"), format!("{}", keep_alive))],
                &vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept-encoding".to_string(), "gzip".to_string()),
                ],
                &body,
            )
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                let new_scroll_response = ScrollResponse::new(json_value);

                self.current_size = new_scroll_response.get_current_size();
                self.total_size = new_scroll_response.get_total_size();
                self.docs_counter += self.current_size;
                self.scroll_id = Some(new_scroll_response.get_scroll_id().clone());
                self.scroll_response = Some(new_scroll_response);

                return self;
            }
        }

        self
    }

    #[time("debug")]
    pub async fn scroll_next(&mut self, index: &Index) -> &mut Self {
        let keep_alive = index.get_keep_alive();

        let body = format!(
            "{{ \"scroll\": \"{}\", \"scroll_id\": \"{}\" }}",
            keep_alive,
            self.scroll_id.clone().unwrap()
        );
        debug!("Querying: {}", body);
        let resp = self
            .call_post(
                &format!("/_search/scroll"),
                &vec![],
                &vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept-encoding".to_string(), "gzip".to_string()),
                ],
                &body,
            )
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                let new_scroll_response = ScrollResponse::new(json_value);

                self.current_size = new_scroll_response.get_current_size();
                self.total_size = new_scroll_response.get_total_size();
                self.docs_counter += self.current_size;
                self.scroll_id = Some(new_scroll_response.get_scroll_id().clone());
                self.scroll_response = Some(new_scroll_response);

                return self;
            }
        }

        self
    }

    #[time("debug")]
    pub async fn scroll_stop(&mut self) -> &mut Self {
        let body = format!(
            "{{ \"scroll_id\": \"{}\" }}",
            self.scroll_id.clone().unwrap()
        );
        debug!("Querying: {}", body);
        let _ = self
            .call_delete(
                &format!("/_search/scroll"),
                &vec![],
                &vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept-encoding".to_string(), "gzip".to_string()),
                ],
                &body,
            )
            .await;

        // TODO: implement check status code?
        self
    }

    #[time("debug")]
    pub async fn copy_content_to(
        &mut self,
        es_client: &mut EsClient,
        index_name_of_copy: &String,
    ) -> &Self {
        let mut bulk_body = String::new();
        if self.has_docs() {
            if let Some(docs) = self.get_docs() {
                for doc in docs {
                    // index name + id + type
                    bulk_body.push_str(&format!("{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\" }} }}",
                        BULK_OPER_INDEX,
                        index_name_of_copy,
                        doc.get_doc_type(),doc.get_id()));
                    bulk_body.push_str("\n");
                    // document source
                    bulk_body.push_str(doc.get_source());
                    bulk_body.push_str("\n");
                }
            }
        }
        let _ = es_client
            .call_post(
                &format!("/{}/_bulk", index_name_of_copy), // ! This is NEW one CLONED index name!
                &vec![],
                &vec![
                    ("Content-Type".to_string(), "application/json".to_string()),
                    ("Accept-encoding".to_string(), "gzip".to_string()),
                ],
                &bulk_body,
            )
            .await;

        // TODO: implement check status code?
        self
    }

    #[time("debug")]
    pub async fn copy_mappings_to(&mut self, es_client: &mut EsClient, index: &Index) -> &Self {
        let index_name = index.get_name();
        let index_name_of_copy = index.get_name_of_copy();
        let mut mappings: Option<Value> = None;
        let mut settings: Option<Value> = None;

        // mapping
        let resp = self
            .call_get(&format!("/{}/_mapping", index_name), &vec![], &vec![])
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                if let Some(obj) = json_value.as_object() {
                    let keys = obj.keys();
                    if keys.len() == 1 {
                        for key in keys {
                            if let Some(value_want) = obj.get(key) {
                                mappings = Some(value_want.clone());
                            }
                        }
                    }
                }
            }
        }

        // settings
        let resp = self
            .call_get(&format!("/{}/_settings", index_name), &vec![], &vec![])
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                if let Some(obj) = json_value.as_object() {
                    let keys = obj.keys();
                    if keys.len() == 1 {
                        for key in keys {
                            if let Some(value_want) = obj.get(key) {
                                let mut fixed_value = value_want.clone();
                                if let Some(settings_val) = fixed_value.get_mut("settings") {
                                    if let Some(index_val) = settings_val.get_mut("index") {
                                        let names = vec![
                                            "uuid",
                                            "provided_name",
                                            "creation_date",
                                            "version",
                                            "number_of_shards",
                                            "number_of_replicas",
                                        ];
                                        for name in names {
                                            if let Some(_) = index_val.get_mut(name) {
                                                index_val.as_object_mut().unwrap().remove(name);
                                            }
                                        }
                                        index_val.as_object_mut().unwrap().insert(
                                            "number_of_shards".to_string(),
                                            index.get_number_of_shards().into(),
                                        );
                                        index_val.as_object_mut().unwrap().insert(
                                            "number_of_replicas".to_string(),
                                            index.get_number_of_replicas().into(),
                                        );
                                    }
                                }
                                settings = Some(fixed_value);
                            }
                        }
                    }
                }
            }
        }

        // ! "mappings" and "settings" are crucial!
        if let Some(mappings_value) = mappings {
            info!("Mappings found {}", mappings_value.to_string());
            if let Some(settings_value) = settings {
                info!("Settings found {}", settings_value.to_string());
                let resp = es_client
                    .call_put(
                        &format!("/{}", index_name_of_copy),
                        &vec![],
                        &vec![
                            ("Content-Type".to_string(), "application/json".to_string()),
                            ("Accept-encoding".to_string(), "gzip".to_string()),
                        ],
                        &format!(
                            "{{ \"settings\": {}, \"mappings\": {} }}",
                            settings_value["settings"].to_string(),
                            mappings_value["mappings"].to_string()
                        ),
                    )
                    .await;

                if let Some(resp_value) = resp {
                    info!("Index mappings and settings (response: {})", resp_value);
                }

                return self;
            } else {
                panic!("No settings found!");
            }
        } else {
            panic!("No mappings found!");
        }
    }

    pub fn get_scroll_id(&self) -> &Option<String> {
        &self.scroll_id
    }

    pub fn has_docs(&self) -> bool {
        if let Some(scroll_response) = &self.scroll_response {
            return scroll_response.has_docs();
        }
        false
    }

    pub fn get_docs(&self) -> Option<&Vec<Document>> {
        if let Some(scroll_response) = &self.scroll_response {
            return Some(scroll_response.get_docs());
        }
        None
    }

    pub fn get_current_size(&self) -> u64 {
        self.current_size
    }

    pub fn get_total_size(&self) -> u64 {
        self.total_size
    }

    pub fn get_docs_counter(&self) -> u64 {
        self.docs_counter
    }
}
