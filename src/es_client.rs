use std::collections::HashSet;
use std::error::Error;
use std::time::Duration;
// use std::time::Duration;
// use chrono::{DateTime, Utc};
use chrono::Utc;
use std::vec;

use logging_timer::time;
use serde_json::Value;

use crate::audit_builder::{AuditBuilder, What};
use crate::conf::{Endpoint, Index};
use crate::models::scroll_response::{Document, ScrollResponse};
use crate::models::server_info::ServerInfo;
use crate::utils;
use tracing::{debug, error, info, warn};

use reqwest::{Client, RequestBuilder};

const BULK_OPER_INDEX: &str = "index";
const BULK_OPER_CREATE: &str = "create";
const DEFAULT_DOC_TYPE: &str = "_doc"; // ! from elastic version_major=7 is "_doc" default type!
const DEFAULT_QUERY: &str = "{ \"match_all\": {} }";
const DEFAULT_SORT: &str = "{ \"_doc\": \"asc\" }"; // ! from elastic version_major=7 is "_doc" default type!

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
    if endpoint.is_basic_auth() {
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

        let call = request_builder
            .timeout(Duration::from_secs(*self.endpoint.get_timeout()))
            .send()
            .await;
        if let Ok(call) = call {
            let text = call.text().await;
            if let Ok(text) = text {
                return Some(text);
            }
        } else {
            debug!("{:#?}", &call);
        }

        todo!("Implement empty response!")
    }

    async fn call_post(
        &mut self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> (u16, Option<String>, Option<String>) {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Posting url: {}", url);
        let mut request_builder = self.http_client.post(url).query(&query).body(body.clone());

        for (key, val) in headers {
            request_builder = request_builder.header(key, val);
        }

        let endpoint = self.endpoint.clone();
        request_builder = inject_auth(request_builder, endpoint);

        let call = request_builder
            .timeout(Duration::from_secs(*self.endpoint.get_timeout()))
            .send()
            .await;

        match call {
            Ok(call) => {
                let code = call.status().as_u16();
                // let text = call.text().await;
                match call.text().await {
                    Ok(value) => {
                        debug!("Post response code: {}, text: {}", code, value);
                        return (code, Some(value), None);
                    }
                    Err(e) => {
                        error!("Call POST error: {:#?}", e);
                        return (code, None, Some(e.to_string()));
                    }
                }

                // if let Ok(text) = text {
                //     debug!("Post response code: {}, text: {}", code, text);
                //     return Some((code, text));
                // } else {
                //     error!("Text: {:#?}", text);
                // }
            }
            Err(e) => {
                error!("Text: {:#?}", e.source());
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

        let call = request_builder
            .timeout(Duration::from_secs(*self.endpoint.get_timeout()))
            .send()
            .await;
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

        let call = request_builder
            .timeout(Duration::from_secs(*self.endpoint.get_timeout()))
            .send()
            .await;
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
            let mut uuid = String::default();
            if let Some(value) = server_info.get_uuid() {
                uuid = value.clone();
            }
            info!(
                "Server details about \"{}\" (hostname={}, name={}, uuid={}, version_major={}, version={}, lucene={})",
                prefix,
                server_info.get_hostname(),
                server_info.get_name(),
                uuid,
                server_info.get_version_major(),
                server_info.get_version(),
                server_info.get_lucene_version()
            );
        }
    }

    pub async fn get_indices_names(&mut self, pattern: &str) -> Vec<String> {
        let mut result: Vec<String> = vec![];

        let resp = self
            .call_get(
                &format!("/_cat/indices/{}", pattern),
                &vec![
                    ("h".to_string(),"health,status,index,id,pri,rep,docs.count,docs.deleted,store.size,creation.date.string".to_string()),
                    ("v".to_string(),"".to_string())],
                &vec![
                    ("Accept".to_string(), "application/json".to_string()),
                ],
            )
            .await;

        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                if let Some(indices) = json_value.as_array() {
                    for index in indices {
                        if let Some(obj) = index.as_object() {
                            if let Some(index_name) = obj["index"].as_str() {
                                result.push(index_name.to_string());
                            }
                        }
                    }
                }
            }
        }

        result
    }

    #[time("debug")]
    pub async fn scroll_start(&mut self, index: &Index, index_name: &String) -> &mut Self {
        // let index_name = index.get_name();
        let keep_alive = index.get_keep_alive();
        let buffer_size = index.get_buffer_size();

        let q_query = match index.get_custom() {
            Some(custom) => match custom.get_query() {
                Some(query) => query,
                _ => DEFAULT_QUERY,
            },
            _ => DEFAULT_QUERY,
        };
        let q_sort = match index.get_custom() {
            Some(custom) => match custom.get_sort() {
                Some(sort) => sort,
                _ => DEFAULT_SORT,
            },
            _ => DEFAULT_SORT,
        };

        let body = format!(
            "{{ {} }}",
            vec![
                format!("\"size\": {}", buffer_size),
                format!("\"query\": {}", q_query),
                format!("\"sort\": {}", q_sort)
            ]
            .join(",")
        );
        debug!("Querying: {}", body);
        let resp = self
            .call_post(
                &(match index.get_custom_doc_type() {
                    Some(doc_type) => format!("/{}/{}/_search", index_name, doc_type),
                    _ => format!("/{}/_search", index_name),
                }),
                &vec![(String::from("scroll"), format!("{}", keep_alive))],
                &vec![("Content-Type".to_string(), "application/json".to_string())],
                &body,
            )
            .await;

        let (_status, _value, _err) = resp;

        if let Some(value) = _value {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                let new_scroll_response = ScrollResponse::new(json_value, None);

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
        let mut body = format!(
            "{{ \"scroll\": \"{}\", \"scroll_id\": \"{}\" }}",
            keep_alive,
            self.scroll_id.clone().unwrap()
        );
        debug!("Querying: {}", body);
        let mut query: Vec<(String, String)> = vec![];
        let mut headers: Vec<(String, String)> = vec![];

        if self.server_info.as_ref().unwrap().get_version_major() <= 2 {
            query.push(("scroll".to_string(), keep_alive.clone()));
            headers.push(("Content-Type".to_string(), "text/plain".to_string()));
            body = self.scroll_id.clone().unwrap();
            warn!(
                "Client is old one, query={:?}, headers={:?}, body={:?}",
                query, headers, body
            )
        } else {
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
        }

        let resp = self
            .call_post(&"/_search/scroll", &query, &headers, &body)
            .await;

        let (_status, _value, _err) = resp;

        if let Some(value) = _value {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                let new_scroll_response =
                    ScrollResponse::new(json_value, Some(self.scroll_id.clone().unwrap()));

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
        self.current_size = 0;
        self.total_size = 0;
        self.docs_counter = 0;
        let body = format!(
            "{{ \"scroll_id\": \"{}\" }}",
            self.scroll_id.clone().unwrap()
        );
        debug!("Querying: {}", body);
        let _ = self
            .call_delete(
                &format!("/_search/scroll"),
                &vec![],
                &vec![("Content-Type".to_string(), "application/json".to_string())],
                &body,
            )
            .await;

        // TODO: implement check status code?
        self
    }

    #[time("debug")]
    async fn auditing(&self, audit_builder: &mut Option<AuditBuilder>, title: &str, text: &str) {
        if let Some(audit) = audit_builder {
            let now = Utc::now().to_rfc3339();
            let _ = audit
                .append_to_file(format!("{} {}\n\n", &title, now).as_str())
                .await;
            let _ = audit.append_to_file(&format!("{}\n\n", &text)).await;
        }
    }

    #[time("debug")]
    pub async fn copy_content_to(
        &mut self,
        es_client: &mut EsClient,
        index: &Index,
        index_name_of_copy: &String,
        audit_builder: &mut Option<AuditBuilder>,
    ) -> &Self {
        // let index_name = index.get_name();
        // let index_name_of_copy = match index.get_name_of_copy() {
        //     Some(name) => name,
        //     None => index_name,
        // };

        let mut bulk_body_pre_create = String::new();
        let mut bulk_body = String::new();

        let is_routing_field = index.is_routing_field();
        let mut routing_field = String::new();

        let is_pre_create_doc_ids = index.is_pre_create_doc_ids();
        if !is_pre_create_doc_ids {
            warn!(
                "Skip pre create doc ids (is_pre_create_doc_ids={})",
                is_pre_create_doc_ids
            );
        }

        if is_routing_field {
            if let Some(value) = index.get_routing_field() {
                routing_field = value.clone();
            }
        }
        let server_major_version = es_client.server_info.as_ref().unwrap().get_version_major();

        if self.has_docs() {
            if let Some(docs) = self.get_docs() {
                let mut pre_create_doc_ids: HashSet<String> = HashSet::new();

                // ! PRE-CREATING DOCs !
                for doc in docs {
                    let mut add_routing_to_bulk: Option<String> = None;

                    if is_routing_field {
                        //for pre_create_pointer in &pre_create_pointers {
                        // debug!("Finding pre create pointer ... {}", pre_create_pointer);
                        let json_value_result: Result<serde_json::Value, serde_json::Error> =
                            serde_json::from_str(doc.get_source());

                        // warn!("json_value = {:#?}", json_value_result);
                        // std::thread::sleep(Duration::from_secs(10));

                        if let Ok(json_value) = json_value_result {
                            // More details about JSON RFC6901 (pointers): https://tools.ietf.org/html/rfc6901
                            let value = json_value.pointer(&routing_field);
                            if let Some(pointer_id) = value {
                                if let Some(id) = pointer_id.as_str() {
                                    debug!(
                                        "Pointer '&routing_field' found: {}, value: {:?}, id: {:?}, doc.get_id(): {:?}",
                                        pointer_id, value, id, doc.get_id()
                                    );
                                    let id = id.to_string();
                                    if is_pre_create_doc_ids {
                                        add_routing_to_bulk = Some(id.clone());
                                    }
                                    // ! child:  doc.get_id()
                                    // ! parent: id
                                    if is_pre_create_doc_ids {
                                        pre_create_doc_ids.insert(id);
                                    }
                                }
                            }
                        }
                        //}
                    }

                    // index name + id + type
                    let id = doc.get_id();
                    if server_major_version <= 7 {
                        if let Some(id_routing) = add_routing_to_bulk {
                            bulk_body.push_str(&format!(
                                "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\", \"routing\": \"{}\" }} }}",
                                BULK_OPER_INDEX,
                                index_name_of_copy,
                                doc.get_doc_type(),
                                id,
                                id_routing));
                        } else {
                            bulk_body.push_str(&format!(
                                "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\" }} }}",
                                BULK_OPER_INDEX,
                                index_name_of_copy,
                                doc.get_doc_type(),
                                id));
                        }
                    // index name + id
                    } else {
                        if let Some(id_routing) = add_routing_to_bulk {
                            bulk_body.push_str(&format!(
                                "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_id\" : \"{}\", \"routing\": \"{}\" }} }}",
                                BULK_OPER_INDEX, index_name_of_copy, id, id_routing
                            ));
                        } else {
                            bulk_body.push_str(&format!(
                                "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_id\" : \"{}\" }} }}",
                                BULK_OPER_INDEX, index_name_of_copy, id
                            ));
                        }
                    }
                    bulk_body.push_str("\n");
                    // document source
                    bulk_body.push_str(doc.get_source());
                    bulk_body.push_str("\n");
                }

                if pre_create_doc_ids.len() > 0 && is_pre_create_doc_ids {
                    info!(
                        "Pre create doc ids ... {:?} ...",
                        pre_create_doc_ids.iter().take(5).collect::<Vec<_>>()
                    );
                    // ! Pre create "parents"
                    for id_parent in pre_create_doc_ids {
                        // index name + id + type
                        if server_major_version <= 7 {
                            bulk_body_pre_create.push_str(
                            &format!("{{ \"{}\" : {{ \"_index\" : \"{}\", \"_type\" : \"{}\", \"_id\" : \"{}\" }} }}",
                                    BULK_OPER_CREATE, // ! must be "create" instead of "index" !
                                    index_name_of_copy,
                                    DEFAULT_DOC_TYPE,
                                    id_parent));
                        // index name + id
                        } else {
                            bulk_body_pre_create.push_str(&format!(
                                "{{ \"{}\" : {{ \"_index\" : \"{}\", \"_id\" : \"{}\" }} }}",
                                BULK_OPER_CREATE, // ! must be "create" instead of "index" !
                                index_name_of_copy,
                                id_parent
                            ));
                        }
                        bulk_body_pre_create.push_str("\n");
                        // Pre create document source
                        let pre_create_doc_source = index.get_pre_create_doc_source();
                        bulk_body_pre_create.push_str(pre_create_doc_source); // ! add { esCopyIndciesPreCreatedParent: true } - this is ONLY PRE-CREATE !
                        bulk_body_pre_create.push_str("\n");
                    }
                }
            }
        }

        if is_routing_field && !bulk_body_pre_create.is_empty() {
            debug!("Pre creating some documents ... {}", bulk_body_pre_create);
            let resp = es_client
                .call_post(
                    &format!("/{}/_bulk", index_name_of_copy), // ! This is NEW one CLONED index name!
                    &vec![],
                    // &vec![("refresh".to_string(), "true".to_string())],
                    &vec![("Content-Type".to_string(), "application/json".to_string())],
                    &bulk_body_pre_create,
                )
                .await;

            self.auditing(
                audit_builder,
                What::PreCreateRequest.as_str(),
                &bulk_body_pre_create,
            )
            .await;
            let (_status, _value, _err) = resp;

            if let Some(text) = _value {
                info!(
                    "Pre create result ... {} ... {}",
                    text[0..100].to_string(),
                    text[text.len() - 100..].to_string(),
                );
                self.auditing(audit_builder, What::PreCreateResponse.as_str(), &text)
                    .await;
            } else if let Some(err) = _err {
                self.auditing(audit_builder, What::PreCreateResponse.as_str(), &err)
                    .await;
            }
        }

        self.auditing(audit_builder, What::BulkRequest.as_str(), &bulk_body)
            .await;

        let resp = es_client
            .call_post(
                &format!("/{}/_bulk", index_name_of_copy), // ! This is NEW one CLONED index name!
                &vec![],
                &vec![("Content-Type".to_string(), "application/json".to_string())],
                &bulk_body,
            )
            .await;

        let (_status, _value, _err) = resp;

        if let Some(text) = _value {
            self.auditing(audit_builder, What::BulkResponse.as_str(), &text)
                .await;

            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&text);
            if let Ok(json_value) = json_value_result {
                if let Some(obj) = json_value.as_object() {
                    if let Some(errors) = obj.get("errors") {
                        if errors.as_bool().unwrap() {
                            error!("Copy content failed ... {}", text);
                        } else {
                            info!(
                                "Content copy success ... {} ... {}",
                                text[0..100].to_string(),
                                text[text.len() - 100..].to_string(),
                            );
                        }
                    }
                }
            }
        } else if let Some(err) = _err {
            self.auditing(audit_builder, What::BulkResponse.as_str(), &err)
                .await;
        }

        // std::thread::sleep(Duration::from_secs(10));

        // TODO: implement check status code?
        self
    }

    #[time("debug")]
    pub async fn copy_mappings_to(
        &mut self,
        es_client: &mut EsClient,
        index: &Index,
        index_name: &String,
        index_name_of_copy: &String,
    ) -> &Self {
        // let index_name = index.get_name();
        // let index_name_of_copy = match index.get_name_of_copy() {
        //     Some(name) => name,
        //     None => index_name,
        // };
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
                                            "mapper",
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
                        &vec![("Content-Type".to_string(), "application/json".to_string())],
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

    #[time("debug")]
    pub async fn copy_custom_mappings_to(
        &mut self,
        es_client: &mut EsClient,
        index: &Index,
        index_name_of_copy: &String,
    ) -> &Self {
        let custom_mapping = match index.get_custom_mapping() {
            Some(value) => utils::string_to_json(value).await,
            _ => None,
        };

        // ! "mappings" and "settings" are crucial!
        if let Some(custom_mapping_value) = custom_mapping {
            let custom_mapping = custom_mapping_value.to_string();
            info!("Custom mappings and settings found {}", &custom_mapping);
            let resp = es_client
                .call_put(
                    &format!("/{}", index_name_of_copy),
                    &vec![],
                    &vec![("Content-Type".to_string(), "application/json".to_string())],
                    &custom_mapping,
                )
                .await;

            if let Some(resp_value) = resp {
                info!(
                    "Index custom mappings and settings (response: {})",
                    resp_value
                );
            }

            return self;
        } else {
            panic!("No custom mappings found!");
        }
    }

    #[time("debug")]
    pub async fn create_alias(&mut self, index: &Index) -> &Self {
        if index.is_alias() {
            let alias_name = index.get_alias_name().unwrap();
            let index_name = index.get_name();
            let index_name_of_copy = match index.get_name_of_copy() {
                Some(name) => name,
                None => index_name,
            };

            let resp = self
                .call_get(&format!("/_alias/{}", alias_name), &vec![], &vec![])
                .await;

            let mut indices_with_same_alias: Vec<String> = vec![];

            if let Some(value) = resp {
                let json_value_result: Result<serde_json::Value, serde_json::Error> =
                    serde_json::from_str(&value);
                if let Ok(json_value) = json_value_result {
                    if let Some(keys) = json_value.as_object() {
                        if !keys.contains_key("error") && !keys.contains_key("status") {
                            for (key, _) in keys {
                                indices_with_same_alias.push(key.clone());
                            }
                        }
                    }
                }
            }

            let mut actions: Vec<String> = vec![];

            debug!("Indices with same alias: {:#?}", indices_with_same_alias);

            if indices_with_same_alias.len() > 0 {
                if index.is_alias_remove_if_exists() {
                    warn!(
                        "Removing alias is enabled by config, refences found {:?}!",
                        indices_with_same_alias
                    );
                    for index_with_same_alias in indices_with_same_alias {
                        info!(
                            "Add action \"remove\" alias \"{}\" for \"{}\"",
                            alias_name, index_with_same_alias
                        );
                        let action = format!(
                            "{{ \"remove\": {{ \"index\": \"{}\", \"alias\": \"{}\" }} }}",
                            index_with_same_alias, alias_name
                        );
                        actions.push(action);
                    }
                } else {
                    warn!("Removing alias is disabled by config!");
                    return self;
                }
            }

            info!(
                "Add action \"add\" alias \"{}\" for \"{}\"",
                alias_name, index_name_of_copy
            );
            let action = format!(
                "{{ \"add\": {{ \"index\": \"{}\", \"alias\": \"{}\" }} }}",
                index_name_of_copy, alias_name
            );
            actions.push(action);

            let _ = self
                .call_post(
                    &format!("/_aliases"),
                    &vec![],
                    &vec![("Content-Type".to_string(), "application/json".to_string())],
                    &format!("{{ \"actions\": [ {} ]  }}", actions.join(",")),
                )
                .await;
        }

        self
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
