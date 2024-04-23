use crate::conf::{Endpoint, Index};
use crate::models::scroll_response::ScrollResponse;
use crate::models::server_info::ServerInfo;
use log::{debug, info};

use reqwest::{Client, RequestBuilder};

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
        debug!("get url: {}", url);
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
        debug!("post url: {}", url);
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
                debug!("post response text: {}", text);
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
        debug!("delete url: {}", url);
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
                debug!("post response text: {}", text);
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
                "info about \"{}\" (hostname={}, name={}, uuid={}, version_major={}, version={}, lucene={})",
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

    pub async fn scroll_start(&mut self, index: &Index) -> &mut Self {
        let index_name = index.get_name();
        let keep_alive = index.get_keep_alive();
        let buffer_size = index.get_buffer_size();

        let body = format!(
            "{{ \"size\": {}, \"query\": {{ \"match_all\": {{}} }} }}",
            buffer_size
        );
        debug!("query: {}", body);
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
                debug!("scroll_start json_value: {:#?}", json_value);
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

    pub async fn scroll_next(&mut self, index: &Index) -> &mut Self {
        let keep_alive = index.get_keep_alive();

        let body = format!(
            "{{ \"scroll\": \"{}\", \"scroll_id\": \"{}\" }}",
            keep_alive,
            self.scroll_id.clone().unwrap()
        );
        debug!("query: {}", body);
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
                debug!("scroll_next json_value: {:#?}", json_value);
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

    pub async fn scroll_stop(&mut self) {
        let body = format!(
            "{{ \"scroll_id\": \"{}\" }}",
            self.scroll_id.clone().unwrap()
        );
        debug!("query: {}", body);
        let resp = self
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
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                debug!("scroll_stop json_value: {:#?}", json_value);
            }
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
