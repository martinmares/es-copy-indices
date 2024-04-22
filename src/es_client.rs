use crate::conf::{Endpoint, Index};
use crate::models::scroll_response::ScrollResponse;
use crate::models::server_info::ServerInfo;
use log::{debug, info};

use reqwest::{Client, RequestBuilder};

#[derive(Debug, Clone)]
pub struct EsClient {
    endpoint: Endpoint,
    http_client: Client,
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
        }
    }

    async fn call_get(
        self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Get url: {}", url);
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
        self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Post url: {}", url);
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

    async fn call_delete(
        self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Delete url: {}", url);
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

        todo!("Implement empty response!")
    }

    pub async fn server_info(self) -> Option<ServerInfo> {
        let resp = self.call_get("/", &vec![], &vec![]).await;
        if let Some(value) = resp {
            let json: ServerInfo =
                serde_json::from_str(&value).expect("Incorrect response for ServerInfo struct");
            return Some(json);
        }

        None
    }

    pub async fn print_server_info(self, prefix: &str) {
        if let Some(server_info) = self.server_info().await {
            info!(
                "{}: hostname={}, name={}, uuid={}, version={}, lucene={}",
                prefix,
                server_info.get_hostname(),
                server_info.get_name(),
                server_info.get_uuid(),
                server_info.get_version(),
                server_info.get_lucene_version()
            );
        }
    }

    pub async fn scroll_start(self, index: &Index) -> Option<ScrollResponse> {
        let index_name = index.get_name();
        let keep_alive = index.get_keep_alive();
        let buffer_size = index.get_buffer_size();

        let body = format!(
            "{{ \"size\": {}, \"query\": {{ \"match_all\": {{}} }} }}",
            buffer_size
        );
        debug!("Query: {}", body);
        let resp = self
            .call_post(
                &format!("/{}/_search", index_name),
                &vec![(String::from("scroll"), format!("{}", keep_alive))],
                &vec![("Content-Type".to_string(), "application/json".to_string())],
                &body,
            )
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                debug!("scroll_start json_value: {:#?}", json_value);
                let scroll_response = ScrollResponse::new(json_value);
                return Some(scroll_response.clone());
            }
        }

        None
    }

    pub async fn scroll_next(self, index: &Index, scroll_id: &str) -> Option<ScrollResponse> {
        let keep_alive = index.get_keep_alive();

        let body = format!(
            "{{ \"scroll\": \"{}\", \"scroll_id\": \"{}\" }}",
            keep_alive, scroll_id
        );
        debug!("Query: {}", body);
        let resp = self
            .call_post(
                &format!("/_search/scroll"),
                &vec![],
                &vec![("Content-Type".to_string(), "application/json".to_string())],
                &body,
            )
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                debug!("scroll_next json_value: {:#?}", json_value);
                let scroll_response = ScrollResponse::new(json_value);
                return Some(scroll_response.clone());
            }
        }

        None
    }

    pub async fn scroll_stop(self, scroll_id: &str) {
        let body = format!("{{ \"scroll_id\": \"{}\" }}", scroll_id);
        debug!("Query: {}", body);
        let resp = self
            .call_delete(
                &format!("/_search/scroll"),
                &vec![],
                &vec![("Content-Type".to_string(), "application/json".to_string())],
                &body,
            )
            .await;
        if let Some(value) = resp {
            let json_value_result: Result<serde_json::Value, serde_json::Error> =
                serde_json::from_str(&value);
            if let Ok(json_value) = json_value_result {
                debug!("scroll_stop json_value: {:#?}", json_value);
                //let scroll_response = ScrollResponse::new(json_value);
                //return Some(scroll_response.clone());
            }
        }
    }
}
