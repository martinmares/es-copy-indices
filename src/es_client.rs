use crate::conf::{Endpoint, Index};
use crate::models::scroll_response::ScrollResponse;
use crate::models::server_info::ServerInfo;
use http_auth_basic::Credentials;
use log::{debug, error, info};
use ureq::Agent;

#[derive(Debug, Clone)]
pub struct EsClient {
    endpoint: Endpoint,
    http_agent: Agent,
}

// fn inject_auth(request_builder: RequestBuilder, endpoint: Endpoint) -> RequestBuilder {
//     if endpoint.has_basic_auth() {
//         request_builder.basic_auth(endpoint.get_username(), endpoint.get_password())
//     } else {
//         request_builder
//     }
// }

impl EsClient {
    // pub async fn get_info(&self) -> Option<String> {}

    pub fn new(endpoint: Endpoint, http_agent: Agent) -> Self {
        Self {
            endpoint,
            http_agent,
        }
    }

    fn call_get(
        self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Get url: {}", url);
        let mut request_builder = self.http_agent.get(&url);

        for (param, value) in query {
            request_builder = request_builder.query(param, value);
        }

        for (key, val) in headers {
            request_builder = request_builder.set(key, val);
        }

        if self.endpoint.has_basic_auth() {
            let credentials = Credentials::new(
                &self.endpoint.get_username(),
                &self.endpoint.get_password().unwrap(),
            );
            request_builder = request_builder.set("Authorization", &credentials.as_http_header());
        }

        match request_builder.call() {
            Ok(call) => {
                if let Ok(text) = call.into_string() {
                    return Some(text);
                }
            }
            Err(e) => error!("Error: {:#?}", e),
        }

        // if let Ok(call) = request_builder.call() {
        //     debug!("Response -> {:#?}", call);
        //     if let Ok(text) = call.into_string() {
        //         return Some(text);
        //     }
        // }

        todo!("Implement empty response!")
    }

    fn call_post(
        self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Post url: {}", url);
        let mut request_builder = self.http_agent.post(&url);

        for (param, value) in query {
            request_builder = request_builder.query(param, value);
        }

        for (key, val) in headers {
            request_builder = request_builder.set(key, val);
        }

        if self.endpoint.has_basic_auth() {
            let credentials = Credentials::new(
                &self.endpoint.get_username(),
                &self.endpoint.get_password().unwrap(),
            );
            request_builder = request_builder.set("Authorization", &credentials.as_http_header());
        }

        if let Ok(call) = request_builder.send_string(&body) {
            if let Ok(text) = call.into_string() {
                debug!("Post response text: {}", text);
                return Some(text);
            }
        }

        todo!("Implement empty response!")
    }

    fn call_delete(
        self,
        path: &str,
        query: &Vec<(String, String)>,
        headers: &Vec<(String, String)>,
        body: &String,
    ) -> Option<String> {
        let url = format!("{}{}", self.endpoint.get_url(), path);
        debug!("Delete url: {}", url);
        let mut request_builder = self.http_agent.delete(&url);

        for (param, value) in query {
            request_builder = request_builder.query(param, value);
        }

        for (key, val) in headers {
            request_builder = request_builder.set(key, val);
        }

        if self.endpoint.has_basic_auth() {
            let credentials = Credentials::new(
                &self.endpoint.get_username(),
                &self.endpoint.get_password().unwrap(),
            );
            request_builder = request_builder.set("Authorization", &credentials.as_http_header());
        }

        if let Ok(call) = request_builder.send_string(&body) {
            let text = call.into_string();
            if let Ok(text) = text {
                debug!("Post response text: {}", text);
                return Some(text);
            }
        }

        todo!("Implement empty response!")
    }

    pub fn server_info(self) -> Option<ServerInfo> {
        let resp = self.call_get("/", &vec![], &vec![]);
        if let Some(value) = resp {
            let json: ServerInfo =
                serde_json::from_str(&value).expect("Incorrect response for ServerInfo struct");
            return Some(json);
        }

        None
    }

    pub fn print_server_info(self, prefix: &str) {
        if let Some(server_info) = self.server_info() {
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

    pub fn scroll_start(self, index: &Index) -> Option<ScrollResponse> {
        let index_name = index.get_name();
        let keep_alive = index.get_keep_alive();
        let buffer_size = index.get_buffer_size();

        let body = format!(
            "{{ \"size\": {}, \"query\": {{ \"match_all\": {{}} }} }}",
            buffer_size
        );
        debug!("Query: {}", body);
        let resp = self.call_post(
            &format!("/{}/_search", index_name),
            &vec![(String::from("scroll"), format!("{}", keep_alive))],
            &vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("Accept-encoding".to_string(), "gzip".to_string()),
            ],
            &body,
        );
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

    pub fn scroll_next(self, index: &Index, scroll_id: &str) -> Option<ScrollResponse> {
        let keep_alive = index.get_keep_alive();

        let body = format!(
            "{{ \"scroll\": \"{}\", \"scroll_id\": \"{}\" }}",
            keep_alive, scroll_id
        );
        debug!("Query: {}", body);
        let resp = self.call_post(
            &format!("/_search/scroll"),
            &vec![],
            &vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("Accept-encoding".to_string(), "gzip".to_string()),
            ],
            &body,
        );
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

    pub fn scroll_stop(self, scroll_id: &str) {
        let body = format!("{{ \"scroll_id\": \"{}\" }}", scroll_id);
        debug!("Query: {}", body);
        let resp = self.call_delete(
            &format!("/_search/scroll"),
            &vec![],
            &vec![
                ("Content-Type".to_string(), "application/json".to_string()),
                ("Accept-encoding".to_string(), "gzip".to_string()),
            ],
            &body,
        );
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
