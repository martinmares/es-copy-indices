use crate::conf::Endpoint;
use crate::models::server_info::ServerInfo;
use core::panic;
use log::{debug, error, info, warn};
use rustls::client;
use std::fmt::Error;

use reqwest::{Certificate, Client, ClientBuilder, RequestBuilder};
use tokio::fs::File;
use tokio::io::AsyncReadExt; // for read_to_end()

#[derive(Debug)]
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

    async fn call_get(self, path: &str) -> Option<String> {
        let mut request_builder =
            self.http_client
                .get(format!("{}{}", self.endpoint.get_url(), path));

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
    pub async fn server_info(self) -> Option<ServerInfo> {
        let resp = self.call_get("/").await;
        if let Some(value) = resp {
            let json: ServerInfo = serde_json::from_str(&value)
                .expect("Incorrect response to deserialize data to ServerInfo struct");
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
}
