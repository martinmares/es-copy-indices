use crate::conf::Endpoint;
use crate::es_client::EsClient;
use serde_json::Value;

use core::panic;
use std::time::Duration;
use tracing::info;

use reqwest::Certificate;
use tokio::fs::File;
use tokio::io::AsyncReadExt; // for read_to_end()

async fn read_content_from(file_name: String) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut file = File::open(file_name).await?;
    let mut content = vec![];
    file.read_to_end(&mut content).await?;
    Ok(content)
}

async fn create_certificate_from(
    file_name: String,
) -> Result<Certificate, Box<dyn std::error::Error>> {
    let content = read_content_from(file_name).await?;
    let cert = reqwest::Certificate::from_pem(&content)?;
    Ok(cert)
}

async fn create_http_client(
    endpoint: &Endpoint,
) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let timeout = endpoint.get_timeout();
    info!("Set timeout: {}", timeout);
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(*timeout));
    let mut root_certificates: Vec<String> = vec![];

    if let Some(root_certificates_path) = endpoint.get_root_certificates() {
        let dir = tokio::fs::read_dir(root_certificates_path).await;
        if let Ok(mut entry) = dir {
            while let Ok(Some(item)) = entry.next_entry().await {
                root_certificates.push(item.path().to_str().unwrap().to_string());
            }
        }
    }

    for certificate_file_name in root_certificates {
        if let Ok(cert) = create_certificate_from(certificate_file_name.to_string()).await {
            info!("Add root certificate \"{}\"", certificate_file_name);
            builder = builder.add_root_certificate(cert);
        }
    }

    if endpoint.get_insecure() {
        info!("Disable TLS verification for {}", endpoint.get_name());
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Ok(client) = builder.build() {
        Ok(client)
    } else {
        panic!(
            "Can't make HTTP/S client for {} => {}",
            endpoint.get_name(),
            endpoint.get_url()
        )
    }
}

pub async fn create_es_client(endpoints: &Vec<Endpoint>, which_one: &String) -> Option<EsClient> {
    for endpoint in endpoints {
        if endpoint.get_name() == which_one {
            let http_client = create_http_client(endpoint).await;

            if let Ok(http_client) = http_client {
                let mut es_client = EsClient::new(endpoint.clone(), http_client);
                es_client.detect_server().await;
                return Some(es_client);
            }
        }
    }
    None
}

pub async fn string_to_json(file_name: &String) -> Option<Value> {
    if file_name.is_empty() {
        None
    } else {
        if let Ok(content) = read_content_from(file_name.clone()).await {
            if let Ok(value) = String::from_utf8(content) {
                return serde_json::from_str(&value).ok();
            }
        }
        None
    }
}

#[macro_export]
macro_rules! memory_stats {
    () => {
        if let Some(usage) = memory_stats::memory_stats() {
            tracing::debug!(
                "memory usage {}",
                human_bytes::human_bytes(usage.physical_mem as f64)
            );
        }
    };
}
