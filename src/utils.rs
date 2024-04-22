use crate::conf::Endpoint;
use crate::es_client::EsClient;

// use core::panic;
use log::{debug, error};
use std::{fs, sync::Arc};

use native_tls::{Certificate, TlsConnector};
use ureq::Agent;

// async fn create_certificate_from(
//     file_name: String,
// ) -> Result<Certificate, Box<dyn std::error::Error>> {
//     let mut file = File::open(file_name).await?;
//     let mut contents = vec![];
//     file.read_to_end(&mut contents).await?;
//     let cert = reqwest::Certificate::from_pem(&contents)?;
//     Ok(cert)
// }

pub fn build_certificate_from(file_name: &String) -> Option<Certificate> {
    match fs::read(file_name) {
        Ok(file_content) => {
            if let Ok(cert) = Certificate::from_pem(&file_content) {
                return Some(cert);
            }
        }
        Err(e) => {
            error!("read PEM from file {} failed: {}", file_name, e)
        }
    }

    None
}
pub fn build_http_agent(root_certificates: &Vec<String>) -> Option<Agent> {
    let mut tls_builder = TlsConnector::builder();

    for file_name in root_certificates {
        if let Some(cert) = build_certificate_from(file_name) {
            tls_builder.add_root_certificate(cert);
            debug!("add PEM \"{}\" to TlsConnector", file_name);
        }
    }

    let tls_connector = tls_builder.build();

    debug!("ureq::Agent builded correctly");

    let agent_builder = ureq::AgentBuilder::new()
        .tls_connector(Arc::new(tls_connector.unwrap()))
        .build();

    Some(agent_builder)
}

pub fn build_es_client(endpoints: &Vec<Endpoint>, which_one: &String) -> Option<EsClient> {
    for endpoint in endpoints {
        if endpoint.get_name() == which_one {
            let http_agent = build_http_agent(endpoint.get_root_certificates());

            if let Some(http_agent) = http_agent {
                let es_client = EsClient::new(endpoint.clone(), http_agent);
                return Some(es_client);
            }
        }
    }

    None
}

// async fn create_http_client(
//     endpoint: &Endpoint,
//     root_certificates: &Vec<String>,
// ) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
//     let mut builder = reqwest::Client::builder();

//     for certificate_file_name in root_certificates {
//         if let Ok(cert) = create_certificate_from(certificate_file_name.to_string()).await {
//             debug!("Add root certificate: {:?}", cert);
//             builder = builder.add_root_certificate(cert);
//         }
//     }

//     if let Ok(client) = builder.build() {
//         Ok(client)
//     } else {
//         panic!(
//             "Can't make HTTP/S client for {} => {}",
//             endpoint.get_name(),
//             endpoint.get_url()
//         )
//     }
// }

// pub async fn create_es_client(endpoints: &Vec<Endpoint>, which_one: &String) -> Option<EsClient> {
//     for endpoint in endpoints {
//         if endpoint.get_name() == which_one {
//             let http_client = create_http_client(endpoint, endpoint.get_root_certificates()).await;

//             if let Ok(http_client) = http_client {
//                 let es_client = EsClient::new(endpoint.clone(), http_client);
//                 return Some(es_client);
//             }
//         }
//     }
//     None
// }

#[macro_export]
macro_rules! memory_stats {
    () => {
        if let Some(usage) = memory_stats::memory_stats() {
            log::debug!(
                "mem: {}",
                human_bytes::human_bytes(usage.physical_mem as f64)
            );
        }
    };
}
