use crate::conf::Endpoint;
use crate::es_client::EsClient;
use core::panic;
use log::debug;

use reqwest::Certificate;
use tokio::fs::File;
use tokio::io::AsyncReadExt; // for read_to_end()

async fn create_certificate_from(
    file_name: String,
) -> Result<Certificate, Box<dyn std::error::Error>> {
    let mut file = File::open(file_name).await?;
    let mut contents = vec![];
    file.read_to_end(&mut contents).await?;
    let cert = reqwest::Certificate::from_pem(&contents)?;
    Ok(cert)
}

async fn create_http_client(
    endpoint: &Endpoint,
    root_certificates: &Vec<String>,
) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let mut builder = reqwest::Client::builder();

    for certificate_file_name in root_certificates {
        if let Ok(cert) = create_certificate_from(certificate_file_name.to_string()).await {
            debug!("Add root certificate: {:?}", cert);
            builder = builder.add_root_certificate(cert);
        }
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
            let http_client = create_http_client(endpoint, endpoint.get_root_certificates()).await;

            if let Ok(http_client) = http_client {
                let es_client = EsClient::new(endpoint.clone(), http_client);
                return Some(es_client);
            }
        }
    }
    None
}
