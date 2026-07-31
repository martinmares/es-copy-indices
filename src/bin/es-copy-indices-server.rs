#[allow(dead_code)]
#[path = "../backup.rs"]
mod backup;
#[allow(dead_code)]
#[path = "../conf.rs"]
mod conf;
#[path = "../server.rs"]
mod server;
#[path = "../server_auth.rs"]
mod server_auth;
#[path = "../server_static.rs"]
mod server_static;

#[tokio::main]
async fn main() {
    server::run().await;
}
