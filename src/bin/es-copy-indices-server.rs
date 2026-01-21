#[path = "../server.rs"]
mod server;
#[allow(dead_code)]
#[path = "../backup.rs"]
mod backup;
#[allow(dead_code)]
#[path = "../conf.rs"]
mod conf;

#[tokio::main]
async fn main() {
    server::run().await;
}
