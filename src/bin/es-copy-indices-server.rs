#[allow(dead_code)]
#[path = "../backup.rs"]
mod backup;
#[allow(dead_code)]
#[path = "../conf.rs"]
mod conf;
#[path = "../server.rs"]
mod server;

#[tokio::main]
async fn main() {
    server::run().await;
}
