#[path = "../server.rs"]
mod server;

#[tokio::main]
async fn main() {
    server::run().await;
}
