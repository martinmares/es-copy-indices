use log::debug;

#[derive(Debug, Clone)]
pub struct ScrollResponse {
    scroll_id: String,
    total_docs: u64,
    docs: Vec<Document>,
}
