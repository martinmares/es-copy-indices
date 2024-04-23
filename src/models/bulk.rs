// use crate::es_client;

#[derive(Debug, Clone)]
pub struct Document {
    operation: &'static str,
    index_name: String,
    doc_type: String,
    doc: String,
}

// impl Bulk {
//     pub fn new() -> Self {
//         Self {
//             operation: es_client::BULK_OPER_INDEX,
//             index_name,
//         }
//     }
// }
