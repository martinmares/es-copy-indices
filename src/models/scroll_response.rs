use log::debug;

#[derive(Debug, Clone)]
pub struct ScrollResponse {
    scroll_id: String,
    total_docs: u64,
    docs: Vec<Document>,
}

#[derive(Debug, Clone)]
pub struct Document {
    id: String,
    index_name: String,
    doc_type: String,
    source: String,
}

impl ScrollResponse {
    pub fn new(json_value: serde_json::Value) -> Self {
        let scroll_id = json_value["_scroll_id"].as_str().unwrap().to_string();
        let total_docs = json_value["hits"]["total"]["value"]
            .as_number()
            .unwrap()
            .as_u64()
            .unwrap();
        let hits = json_value["hits"]["hits"].as_array().unwrap();

        let mut docs: Vec<Document> = vec![];

        for hit in hits {
            debug!("hit: {:#?}", hit);
            let id = hit["_id"].as_str().unwrap().to_string();
            let index_name = hit["_index"].as_str().unwrap().to_string();
            let doc_type = hit["_type"].as_str().unwrap().to_string();
            let source_value = hit["_source"].as_object().unwrap();
            let source = serde_json::to_string(&source_value)
                .expect("failed convert Objec to String (serde_json)");
            debug!("source_value: {:#?}", source_value);
            debug!("source_string: {}", source);

            docs.push(Document {
                id,
                index_name,
                doc_type,
                source,
            })
        }

        Self {
            scroll_id,
            total_docs,
            docs,
        }
    }

    pub fn get_scroll_id(&self) -> &String {
        &self.scroll_id
    }
    pub fn get_docs(&self) -> &Vec<Document> {
        &self.docs
    }
    pub fn get_current_size(&self) -> u64 {
        *&self.docs.len() as u64
    }
    pub fn has_docs(&self) -> bool {
        *&self.get_current_size() > 0
    }
    pub fn get_total_size(&self) -> u64 {
        *&self.total_docs
    }
}
