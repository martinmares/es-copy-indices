use serde_json::Value;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct ScrollResponse {
    scroll_id: String,
    total_docs: u64,
    docs: Vec<Document>,
    last_sort: Option<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub struct Document {
    id: String,
    index_name: String,
    doc_type: String,
    source: String,
}

impl ScrollResponse {
    pub fn new(json_value: serde_json::Value, preview_scroll_id: Option<String>) -> Self {
        let mut scroll_id = String::default();

        if let Some(value) = json_value["_scroll_id"].as_str() {
            scroll_id = value.to_string();
        }
        let mut total_docs: u64 = 0;

        // try parse from new one -> ES >= 5
        if let Some(value) = json_value["hits"]["total"]["value"].as_number() {
            total_docs = value.as_u64().unwrap();
        // try parse from old one -> ES >= 2
        } else if let Some(value) = json_value["hits"]["total"].as_number() {
            total_docs = value.as_u64().unwrap();
        }

        let mut hits: &Vec<Value> = &vec![];

        if let Some(value) = json_value["hits"]["hits"].as_array() {
            hits = &value;
        }

        if scroll_id.is_empty() {
            if let Some(value) = preview_scroll_id {
                scroll_id = value;
                warn!("Setting scroll id from preview one {:?}", scroll_id);
            }
        }
        let mut docs: Vec<Document> = vec![];
        let mut last_sort: Option<Vec<Value>> = None;

        for hit in hits {
            debug!("hit: {:#?}", hit);
            let id = hit["_id"].as_str().unwrap().to_string();
            let index_name = hit["_index"].as_str().unwrap().to_string();
            let doc_type = match hit["_type"].as_str() {
                Some(value) => value.to_string(),
                None => "_doc".to_string(),
            };
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

        if let Some(last_hit) = hits.last() {
            if let Some(sort_values) = last_hit["sort"].as_array() {
                last_sort = Some(sort_values.clone());
            }
        }

        Self {
            scroll_id,
            total_docs,
            docs,
            last_sort,
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
    pub fn get_last_sort(&self) -> &Option<Vec<Value>> {
        &self.last_sort
    }
}

impl Document {
    pub fn get_id(&self) -> &String {
        &self.id
    }
    pub fn get_doc_type(&self) -> &String {
        &self.doc_type
    }
    pub fn get_source(&self) -> &String {
        &self.source
    }
}
