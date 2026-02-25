use crate::engine::MiracleEngine;
use std::sync::Arc;

pub struct ToolRegistry {
    engine: Arc<MiracleEngine>,
}

impl ToolRegistry {
    pub fn new(engine: Arc<MiracleEngine>) -> Self {
        Self { engine }
    }

    pub async fn list_tools(&self) -> Vec<String> {
        vec![
            "sql_query".to_string(),
            "vector_search".to_string(),
            "graph_traversal".to_string(),
            "read_nucleus".to_string(),
        ]
    }
}
