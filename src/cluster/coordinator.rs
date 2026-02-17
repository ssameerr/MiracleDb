use super::registry::NodeRegistry;
use std::sync::Arc;

pub struct QueryCoordinator {
    registry: Arc<NodeRegistry>,
}

impl QueryCoordinator {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        Self { registry }
    }

    /// Select nodes for query execution using round-robin
    pub fn select_nodes(&self, count: usize) -> Vec<super::node::ClusterNode> {
        let nodes = self.registry.active_nodes();
        nodes.into_iter().take(count).collect()
    }

    /// Distribute a query across nodes and collect results
    pub async fn distribute_query(
        &self,
        query: &str,
        node_count: usize,
    ) -> Result<Vec<serde_json::Value>, String> {
        let nodes = self.select_nodes(node_count);
        if nodes.is_empty() {
            return Err("No active nodes available".to_string());
        }
        // In full implementation: send query to each node via Arrow Flight
        // For now return stub indicating distribution would happen
        Ok(vec![serde_json::json!({
            "status": "distributed",
            "nodes": nodes.len(),
            "query": query
        })])
    }
}
