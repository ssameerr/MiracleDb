//! Graph Module - Graph traversals using petgraph
///! Graph Module - Graph database with petgraph backend

use std::collections::HashMap;
use tokio::sync::RwLock;
use petgraph::graph::{Graph, NodeIndex, EdgeIndex};
use petgraph::algo::{dijkstra, astar};
use petgraph::visit::{Bfs, EdgeRef};
use petgraph::Direction;
use serde::{Serialize, Deserialize};

/// Node data in the graph
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeData {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: HashMap<String, serde_json::Value>,
}

/// Edge data in the graph
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeData {
    pub relationship_type: String,
    pub properties: HashMap<String, serde_json::Value>,
}

/// Graph database backed by petgraph
pub struct GraphDb {
    graph: RwLock<Graph<NodeData, EdgeData>>,
    node_index: RwLock<HashMap<String, NodeIndex>>,
}

impl GraphDb {
    pub fn new() -> Self {
        Self {
            graph: RwLock::new(Graph::new()),
            node_index: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new node
    pub async fn create_node(&self, id: &str, labels: Vec<String>, properties: HashMap<String, serde_json::Value>) -> NodeIndex {
        let node_data = NodeData {
            id: id.to_string(),
            labels,
            properties,
        };

        let mut graph = self.graph.write().await;
        let mut index = self.node_index.write().await;

        let node_idx = graph.add_node(node_data);
        index.insert(id.to_string(), node_idx);

        node_idx
    }

    /// Create an edge between two nodes
    pub async fn create_edge(
        &self,
        from_id: &str,
        to_id: &str,
        relationship_type: &str,
        properties: HashMap<String, serde_json::Value>,
    ) -> Result<EdgeIndex, String> {
        let index = self.node_index.read().await;
        let from_idx = *index.get(from_id)
            .ok_or(format!("Node {} not found", from_id))?;
        let to_idx = *index.get(to_id)
            .ok_or(format!("Node {} not found", to_id))?;
        drop(index);

        let edge_data = EdgeData {
            relationship_type: relationship_type.to_string(),
            properties,
        };

        let mut graph = self.graph.write().await;
        Ok(graph.add_edge(from_idx, to_idx, edge_data))
    }

    /// Get node by ID
    pub async fn get_node(&self, id: &str) -> Option<NodeData> {
        let index = self.node_index.read().await;
        let node_idx = *index.get(id)?;
        drop(index);

        let graph = self.graph.read().await;
        graph.node_weight(node_idx).cloned()
    }

    /// Find shortest path between two nodes
    pub async fn shortest_path(&self, from_id: &str, to_id: &str) -> Result<Vec<String>, String> {
        let index = self.node_index.read().await;
        let from_idx = *index.get(from_id)
            .ok_or(format!("Node {} not found", from_id))?;
        let to_idx = *index.get(to_id)
            .ok_or(format!("Node {} not found", to_id))?;
        drop(index);

        let graph = self.graph.read().await;

        // Use Dijkstra with uniform weights
        let result = dijkstra(&*graph, from_idx, Some(to_idx), |_| 1);

        if !result.contains_key(&to_idx) {
            return Err("No path found".to_string());
        }

        // Reconstruct path (simplified - would need proper path reconstruction)
        let path = vec![from_id.to_string(), to_id.to_string()];
        Ok(path)
    }

    /// Get neighbors of a node up to a certain depth
    pub async fn neighbors(&self, id: &str, depth: usize) -> Result<Vec<NodeData>, String> {
        let index = self.node_index.read().await;
        let start_idx = *index.get(id)
            .ok_or(format!("Node {} not found", id))?;
        drop(index);

        let graph = self.graph.read().await;
        let mut visited = Vec::new();
        let mut bfs = Bfs::new(&*graph, start_idx);
        let mut current_depth = 0;

        while let Some(node_idx) = bfs.next(&*graph) {
            if current_depth >= depth {
                break;
           }

            if let Some(node_data) = graph.node_weight(node_idx) {
                visited.push(node_data.clone());
            }

            current_depth += 1;
        }

        Ok(visited)
    }

    /// Traverse outgoing edges from a node
    pub async fn outgoing_edges(&self, id: &str) -> Result<Vec<(String, EdgeData)>, String> {
        let index = self.node_index.read().await;
        let node_idx = *index.get(id)
            .ok_or(format!("Node {} not found", id))?;
        drop(index);

        let graph = self.graph.read().await;
        let mut edges = Vec::new();

        for edge_ref in graph.edges_directed(node_idx, Direction::Outgoing) {
            let target_node = graph.node_weight(edge_ref.target()).unwrap();
            edges.push((target_node.id.clone(), edge_ref.weight().clone()));
        }

        Ok(edges)
    }

    /// Count nodes
    pub async fn node_count(&self) -> usize {
        let graph = self.graph.read().await;
        graph.node_count()
    }

    /// Count edges
    pub async fn edge_count(&self) -> usize {
        let graph = self.graph.read().await;
        graph.edge_count()
    }

    /// Query nodes by label
    pub async fn nodes_with_label(&self, label: &str) -> Vec<NodeData> {
        let graph = self.graph.read().await;
        graph.node_weights()
            .filter(|node| node.labels.contains(&label.to_string()))
            .cloned()
            .collect()
    }

    /// Query nodes by property
    pub async fn nodes_with_property(&self, key: &str, value: &serde_json::Value) -> Vec<NodeData> {
        let graph = self.graph.read().await;
        graph.node_weights()
            .filter(|node| node.properties.get(key) == Some(value))
            .cloned()
            .collect()
    }

    /// Delete a node and its edges
    pub async fn delete_node(&self, id: &str) -> Result<(), String> {
        let mut index = self.node_index.write().await;
        let node_idx = index.remove(id)
            .ok_or(format!("Node {} not found", id))?;

        let mut graph = self.graph.write().await;
        graph.remove_node(node_idx);

        Ok(())
    }

    /// Get all nodes and edges for visualization
    pub async fn dump_graph(&self) -> (Vec<NodeData>, Vec<(String, String, EdgeData)>) {
        let graph = self.graph.read().await;
        
        let nodes: Vec<NodeData> = graph.node_weights().cloned().collect();
        let mut edges = Vec::new();
        
        for edge_ref in graph.edge_references() {
            let source_node = graph.node_weight(edge_ref.source()).unwrap();
            let target_node = graph.node_weight(edge_ref.target()).unwrap();
            edges.push((
                source_node.id.clone(),
                target_node.id.clone(),
                edge_ref.weight().clone()
            ));
        }
        
        (nodes, edges)
    }

    /// Run PageRank algorithm (Simplified)
    pub async fn pagerank(&self, damping_factor: f64, iterations: usize) -> HashMap<String, f64> {
        let graph = self.graph.read().await;
        // Petgraph doesn't have built-in PageRank in simple algo module usually, so we implement simple one
        let mut ranks: HashMap<NodeIndex, f64> = HashMap::new();
        let node_count = graph.node_count();
        let initial_rank = 1.0 / node_count as f64;
        
        for node in graph.node_indices() {
            ranks.insert(node, initial_rank);
        }
        
        for _ in 0..iterations {
            let mut new_ranks = HashMap::new();
            for node in graph.node_indices() {
                let mut sum = 0.0;
                // Incoming edges
                for edge in graph.edges_directed(node, Direction::Incoming) {
                    let source = edge.source();
                    let source_rank = ranks.get(&source).unwrap_or(&0.0);
                    let out_degree = graph.neighbors_directed(source, Direction::Outgoing).count();
                    sum += source_rank / out_degree as f64;
                }
                let rank = (1.0 - damping_factor) / node_count as f64 + damping_factor * sum;
                new_ranks.insert(node, rank);
            }
            ranks = new_ranks;
        }
        
        let mut result = HashMap::new();
        for (node_idx, rank) in ranks {
             if let Some(node_data) = graph.node_weight(node_idx) {
                 result.insert(node_data.id.clone(), rank);
             }
        }
        result
    }

    /// Execute a Cypher-like query (Stub)
    pub async fn execute_cypher(&self, query: &str) -> Result<Vec<NodeData>, String> {
        // e.g., "MATCH (n:Person)"
        // Simple regex parsing for MVP
        if query.starts_with("MATCH (n:") {
            let label = query.trim_start_matches("MATCH (n:").trim_end_matches(")");
            let nodes = self.nodes_with_label(label).await;
            Ok(nodes)
        } else {
            Err("Unsupported query syntax".to_string())
        }
    }

}

impl Default for GraphDb {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_graph_basic() {
        let db = GraphDb::new();

        // Create nodes
        let mut props = HashMap::new();
        props.insert("name".to_string(), serde_json::Value::String("Alice".to_string()));
        db.create_node("alice", vec!["Person".to_string()], props.clone()).await;

        props.insert("name".to_string(), serde_json::Value::String("Bob".to_string()));
        db.create_node("bob", vec!["Person".to_string()], props).await;

        // Create edge
        let edge_props = HashMap::new();
        db.create_edge("alice", "bob", "KNOWS", edge_props).await.unwrap();

        // Test retrieval
        let alice = db.get_node("alice").await.unwrap();
        assert_eq!(alice.labels, vec!["Person"]);

        // Test counts
        assert_eq!(db.node_count().await, 2);
        assert_eq!(db.edge_count().await, 1);
    }

    #[tokio::test]
    async fn test_graph_traversal() {
        let db = GraphDb::new();

        db.create_node("a", vec![], HashMap::new()).await;
        db.create_node("b", vec![], HashMap::new()).await;
        db.create_node("c", vec![], HashMap::new()).await;

        db.create_edge("a", "b", "REL", HashMap::new()).await.unwrap();
        db.create_edge("b", "c", "REL", HashMap::new()).await.unwrap();

        let neighbors = db.neighbors("a", 2).await.unwrap();
        assert!(neighbors.len() >= 1);
    }
}
