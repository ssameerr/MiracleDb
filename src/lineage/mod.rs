//! Lineage Module - Data lineage and impact analysis

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Lineage node type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LineageNodeType {
    Table,
    Column,
    View,
    Query,
    Pipeline,
    ExternalSource,
}

/// Lineage node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageNode {
    pub id: String,
    pub name: String,
    pub node_type: LineageNodeType,
    pub metadata: HashMap<String, String>,
}

/// Lineage edge (dependency)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageEdge {
    pub source_id: String,
    pub target_id: String,
    pub edge_type: EdgeType,
    pub transformation: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EdgeType {
    DependsOn,
    DerivedFrom,
    Transforms,
    Joins,
    Filters,
}

/// Lineage graph
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LineageGraph {
    pub nodes: HashMap<String, LineageNode>,
    pub edges: Vec<LineageEdge>,
}

/// Lineage manager
pub struct LineageManager {
    graph: RwLock<LineageGraph>,
}

impl LineageManager {
    pub fn new() -> Self {
        Self {
            graph: RwLock::new(LineageGraph::default()),
        }
    }

    /// Add a node to the lineage graph
    pub async fn add_node(&self, node: LineageNode) {
        let mut graph = self.graph.write().await;
        graph.nodes.insert(node.id.clone(), node);
    }

    /// Add an edge to the lineage graph
    pub async fn add_edge(&self, edge: LineageEdge) {
        let mut graph = self.graph.write().await;
        graph.edges.push(edge);
    }

    /// Get upstream dependencies (what does this depend on?)
    pub async fn get_upstream(&self, node_id: &str) -> Vec<LineageNode> {
        let graph = self.graph.read().await;
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        self.traverse_upstream(&graph, node_id, &mut visited, &mut result);
        result
    }

    fn traverse_upstream(&self, graph: &LineageGraph, node_id: &str, visited: &mut HashSet<String>, result: &mut Vec<LineageNode>) {
        if visited.contains(node_id) {
            return;
        }
        visited.insert(node_id.to_string());

        for edge in &graph.edges {
            if edge.target_id == node_id {
                if let Some(node) = graph.nodes.get(&edge.source_id) {
                    result.push(node.clone());
                    self.traverse_upstream(graph, &edge.source_id, visited, result);
                }
            }
        }
    }

    /// Get downstream dependencies (what depends on this?)
    pub async fn get_downstream(&self, node_id: &str) -> Vec<LineageNode> {
        let graph = self.graph.read().await;
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        self.traverse_downstream(&graph, node_id, &mut visited, &mut result);
        result
    }

    fn traverse_downstream(&self, graph: &LineageGraph, node_id: &str, visited: &mut HashSet<String>, result: &mut Vec<LineageNode>) {
        if visited.contains(node_id) {
            return;
        }
        visited.insert(node_id.to_string());

        for edge in &graph.edges {
            if edge.source_id == node_id {
                if let Some(node) = graph.nodes.get(&edge.target_id) {
                    result.push(node.clone());
                    self.traverse_downstream(graph, &edge.target_id, visited, result);
                }
            }
        }
    }

    /// Impact analysis: what would be affected by changing this node?
    pub async fn impact_analysis(&self, node_id: &str) -> ImpactReport {
        let downstream = self.get_downstream(node_id).await;
        
        let mut tables = Vec::new();
        let mut views = Vec::new();
        let mut pipelines = Vec::new();

        for node in &downstream {
            match node.node_type {
                LineageNodeType::Table => tables.push(node.name.clone()),
                LineageNodeType::View => views.push(node.name.clone()),
                LineageNodeType::Pipeline => pipelines.push(node.name.clone()),
                _ => {}
            }
        }

        ImpactReport {
            node_id: node_id.to_string(),
            affected_count: downstream.len(),
            tables,
            views,
            pipelines,
        }
    }

    /// Track lineage from a SQL query
    pub async fn track_query(&self, query_id: &str, sql: &str, input_tables: Vec<String>, output_table: Option<String>) {
        // Add query node
        let query_node = LineageNode {
            id: query_id.to_string(),
            name: format!("Query: {}", &sql[..sql.len().min(50)]),
            node_type: LineageNodeType::Query,
            metadata: HashMap::from([("sql".to_string(), sql.to_string())]),
        };
        self.add_node(query_node).await;

        // Add edges from inputs
        for table in input_tables {
            let edge = LineageEdge {
                source_id: table,
                target_id: query_id.to_string(),
                edge_type: EdgeType::DependsOn,
                transformation: Some(sql.to_string()),
            };
            self.add_edge(edge).await;
        }

        // Add edge to output
        if let Some(output) = output_table {
            let edge = LineageEdge {
                source_id: query_id.to_string(),
                target_id: output,
                edge_type: EdgeType::DerivedFrom,
                transformation: None,
            };
            self.add_edge(edge).await;
        }
    }

    /// Get the full lineage graph
    pub async fn get_graph(&self) -> LineageGraph {
        let graph = self.graph.read().await;
        graph.clone()
    }

    /// Export as DOT for GraphViz
    pub async fn to_dot(&self) -> String {
        let graph = self.graph.read().await;
        let mut output = String::from("digraph Lineage {\n");
        output.push_str("  rankdir=LR;\n");

        for (id, node) in &graph.nodes {
            let shape = match node.node_type {
                LineageNodeType::Table => "box",
                LineageNodeType::Column => "ellipse",
                LineageNodeType::View => "parallelogram",
                LineageNodeType::Query => "diamond",
                LineageNodeType::Pipeline => "hexagon",
                LineageNodeType::ExternalSource => "cylinder",
            };
            output.push_str(&format!("  \"{}\" [label=\"{}\" shape={}];\n", id, node.name, shape));
        }

        for edge in &graph.edges {
            output.push_str(&format!("  \"{}\" -> \"{}\";\n", edge.source_id, edge.target_id));
        }

        output.push_str("}\n");
        output
    }
}

impl Default for LineageManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Impact analysis report
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImpactReport {
    pub node_id: String,
    pub affected_count: usize,
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub pipelines: Vec<String>,
}
