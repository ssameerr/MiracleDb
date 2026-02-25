//! Dependency Module - Object dependency tracking

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Dependency type
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum DependencyType {
    References,
    Extends,
    Uses,
    Implements,
    Triggers,
    Calls,
    DependsOn,
}

/// Database object reference
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRef {
    pub object_type: String,
    pub schema: Option<String>,
    pub name: String,
}

impl ObjectRef {
    pub fn new(object_type: &str, name: &str) -> Self {
        Self {
            object_type: object_type.to_string(),
            schema: None,
            name: name.to_string(),
        }
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    pub fn full_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }
}

/// Dependency edge
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Dependency {
    pub from: ObjectRef,
    pub to: ObjectRef,
    pub dep_type: DependencyType,
    pub created_at: i64,
}

/// Dependency graph
pub struct DependencyGraph {
    edges: RwLock<Vec<Dependency>>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            edges: RwLock::new(Vec::new()),
        }
    }

    /// Add dependency
    pub async fn add(&self, from: ObjectRef, to: ObjectRef, dep_type: DependencyType) {
        let dep = Dependency {
            from,
            to,
            dep_type,
            created_at: chrono::Utc::now().timestamp(),
        };

        let mut edges = self.edges.write().await;
        edges.push(dep);
    }

    /// Remove dependency
    pub async fn remove(&self, from: &ObjectRef, to: &ObjectRef) {
        let mut edges = self.edges.write().await;
        edges.retain(|d| &d.from != from || &d.to != to);
    }

    /// Remove all dependencies for object
    pub async fn remove_object(&self, obj: &ObjectRef) {
        let mut edges = self.edges.write().await;
        edges.retain(|d| &d.from != obj && &d.to != obj);
    }

    /// Get objects that depend on this object
    pub async fn get_dependents(&self, obj: &ObjectRef) -> Vec<ObjectRef> {
        let edges = self.edges.read().await;
        edges.iter()
            .filter(|d| &d.to == obj)
            .map(|d| d.from.clone())
            .collect()
    }

    /// Get objects this object depends on
    pub async fn get_dependencies(&self, obj: &ObjectRef) -> Vec<ObjectRef> {
        let edges = self.edges.read().await;
        edges.iter()
            .filter(|d| &d.from == obj)
            .map(|d| d.to.clone())
            .collect()
    }

    /// Get all transitive dependents (cascade analysis)
    pub async fn get_all_dependents(&self, obj: &ObjectRef) -> HashSet<ObjectRef> {
        let mut result = HashSet::new();
        let mut to_process = vec![obj.clone()];

        while let Some(current) = to_process.pop() {
            let deps = self.get_dependents(&current).await;
            for dep in deps {
                if result.insert(dep.clone()) {
                    to_process.push(dep);
                }
            }
        }

        result
    }

    /// Check if dropping object would cause issues
    pub async fn can_drop(&self, obj: &ObjectRef) -> DropAnalysis {
        let dependents = self.get_all_dependents(obj).await;
        
        DropAnalysis {
            can_drop: dependents.is_empty(),
            affected_objects: dependents.iter().cloned().collect(),
            cascade_required: !dependents.is_empty(),
        }
    }

    /// Get dependency chain
    pub async fn get_dependency_chain(&self, from: &ObjectRef, to: &ObjectRef) -> Option<Vec<ObjectRef>> {
        let mut visited = HashSet::new();
        let mut queue = vec![(from.clone(), vec![from.clone()])];

        while let Some((current, path)) = queue.pop() {
            if &current == to {
                return Some(path);
            }

            if visited.insert(current.clone()) {
                let deps = self.get_dependencies(&current).await;
                for dep in deps {
                    let mut new_path = path.clone();
                    new_path.push(dep.clone());
                    queue.push((dep, new_path));
                }
            }
        }

        None
    }

    /// Detect circular dependencies
    pub async fn detect_cycles(&self) -> Vec<Vec<ObjectRef>> {
        let edges = self.edges.read().await;
        let mut cycles = Vec::new();

        // Build adjacency list
        let mut adj: HashMap<ObjectRef, Vec<ObjectRef>> = HashMap::new();
        for edge in edges.iter() {
            adj.entry(edge.from.clone())
                .or_insert_with(Vec::new)
                .push(edge.to.clone());
        }

        // DFS for each node
        let nodes: Vec<_> = adj.keys().cloned().collect();
        for start in nodes {
            let mut visited = HashSet::new();
            let mut path = Vec::new();
            self.dfs_cycle(&adj, &start, &mut visited, &mut path, &mut cycles);
        }

        cycles
    }

    fn dfs_cycle(
        &self,
        adj: &HashMap<ObjectRef, Vec<ObjectRef>>,
        node: &ObjectRef,
        visited: &mut HashSet<ObjectRef>,
        path: &mut Vec<ObjectRef>,
        cycles: &mut Vec<Vec<ObjectRef>>,
    ) {
        if path.contains(node) {
            let cycle_start = path.iter().position(|n| n == node).unwrap();
            cycles.push(path[cycle_start..].to_vec());
            return;
        }

        if visited.contains(node) {
            return;
        }

        visited.insert(node.clone());
        path.push(node.clone());

        if let Some(neighbors) = adj.get(node) {
            for neighbor in neighbors {
                self.dfs_cycle(adj, neighbor, visited, path, cycles);
            }
        }

        path.pop();
    }

    /// Export to DOT format
    pub async fn to_dot(&self) -> String {
        let edges = self.edges.read().await;
        let mut dot = String::from("digraph Dependencies {\n");
        dot.push_str("  rankdir=LR;\n");

        for edge in edges.iter() {
            dot.push_str(&format!(
                "  \"{}\" -> \"{}\" [label=\"{:?}\"];\n",
                edge.from.full_name(),
                edge.to.full_name(),
                edge.dep_type
            ));
        }

        dot.push_str("}\n");
        dot
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Drop analysis result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DropAnalysis {
    pub can_drop: bool,
    pub affected_objects: Vec<ObjectRef>,
    pub cascade_required: bool,
}
