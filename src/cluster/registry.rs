use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use super::node::{ClusterNode, NodeId, NodeStatus};

pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<NodeId, ClusterNode>>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self { nodes: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn register(&self, node: ClusterNode) {
        self.nodes.write().unwrap().insert(node.id.clone(), node);
    }

    pub fn deregister(&self, id: &NodeId) {
        self.nodes.write().unwrap().remove(id);
    }

    pub fn active_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().unwrap().values()
            .filter(|n| matches!(n.status, NodeStatus::Active))
            .cloned()
            .collect()
    }

    pub fn heartbeat(&self, id: &NodeId) {
        if let Some(node) = self.nodes.write().unwrap().get_mut(id) {
            node.last_heartbeat = std::time::SystemTime::now();
        }
    }
}
