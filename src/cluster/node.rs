use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus { Active, Draining, Dead }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: NodeId,
    pub address: String,
    pub port: u16,
    pub status: NodeStatus,
    pub last_heartbeat: std::time::SystemTime,
}

impl ClusterNode {
    pub fn new(id: &str, address: &str, port: u16) -> Self {
        Self {
            id: NodeId(id.to_string()),
            address: address.to_string(),
            port,
            status: NodeStatus::Active,
            last_heartbeat: std::time::SystemTime::now(),
        }
    }

    pub fn is_alive(&self, timeout_secs: u64) -> bool {
        matches!(self.status, NodeStatus::Active) &&
        self.last_heartbeat.elapsed().map(|d| d.as_secs() < timeout_secs).unwrap_or(false)
    }
}
