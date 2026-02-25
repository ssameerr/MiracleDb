//! Discovery - Node discovery, health checks, and gossip protocol

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;
use serde::{Serialize, Deserialize};
use tracing::{info, warn};

/// Node information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub address: String,
    pub role: NodeRole,
    pub status: NodeStatus,
    pub version: String,
    pub last_seen: i64,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
    Observer,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// Health check result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheck {
    pub node_id: String,
    pub status: NodeStatus,
    pub latency_ms: u64,
    pub checks: Vec<ServiceCheck>,
    pub timestamp: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceCheck {
    pub name: String,
    pub healthy: bool,
    pub message: Option<String>,
}

/// Gossip message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GossipMessage {
    Ping { from: String, generation: u64 },
    Pong { from: String, generation: u64 },
    NodeJoin { node: NodeInfo },
    NodeLeave { node_id: String },
    StateSync { nodes: Vec<NodeInfo> },
}

/// Discovery service
pub struct Discovery {
    node_id: String,
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    known_peers: RwLock<HashSet<String>>,
    heartbeat_interval_secs: u64,
    failure_threshold: u32,
    failure_counts: RwLock<HashMap<String, u32>>,
    generation: std::sync::atomic::AtomicU64,
}

impl Discovery {
    pub fn new(node_id: &str, heartbeat_interval: u64) -> Self {
        Self {
            node_id: node_id.to_string(),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            known_peers: RwLock::new(HashSet::new()),
            heartbeat_interval_secs: heartbeat_interval,
            failure_threshold: 3,
            failure_counts: RwLock::new(HashMap::new()),
            generation: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Register this node
    pub async fn register_self(&self, address: &str, role: NodeRole) {
        let node = NodeInfo {
            id: self.node_id.clone(),
            address: address.to_string(),
            role,
            status: NodeStatus::Healthy,
            version: env!("CARGO_PKG_VERSION").to_string(),
            last_seen: chrono::Utc::now().timestamp(),
            metadata: HashMap::new(),
        };

        let mut nodes = self.nodes.write().await;
        nodes.insert(self.node_id.clone(), node);
    }

    /// Add a peer
    pub async fn add_peer(&self, address: String) {
        let mut peers = self.known_peers.write().await;
        peers.insert(address);
    }

    /// Remove a peer
    pub async fn remove_peer(&self, address: &str) {
        let mut peers = self.known_peers.write().await;
        peers.remove(address);
    }

    /// Get all peers
    pub async fn get_peers(&self) -> Vec<String> {
        let peers = self.known_peers.read().await;
        peers.iter().cloned().collect()
    }

    /// Handle gossip message
    pub async fn handle_gossip(&self, msg: GossipMessage) -> Option<GossipMessage> {
        match msg {
            GossipMessage::Ping { from, generation } => {
                self.update_node_seen(&from).await;
                Some(GossipMessage::Pong {
                    from: self.node_id.clone(),
                    generation: self.generation.load(std::sync::atomic::Ordering::Relaxed),
                })
            }
            GossipMessage::Pong { from, .. } => {
                self.update_node_seen(&from).await;
                self.reset_failure_count(&from).await;
                None
            }
            GossipMessage::NodeJoin { node } => {
                info!("Node joined: {}", node.id);
                let mut nodes = self.nodes.write().await;
                nodes.insert(node.id.clone(), node);
                None
            }
            GossipMessage::NodeLeave { node_id } => {
                info!("Node left: {}", node_id);
                let mut nodes = self.nodes.write().await;
                nodes.remove(&node_id);
                None
            }
            GossipMessage::StateSync { nodes: new_nodes } => {
                let mut nodes = self.nodes.write().await;
                for node in new_nodes {
                    nodes.insert(node.id.clone(), node);
                }
                None
            }
        }
    }

    /// Update node last seen
    async fn update_node_seen(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.last_seen = chrono::Utc::now().timestamp();
            node.status = NodeStatus::Healthy;
        }
    }

    /// Reset failure count
    async fn reset_failure_count(&self, node_id: &str) {
        let mut counts = self.failure_counts.write().await;
        counts.remove(node_id);
    }

    /// Increment failure count
    async fn increment_failure(&self, node_id: &str) -> u32 {
        let mut counts = self.failure_counts.write().await;
        let count = counts.entry(node_id.to_string()).or_insert(0);
        *count += 1;
        *count
    }

    /// Perform health check on a node
    pub async fn health_check(&self, address: &str) -> HealthCheck {
        let start = Instant::now();
        
        // In production: send HTTP health check request
        // let response = reqwest::get(&format!("{}/health", address)).await;
        
        let latency = start.elapsed().as_millis() as u64;
        let status = NodeStatus::Healthy; // Would depend on response

        HealthCheck {
            node_id: address.to_string(),
            status,
            latency_ms: latency,
            checks: vec![
                ServiceCheck { name: "storage".to_string(), healthy: true, message: None },
                ServiceCheck { name: "query_engine".to_string(), healthy: true, message: None },
                ServiceCheck { name: "replication".to_string(), healthy: true, message: None },
            ],
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    /// Get all healthy nodes
    pub async fn get_healthy_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|n| n.status == NodeStatus::Healthy)
            .cloned()
            .collect()
    }

    /// Get node by ID
    pub async fn get_node(&self, node_id: &str) -> Option<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    /// Get cluster state
    pub async fn get_cluster_state(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Start background health check loop
    pub fn start_health_check_loop(self: Arc<Self>) {
        let discovery = Arc::clone(&self);
        
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(discovery.heartbeat_interval_secs));
            
            loop {
                ticker.tick().await;
                
                let peers = discovery.get_peers().await;
                for peer in peers {
                    let health = discovery.health_check(&peer).await;
                    
                    if health.status != NodeStatus::Healthy {
                        let failures = discovery.increment_failure(&peer).await;
                        if failures >= discovery.failure_threshold {
                            warn!("Node {} marked as unhealthy after {} failures", peer, failures);
                            let mut nodes = discovery.nodes.write().await;
                            if let Some(node) = nodes.get_mut(&peer) {
                                node.status = NodeStatus::Unhealthy;
                            }
                        }
                    } else {
                        discovery.reset_failure_count(&peer).await;
                    }
                }
            }
        });
    }
}

impl Default for Discovery {
    fn default() -> Self {
        Self::new("node-1", 10)
    }
}
