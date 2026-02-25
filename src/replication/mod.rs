//! Replication Module - Primary-replica and multi-master replication

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Replication node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaNode {
    pub id: String,
    pub endpoint: String,
    pub role: NodeRole,
    pub status: NodeStatus,
    pub last_sync: Option<i64>,
    pub lag_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeRole {
    Primary,
    Replica,
    Witness,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Online,
    Syncing,
    Offline,
    Failed,
}

/// Write-ahead log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WALEntry {
    pub lsn: u64, // Log Sequence Number
    pub timestamp: i64,
    pub operation: String,
    pub data: Vec<u8>,
    pub checksum: u32,
}

/// Replication manager
pub struct ReplicationManager {
    node_id: String,
    role: RwLock<NodeRole>,
    replicas: RwLock<HashMap<String, ReplicaNode>>,
    wal: RwLock<Vec<WALEntry>>,
    current_lsn: RwLock<u64>,
    committed_lsn: RwLock<u64>,
}

impl ReplicationManager {
    pub fn new(node_id: &str, role: NodeRole) -> Self {
        Self {
            node_id: node_id.to_string(),
            role: RwLock::new(role),
            replicas: RwLock::new(HashMap::new()),
            wal: RwLock::new(Vec::new()),
            current_lsn: RwLock::new(0),
            committed_lsn: RwLock::new(0),
        }
    }

    /// Get current node role
    pub async fn get_role(&self) -> NodeRole {
        *self.role.read().await
    }

    /// Set node role (for failover)
    pub async fn set_role(&self, role: NodeRole) {
        let mut r = self.role.write().await;
        *r = role;
    }

    /// Register a replica node
    pub async fn add_replica(&self, node: ReplicaNode) {
        let mut replicas = self.replicas.write().await;
        replicas.insert(node.id.clone(), node);
    }

    /// Remove a replica
    pub async fn remove_replica(&self, node_id: &str) {
        let mut replicas = self.replicas.write().await;
        replicas.remove(node_id);
    }

    /// Append to WAL
    pub async fn append_wal(&self, operation: &str, data: Vec<u8>) -> u64 {
        let mut lsn = self.current_lsn.write().await;
        *lsn += 1;
        let new_lsn = *lsn;
        drop(lsn);

        let checksum = crc32fast::hash(&data);

        let entry = WALEntry {
            lsn: new_lsn,
            timestamp: chrono::Utc::now().timestamp(),
            operation: operation.to_string(),
            data,
            checksum,
        };

        let mut wal = self.wal.write().await;
        wal.push(entry);

        // Trim old entries
        if wal.len() > 100000 {
            wal.drain(0..10000);
        }

        new_lsn
    }

    /// Get WAL entries since LSN
    pub async fn get_wal_since(&self, since_lsn: u64) -> Vec<WALEntry> {
        let wal = self.wal.read().await;
        wal.iter()
            .filter(|e| e.lsn > since_lsn)
            .cloned()
            .collect()
    }

    /// Apply WAL entries (on replica)
    pub async fn apply_wal(&self, entries: Vec<WALEntry>) -> Result<u64, String> {
        let role = self.get_role().await;
        if role != NodeRole::Replica {
            return Err("Can only apply WAL on replica".to_string());
        }

        let mut max_lsn = 0u64;

        for entry in entries {
            // Verify checksum
            let computed = crc32fast::hash(&entry.data);
            if computed != entry.checksum {
                return Err(format!("Checksum mismatch for LSN {}", entry.lsn));
            }

            // Apply operation (in production: would execute SQL)
            max_lsn = max_lsn.max(entry.lsn);
        }

        // Update current LSN
        let mut lsn = self.current_lsn.write().await;
        *lsn = max_lsn;

        Ok(max_lsn)
    }

    /// Commit up to LSN
    pub async fn commit(&self, lsn: u64) {
        let mut committed = self.committed_lsn.write().await;
        *committed = lsn;
    }

    /// Get replication lag for all replicas
    pub async fn get_replication_lag(&self) -> HashMap<String, u64> {
        let current = *self.current_lsn.read().await;
        let replicas = self.replicas.read().await;
        
        replicas.iter()
            .map(|(id, node)| (id.clone(), current.saturating_sub(node.lag_bytes)))
            .collect()
    }

    /// Update replica status
    pub async fn update_replica_status(&self, node_id: &str, status: NodeStatus, lag: u64) {
        let mut replicas = self.replicas.write().await;
        if let Some(node) = replicas.get_mut(node_id) {
            node.status = status;
            node.lag_bytes = lag;
            node.last_sync = Some(chrono::Utc::now().timestamp());
        }
    }

    /// Promote replica to primary (failover)
    pub async fn promote(&self) -> Result<(), String> {
        let role = self.get_role().await;
        if role != NodeRole::Replica {
            return Err("Only replicas can be promoted".to_string());
        }

        self.set_role(NodeRole::Primary).await;
        
        // Would notify other nodes of new primary
        Ok(())
    }

    /// Get all replicas
    pub async fn list_replicas(&self) -> Vec<ReplicaNode> {
        let replicas = self.replicas.read().await;
        replicas.values().cloned().collect()
    }

    /// Get current LSN
    pub async fn get_current_lsn(&self) -> u64 {
        *self.current_lsn.read().await
    }

    /// Get committed LSN
    pub async fn get_committed_lsn(&self) -> u64 {
        *self.committed_lsn.read().await
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new("node-1", NodeRole::Primary)
    }
}
