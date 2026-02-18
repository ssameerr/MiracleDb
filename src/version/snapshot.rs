use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub tag: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub tables: HashMap<String, Vec<serde_json::Value>>, // table → rows at this snapshot
    pub parent_id: Option<String>,
}

pub struct SnapshotStore {
    snapshots: Arc<RwLock<Vec<Snapshot>>>,
}

impl SnapshotStore {
    pub fn new() -> Self {
        Self { snapshots: Arc::new(RwLock::new(vec![])) }
    }

    pub fn create_snapshot(&self, tag: Option<&str>, tables: HashMap<String, Vec<serde_json::Value>>) -> Snapshot {
        let mut snaps = self.snapshots.write().unwrap();
        let parent_id = snaps.last().map(|s| s.id.clone());
        let snap = Snapshot {
            id: uuid::Uuid::new_v4().to_string(),
            tag: tag.map(|t| t.to_string()),
            timestamp: Utc::now(),
            tables,
            parent_id,
        };
        snaps.push(snap.clone());
        snap
    }

    /// Get snapshot at or before a given timestamp (AS OF query support)
    pub fn as_of(&self, ts: &DateTime<Utc>) -> Option<Snapshot> {
        self.snapshots.read().unwrap()
            .iter()
            .filter(|s| &s.timestamp <= ts)
            .last()
            .cloned()
    }

    /// Get snapshot by tag name
    pub fn by_tag(&self, tag: &str) -> Option<Snapshot> {
        self.snapshots.read().unwrap()
            .iter()
            .find(|s| s.tag.as_deref() == Some(tag))
            .cloned()
    }

    pub fn list(&self) -> Vec<Snapshot> {
        self.snapshots.read().unwrap().clone()
    }

    /// Garbage collect snapshots older than retention period
    pub fn gc(&self, retain_last_n: usize) {
        let mut snaps = self.snapshots.write().unwrap();
        if snaps.len() > retain_last_n {
            let drain_until = snaps.len() - retain_last_n;
            snaps.drain(..drain_until);
        }
    }
}
