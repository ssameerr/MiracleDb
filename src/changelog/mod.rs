//! Changelog Module - Change data capture and tracking

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Change type
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Change record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub id: u64,
    pub table: String,
    pub change_type: ChangeType,
    pub old_data: Option<serde_json::Value>,
    pub new_data: Option<serde_json::Value>,
    pub changed_columns: Vec<String>,
    pub timestamp: i64,
    pub transaction_id: Option<String>,
    pub user_id: Option<String>,
    pub sequence_number: u64,
}

/// Change log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeLogEntry {
    pub lsn: u64, // Log sequence number
    pub timestamp: i64,
    pub changes: Vec<ChangeRecord>,
    pub commit: bool,
}

/// Change log
pub struct ChangeLog {
    entries: RwLock<Vec<ChangeLogEntry>>,
    sequence: std::sync::atomic::AtomicU64,
    max_entries: usize,
    subscribers: RwLock<HashMap<String, ChangeSubscriber>>,
}

/// Change subscriber
#[derive(Clone)]
pub struct ChangeSubscriber {
    pub id: String,
    pub tables: Option<Vec<String>>,
    pub change_types: Option<Vec<ChangeType>>,
    pub from_lsn: u64,
}

impl ChangeLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            sequence: std::sync::atomic::AtomicU64::new(1),
            max_entries,
            subscribers: RwLock::new(HashMap::new()),
        }
    }

    /// Append a change
    pub async fn append(&self, change: ChangeRecord) -> u64 {
        let lsn = self.sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let entry = ChangeLogEntry {
            lsn,
            timestamp: chrono::Utc::now().timestamp(),
            changes: vec![change],
            commit: true,
        };

        let mut entries = self.entries.write().await;
        entries.push(entry);

        // Trim old entries
        if entries.len() > self.max_entries {
            let drain_count = entries.len() - self.max_entries;
            entries.drain(..drain_count);
        }

        lsn
    }

    /// Append multiple changes as a transaction
    pub async fn append_batch(&self, changes: Vec<ChangeRecord>, transaction_id: &str) -> u64 {
        let lsn = self.sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let entry = ChangeLogEntry {
            lsn,
            timestamp: chrono::Utc::now().timestamp(),
            changes,
            commit: true,
        };

        let mut entries = self.entries.write().await;
        entries.push(entry);

        lsn
    }

    /// Get changes since LSN
    pub async fn get_changes(&self, from_lsn: u64) -> Vec<ChangeLogEntry> {
        let entries = self.entries.read().await;
        entries.iter()
            .filter(|e| e.lsn > from_lsn)
            .cloned()
            .collect()
    }

    /// Get changes for table
    pub async fn get_table_changes(&self, table: &str, from_lsn: u64) -> Vec<ChangeRecord> {
        let entries = self.entries.read().await;
        entries.iter()
            .filter(|e| e.lsn > from_lsn)
            .flat_map(|e| e.changes.iter())
            .filter(|c| c.table == table)
            .cloned()
            .collect()
    }

    /// Subscribe to changes
    pub async fn subscribe(&self, subscriber: ChangeSubscriber) {
        let mut subscribers = self.subscribers.write().await;
        subscribers.insert(subscriber.id.clone(), subscriber);
    }

    /// Unsubscribe
    pub async fn unsubscribe(&self, subscriber_id: &str) {
        let mut subscribers = self.subscribers.write().await;
        subscribers.remove(subscriber_id);
    }

    /// Get subscriber's pending changes
    pub async fn poll(&self, subscriber_id: &str) -> Vec<ChangeRecord> {
        let subscribers = self.subscribers.read().await;
        let subscriber = match subscribers.get(subscriber_id) {
            Some(s) => s.clone(),
            None => return vec![],
        };
        drop(subscribers);

        let entries = self.entries.read().await;
        let changes: Vec<ChangeRecord> = entries.iter()
            .filter(|e| e.lsn > subscriber.from_lsn)
            .flat_map(|e| e.changes.iter())
            .filter(|c| {
                // Filter by tables
                if let Some(ref tables) = subscriber.tables {
                    if !tables.contains(&c.table) {
                        return false;
                    }
                }
                // Filter by change types
                if let Some(ref types) = subscriber.change_types {
                    if !types.contains(&c.change_type) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        changes
    }

    /// Acknowledge changes up to LSN
    pub async fn acknowledge(&self, subscriber_id: &str, lsn: u64) {
        let mut subscribers = self.subscribers.write().await;
        if let Some(subscriber) = subscribers.get_mut(subscriber_id) {
            subscriber.from_lsn = lsn;
        }
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        self.sequence.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Create insert change
    pub fn insert(table: &str, data: serde_json::Value) -> ChangeRecord {
        ChangeRecord {
            id: 0,
            table: table.to_string(),
            change_type: ChangeType::Insert,
            old_data: None,
            new_data: Some(data),
            changed_columns: vec![],
            timestamp: chrono::Utc::now().timestamp(),
            transaction_id: None,
            user_id: None,
            sequence_number: 0,
        }
    }

    /// Create update change
    pub fn update(table: &str, old: serde_json::Value, new: serde_json::Value, columns: Vec<String>) -> ChangeRecord {
        ChangeRecord {
            id: 0,
            table: table.to_string(),
            change_type: ChangeType::Update,
            old_data: Some(old),
            new_data: Some(new),
            changed_columns: columns,
            timestamp: chrono::Utc::now().timestamp(),
            transaction_id: None,
            user_id: None,
            sequence_number: 0,
        }
    }

    /// Create delete change
    pub fn delete(table: &str, data: serde_json::Value) -> ChangeRecord {
        ChangeRecord {
            id: 0,
            table: table.to_string(),
            change_type: ChangeType::Delete,
            old_data: Some(data),
            new_data: None,
            changed_columns: vec![],
            timestamp: chrono::Utc::now().timestamp(),
            transaction_id: None,
            user_id: None,
            sequence_number: 0,
        }
    }
}

impl Default for ChangeLog {
    fn default() -> Self {
        Self::new(100000)
    }
}

/// Data Branch (Git-like)
#[derive(Clone, Debug)]
pub struct Branch {
    pub name: String,
    pub parent: Option<String>,
    pub lsn_at_branch: u64,
}

pub struct VersionControl {
    branches: RwLock<HashMap<String, Branch>>,
}

impl VersionControl {
    pub fn new() -> Self {
        let mut branches = HashMap::new();
        branches.insert("main".to_string(), Branch {
            name: "main".to_string(),
            parent: None,
            lsn_at_branch: 0,
        });
        Self {
            branches: RwLock::new(branches),
        }
    }

    pub async fn create_branch(&self, name: &str, parent: &str, current_lsn: u64) -> Result<(), String> {
        let mut branches = self.branches.write().await;
        if branches.contains_key(name) {
            return Err("Branch exists".to_string());
        }
        if !branches.contains_key(parent) {
            return Err("Parent branch does not exist".to_string());
        }
        
        branches.insert(name.to_string(), Branch {
            name: name.to_string(),
            parent: Some(parent.to_string()),
            lsn_at_branch: current_lsn,
        });
        Ok(())
    }
    
    pub async fn merge(&self, source: &str, target: &str) -> Result<(), String> {
        // In production: perform 3-way merge of data changes
        // Stub: Just verify existence
        let branches = self.branches.read().await;
        if !branches.contains_key(source) || !branches.contains_key(target) {
            Err("Branch not found".to_string())
        } else {
            Ok(())
        }
    }
}
