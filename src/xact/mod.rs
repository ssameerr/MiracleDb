//! Xact Module - Transaction management internals

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Transaction state
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum XactState {
    InProgress,
    Committed,
    Aborted,
    SubCommitted,
    Prepared,
}

/// Transaction info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionInfo {
    pub xid: u64,
    pub state: XactState,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub database_id: u64,
    pub user_id: String,
    pub is_read_only: bool,
    pub isolation_level: IsolationLevel,
    pub parent_xid: Option<u64>,
    pub savepoints: Vec<Savepoint>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Savepoint
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Savepoint {
    pub name: String,
    pub sub_xid: u64,
    pub created_at: i64,
}

/// Transaction statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct XactStats {
    pub xact_commit: u64,
    pub xact_rollback: u64,
    pub blks_read: u64,
    pub blks_hit: u64,
    pub tup_returned: u64,
    pub tup_fetched: u64,
    pub tup_inserted: u64,
    pub tup_updated: u64,
    pub tup_deleted: u64,
}

/// Transaction manager
pub struct XactManager {
    next_xid: AtomicU64,
    oldest_xmin: AtomicU64,
    transactions: RwLock<HashMap<u64, TransactionInfo>>,
    active_xids: RwLock<HashSet<u64>>,
    stats: RwLock<XactStats>,
}

impl XactManager {
    pub fn new() -> Self {
        Self {
            next_xid: AtomicU64::new(3), // Reserved XIDs: 0, 1, 2
            oldest_xmin: AtomicU64::new(3),
            transactions: RwLock::new(HashMap::new()),
            active_xids: RwLock::new(HashSet::new()),
            stats: RwLock::new(XactStats::default()),
        }
    }

    /// Start a new transaction
    pub async fn begin(&self, user_id: &str, database_id: u64, isolation: IsolationLevel) -> u64 {
        let xid = self.next_xid.fetch_add(1, Ordering::SeqCst);

        let info = TransactionInfo {
            xid,
            state: XactState::InProgress,
            start_time: chrono::Utc::now().timestamp(),
            end_time: None,
            database_id,
            user_id: user_id.to_string(),
            is_read_only: false,
            isolation_level: isolation,
            parent_xid: None,
            savepoints: vec![],
        };

        let mut transactions = self.transactions.write().await;
        let mut active = self.active_xids.write().await;

        transactions.insert(xid, info);
        active.insert(xid);

        xid
    }

    /// Commit a transaction
    pub async fn commit(&self, xid: u64) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        let mut active = self.active_xids.write().await;

        let info = transactions.get_mut(&xid)
            .ok_or_else(|| format!("Transaction {} not found", xid))?;

        if info.state != XactState::InProgress {
            return Err(format!("Transaction {} is not in progress", xid));
        }

        info.state = XactState::Committed;
        info.end_time = Some(chrono::Utc::now().timestamp());
        active.remove(&xid);

        // Update oldest xmin
        self.update_oldest_xmin().await;

        let mut stats = self.stats.write().await;
        stats.xact_commit += 1;

        Ok(())
    }

    /// Rollback a transaction
    pub async fn rollback(&self, xid: u64) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        let mut active = self.active_xids.write().await;

        let info = transactions.get_mut(&xid)
            .ok_or_else(|| format!("Transaction {} not found", xid))?;

        if info.state != XactState::InProgress {
            return Err(format!("Transaction {} is not in progress", xid));
        }

        info.state = XactState::Aborted;
        info.end_time = Some(chrono::Utc::now().timestamp());
        active.remove(&xid);

        self.update_oldest_xmin().await;

        let mut stats = self.stats.write().await;
        stats.xact_rollback += 1;

        Ok(())
    }

    /// Create a savepoint
    pub async fn create_savepoint(&self, xid: u64, name: &str) -> Result<u64, String> {
        let sub_xid = self.next_xid.fetch_add(1, Ordering::SeqCst);

        let mut transactions = self.transactions.write().await;
        let info = transactions.get_mut(&xid)
            .ok_or_else(|| format!("Transaction {} not found", xid))?;

        info.savepoints.push(Savepoint {
            name: name.to_string(),
            sub_xid,
            created_at: chrono::Utc::now().timestamp(),
        });

        Ok(sub_xid)
    }

    /// Rollback to savepoint
    pub async fn rollback_to_savepoint(&self, xid: u64, name: &str) -> Result<(), String> {
        let mut transactions = self.transactions.write().await;
        let info = transactions.get_mut(&xid)
            .ok_or_else(|| format!("Transaction {} not found", xid))?;

        let pos = info.savepoints.iter().position(|s| s.name == name)
            .ok_or_else(|| format!("Savepoint {} not found", name))?;

        info.savepoints.truncate(pos + 1);
        Ok(())
    }

    async fn update_oldest_xmin(&self) {
        let active = self.active_xids.read().await;
        if let Some(&min) = active.iter().min() {
            self.oldest_xmin.store(min, Ordering::SeqCst);
        }
    }

    /// Get oldest xmin
    pub fn get_oldest_xmin(&self) -> u64 {
        self.oldest_xmin.load(Ordering::SeqCst)
    }

    /// Get active transactions
    pub async fn get_active(&self) -> Vec<TransactionInfo> {
        let transactions = self.transactions.read().await;
        let active = self.active_xids.read().await;

        active.iter()
            .filter_map(|xid| transactions.get(xid).cloned())
            .collect()
    }

    /// Get transaction state
    pub async fn get_state(&self, xid: u64) -> Option<XactState> {
        let transactions = self.transactions.read().await;
        transactions.get(&xid).map(|t| t.state)
    }

    /// Get statistics
    pub async fn get_stats(&self) -> XactStats {
        self.stats.read().await.clone()
    }
}

impl Default for XactManager {
    fn default() -> Self {
        Self::new()
    }
}
