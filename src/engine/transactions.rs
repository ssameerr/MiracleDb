//! Transaction Manager - MVCC with time-travel support
//!
//! Provides multi-version concurrency control using Lance dataset versioning

use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use tokio::sync::RwLock;
use datafusion::error::{DataFusionError, Result};
use tracing::{info, warn};

/// Transaction isolation levels
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction state
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Aborted,
}

/// A single transaction
#[derive(Clone, Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub start_version: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub modified_tables: Vec<String>,
}

use std::sync::Arc;
use crate::wal::{WalManager, WalRecordType};

/// Manages all active transactions
pub struct TransactionManager {
    /// Next transaction ID
    next_tx_id: Arc<AtomicU64>,
    /// Current global version (for MVCC)
    current_version: Arc<AtomicU64>,
    /// Active transactions
    transactions: Arc<RwLock<HashMap<u64, Transaction>>>,
    /// Write-Ahead Log Manager
    wal_manager: Arc<WalManager>,
    /// Data directory for persistence
    data_dir: String,
}

impl TransactionManager {
    pub fn new(data_dir: &str) -> Self {
        let wal_path = std::path::PathBuf::from(data_dir).join("pg_wal");
        Self {
            next_tx_id: Arc::new(AtomicU64::new(1)),
            current_version: Arc::new(AtomicU64::new(1)),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            wal_manager: Arc::new(WalManager::new(wal_path)),
            data_dir: data_dir.to_string(),
        }
    }

    /// Begin a new transaction
    pub async fn begin(&self) -> Result<u64> {
        self.begin_with_isolation(IsolationLevel::ReadCommitted).await
    }

    /// Begin a new transaction with specific isolation level
    pub async fn begin_with_isolation(&self, isolation_level: IsolationLevel) -> Result<u64> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let start_version = self.current_version.load(Ordering::SeqCst);
        
        // Log Start Transaction
        self.wal_manager.write(WalRecordType::Xact, tx_id, b"START".to_vec()).await;

        let transaction = Transaction {
            id: tx_id,
            state: TransactionState::Active,
            isolation_level,
            start_version,
            started_at: chrono::Utc::now(),
            modified_tables: Vec::new(),
        };
        
        let mut txs = self.transactions.write().await;
        txs.insert(tx_id, transaction);
        
        info!("Transaction {} started at version {}", tx_id, start_version);
        Ok(tx_id)
    }

    /// Commit a transaction
    pub async fn commit(&self, tx_id: u64) -> Result<()> {
        let mut txs = self.transactions.write().await;
        
        let tx = txs.get_mut(&tx_id)
            .ok_or_else(|| DataFusionError::Execution(format!("Transaction {} not found", tx_id)))?;
        
        if tx.state != TransactionState::Active {
            return Err(DataFusionError::Execution(
                format!("Transaction {} is not active", tx_id)
            ));
        }
        
        // Check for conflicts (simplified - in production would check write sets)
        let new_version = self.current_version.fetch_add(1, Ordering::SeqCst) + 1;
        
        // Log Commit Record
        self.wal_manager.write(WalRecordType::Commit, tx_id, b"COMMIT".to_vec()).await;
        
        // Force Flush for Durability
        self.wal_manager.flush().await.map_err(|e| DataFusionError::Execution(format!("WAL flush failed: {}", e)))?;

        tx.state = TransactionState::Committed;
        
        info!("Transaction {} committed at version {}", tx_id, new_version);
        
        // Cleanup after short delay
        let tx_id_clone = tx_id;
        let txs_clone = self.transactions.clone(); // Arc clone, now valid!
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            let mut txs = txs_clone.write().await;
            txs.remove(&tx_id_clone);
        });
        
        Ok(())
    }

    /// Rollback a transaction
    pub async fn rollback(&self, tx_id: u64) -> Result<()> {
        let mut txs = self.transactions.write().await;
        
        let tx = txs.get_mut(&tx_id)
            .ok_or_else(|| DataFusionError::Execution(format!("Transaction {} not found", tx_id)))?;
        
        if tx.state != TransactionState::Active {
            return Err(DataFusionError::Execution(
                format!("Transaction {} is not active", tx_id)
            ));
        }
        
        // Log Abort Record
        self.wal_manager.write(WalRecordType::Abort, tx_id, b"ABORT".to_vec()).await;

        tx.state = TransactionState::RolledBack;
        warn!("Transaction {} rolled back", tx_id);
        
        // DO NOT remove from map yet. 
        // If we remove it, it falls out of active_xids and looks "Committed" to snapshot logic.
        // We must keep it as "invisible" (in active list) until global xmin advances past it (vacuum).
        
        Ok(())
    }

    /// Verify a transaction is active
    pub fn verify_active(&self, tx_id: u64) -> Result<()> {
        // For now, just check the ID is valid
        if tx_id == 0 {
            return Err(DataFusionError::Execution("Invalid transaction ID".to_string()));
        }
        Ok(())
    }

    /// Get transaction state
    pub async fn get_state(&self, tx_id: u64) -> Option<TransactionState> {
        let txs = self.transactions.read().await;
        txs.get(&tx_id).map(|tx| tx.state.clone())
    }

    /// Get current version for time-travel queries
    pub fn current_version(&self) -> u64 {
        self.current_version.load(Ordering::SeqCst)
    }

    /// Get the snapshot version for a transaction ID
    pub async fn get_snapshot_version(&self, tx_id: u64) -> Option<u64> {
        let txs = self.transactions.read().await;
        txs.get(&tx_id).map(|tx| tx.start_version)
    }

    /// Record a table modification in a transaction
    pub async fn record_modification(&self, tx_id: u64, table: &str) -> Result<()> {
        let mut txs = self.transactions.write().await;
        
        if let Some(tx) = txs.get_mut(&tx_id) {
            if !tx.modified_tables.contains(&table.to_string()) {
                tx.modified_tables.push(table.to_string());
            }
        }
        
        Ok(())
    }

    /// Get all active transaction IDs (including aborted ones that must be hidden)
    pub async fn active_transactions(&self) -> Vec<u64> {
        let txs = self.transactions.read().await;
        txs.iter()
            .filter(|(_, tx)| tx.state == TransactionState::Active || tx.state == TransactionState::RolledBack || tx.state == TransactionState::Aborted)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Recover state from WAL
    pub async fn recover(&self) -> Result<()> {
        info!("Starting transaction recovery from WAL");
        let records = self.wal_manager.replay().await.map_err(|e| DataFusionError::Execution(e))?;
        
        let mut txs = self.transactions.write().await;
        let mut max_xid = 0;
        
        for record in records {
            if record.xid > max_xid {
                max_xid = record.xid;
            }
            
            match record.record_type {
                WalRecordType::Xact => {
                    txs.insert(record.xid, Transaction {
                        id: record.xid,
                        state: TransactionState::Active,
                        isolation_level: IsolationLevel::ReadCommitted,
                        start_version: 0,
                        started_at: chrono::DateTime::from_timestamp(record.timestamp, 0).unwrap_or(chrono::Utc::now()),
                        modified_tables: Vec::new(),
                    });
                },
                WalRecordType::Commit => {
                    // Committed can be removed or marked?
                    // For consistency with memory logic, we can remove them if we assume safe.
                    // But let's mark them Committed.
                    if let Some(tx) = txs.get_mut(&record.xid) {
                        tx.state = TransactionState::Committed;
                    }
                },
                WalRecordType::Abort => {
                    if let Some(tx) = txs.get_mut(&record.xid) {
                        tx.state = TransactionState::RolledBack;
                    }
                },
                _ => {}
            }
        }
        
        self.next_tx_id.store(max_xid + 1, Ordering::SeqCst);
        info!("Recovery complete. Next XID: {}", max_xid + 1);
        Ok(())
    }

    /// Create a MVCC Snapshot
    pub async fn create_snapshot(&self) -> TransactionSnapshot {
        let txs = self.transactions.read().await;
        let current_xid = self.next_tx_id.load(Ordering::SeqCst);
        
        let mut active_xids = Vec::new();
        let mut xmin = current_xid;
        let xmax = current_xid;
        
        for (&id, tx) in txs.iter() {
            // Include Active AND Aborted/RolledBack in "active list" so they are excluded from visibility
            if tx.state == TransactionState::Active || tx.state == TransactionState::RolledBack || tx.state == TransactionState::Aborted {
                active_xids.push(id);
                if id < xmin {
                    xmin = id;
                }
            }
        }
        
        active_xids.sort();
        
        TransactionSnapshot {
            xmin,
            xmax,
            active_xids,
        }
    }
    
    /// Check if a tuple xmin/xmax is visible to generic snapshot
    pub fn check_visibility(snapshot: &TransactionSnapshot, t_xmin: u64, t_xmax: u64) -> bool {
        // 1. Check xmin (creator)
        if !Self::is_xid_visible(snapshot, t_xmin) {
            return false;
        }
        
        // 2. Check xmax (deleter)
        if t_xmax == 0 {
            return true; // Not deleted
        }
        
        if Self::is_xid_visible(snapshot, t_xmax) {
            return false; // Deleted by a visible transaction
        }
        
        true // Deleted by invisible transaction (e.g. in progress or future), so version is still visible
    }
    
    fn is_xid_visible(snapshot: &TransactionSnapshot, xid: u64) -> bool {
        if xid < snapshot.xmin {
            return true; // Too old, must be committed (simplified)
        }
        if xid >= snapshot.xmax {
            return false; // Future transaction
        }
        if snapshot.active_xids.contains(&xid) {
            return false; // Still active in snapshot
        }
        true // In range [xmin, xmax) and not active -> Committed
    }
}

/// MVCC Snapshot data
#[derive(Clone, Debug)]
pub struct TransactionSnapshot {
    /// Earliest active XID
    pub xmin: u64,
    /// First not-yet-assigned XID
    pub xmax: u64,
    /// List of active XIDs at snapshot time
    pub active_xids: Vec<u64>,
}

impl Clone for TransactionManager {
    fn clone(&self) -> Self {
        Self {
            next_tx_id: self.next_tx_id.clone(),
            current_version: self.current_version.clone(),
            transactions: self.transactions.clone(),
            wal_manager: self.wal_manager.clone(),
            data_dir: self.data_dir.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let dir = tempdir().unwrap();
        let mgr = TransactionManager::new(dir.path().to_str().unwrap());
        
        let tx_id = mgr.begin().await.unwrap();
        assert!(tx_id > 0);
        
        let state = mgr.get_state(tx_id).await;
        assert_eq!(state, Some(TransactionState::Active));
        
        mgr.commit(tx_id).await.unwrap();
        
        let state = mgr.get_state(tx_id).await;
        assert_eq!(state, Some(TransactionState::Committed));
    }

    #[tokio::test]
    async fn test_rollback() {
        let dir = tempdir().unwrap();
        let mgr = TransactionManager::new(dir.path().to_str().unwrap());
        
        let tx_id = mgr.begin().await.unwrap();
        mgr.rollback(tx_id).await.unwrap();
        
        let state = mgr.get_state(tx_id).await;
        // RolledBack transactions are kept in the map for MVCC visibility tracking
        assert_eq!(state, Some(TransactionState::RolledBack));
    }
}
