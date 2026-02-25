//! Monitor Module - Database monitoring and diagnostics

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Database status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatabaseStatus {
    pub state: DatabaseState,
    pub uptime_seconds: u64,
    pub connections: ConnectionStats,
    pub queries: QueryStats,
    pub storage: StorageStats,
    pub memory: MemoryStats,
    pub replication: Option<ReplicationStats>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum DatabaseState {
    Starting,
    Running,
    Standby,
    Recovery,
    Maintenance,
    ShuttingDown,
}

/// Connection statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConnectionStats {
    pub active: u32,
    pub idle: u32,
    pub waiting: u32,
    pub max: u32,
    pub total_connections: u64,
}

/// Query statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryStats {
    pub queries_per_second: f64,
    pub active_queries: u32,
    pub total_queries: u64,
    pub slow_queries: u64,
    pub avg_query_time_ms: f64,
    pub cache_hit_ratio: f64,
}

/// Storage statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageStats {
    pub data_size_bytes: u64,
    pub index_size_bytes: u64,
    pub temp_size_bytes: u64,
    pub wal_size_bytes: u64,
    pub total_size_bytes: u64,
    pub tables_count: u32,
    pub indexes_count: u32,
}

/// Memory statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryStats {
    pub used_bytes: u64,
    pub shared_buffers_bytes: u64,
    pub cache_bytes: u64,
    pub connections_bytes: u64,
    pub total_bytes: u64,
}

/// Replication statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationStats {
    pub role: ReplicationRole,
    pub replicas: Vec<ReplicaInfo>,
    pub lag_bytes: u64,
    pub lag_seconds: f64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ReplicationRole {
    Primary,
    Replica,
    Standalone,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub name: String,
    pub state: String,
    pub lag_bytes: u64,
    pub last_sync: i64,
}

/// Active query info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveQuery {
    pub id: String,
    pub user: String,
    pub database: String,
    pub query: String,
    pub state: QueryState,
    pub started_at: i64,
    pub duration_ms: u64,
    pub waiting: bool,
    pub blocked_by: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum QueryState {
    Running,
    Idle,
    IdleInTransaction,
    Waiting,
    Blocked,
}

/// Lock info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockInfo {
    pub id: String,
    pub lock_type: String,
    pub mode: String,
    pub granted: bool,
    pub holder: Option<String>,
    pub waiting: Vec<String>,
    pub object_type: String,
    pub object_name: String,
}

/// Database monitor
pub struct DatabaseMonitor {
    start_time: std::time::Instant,
    state: RwLock<DatabaseState>,
    connection_stats: RwLock<ConnectionStats>,
    query_stats: RwLock<QueryStats>,
    storage_stats: RwLock<StorageStats>,
    memory_stats: RwLock<MemoryStats>,
    active_queries: RwLock<HashMap<String, ActiveQuery>>,
    locks: RwLock<HashMap<String, LockInfo>>,
}

impl DatabaseMonitor {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            state: RwLock::new(DatabaseState::Starting),
            connection_stats: RwLock::new(ConnectionStats::default()),
            query_stats: RwLock::new(QueryStats::default()),
            storage_stats: RwLock::new(StorageStats::default()),
            memory_stats: RwLock::new(MemoryStats::default()),
            active_queries: RwLock::new(HashMap::new()),
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Set database state
    pub async fn set_state(&self, state: DatabaseState) {
        let mut s = self.state.write().await;
        *s = state;
    }

    /// Get database status
    pub async fn get_status(&self) -> DatabaseStatus {
        let state = *self.state.read().await;
        let connections = self.connection_stats.read().await.clone();
        let queries = self.query_stats.read().await.clone();
        let storage = self.storage_stats.read().await.clone();
        let memory = self.memory_stats.read().await.clone();

        DatabaseStatus {
            state,
            uptime_seconds: self.start_time.elapsed().as_secs(),
            connections,
            queries,
            storage,
            memory,
            replication: None,
        }
    }

    /// Record query start
    pub async fn query_start(&self, id: &str, user: &str, database: &str, query: &str) {
        let mut queries = self.active_queries.write().await;
        queries.insert(id.to_string(), ActiveQuery {
            id: id.to_string(),
            user: user.to_string(),
            database: database.to_string(),
            query: query.to_string(),
            state: QueryState::Running,
            started_at: chrono::Utc::now().timestamp(),
            duration_ms: 0,
            waiting: false,
            blocked_by: None,
        });

        let mut stats = self.query_stats.write().await;
        stats.active_queries += 1;
    }

    /// Record query end
    pub async fn query_end(&self, id: &str, duration_ms: u64) {
        let mut queries = self.active_queries.write().await;
        queries.remove(id);

        let mut stats = self.query_stats.write().await;
        stats.active_queries = stats.active_queries.saturating_sub(1);
        stats.total_queries += 1;
        
        if duration_ms > 1000 {
            stats.slow_queries += 1;
        }
    }

    /// Get active queries
    pub async fn get_active_queries(&self) -> Vec<ActiveQuery> {
        let queries = self.active_queries.read().await;
        queries.values().cloned().collect()
    }

    /// Connection opened
    pub async fn connection_opened(&self) {
        let mut stats = self.connection_stats.write().await;
        stats.active += 1;
        stats.total_connections += 1;
    }

    /// Connection closed
    pub async fn connection_closed(&self) {
        let mut stats = self.connection_stats.write().await;
        stats.active = stats.active.saturating_sub(1);
    }

    /// Update storage stats
    pub async fn update_storage(&self, storage: StorageStats) {
        let mut s = self.storage_stats.write().await;
        *s = storage;
    }

    /// Update memory stats
    pub async fn update_memory(&self, memory: MemoryStats) {
        let mut m = self.memory_stats.write().await;
        *m = memory;
    }

    /// Register lock
    pub async fn register_lock(&self, lock: LockInfo) {
        let mut locks = self.locks.write().await;
        locks.insert(lock.id.clone(), lock);
    }

    /// Release lock
    pub async fn release_lock(&self, id: &str) {
        let mut locks = self.locks.write().await;
        locks.remove(id);
    }

    /// Get locks
    pub async fn get_locks(&self) -> Vec<LockInfo> {
        let locks = self.locks.read().await;
        locks.values().cloned().collect()
    }
}

impl Default for DatabaseMonitor {
    fn default() -> Self {
        Self::new()
    }
}
