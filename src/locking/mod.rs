//! Distributed Locking Module - Coordination primitives

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore, OwnedSemaphorePermit};
use serde::{Serialize, Deserialize};

/// Lock info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockInfo {
    pub key: String,
    pub owner: String,
    pub acquired_at: i64,
    pub expires_at: i64,
    pub fencing_token: u64,
}

/// Lock result
pub enum LockResult {
    Acquired(LockGuard),
    AlreadyLocked(LockInfo),
    Timeout,
}

/// Lock guard - releases lock when dropped
pub struct LockGuard {
    key: String,
    fencing_token: u64,
    manager: Arc<LockManager>,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let key = self.key.clone();
        let token = self.fencing_token;
        let manager = Arc::clone(&self.manager);
        
        tokio::spawn(async move {
            manager.release_internal(&key, token).await;
        });
    }
}

impl LockGuard {
    pub fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Distributed lock manager
pub struct LockManager {
    locks: Arc<RwLock<HashMap<String, LockInfo>>>,
    fencing_counter: std::sync::atomic::AtomicU64,
    default_ttl_seconds: u64,
}

impl LockManager {
    pub fn new(default_ttl_seconds: u64) -> Arc<Self> {
        let manager = Arc::new(Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
            fencing_counter: std::sync::atomic::AtomicU64::new(0),
            default_ttl_seconds,
        });

        // Start cleanup task
        let locks = Arc::clone(&manager.locks);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let now = chrono::Utc::now().timestamp();
                let mut locks_guard = locks.write().await;
                locks_guard.retain(|_, info| info.expires_at > now);
            }
        });

        manager
    }

    /// Try to acquire a lock
    pub async fn try_lock(self: &Arc<Self>, key: &str, owner: &str) -> LockResult {
        self.try_lock_with_ttl(key, owner, self.default_ttl_seconds).await
    }

    /// Try to acquire a lock with custom TTL
    pub async fn try_lock_with_ttl(self: &Arc<Self>, key: &str, owner: &str, ttl_seconds: u64) -> LockResult {
        let now = chrono::Utc::now().timestamp();
        let mut locks = self.locks.write().await;

        // Check if already locked
        if let Some(info) = locks.get(key) {
            if info.expires_at > now {
                return LockResult::AlreadyLocked(info.clone());
            }
        }

        // Acquire lock
        let fencing_token = self.fencing_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let info = LockInfo {
            key: key.to_string(),
            owner: owner.to_string(),
            acquired_at: now,
            expires_at: now + ttl_seconds as i64,
            fencing_token,
        };

        locks.insert(key.to_string(), info);

        LockResult::Acquired(LockGuard {
            key: key.to_string(),
            fencing_token,
            manager: Arc::clone(self),
        })
    }

    /// Try to acquire with timeout
    pub async fn lock_with_timeout(self: &Arc<Self>, key: &str, owner: &str, timeout: Duration) -> LockResult {
        let start = Instant::now();
        let poll_interval = Duration::from_millis(50);

        loop {
            match self.try_lock(key, owner).await {
                LockResult::Acquired(guard) => return LockResult::Acquired(guard),
                LockResult::AlreadyLocked(_) => {
                    if start.elapsed() >= timeout {
                        return LockResult::Timeout;
                    }
                    tokio::time::sleep(poll_interval).await;
                }
                LockResult::Timeout => return LockResult::Timeout,
            }
        }
    }

    /// Release a lock
    pub async fn release(&self, key: &str, owner: &str) -> bool {
        let mut locks = self.locks.write().await;
        if let Some(info) = locks.get(key) {
            if info.owner == owner {
                locks.remove(key);
                return true;
            }
        }
        false
    }

    /// Internal release by fencing token
    async fn release_internal(&self, key: &str, token: u64) {
        let mut locks = self.locks.write().await;
        if let Some(info) = locks.get(key) {
            if info.fencing_token == token {
                locks.remove(key);
            }
        }
    }

    /// Extend lock TTL
    pub async fn extend(&self, key: &str, owner: &str, additional_seconds: u64) -> bool {
        let mut locks = self.locks.write().await;
        if let Some(info) = locks.get_mut(key) {
            if info.owner == owner {
                info.expires_at += additional_seconds as i64;
                return true;
            }
        }
        false
    }

    /// Get lock info
    pub async fn get_info(&self, key: &str) -> Option<LockInfo> {
        let locks = self.locks.read().await;
        locks.get(key).cloned()
    }

    /// List all locks
    pub async fn list(&self) -> Vec<LockInfo> {
        let locks = self.locks.read().await;
        locks.values().cloned().collect()
    }
}

/// Read-write lock manager
pub struct RwLockManager {
    readers: RwLock<HashMap<String, Vec<String>>>,
    writers: RwLock<HashMap<String, String>>,
}

impl RwLockManager {
    pub fn new() -> Self {
        Self {
            readers: RwLock::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire read lock
    pub async fn acquire_read(&self, key: &str, owner: &str) -> bool {
        let writers = self.writers.read().await;
        if writers.contains_key(key) {
            return false;
        }
        drop(writers);

        let mut readers = self.readers.write().await;
        readers.entry(key.to_string())
            .or_insert_with(Vec::new)
            .push(owner.to_string());

        true
    }

    /// Acquire write lock
    pub async fn acquire_write(&self, key: &str, owner: &str) -> bool {
        let readers = self.readers.read().await;
        if readers.get(key).map(|r| !r.is_empty()).unwrap_or(false) {
            return false;
        }
        drop(readers);

        let mut writers = self.writers.write().await;
        if writers.contains_key(key) {
            return false;
        }

        writers.insert(key.to_string(), owner.to_string());
        true
    }

    /// Release read lock
    pub async fn release_read(&self, key: &str, owner: &str) {
        let mut readers = self.readers.write().await;
        if let Some(list) = readers.get_mut(key) {
            list.retain(|o| o != owner);
        }
    }

    /// Release write lock
    pub async fn release_write(&self, key: &str, owner: &str) {
        let mut writers = self.writers.write().await;
        if writers.get(key) == Some(&owner.to_string()) {
            writers.remove(key);
        }
    }
}

impl Default for RwLockManager {
    fn default() -> Self {
        Self::new()
    }
}
