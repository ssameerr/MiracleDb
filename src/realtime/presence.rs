use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

pub struct PresenceEntry {
    pub user_id: String,
    pub metadata: serde_json::Value,
    pub last_seen: Instant,
    pub ttl: Duration,
}

pub struct PresenceSystem {
    entries: Arc<RwLock<HashMap<String, PresenceEntry>>>,
}

impl PresenceSystem {
    pub fn new() -> Self {
        Self { entries: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn heartbeat(&self, user_id: &str, metadata: serde_json::Value, ttl_secs: u64) {
        let mut entries = self.entries.write().unwrap();
        entries.insert(user_id.to_string(), PresenceEntry {
            user_id: user_id.to_string(),
            metadata,
            last_seen: Instant::now(),
            ttl: Duration::from_secs(ttl_secs),
        });
    }

    pub fn online_users(&self) -> Vec<String> {
        let entries = self.entries.read().unwrap();
        entries.values()
            .filter(|e| e.last_seen.elapsed() < e.ttl)
            .map(|e| e.user_id.clone())
            .collect()
    }

    pub fn is_online(&self, user_id: &str) -> bool {
        self.entries.read().unwrap()
            .get(user_id)
            .map(|e| e.last_seen.elapsed() < e.ttl)
            .unwrap_or(false)
    }

    /// Remove expired entries
    pub fn evict_expired(&self) {
        self.entries.write().unwrap().retain(|_, e| e.last_seen.elapsed() < e.ttl);
    }
}
