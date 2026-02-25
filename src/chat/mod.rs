use std::sync::Arc;
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum UserStatus {
    Online,
    Away,
    Offline,
    Busy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPresence {
    pub user_id: String,
    pub status: UserStatus,
    pub last_seen_ts: u64,
}

pub struct PresenceStore {
    presence: Arc<DashMap<String, UserPresence>>,
    ttl_seconds: u64,
}

impl PresenceStore {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            presence: Arc::new(DashMap::new()),
            ttl_seconds,
        }
    }

    pub fn update_status(&self, user_id: &str, status: UserStatus) {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.presence.insert(user_id.to_string(), UserPresence {
            user_id: user_id.to_string(),
            status,
            last_seen_ts: ts,
        });
    }

    pub fn get_status(&self, user_id: &str) -> Option<UserPresence> {
        let presence = self.presence.get(user_id).map(|p| p.clone());
        if let Some(p) = presence {
             let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
             if ts - p.last_seen_ts > self.ttl_seconds {
                 // Expired
                 self.presence.remove(user_id);
                 return None;
             }
             return Some(p);
        }
        None
    }
    
    // Stub for pub/sub message (Phase 13 feature)
    pub async fn publish_message(&self, _channel: &str, _message: &str) {
        // Integrate with Redis or internal channel
    }
}
