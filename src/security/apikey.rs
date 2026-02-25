//! API Key Management
//!
//! Handles verification and management of API keys for programmatic access.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub struct ApiKey {
    pub key: String,
    pub user_id: String,
    pub roles: Vec<String>,
    pub Active: bool,
}

pub struct ApiKeyManager {
    keys: RwLock<HashMap<String, ApiKey>>,
}

impl ApiKeyManager {
    pub fn new() -> Self {
        let mut keys = HashMap::new();
        
        // Add a default test key for development
        // In production, keys would be loaded from DB/Vault
        keys.insert("test-key".to_string(), ApiKey {
            key: "test-key".to_string(),
            user_id: "api_user".to_string(),
            roles: vec!["admin".to_string()],
            Active: true,
        });

        Self {
            keys: RwLock::new(keys),
        }
    }

    pub async fn validate(&self, key: &str) -> Option<ApiKey> {
        let keys = self.keys.read().await;
        if let Some(api_key) = keys.get(key) {
            if api_key.Active {
                return Some(api_key.clone());
            }
        }
        None
    }

    pub async fn create_key(&self, user_id: &str, roles: Vec<String>) -> String {
        let key = uuid::Uuid::new_v4().to_string();
        let api_key = ApiKey {
            key: key.clone(),
            user_id: user_id.to_string(),
            roles,
            Active: true,
        };
        
        self.keys.write().await.insert(key.clone(), api_key);
        key
    }
    
    pub async fn revoke_key(&self, key: &str) -> bool {
        let mut keys = self.keys.write().await;
        if let Some(api_key) = keys.get_mut(key) {
            api_key.Active = false;
            return true;
        }
        false
    }
}

impl Default for ApiKeyManager {
    fn default() -> Self {
        Self::new()
    }
}
