//! Subscription Module - Logical replication subscriptions

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::crypto::Crypto;

/// Subscription definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscription {
    pub name: String,
    pub owner: String,
    pub connection_info: ConnectionInfo,
    pub publications: Vec<String>,
    pub enabled: bool,
    pub slot_name: Option<String>,
    pub synchronous_commit: SyncCommit,
    pub binary: bool,
    pub streaming: StreamingMode,
    pub created_at: i64,
    pub state: SubscriptionState,
}

/// Connection info with encrypted password storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    /// Password stored as encrypted blob (AES-256-GCM)
    /// Format: base64(nonce || ciphertext || tag)
    pub encrypted_password: Option<String>,
    pub ssl_mode: SslMode,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum SslMode {
    Disable,
    Allow,
    #[default]
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum SyncCommit {
    Off,
    #[default]
    On,
    Remote,
    RemoteWrite,
    RemoteApply,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum StreamingMode {
    #[default]
    Off,
    On,
    Parallel,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum SubscriptionState {
    #[default]
    Initializing,
    Ready,
    CopyData,
    Syncing,
    Streaming,
    Disabled,
    Error,
}

impl Subscription {
    pub fn new(name: &str, conn_info: ConnectionInfo, publications: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            owner: "postgres".to_string(),
            connection_info: conn_info,
            publications,
            enabled: true,
            slot_name: None,
            synchronous_commit: SyncCommit::default(),
            binary: false,
            streaming: StreamingMode::default(),
            created_at: chrono::Utc::now().timestamp(),
            state: SubscriptionState::default(),
        }
    }

    pub fn with_slot(mut self, slot: &str) -> Self {
        self.slot_name = Some(slot.to_string());
        self
    }

    pub fn with_binary(mut self) -> Self {
        self.binary = true;
        self
    }

    pub fn with_streaming(mut self, mode: StreamingMode) -> Self {
        self.streaming = mode;
        self
    }
}

impl ConnectionInfo {
    /// Encryption key for subscription passwords (derived from system secret)
    fn get_encryption_key() -> [u8; 32] {
        let system_secret = std::env::var("MIRACLEDB_SYSTEM_SECRET")
            .unwrap_or_else(|_| {
                // Generate a stable key from hostname for development
                // In production, MIRACLEDB_SYSTEM_SECRET must be set
                let hostname = std::env::var("HOSTNAME")
                    .or_else(|_| std::env::var("COMPUTERNAME"))
                    .unwrap_or_else(|_| "miracledb".to_string());
                format!("dev-key-{}-do-not-use-in-prod", hostname)
            });
        let key = Crypto::derive_key(system_secret.as_bytes(), b"subscription-password-key", 100_000, 32);
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key);
        arr
    }

    pub fn new(host: &str, port: u16, database: &str, user: &str) -> Self {
        Self {
            host: host.to_string(),
            port,
            database: database.to_string(),
            user: user.to_string(),
            encrypted_password: None,
            ssl_mode: SslMode::default(),
        }
    }

    /// Set password with encryption
    pub fn with_password(mut self, password: &str) -> Self {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        let key_bytes = Self::get_encryption_key();
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce_bytes = Crypto::random_bytes(12);
        let nonce = Nonce::from_slice(&nonce_bytes);

        match cipher.encrypt(nonce, password.as_bytes()) {
            Ok(ciphertext) => {
                // Format: nonce (12 bytes) || ciphertext
                let mut encrypted = nonce_bytes;
                encrypted.extend(ciphertext);
                self.encrypted_password = Some(Crypto::base64_encode(&encrypted));
            }
            Err(e) => {
                tracing::error!("Failed to encrypt password: {:?}", e);
            }
        }
        self
    }

    /// Decrypt and retrieve password
    fn decrypt_password(&self) -> Option<String> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        let encrypted = self.encrypted_password.as_ref()?;
        let encrypted_bytes = Crypto::base64_decode(encrypted).ok()?;

        if encrypted_bytes.len() < 12 {
            return None;
        }

        let (nonce_bytes, ciphertext) = encrypted_bytes.split_at(12);
        let key_bytes = Self::get_encryption_key();
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);

        cipher.decrypt(nonce, ciphertext)
            .ok()
            .and_then(|plaintext| String::from_utf8(plaintext).ok())
    }

    pub fn connection_string(&self) -> String {
        let mut conn = format!(
            "host={} port={} dbname={} user={}",
            self.host, self.port, self.database, self.user
        );
        if let Some(pass) = self.decrypt_password() {
            conn.push_str(&format!(" password={}", pass));
        }
        conn
    }
}

/// Subscription statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SubscriptionStats {
    pub received_lsn: String,
    pub latest_end_lsn: String,
    pub last_msg_send_time: Option<i64>,
    pub last_msg_receive_time: Option<i64>,
    pub apply_error_count: u64,
}

/// Subscription manager
pub struct SubscriptionManager {
    subscriptions: RwLock<HashMap<String, Subscription>>,
    stats: RwLock<HashMap<String, SubscriptionStats>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
        }
    }

    /// Create a subscription
    pub async fn create(&self, subscription: Subscription) -> Result<(), String> {
        let mut subscriptions = self.subscriptions.write().await;

        if subscriptions.contains_key(&subscription.name) {
            return Err(format!("Subscription {} already exists", subscription.name));
        }

        subscriptions.insert(subscription.name.clone(), subscription);
        Ok(())
    }

    /// Drop a subscription
    pub async fn drop(&self, name: &str) -> Result<(), String> {
        let mut subscriptions = self.subscriptions.write().await;
        let mut stats = self.stats.write().await;

        subscriptions.remove(name)
            .ok_or_else(|| format!("Subscription {} not found", name))?;
        stats.remove(name);
        Ok(())
    }

    /// Get a subscription
    pub async fn get(&self, name: &str) -> Option<Subscription> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.get(name).cloned()
    }

    /// List subscriptions
    pub async fn list(&self) -> Vec<Subscription> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.values().cloned().collect()
    }

    /// Enable/disable subscription
    pub async fn set_enabled(&self, name: &str, enabled: bool) -> Result<(), String> {
        let mut subscriptions = self.subscriptions.write().await;

        let sub = subscriptions.get_mut(name)
            .ok_or_else(|| format!("Subscription {} not found", name))?;

        sub.enabled = enabled;
        sub.state = if enabled { SubscriptionState::Ready } else { SubscriptionState::Disabled };
        Ok(())
    }

    /// Update state
    pub async fn update_state(&self, name: &str, state: SubscriptionState) {
        let mut subscriptions = self.subscriptions.write().await;
        if let Some(sub) = subscriptions.get_mut(name) {
            sub.state = state;
        }
    }

    /// Get statistics
    pub async fn get_stats(&self, name: &str) -> Option<SubscriptionStats> {
        let stats = self.stats.read().await;
        stats.get(name).cloned()
    }

    /// Refresh subscription
    pub async fn refresh(&self, name: &str, copy_data: bool) -> Result<(), String> {
        let mut subscriptions = self.subscriptions.write().await;

        let sub = subscriptions.get_mut(name)
            .ok_or_else(|| format!("Subscription {} not found", name))?;

        sub.state = if copy_data {
            SubscriptionState::CopyData
        } else {
            SubscriptionState::Syncing
        };

        Ok(())
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}
