//! Security Module - Authentication, Authorization, and Encryption
//!
//! Provides comprehensive security features:
//! - RBAC and ABAC access control
//! - Post-Quantum Cryptography (Kyber + Dilithium)
//! - Field-level encryption
//! - Differential privacy

pub mod rbac;
pub mod pqc;
pub mod encryption;
pub mod masking;
pub mod privacy;
pub mod mtls;
pub mod ad;
pub mod apikey;

pub use rbac::*;
pub use pqc::*;
pub use encryption::*;
pub use masking::*;
pub use privacy::*;
pub use mtls::*;
pub use ad::*;
pub use apikey::*;

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Central security manager
pub struct SecurityManager {
    pub rbac: Arc<RbacManager>,
    pub encryption: Arc<EncryptionManager>,
    pub privacy: Arc<DifferentialPrivacyEngine>,
    pub pqc: Arc<PqcProvider>,
    pub ad: Arc<AdProvider>,
    pub api_keys: Arc<ApiKeyManager>,
}

impl SecurityManager {
    pub fn new() -> Self {
        Self {
            rbac: Arc::new(RbacManager::new()),
            encryption: Arc::new(EncryptionManager::new()),
            privacy: Arc::new(DifferentialPrivacyEngine::new(1.0)), // epsilon=1.0
            pqc: Arc::new(PqcProvider::new()),
            ad: Arc::new(AdProvider::new(Arc::new(RwLock::new(crate::config::AdConfig::default())))), // Placeholder config
            api_keys: Arc::new(ApiKeyManager::new()),
        }
    }

    pub fn with_config(ad_config: Arc<RwLock<crate::config::AdConfig>>) -> Self {
         Self {
            rbac: Arc::new(RbacManager::new()),
            encryption: Arc::new(EncryptionManager::new()),
            privacy: Arc::new(DifferentialPrivacyEngine::new(1.0)), 
            pqc: Arc::new(PqcProvider::new()),
            ad: Arc::new(AdProvider::new(ad_config)),
            api_keys: Arc::new(ApiKeyManager::new()),
        }
    }

    /// Check if a user can access a resource
    pub async fn check_access(
        &self,
        user_id: &str,
        resource: &str,
        action: Action,
    ) -> bool {
        self.rbac.check_permission(user_id, resource, action).await
    }

    /// Encrypt a value for storage
    pub fn encrypt(&self, plaintext: &[u8], key_id: &str) -> Result<Vec<u8>, SecurityError> {
        self.encryption.encrypt(plaintext, key_id)
    }

    /// Decrypt a stored value
    pub fn decrypt(&self, ciphertext: &[u8], key_id: &str) -> Result<Vec<u8>, SecurityError> {
        self.encryption.decrypt(ciphertext, key_id)
    }

    /// Add noise for differential privacy
    pub fn add_dp_noise(&self, value: f64, sensitivity: f64) -> f64 {
        self.privacy.add_laplace_noise(value, sensitivity)
    }
}

impl Default for SecurityManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Security-related errors
#[derive(Debug, Clone)]
pub enum SecurityError {
    AccessDenied(String),
    EncryptionError(String),
    DecryptionError(String),
    KeyNotFound(String),
    InvalidToken(String),
    PrivacyBudgetExceeded,
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityError::AccessDenied(msg) => write!(f, "Access denied: {}", msg),
            SecurityError::EncryptionError(msg) => write!(f, "Encryption error: {}", msg),
            SecurityError::DecryptionError(msg) => write!(f, "Decryption error: {}", msg),
            SecurityError::KeyNotFound(key) => write!(f, "Key not found: {}", key),
            SecurityError::InvalidToken(msg) => write!(f, "Invalid token: {}", msg),
            SecurityError::PrivacyBudgetExceeded => write!(f, "Differential privacy budget exceeded"),
        }
    }
}

impl std::error::Error for SecurityError {}
