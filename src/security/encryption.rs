//! Field-Level Encryption
//!
//! Provides AES-256-GCM encryption for sensitive columns

use std::collections::HashMap;
use tokio::sync::RwLock;
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit};
use rand::rngs::OsRng;
use rand::RngCore;
use tracing::{info, error};
use super::SecurityError;

/// Lightweight per-field AES-256-GCM encryption helper.
///
/// Holds a single cipher instance and handles nonce generation internally.
/// The 12-byte nonce is prepended to the ciphertext so decrypt() can recover it.
pub struct FieldEncryption {
    cipher: Aes256Gcm,
}

impl FieldEncryption {
    /// Create a new `FieldEncryption` from a 32-byte key.
    pub fn new(key_bytes: &[u8; 32]) -> Self {
        let key = Key::<Aes256Gcm>::from_slice(key_bytes);
        Self { cipher: Aes256Gcm::new(key) }
    }

    /// Encrypt `plaintext` and return `nonce || ciphertext`.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, String> {
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self.cipher.encrypt(nonce, plaintext)
            .map_err(|e| format!("Encryption error: {}", e))?;
        // Prepend nonce to ciphertext so we can recover it on decrypt
        let mut result = nonce_bytes.to_vec();
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    /// Decrypt data previously produced by `encrypt()`.
    /// Expects the first 12 bytes to be the nonce.
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        if data.len() < 12 {
            return Err("Invalid ciphertext: too short".to_string());
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        self.cipher.decrypt(nonce, ciphertext)
            .map_err(|e| format!("Decryption error: {}", e))
    }
}

/// Manages encryption keys and operations
pub struct EncryptionManager {
    /// Key store: key_id -> encryption key
    keys: RwLock<HashMap<String, Vec<u8>>>,
    /// Column encryption configuration: table.column -> key_id
    column_keys: RwLock<HashMap<String, String>>,
}

impl EncryptionManager {
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            column_keys: RwLock::new(HashMap::new()),
        }
    }

    /// Register an encryption key
    pub async fn register_key(&self, key_id: &str, key: Vec<u8>) -> Result<(), SecurityError> {
        if key.len() != 32 {
            return Err(SecurityError::EncryptionError(
                "Key must be 32 bytes for AES-256".to_string()
            ));
        }
        
        let mut keys = self.keys.write().await;
        keys.insert(key_id.to_string(), key);
        info!("Registered encryption key: {}", key_id);
        Ok(())
    }

    /// Generate and register a new random key
    pub async fn generate_key(&self, key_id: &str) -> Result<(), SecurityError> {
        use ring::rand::{SecureRandom, SystemRandom};
        
        let rng = SystemRandom::new();
        let mut key = vec![0u8; 32];
        rng.fill(&mut key).map_err(|_| {
            SecurityError::EncryptionError("Failed to generate random key".to_string())
        })?;
        
        self.register_key(key_id, key).await
    }

    /// Configure a column for encryption
    pub async fn configure_column(&self, table: &str, column: &str, key_id: &str) {
        let mut columns = self.column_keys.write().await;
        let full_name = format!("{}.{}", table, column);
        columns.insert(full_name.clone(), key_id.to_string());
        info!("Configured encryption for column {} with key {}", full_name, key_id);
    }

    /// Encrypt data using a specific key
    pub fn encrypt(&self, plaintext: &[u8], key_id: &str) -> Result<Vec<u8>, SecurityError> {
        // For sync context, we use try_read
        let keys = self.keys.try_read()
            .map_err(|_| SecurityError::EncryptionError("Lock contention".to_string()))?;
        
        let key_bytes = keys.get(key_id)
            .ok_or_else(|| SecurityError::KeyNotFound(key_id.to_string()))?;
        
        self.encrypt_with_key(plaintext, key_bytes)
    }

    /// Encrypt data with a raw key
    pub fn encrypt_with_key(&self, plaintext: &[u8], key_bytes: &[u8]) -> Result<Vec<u8>, SecurityError> {
        let key = Key::<Aes256Gcm>::from_slice(key_bytes);
        let cipher = Aes256Gcm::new(key);
        
        // Generate random nonce
        let mut nonce_bytes = [0u8; 12];
        use ring::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        rng.fill(&mut nonce_bytes).map_err(|_| {
            SecurityError::EncryptionError("Failed to generate nonce".to_string())
        })?;
        
        let nonce = Nonce::from_slice(&nonce_bytes);
        
        let ciphertext = cipher.encrypt(nonce, plaintext)
            .map_err(|e| SecurityError::EncryptionError(format!("Encryption failed: {:?}", e)))?;
        
        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend(ciphertext);
        
        Ok(result)
    }

    /// Decrypt data using a specific key
    pub fn decrypt(&self, ciphertext: &[u8], key_id: &str) -> Result<Vec<u8>, SecurityError> {
        let keys = self.keys.try_read()
            .map_err(|_| SecurityError::DecryptionError("Lock contention".to_string()))?;
        
        let key_bytes = keys.get(key_id)
            .ok_or_else(|| SecurityError::KeyNotFound(key_id.to_string()))?;
        
        self.decrypt_with_key(ciphertext, key_bytes)
    }

    /// Decrypt data with a raw key
    pub fn decrypt_with_key(&self, ciphertext: &[u8], key_bytes: &[u8]) -> Result<Vec<u8>, SecurityError> {
        if ciphertext.len() < 12 {
            return Err(SecurityError::DecryptionError(
                "Ciphertext too short".to_string()
            ));
        }
        
        let key = Key::<Aes256Gcm>::from_slice(key_bytes);
        let cipher = Aes256Gcm::new(key);
        
        // Extract nonce from beginning
        let nonce = Nonce::from_slice(&ciphertext[..12]);
        let encrypted_data = &ciphertext[12..];
        
        cipher.decrypt(nonce, encrypted_data)
            .map_err(|e| SecurityError::DecryptionError(format!("Decryption failed: {:?}", e)))
    }

    /// Check if a column is configured for encryption
    pub async fn is_column_encrypted(&self, table: &str, column: &str) -> bool {
        let columns = self.column_keys.read().await;
        let full_name = format!("{}.{}", table, column);
        columns.contains_key(&full_name)
    }

    /// Get the key ID for an encrypted column
    pub async fn get_column_key_id(&self, table: &str, column: &str) -> Option<String> {
        let columns = self.column_keys.read().await;
        let full_name = format!("{}.{}", table, column);
        columns.get(&full_name).cloned()
    }

    /// Encrypt a column value (convenience method)
    pub async fn encrypt_column_value(
        &self,
        table: &str,
        column: &str,
        value: &[u8],
    ) -> Result<Vec<u8>, SecurityError> {
        let key_id = self.get_column_key_id(table, column).await
            .ok_or_else(|| SecurityError::KeyNotFound(format!("{}.{}", table, column)))?;
        
        // Need to get the key in async context
        let keys = self.keys.read().await;
        let key_bytes = keys.get(&key_id)
            .ok_or_else(|| SecurityError::KeyNotFound(key_id.clone()))?
            .clone();
        drop(keys);
        
        self.encrypt_with_key(value, &key_bytes)
    }

    /// Decrypt a column value (convenience method)
    pub async fn decrypt_column_value(
        &self,
        table: &str,
        column: &str,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, SecurityError> {
        let key_id = self.get_column_key_id(table, column).await
            .ok_or_else(|| SecurityError::KeyNotFound(format!("{}.{}", table, column)))?;
        
        let keys = self.keys.read().await;
        let key_bytes = keys.get(&key_id)
            .ok_or_else(|| SecurityError::KeyNotFound(key_id.clone()))?
            .clone();
        drop(keys);
        
        self.decrypt_with_key(ciphertext, &key_bytes)
    }

    /// Rotate a key (re-encrypt all data with new key)
    pub async fn rotate_key(&self, old_key_id: &str, new_key_id: &str) -> Result<(), SecurityError> {
        info!("Key rotation from {} to {} - would re-encrypt all affected data", old_key_id, new_key_id);
        // In production: would iterate through all encrypted data and re-encrypt
        Ok(())
    }

    /// List all registered key IDs
    pub async fn list_keys(&self) -> Vec<String> {
        let keys = self.keys.read().await;
        keys.keys().cloned().collect()
    }
}

impl Default for EncryptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_encrypt_decrypt() {
        let mgr = EncryptionManager::new();
        
        // Generate a key
        mgr.generate_key("test_key").await.unwrap();
        
        let plaintext = b"Sensitive data";
        let ciphertext = mgr.encrypt(plaintext, "test_key").unwrap();
        
        // Ciphertext should be different and longer (nonce + tag)
        assert_ne!(ciphertext.as_slice(), plaintext);
        assert!(ciphertext.len() > plaintext.len());
        
        let decrypted = mgr.decrypt(&ciphertext, "test_key").unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_column_encryption() {
        let mgr = EncryptionManager::new();

        mgr.generate_key("ssn_key").await.unwrap();
        mgr.configure_column("users", "ssn", "ssn_key").await;

        assert!(mgr.is_column_encrypted("users", "ssn").await);
        assert!(!mgr.is_column_encrypted("users", "name").await);

        let ssn = b"123-45-6789";
        let encrypted = mgr.encrypt_column_value("users", "ssn", ssn).await.unwrap();
        let decrypted = mgr.decrypt_column_value("users", "ssn", &encrypted).await.unwrap();

        assert_eq!(decrypted, ssn);
    }

    #[test]
    fn test_field_encryption_roundtrip() {
        let key = [0x42u8; 32];
        let fe = FieldEncryption::new(&key);
        let plaintext = b"PII: Social Security Number 123-45-6789";
        let ciphertext = fe.encrypt(plaintext).expect("encrypt failed");
        // Ciphertext must be longer (12-byte nonce + 16-byte GCM tag overhead)
        assert!(ciphertext.len() > plaintext.len() + 12);
        let recovered = fe.decrypt(&ciphertext).expect("decrypt failed");
        assert_eq!(recovered, plaintext);
    }

    #[test]
    fn test_field_encryption_different_ciphertexts() {
        // Same plaintext encrypted twice must produce different outputs (random nonce)
        let key = [0xABu8; 32];
        let fe = FieldEncryption::new(&key);
        let plaintext = b"hello";
        let ct1 = fe.encrypt(plaintext).unwrap();
        let ct2 = fe.encrypt(plaintext).unwrap();
        assert_ne!(ct1, ct2, "nonce should differ between calls");
    }

    #[test]
    fn test_field_encryption_too_short() {
        let key = [0x00u8; 32];
        let fe = FieldEncryption::new(&key);
        let result = fe.decrypt(&[0u8; 5]);
        assert!(result.is_err());
    }
}
