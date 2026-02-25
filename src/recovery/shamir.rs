//! Shamir Secret Sharing for Key Recovery
//!
//! Implements social recovery where a secret is split into N shares
//! and can be reconstructed from any T shares.

use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::crypto::Crypto;
use crate::security::pqc::PqcProvider;

/// Recovery share
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryShare {
    /// Share index (1 to N)
    pub index: u8,
    /// Share value (32 bytes)
    pub value: [u8; 32],
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Share ID for tracking
    pub share_id: Uuid,
}

/// Encrypted recovery share for guardian storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedShare {
    /// Guardian identifier
    pub guardian_id: String,
    /// Kyber-encrypted share
    pub kyber_ciphertext: Vec<u8>,
    /// AES-encrypted share data
    pub encrypted_data: Vec<u8>,
    /// Verification hash
    pub verification_hash: [u8; 32],
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

/// Guardian configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Guardian {
    /// Guardian ID
    pub id: String,
    /// Guardian name/label
    pub name: String,
    /// Guardian's Kyber public key
    pub kyber_public_key: Vec<u8>,
    /// Contact method (email, phone, etc.)
    pub contact: String,
    /// Trust level (1-10)
    pub trust_level: u8,
}

/// Social recovery configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SocialRecoveryConfig {
    /// Unique config ID
    pub config_id: Uuid,
    /// Recovery threshold
    pub threshold: u8,
    /// Total shares
    pub total_shares: u8,
    /// Guardians
    pub guardians: Vec<Guardian>,
    /// Encrypted shares (indexed by guardian ID)
    pub encrypted_shares: HashMap<String, EncryptedShare>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// User ID this recovery is for
    pub user_id: String,
}

/// Shamir error types
#[derive(Debug, Clone)]
pub enum ShamirError {
    InsufficientShares(String),
    InvalidShare(String),
    CryptoError(String),
    ConfigError(String),
}

impl std::fmt::Display for ShamirError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShamirError::InsufficientShares(msg) => write!(f, "Insufficient shares: {}", msg),
            ShamirError::InvalidShare(msg) => write!(f, "Invalid share: {}", msg),
            ShamirError::CryptoError(msg) => write!(f, "Crypto error: {}", msg),
            ShamirError::ConfigError(msg) => write!(f, "Config error: {}", msg),
        }
    }
}

impl std::error::Error for ShamirError {}

/// Shamir Secret Sharing implementation
pub struct ShamirRecovery {
    pqc: PqcProvider,
}

impl ShamirRecovery {
    pub fn new() -> Self {
        Self {
            pqc: PqcProvider::new(),
        }
    }

    /// Split a 32-byte secret into N shares with threshold T
    pub fn split(
        &self,
        secret: &[u8; 32],
        threshold: u8,
        total_shares: u8,
    ) -> Result<Vec<RecoveryShare>, ShamirError> {
        if threshold == 0 || threshold > total_shares {
            return Err(ShamirError::ConfigError(
                "Invalid threshold/total configuration".to_string()
            ));
        }

        // Generate random polynomial coefficients
        // f(x) = secret + a_1*x + a_2*x^2 + ... + a_{t-1}*x^{t-1}
        let mut coefficients: Vec<[u8; 32]> = vec![*secret];
        for _ in 1..threshold {
            let coeff: [u8; 32] = Crypto::random_bytes(32)
                .try_into()
                .map_err(|_| ShamirError::CryptoError("RNG failed".to_string()))?;
            coefficients.push(coeff);
        }

        // Evaluate polynomial at points 1, 2, ..., N
        let mut shares = Vec::with_capacity(total_shares as usize);
        for i in 1..=total_shares {
            let value = self.evaluate_polynomial(&coefficients, i);
            shares.push(RecoveryShare {
                index: i,
                value,
                created_at: Utc::now(),
                share_id: Uuid::new_v4(),
            });
        }

        Ok(shares)
    }

    /// Reconstruct secret from shares using Lagrange interpolation
    pub fn reconstruct(&self, shares: &[RecoveryShare]) -> Result<[u8; 32], ShamirError> {
        if shares.is_empty() {
            return Err(ShamirError::InsufficientShares("No shares provided".to_string()));
        }

        // Check for duplicate indices
        let mut seen_indices = std::collections::HashSet::new();
        for share in shares {
            if !seen_indices.insert(share.index) {
                return Err(ShamirError::InvalidShare(
                    format!("Duplicate share index: {}", share.index)
                ));
            }
        }

        // Lagrange interpolation at x = 0
        let mut result = [0u8; 32];

        for (i, share_i) in shares.iter().enumerate() {
            // Compute Lagrange basis polynomial L_i(0)
            let mut numerator: i128 = 1;
            let mut denominator: i128 = 1;

            for (j, share_j) in shares.iter().enumerate() {
                if i != j {
                    // L_i(0) = Π(0 - x_j) / Π(x_i - x_j) for j ≠ i
                    numerator = numerator.wrapping_mul(-(share_j.index as i128));
                    denominator = denominator.wrapping_mul(
                        (share_i.index as i128) - (share_j.index as i128)
                    );
                }
            }

            // Compute coefficient
            let coeff = numerator as f64 / denominator as f64;

            // Add contribution: L_i(0) * y_i
            for k in 0..32 {
                let contribution = (share_i.value[k] as f64 * coeff) as i32;
                result[k] = ((result[k] as i32).wrapping_add(contribution) & 0xFF) as u8;
            }
        }

        Ok(result)
    }

    /// Evaluate polynomial at point x
    fn evaluate_polynomial(&self, coefficients: &[[u8; 32]], x: u8) -> [u8; 32] {
        let mut result = [0u8; 32];
        let mut x_power: u64 = 1;

        for coeff in coefficients {
            for i in 0..32 {
                let term = ((coeff[i] as u64).wrapping_mul(x_power)) & 0xFF;
                result[i] = result[i].wrapping_add(term as u8);
            }
            x_power = x_power.wrapping_mul(x as u64);
        }

        result
    }

    /// Create encrypted share for a guardian
    pub fn encrypt_share_for_guardian(
        &self,
        share: &RecoveryShare,
        guardian: &Guardian,
    ) -> Result<EncryptedShare, ShamirError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        // Encapsulate with guardian's Kyber public key
        let encap = self.pqc.kyber_encapsulate(&guardian.kyber_public_key)
            .map_err(|e| ShamirError::CryptoError(format!("{:?}", e)))?;

        // Serialize share data
        let share_data = bincode::serialize(share)
            .map_err(|e| ShamirError::CryptoError(format!("Serialization failed: {}", e)))?;

        // Encrypt with AES-256-GCM
        let key = Key::<Aes256Gcm>::from_slice(&encap.shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce_bytes = Crypto::random_bytes(12);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher.encrypt(nonce, share_data.as_slice())
            .map_err(|e| ShamirError::CryptoError(format!("Encryption failed: {:?}", e)))?;

        // Combine nonce + ciphertext
        let mut encrypted_data = nonce_bytes;
        encrypted_data.extend(ciphertext);

        // Compute verification hash
        let verification_hash: [u8; 32] = blake3::hash(&share.value).into();

        Ok(EncryptedShare {
            guardian_id: guardian.id.clone(),
            kyber_ciphertext: encap.ciphertext,
            encrypted_data,
            verification_hash,
            created_at: Utc::now(),
        })
    }

    /// Decrypt a share received from a guardian
    pub fn decrypt_share(
        &self,
        encrypted: &EncryptedShare,
        kyber_secret_key: &[u8],
    ) -> Result<RecoveryShare, ShamirError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        // Decapsulate Kyber
        let shared_secret = self.pqc.kyber_decapsulate(&encrypted.kyber_ciphertext, kyber_secret_key)
            .map_err(|e| ShamirError::CryptoError(format!("{:?}", e)))?;

        // Decrypt share data
        if encrypted.encrypted_data.len() < 12 {
            return Err(ShamirError::InvalidShare("Encrypted data too short".to_string()));
        }

        let (nonce_bytes, ciphertext) = encrypted.encrypted_data.split_at(12);
        let key = Key::<Aes256Gcm>::from_slice(&shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);

        let share_data = cipher.decrypt(nonce, ciphertext)
            .map_err(|e| ShamirError::CryptoError(format!("Decryption failed: {:?}", e)))?;

        // Deserialize share
        let share: RecoveryShare = bincode::deserialize(&share_data)
            .map_err(|e| ShamirError::CryptoError(format!("Deserialization failed: {}", e)))?;

        // Verify hash
        let computed_hash: [u8; 32] = blake3::hash(&share.value).into();
        if computed_hash != encrypted.verification_hash {
            return Err(ShamirError::InvalidShare("Verification hash mismatch".to_string()));
        }

        Ok(share)
    }

    /// Setup social recovery for a user
    pub fn setup_social_recovery(
        &self,
        user_id: &str,
        secret: &[u8; 32],
        threshold: u8,
        guardians: Vec<Guardian>,
    ) -> Result<SocialRecoveryConfig, ShamirError> {
        let total_shares = guardians.len() as u8;

        if threshold > total_shares {
            return Err(ShamirError::ConfigError(
                format!("Threshold {} > guardian count {}", threshold, total_shares)
            ));
        }

        // Split the secret
        let shares = self.split(secret, threshold, total_shares)?;

        // Encrypt shares for each guardian
        let mut encrypted_shares = HashMap::new();
        for (share, guardian) in shares.iter().zip(guardians.iter()) {
            let encrypted = self.encrypt_share_for_guardian(share, guardian)?;
            encrypted_shares.insert(guardian.id.clone(), encrypted);
        }

        Ok(SocialRecoveryConfig {
            config_id: Uuid::new_v4(),
            threshold,
            total_shares,
            guardians,
            encrypted_shares,
            created_at: Utc::now(),
            user_id: user_id.to_string(),
        })
    }
}

impl Default for ShamirRecovery {
    fn default() -> Self {
        Self::new()
    }
}

/// Recovery session for collecting shares from guardians
pub struct KeyRecoverySession {
    /// Session ID
    pub session_id: Uuid,
    /// User ID being recovered
    pub user_id: String,
    /// Required threshold
    pub threshold: u8,
    /// Collected shares
    shares: Vec<RecoveryShare>,
    /// Guardians who have responded
    responded_guardians: Vec<String>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Expiration timestamp
    pub expires_at: DateTime<Utc>,
}

impl KeyRecoverySession {
    pub fn new(user_id: &str, threshold: u8, validity_hours: i64) -> Self {
        let now = Utc::now();
        Self {
            session_id: Uuid::new_v4(),
            user_id: user_id.to_string(),
            threshold,
            shares: Vec::new(),
            responded_guardians: Vec::new(),
            created_at: now,
            expires_at: now + chrono::Duration::hours(validity_hours),
        }
    }

    /// Add a recovered share
    pub fn add_share(&mut self, share: RecoveryShare, guardian_id: &str) -> Result<(), ShamirError> {
        if self.is_expired() {
            return Err(ShamirError::ConfigError("Session expired".to_string()));
        }

        if self.responded_guardians.contains(&guardian_id.to_string()) {
            return Err(ShamirError::InvalidShare("Guardian already responded".to_string()));
        }

        self.shares.push(share);
        self.responded_guardians.push(guardian_id.to_string());
        Ok(())
    }

    /// Check if we have enough shares
    pub fn is_complete(&self) -> bool {
        self.shares.len() >= self.threshold as usize
    }

    /// Check if session is expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Attempt recovery
    pub fn recover(&self, shamir: &ShamirRecovery) -> Result<[u8; 32], ShamirError> {
        if !self.is_complete() {
            return Err(ShamirError::InsufficientShares(format!(
                "Need {} shares, have {}",
                self.threshold,
                self.shares.len()
            )));
        }

        shamir.reconstruct(&self.shares)
    }

    /// Get progress
    pub fn progress(&self) -> (usize, u8) {
        (self.shares.len(), self.threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_creates_correct_number_of_shares() {
        let shamir = ShamirRecovery::new();
        let secret = [42u8; 32];

        let shares = shamir.split(&secret, 3, 5).unwrap();
        assert_eq!(shares.len(), 5);
    }

    #[test]
    fn test_invalid_threshold() {
        let shamir = ShamirRecovery::new();
        let secret = [42u8; 32];

        let result = shamir.split(&secret, 6, 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_recovery_session() {
        let mut session = KeyRecoverySession::new("user123", 3, 24);

        assert!(!session.is_complete());
        assert!(!session.is_expired());

        for i in 1..=3 {
            let share = RecoveryShare {
                index: i,
                value: [i; 32],
                created_at: Utc::now(),
                share_id: Uuid::new_v4(),
            };
            session.add_share(share, &format!("guardian{}", i)).unwrap();
        }

        assert!(session.is_complete());
    }
}
