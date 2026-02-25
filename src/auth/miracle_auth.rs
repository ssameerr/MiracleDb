//! MiracleAuth - Unified Post-Quantum Security API
//!
//! A comprehensive quantum-resistant security suite providing:
//! - Authentication (PQToken)
//! - Encryption (Kyber-1024 + AES-256-GCM)
//! - E2E Messaging (Double Ratchet)
//! - Request Signing (Dilithium-3)
//! - Key Recovery (Shamir)
//! - Audit Trail (Merkle + Dilithium)

use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::auth::{TokenManager, TokenBundle, PqTokenClaims, Scope, TokenError};
use crate::cache::{MiracleCache, CachedVerification};
use crate::ratchet::{RatchetSession, RatchetMessage, RatchetError, RatchetManager};
use crate::mpc::{ThresholdConfig, ThresholdSigner, ThresholdCoordinator, Dkg, MpcError};
use crate::recovery::shamir::{ShamirRecovery, RecoveryShare, Guardian, SocialRecoveryConfig, ShamirError};
use crate::audit::merkle::{MerkleAuditTree, MerkleAuditEntry, MerkleProof, AuditActor, AuditAction, MerkleAuditError};
use crate::signing::{RequestSigner, RequestVerifier, SignedRequestHeaders, VerifiedRequest, SigningError};
use crate::security::pqc::{PqcProvider, KyberKeyPair, DilithiumKeyPair, HybridCiphertext};

/// MiracleAuth error types
#[derive(Debug)]
pub enum MiracleAuthError {
    Token(TokenError),
    Ratchet(RatchetError),
    Mpc(MpcError),
    Shamir(ShamirError),
    Audit(MerkleAuditError),
    Signing(SigningError),
    Crypto(String),
    NotInitialized(String),
}

impl std::fmt::Display for MiracleAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MiracleAuthError::Token(e) => write!(f, "Token error: {}", e),
            MiracleAuthError::Ratchet(e) => write!(f, "Ratchet error: {}", e),
            MiracleAuthError::Mpc(e) => write!(f, "MPC error: {}", e),
            MiracleAuthError::Shamir(e) => write!(f, "Shamir error: {}", e),
            MiracleAuthError::Audit(e) => write!(f, "Audit error: {}", e),
            MiracleAuthError::Signing(e) => write!(f, "Signing error: {}", e),
            MiracleAuthError::Crypto(msg) => write!(f, "Crypto error: {}", msg),
            MiracleAuthError::NotInitialized(msg) => write!(f, "Not initialized: {}", msg),
        }
    }
}

impl std::error::Error for MiracleAuthError {}

impl From<TokenError> for MiracleAuthError {
    fn from(e: TokenError) -> Self {
        MiracleAuthError::Token(e)
    }
}

impl From<RatchetError> for MiracleAuthError {
    fn from(e: RatchetError) -> Self {
        MiracleAuthError::Ratchet(e)
    }
}

impl From<MpcError> for MiracleAuthError {
    fn from(e: MpcError) -> Self {
        MiracleAuthError::Mpc(e)
    }
}

impl From<ShamirError> for MiracleAuthError {
    fn from(e: ShamirError) -> Self {
        MiracleAuthError::Shamir(e)
    }
}

impl From<MerkleAuditError> for MiracleAuthError {
    fn from(e: MerkleAuditError) -> Self {
        MiracleAuthError::Audit(e)
    }
}

impl From<SigningError> for MiracleAuthError {
    fn from(e: SigningError) -> Self {
        MiracleAuthError::Signing(e)
    }
}

/// Encrypted data container
#[derive(Clone, Debug)]
pub struct EncryptedData {
    /// Kyber ciphertext
    pub kyber_ciphertext: Vec<u8>,
    /// AES-GCM encrypted data (nonce prepended)
    pub encrypted_data: Vec<u8>,
}

/// MiracleAuth configuration
#[derive(Clone)]
pub struct MiracleAuthConfig {
    /// Issuer name for tokens
    pub issuer: String,
    /// Default token audience
    pub audience: Vec<String>,
    /// Cache size for verified tokens
    pub cache_size: usize,
    /// Expected revocation count (for bloom filter)
    pub expected_revocations: usize,
    /// Enable audit logging
    pub enable_audit: bool,
    /// Threshold signing config (optional)
    pub threshold_config: Option<ThresholdConfig>,
}

impl Default for MiracleAuthConfig {
    fn default() -> Self {
        Self {
            issuer: "miracle-auth".to_string(),
            audience: vec![],
            cache_size: 100_000,
            expected_revocations: 10_000,
            enable_audit: true,
            threshold_config: None,
        }
    }
}

/// MiracleAuth - Complete PQC Security Suite
pub struct MiracleAuth {
    /// Configuration
    config: MiracleAuthConfig,
    /// PQC provider
    pqc: Arc<PqcProvider>,
    /// Token manager
    token_manager: Arc<TokenManager>,
    /// Token cache (QAPC)
    cache: Arc<MiracleCache>,
    /// Ratchet session manager
    ratchet_manager: Arc<RatchetManager>,
    /// Shamir recovery
    shamir: Arc<ShamirRecovery>,
    /// Audit tree
    audit_tree: Arc<RwLock<MerkleAuditTree>>,
    /// Request verifier
    request_verifier: Arc<RequestVerifier>,
    /// User encryption keys (user_id -> keypair)
    user_keys: Arc<RwLock<std::collections::HashMap<String, KyberKeyPair>>>,
    /// User signing keys (user_id -> keypair)
    user_signing_keys: Arc<RwLock<std::collections::HashMap<String, DilithiumKeyPair>>>,
}

impl MiracleAuth {
    /// Create a new MiracleAuth instance
    pub fn new(config: MiracleAuthConfig) -> Result<Self, MiracleAuthError> {
        let pqc = Arc::new(PqcProvider::new());
        let token_manager = Arc::new(
            TokenManager::new(&config.issuer)
                .map_err(|e| MiracleAuthError::Token(e))?
                .with_audience(config.audience.clone())
        );

        let cache = Arc::new(MiracleCache::new(
            config.cache_size,
            config.expected_revocations,
        ));

        Ok(Self {
            config,
            pqc,
            token_manager,
            cache,
            ratchet_manager: Arc::new(RatchetManager::new()),
            shamir: Arc::new(ShamirRecovery::new()),
            audit_tree: Arc::new(RwLock::new(MerkleAuditTree::new())),
            request_verifier: Arc::new(RequestVerifier::new()),
            user_keys: Arc::new(RwLock::new(std::collections::HashMap::new())),
            user_signing_keys: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    // ══════════════════════════════════════════════════════════════════
    // AUTHENTICATION
    // ══════════════════════════════════════════════════════════════════

    /// Login and get PQC tokens
    pub async fn login(
        &self,
        user_id: &str,
        scopes: Vec<Scope>,
        device_fingerprint: &[u8],
        trust_score: f32,
    ) -> Result<TokenBundle, MiracleAuthError> {
        let bundle = self.token_manager.issue_tokens(
            user_id,
            scopes,
            device_fingerprint,
            trust_score,
        ).await?;

        // Log to audit
        if self.config.enable_audit {
            self.log_audit(
                user_id,
                AuditAction::Login { method: "pqtoken".to_string() },
                "auth",
                true,
            ).await.ok();
        }

        Ok(bundle)
    }

    /// Verify a PQToken (uses QAPC cache)
    pub async fn verify(&self, token: &str) -> Result<PqTokenClaims, MiracleAuthError> {
        // Try cache first (QAPC Layer 1 - fingerprint lookup)
        let fingerprint = self.compute_token_fingerprint(token);
        if let Some(claims) = self.cache.get(fingerprint) {
            return Ok(claims);
        }

        // Full verification
        let claims = self.token_manager.verify(token).await?;

        // Add to cache
        self.cache.insert(fingerprint, claims.clone());

        Ok(claims)
    }

    /// Verify token with device binding
    pub async fn verify_with_device(
        &self,
        token: &str,
        device_fingerprint: &[u8],
    ) -> Result<PqTokenClaims, MiracleAuthError> {
        let claims = self.token_manager.verify_with_device(token, device_fingerprint).await?;
        Ok(claims)
    }

    /// Refresh tokens
    pub async fn refresh(
        &self,
        refresh_token: &str,
        device_fingerprint: &[u8],
    ) -> Result<TokenBundle, MiracleAuthError> {
        let bundle = self.token_manager.refresh(refresh_token, device_fingerprint).await?;
        Ok(bundle)
    }

    /// Revoke a token
    pub async fn revoke(&self, token_id: Uuid, reason: &str) -> Result<(), MiracleAuthError> {
        self.token_manager.revoke(token_id, reason).await?;
        self.cache.add_revocation(&token_id);

        if self.config.enable_audit {
            self.log_audit(
                &token_id.to_string(),
                AuditAction::TokenRevoke { token_id },
                "auth",
                true,
            ).await.ok();
        }

        Ok(())
    }

    /// Revoke all tokens for a user
    pub async fn revoke_all_for_user(&self, user_id: &str, reason: &str) -> Result<u32, MiracleAuthError> {
        let count = self.token_manager.revoke_all_for_user(user_id, reason).await?;
        Ok(count)
    }

    // ══════════════════════════════════════════════════════════════════
    // ENCRYPTION
    // ══════════════════════════════════════════════════════════════════

    /// Generate encryption keypair for a user
    pub async fn generate_encryption_keypair(&self, user_id: &str) -> Result<Vec<u8>, MiracleAuthError> {
        let keypair = self.pqc.generate_kyber_keypair()
            .map_err(|e| MiracleAuthError::Crypto(format!("{:?}", e)))?;

        let public_key = keypair.public_key.clone();

        let mut keys = self.user_keys.write().await;
        keys.insert(user_id.to_string(), keypair);

        Ok(public_key)
    }

    /// Encrypt data with a user's public key
    pub fn encrypt(&self, data: &[u8], public_key: &[u8]) -> Result<EncryptedData, MiracleAuthError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};
        use crate::crypto::Crypto;

        // Encapsulate with Kyber
        let encap = self.pqc.kyber_encapsulate(public_key)
            .map_err(|e| MiracleAuthError::Crypto(format!("{:?}", e)))?;

        // Encrypt with AES-256-GCM
        let key = Key::<Aes256Gcm>::from_slice(&encap.shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce_bytes = Crypto::random_bytes(12);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher.encrypt(nonce, data)
            .map_err(|e| MiracleAuthError::Crypto(format!("{:?}", e)))?;

        // Combine nonce + ciphertext
        let mut encrypted_data = nonce_bytes;
        encrypted_data.extend(ciphertext);

        Ok(EncryptedData {
            kyber_ciphertext: encap.ciphertext,
            encrypted_data,
        })
    }

    /// Decrypt data with a user's secret key
    pub async fn decrypt(&self, encrypted: &EncryptedData, user_id: &str) -> Result<Vec<u8>, MiracleAuthError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        let keys = self.user_keys.read().await;
        let keypair = keys.get(user_id)
            .ok_or_else(|| MiracleAuthError::NotInitialized("User keys not found".to_string()))?;

        // Decapsulate Kyber
        let shared_secret = self.pqc.kyber_decapsulate(&encrypted.kyber_ciphertext, &keypair.secret_key)
            .map_err(|e| MiracleAuthError::Crypto(format!("{:?}", e)))?;

        // Decrypt with AES-256-GCM
        if encrypted.encrypted_data.len() < 12 {
            return Err(MiracleAuthError::Crypto("Encrypted data too short".to_string()));
        }

        let (nonce_bytes, ciphertext) = encrypted.encrypted_data.split_at(12);
        let key = Key::<Aes256Gcm>::from_slice(&shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = cipher.decrypt(nonce, ciphertext)
            .map_err(|e| MiracleAuthError::Crypto(format!("{:?}", e)))?;

        Ok(plaintext)
    }

    // ══════════════════════════════════════════════════════════════════
    // E2E MESSAGING (Double Ratchet)
    // ══════════════════════════════════════════════════════════════════

    /// Create E2E messaging session
    pub fn create_session(&self, session_id: &str, recipient_public_key: &[u8]) -> Result<(), MiracleAuthError> {
        self.ratchet_manager.create_session(session_id, recipient_public_key)?;
        Ok(())
    }

    /// Encrypt message in session
    pub fn session_encrypt(&self, session_id: &str, plaintext: &[u8]) -> Result<RatchetMessage, MiracleAuthError> {
        let message = self.ratchet_manager.encrypt(session_id, plaintext)?;
        Ok(message)
    }

    /// Decrypt message in session
    pub fn session_decrypt(&self, session_id: &str, message: &RatchetMessage) -> Result<Vec<u8>, MiracleAuthError> {
        let plaintext = self.ratchet_manager.decrypt(session_id, message)?;
        Ok(plaintext)
    }

    /// End E2E session
    pub fn end_session(&self, session_id: &str) {
        self.ratchet_manager.remove_session(session_id);
    }

    // ══════════════════════════════════════════════════════════════════
    // REQUEST SIGNING
    // ══════════════════════════════════════════════════════════════════

    /// Generate signing keypair for a user
    pub async fn generate_signing_keypair(&self, user_id: &str) -> Result<Vec<u8>, MiracleAuthError> {
        let keypair = self.pqc.generate_dilithium_keypair()
            .map_err(|e| MiracleAuthError::Crypto(format!("{:?}", e)))?;

        let public_key = keypair.public_key.clone();

        // Register with verifier
        self.request_verifier.register_public_key(&public_key);

        let mut keys = self.user_signing_keys.write().await;
        keys.insert(user_id.to_string(), keypair);

        Ok(public_key)
    }

    /// Create request signer for a user
    pub async fn get_request_signer(&self, user_id: &str) -> Result<RequestSigner, MiracleAuthError> {
        let keys = self.user_signing_keys.read().await;
        let keypair = keys.get(user_id)
            .ok_or_else(|| MiracleAuthError::NotInitialized("Signing keys not found".to_string()))?;

        Ok(RequestSigner::new(
            keypair.secret_key.clone(),
            keypair.public_key.clone(),
        ))
    }

    /// Verify a signed request
    pub fn verify_request(
        &self,
        headers: &SignedRequestHeaders,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<VerifiedRequest, MiracleAuthError> {
        let verified = self.request_verifier.verify(headers, method, path, body)?;
        Ok(verified)
    }

    // ══════════════════════════════════════════════════════════════════
    // KEY RECOVERY
    // ══════════════════════════════════════════════════════════════════

    /// Create recovery shares for a secret
    pub fn create_recovery_shares(
        &self,
        secret: &[u8; 32],
        threshold: u8,
        total_shares: u8,
    ) -> Result<Vec<RecoveryShare>, MiracleAuthError> {
        let shares = self.shamir.split(secret, threshold, total_shares)?;
        Ok(shares)
    }

    /// Recover secret from shares
    pub fn recover_from_shares(&self, shares: &[RecoveryShare]) -> Result<[u8; 32], MiracleAuthError> {
        let secret = self.shamir.reconstruct(shares)?;
        Ok(secret)
    }

    /// Setup social recovery
    pub fn setup_social_recovery(
        &self,
        user_id: &str,
        secret: &[u8; 32],
        threshold: u8,
        guardians: Vec<Guardian>,
    ) -> Result<SocialRecoveryConfig, MiracleAuthError> {
        let config = self.shamir.setup_social_recovery(user_id, secret, threshold, guardians)?;
        Ok(config)
    }

    // ══════════════════════════════════════════════════════════════════
    // AUDIT
    // ══════════════════════════════════════════════════════════════════

    /// Log an audit entry
    pub async fn log_audit(
        &self,
        user_id: &str,
        action: AuditAction,
        resource: &str,
        success: bool,
    ) -> Result<MerkleProof, MiracleAuthError> {
        let actor = AuditActor {
            user_id: Some(user_id.to_string()),
            service_id: None,
            device_hash: [0u8; 32],
            ip_address: None,
            geo_location: None,
        };

        let entry = MerkleAuditEntry::new(actor, action, resource, success);

        let mut tree = self.audit_tree.write().await;
        let proof = tree.append(entry)?;

        Ok(proof)
    }

    /// Verify an audit proof
    pub async fn verify_audit_proof(
        &self,
        entry_hash: &[u8; 32],
        proof: &MerkleProof,
    ) -> Result<bool, MiracleAuthError> {
        let tree = self.audit_tree.read().await;
        let valid = tree.verify_proof(entry_hash, proof)?;
        Ok(valid)
    }

    /// Get audit trail for a user
    pub async fn get_audit_trail(&self, user_id: &str) -> Vec<MerkleAuditEntry> {
        let tree = self.audit_tree.read().await;
        tree.query_by_actor(user_id).into_iter().cloned().collect()
    }

    // ══════════════════════════════════════════════════════════════════
    // UTILITIES
    // ══════════════════════════════════════════════════════════════════

    /// Compute token fingerprint for caching
    fn compute_token_fingerprint(&self, token: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        // Use first 50 chars for fingerprint
        token.chars().take(50).for_each(|c| c.hash(&mut hasher));
        hasher.finish()
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (u64, f64) {
        let stats = &self.cache.stats;
        let hits = stats.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = stats.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        let rate = if total == 0 { 0.0 } else { hits as f64 / total as f64 };
        (total, rate)
    }

    /// Rotate server keys
    pub async fn rotate_keys(&self) -> Result<u32, MiracleAuthError> {
        let version = self.token_manager.rotate_keys().await?;
        Ok(version)
    }

    /// Get current key version
    pub async fn key_version(&self) -> u32 {
        self.token_manager.key_version().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_miracle_auth_login() {
        let config = MiracleAuthConfig::default();
        let auth = MiracleAuth::new(config).unwrap();

        let bundle = auth.login(
            "user123",
            vec![Scope::new("users", "read")],
            b"device-123",
            1.0,
        ).await.unwrap();

        assert!(!bundle.access_token.is_empty());
    }

    #[tokio::test]
    async fn test_miracle_auth_verify() {
        let config = MiracleAuthConfig::default();
        let auth = MiracleAuth::new(config).unwrap();

        let bundle = auth.login(
            "user123",
            vec![],
            b"device-123",
            1.0,
        ).await.unwrap();

        let claims = auth.verify(&bundle.access_token).await.unwrap();
        assert_eq!(claims.sub, "user123");
    }

    #[tokio::test]
    async fn test_encryption() {
        let config = MiracleAuthConfig::default();
        let auth = MiracleAuth::new(config).unwrap();

        // Generate keys
        let public_key = auth.generate_encryption_keypair("user123").await.unwrap();

        // Encrypt
        let plaintext = b"Hello, quantum world!";
        let encrypted = auth.encrypt(plaintext, &public_key).unwrap();

        // Decrypt
        let decrypted = auth.decrypt(&encrypted, "user123").await.unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_audit_logging() {
        let config = MiracleAuthConfig {
            enable_audit: true,
            ..Default::default()
        };
        let auth = MiracleAuth::new(config).unwrap();

        let proof = auth.log_audit(
            "user123",
            AuditAction::Login { method: "password".to_string() },
            "auth",
            true,
        ).await.unwrap();

        assert_eq!(proof.tree_size, 1);
    }
}
