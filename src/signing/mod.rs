//! Request Signing with Dilithium
//!
//! Provides anti-tampering protection for HTTP requests:
//! - Sign requests with Dilithium-3
//! - Verify signatures on incoming requests
//! - Prevent replay attacks with nonces and timestamps

use std::collections::HashSet;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

use crate::crypto::Crypto;
use crate::security::pqc::PqcProvider;

/// Maximum allowed time skew (in seconds)
const MAX_TIME_SKEW_SECS: i64 = 30;

/// Nonce cache TTL
const NONCE_CACHE_TTL_SECS: u64 = 300; // 5 minutes

/// What gets signed in a request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignatureInput {
    /// HTTP method
    pub method: String,
    /// Request path (with query string)
    pub path: String,
    /// Request body hash (BLAKE3)
    pub body_hash: [u8; 32],
    /// Timestamp (milliseconds since epoch)
    pub timestamp: i64,
    /// Random nonce (prevents replay)
    pub nonce: [u8; 16],
    /// Target host
    pub host: String,
    /// Content-Type header
    pub content_type: Option<String>,
}

/// Signed request headers
#[derive(Clone, Debug)]
pub struct SignedRequestHeaders {
    /// Signature (base64)
    pub signature: String,
    /// Signature input (base64)
    pub signature_input: String,
    /// Timestamp
    pub timestamp: i64,
    /// Nonce (hex)
    pub nonce: String,
    /// Public key fingerprint (hex)
    pub public_key_fingerprint: String,
}

/// Request signer (client-side)
pub struct RequestSigner {
    /// Dilithium signing key
    secret_key: Vec<u8>,
    /// Dilithium public key
    public_key: Vec<u8>,
    /// Public key fingerprint
    fingerprint: [u8; 16],
    /// PQC provider
    pqc: PqcProvider,
}

impl RequestSigner {
    /// Create a new request signer with existing keys
    pub fn new(secret_key: Vec<u8>, public_key: Vec<u8>) -> Self {
        let fingerprint: [u8; 16] = blake3::hash(&public_key).as_bytes()[..16]
            .try_into()
            .expect("Should be 16 bytes");

        Self {
            secret_key,
            public_key,
            fingerprint,
            pqc: PqcProvider::new(),
        }
    }

    /// Create a new request signer with generated keys
    pub fn generate() -> Result<Self, SigningError> {
        let pqc = PqcProvider::new();
        let keypair = pqc.generate_dilithium_keypair()
            .map_err(|e| SigningError::CryptoError(format!("{:?}", e)))?;

        Ok(Self::new(keypair.secret_key, keypair.public_key))
    }

    /// Sign a request
    pub fn sign(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        host: &str,
        content_type: Option<&str>,
    ) -> Result<SignedRequestHeaders, SigningError> {
        // Generate nonce
        let nonce: [u8; 16] = Crypto::random_bytes(16)
            .try_into()
            .map_err(|_| SigningError::CryptoError("Failed to generate nonce".to_string()))?;

        // Get current timestamp
        let timestamp = Utc::now().timestamp_millis();

        // Hash body
        let body_hash: [u8; 32] = blake3::hash(body).into();

        // Create signature input
        let sig_input = SignatureInput {
            method: method.to_string(),
            path: path.to_string(),
            body_hash,
            timestamp,
            nonce,
            host: host.to_string(),
            content_type: content_type.map(|s| s.to_string()),
        };

        // Serialize signature input
        let sig_input_bytes = bincode::serialize(&sig_input)
            .map_err(|e| SigningError::SerializationError(e.to_string()))?;

        // Sign with Dilithium
        let signature = self.pqc.dilithium_sign(&sig_input_bytes, &self.secret_key)
            .map_err(|e| SigningError::CryptoError(format!("{:?}", e)))?;

        Ok(SignedRequestHeaders {
            signature: Crypto::base64_encode(&signature),
            signature_input: Crypto::base64_encode(&sig_input_bytes),
            timestamp,
            nonce: hex::encode(&nonce),
            public_key_fingerprint: hex::encode(&self.fingerprint),
        })
    }

    /// Get public key
    pub fn public_key(&self) -> &[u8] {
        &self.public_key
    }

    /// Get fingerprint
    pub fn fingerprint(&self) -> &[u8; 16] {
        &self.fingerprint
    }
}

/// Nonce cache entry
struct NonceEntry {
    nonce: [u8; 16],
    added_at: Instant,
}

/// Nonce cache for replay prevention
pub struct NonceCache {
    /// Stored nonces
    nonces: RwLock<HashSet<[u8; 16]>>,
    /// Expiry times
    expiry: RwLock<Vec<NonceEntry>>,
    /// TTL
    ttl: Duration,
}

impl NonceCache {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            nonces: RwLock::new(HashSet::new()),
            expiry: RwLock::new(Vec::new()),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Check if nonce is new and add it
    /// Returns true if nonce was new, false if already seen
    pub fn check_and_add(&self, nonce: &[u8; 16]) -> bool {
        // Clean up expired nonces first
        self.cleanup();

        let mut nonces = self.nonces.write().unwrap();
        if nonces.contains(nonce) {
            return false;
        }

        nonces.insert(*nonce);

        let mut expiry = self.expiry.write().unwrap();
        expiry.push(NonceEntry {
            nonce: *nonce,
            added_at: Instant::now(),
        });

        true
    }

    /// Clean up expired nonces
    fn cleanup(&self) {
        let cutoff = Instant::now() - self.ttl;

        let mut nonces = self.nonces.write().unwrap();
        let mut expiry = self.expiry.write().unwrap();

        expiry.retain(|entry| {
            if entry.added_at < cutoff {
                nonces.remove(&entry.nonce);
                false
            } else {
                true
            }
        });
    }
}

impl Default for NonceCache {
    fn default() -> Self {
        Self::new(NONCE_CACHE_TTL_SECS)
    }
}

/// Request verifier (server-side)
pub struct RequestVerifier {
    /// Known public keys (fingerprint -> public key)
    public_keys: RwLock<std::collections::HashMap<[u8; 16], Vec<u8>>>,
    /// Nonce cache
    nonce_cache: NonceCache,
    /// Maximum time skew
    max_time_skew: i64,
    /// PQC provider
    pqc: PqcProvider,
}

impl RequestVerifier {
    pub fn new() -> Self {
        Self {
            public_keys: RwLock::new(std::collections::HashMap::new()),
            nonce_cache: NonceCache::default(),
            max_time_skew: MAX_TIME_SKEW_SECS,
            pqc: PqcProvider::new(),
        }
    }

    /// Register a public key
    pub fn register_public_key(&self, public_key: &[u8]) {
        let fingerprint: [u8; 16] = blake3::hash(public_key).as_bytes()[..16]
            .try_into()
            .expect("Should be 16 bytes");

        let mut keys = self.public_keys.write().unwrap();
        keys.insert(fingerprint, public_key.to_vec());
    }

    /// Verify a signed request
    pub fn verify(
        &self,
        headers: &SignedRequestHeaders,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<VerifiedRequest, SigningError> {
        // 1. Check timestamp
        let now = Utc::now().timestamp_millis();
        let time_diff = (now - headers.timestamp).abs();
        if time_diff > self.max_time_skew * 1000 {
            return Err(SigningError::TimestampOutOfRange);
        }

        // 2. Check nonce
        let nonce: [u8; 16] = hex::decode(&headers.nonce)
            .map_err(|_| SigningError::InvalidNonce)?
            .try_into()
            .map_err(|_| SigningError::InvalidNonce)?;

        if !self.nonce_cache.check_and_add(&nonce) {
            return Err(SigningError::NonceReused);
        }

        // 3. Get public key
        let fingerprint: [u8; 16] = hex::decode(&headers.public_key_fingerprint)
            .map_err(|_| SigningError::UnknownPublicKey)?
            .try_into()
            .map_err(|_| SigningError::UnknownPublicKey)?;

        let public_key = {
            let keys = self.public_keys.read().unwrap();
            keys.get(&fingerprint).cloned()
                .ok_or(SigningError::UnknownPublicKey)?
        };

        // 4. Decode signature input
        let sig_input_bytes = Crypto::base64_decode(&headers.signature_input)
            .map_err(|e| SigningError::InvalidSignature(e))?;

        let sig_input: SignatureInput = bincode::deserialize(&sig_input_bytes)
            .map_err(|e| SigningError::SerializationError(e.to_string()))?;

        // 5. Verify signature input matches request
        if sig_input.method != method {
            return Err(SigningError::RequestMismatch("Method mismatch".to_string()));
        }

        if sig_input.path != path {
            return Err(SigningError::RequestMismatch("Path mismatch".to_string()));
        }

        let actual_body_hash: [u8; 32] = blake3::hash(body).into();
        if sig_input.body_hash != actual_body_hash {
            return Err(SigningError::BodyTampered);
        }

        // 6. Verify Dilithium signature
        let signature = Crypto::base64_decode(&headers.signature)
            .map_err(|e| SigningError::InvalidSignature(e))?;

        let valid = self.pqc.dilithium_verify(&sig_input_bytes, &signature, &public_key)
            .map_err(|e| SigningError::CryptoError(format!("{:?}", e)))?;

        if !valid {
            return Err(SigningError::InvalidSignature("Signature verification failed".to_string()));
        }

        Ok(VerifiedRequest {
            signer_fingerprint: fingerprint,
            method: sig_input.method,
            path: sig_input.path,
            timestamp: sig_input.timestamp,
            verified_at: Utc::now(),
        })
    }
}

impl Default for RequestVerifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Verified request info
#[derive(Clone, Debug)]
pub struct VerifiedRequest {
    /// Signer's public key fingerprint
    pub signer_fingerprint: [u8; 16],
    /// Verified method
    pub method: String,
    /// Verified path
    pub path: String,
    /// Request timestamp
    pub timestamp: i64,
    /// Verification timestamp
    pub verified_at: DateTime<Utc>,
}

/// Signing errors
#[derive(Debug, Clone)]
pub enum SigningError {
    CryptoError(String),
    SerializationError(String),
    TimestampOutOfRange,
    NonceReused,
    UnknownPublicKey,
    InvalidNonce,
    InvalidSignature(String),
    RequestMismatch(String),
    BodyTampered,
}

impl std::fmt::Display for SigningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SigningError::CryptoError(msg) => write!(f, "Crypto error: {}", msg),
            SigningError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            SigningError::TimestampOutOfRange => write!(f, "Timestamp out of range"),
            SigningError::NonceReused => write!(f, "Nonce reused"),
            SigningError::UnknownPublicKey => write!(f, "Unknown public key"),
            SigningError::InvalidNonce => write!(f, "Invalid nonce"),
            SigningError::InvalidSignature(msg) => write!(f, "Invalid signature: {}", msg),
            SigningError::RequestMismatch(msg) => write!(f, "Request mismatch: {}", msg),
            SigningError::BodyTampered => write!(f, "Body tampered"),
        }
    }
}

impl std::error::Error for SigningError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let signer = RequestSigner::generate().unwrap();
        let verifier = RequestVerifier::new();

        // Register the signer's public key
        verifier.register_public_key(signer.public_key());

        // Sign a request
        let headers = signer.sign(
            "POST",
            "/api/users",
            b"{\"name\": \"test\"}",
            "api.example.com",
            Some("application/json"),
        ).unwrap();

        // Verify the request
        let result = verifier.verify(
            &headers,
            "POST",
            "/api/users",
            b"{\"name\": \"test\"}",
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_body_tampering_detected() {
        let signer = RequestSigner::generate().unwrap();
        let verifier = RequestVerifier::new();
        verifier.register_public_key(signer.public_key());

        let headers = signer.sign(
            "POST",
            "/api/users",
            b"{\"name\": \"test\"}",
            "api.example.com",
            None,
        ).unwrap();

        // Try to verify with tampered body
        let result = verifier.verify(
            &headers,
            "POST",
            "/api/users",
            b"{\"name\": \"tampered\"}",
        );

        assert!(matches!(result, Err(SigningError::BodyTampered)));
    }

    #[test]
    fn test_nonce_replay_prevention() {
        let cache = NonceCache::new(60);
        let nonce = [1u8; 16];

        // First use should succeed
        assert!(cache.check_and_add(&nonce));

        // Second use should fail (replay)
        assert!(!cache.check_and_add(&nonce));
    }

    #[test]
    fn test_unknown_public_key() {
        let signer = RequestSigner::generate().unwrap();
        let verifier = RequestVerifier::new();
        // Note: NOT registering the public key

        let headers = signer.sign(
            "GET",
            "/api/test",
            b"",
            "api.example.com",
            None,
        ).unwrap();

        let result = verifier.verify(&headers, "GET", "/api/test", b"");

        assert!(matches!(result, Err(SigningError::UnknownPublicKey)));
    }
}
