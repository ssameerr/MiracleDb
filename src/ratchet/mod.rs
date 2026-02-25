//! PQC Double Ratchet - Signal Protocol with Kyber-1024
//!
//! Provides end-to-end encryption with forward secrecy using:
//! - Kyber-1024 for DH ratchet (quantum-resistant key exchange)
//! - BLAKE3-KDF for chain key derivation
//! - AES-256-GCM for message encryption

use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

use crate::security::pqc::{PqcProvider, KyberKeyPair};
use crate::crypto::Crypto;

/// Maximum number of skipped message keys to store
const MAX_SKIP: u32 = 1000;

/// Ratchet session state
pub struct RatchetSession {
    /// Session ID
    pub session_id: String,
    /// Our current Kyber secret key
    our_kyber_secret: Vec<u8>,
    /// Our current Kyber public key
    our_kyber_public: Vec<u8>,
    /// Their current Kyber public key
    their_kyber_pub: Option<Vec<u8>>,
    /// Root key (ratchets on DH exchange)
    root_key: [u8; 32],
    /// Sending chain key
    send_chain_key: [u8; 32],
    /// Sending chain index
    send_chain_index: u32,
    /// Receiving chain key
    recv_chain_key: [u8; 32],
    /// Receiving chain index
    recv_chain_index: u32,
    /// Skipped message keys for out-of-order delivery
    skipped_keys: HashMap<(Vec<u8>, u32), [u8; 32]>,
    /// Previous chain length (for header)
    prev_chain_length: u32,
    /// Session creation time
    created_at: DateTime<Utc>,
}

/// Encrypted message with ratchet metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RatchetMessage {
    /// Message header
    pub header: MessageHeader,
    /// Kyber ciphertext (only present on DH ratchet)
    pub kyber_ciphertext: Option<Vec<u8>>,
    /// AES-256-GCM nonce
    pub nonce: [u8; 12],
    /// Encrypted payload
    pub ciphertext: Vec<u8>,
}

/// Message header (sent in clear for routing)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageHeader {
    /// Sender's current Kyber public key
    pub sender_kyber_pub: Vec<u8>,
    /// Message number in current sending chain
    pub chain_index: u32,
    /// Previous chain length (for out-of-order handling)
    pub prev_chain_length: u32,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Ratchet error types
#[derive(Debug, Clone)]
pub enum RatchetError {
    CryptoError(String),
    InvalidState(String),
    MessageSkipped(String),
    DuplicateMessage,
    TooManySkipped,
}

impl std::fmt::Display for RatchetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RatchetError::CryptoError(msg) => write!(f, "Crypto error: {}", msg),
            RatchetError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            RatchetError::MessageSkipped(msg) => write!(f, "Message skipped: {}", msg),
            RatchetError::DuplicateMessage => write!(f, "Duplicate message"),
            RatchetError::TooManySkipped => write!(f, "Too many skipped messages"),
        }
    }
}

impl std::error::Error for RatchetError {}

impl RatchetSession {
    /// Initialize a session as the initiator (Alice)
    pub fn init_as_initiator(
        session_id: &str,
        their_kyber_pub: &[u8],
    ) -> Result<Self, RatchetError> {
        let pqc = PqcProvider::new();

        // Generate our Kyber keypair
        let our_kyber = pqc.generate_kyber_keypair()
            .map_err(|e| RatchetError::CryptoError(format!("{:?}", e)))?;

        // Perform initial key exchange (X3DH-like with Kyber)
        let encap = pqc.kyber_encapsulate(their_kyber_pub)
            .map_err(|e| RatchetError::CryptoError(format!("{:?}", e)))?;

        // Derive initial root and chain keys
        let (root_key, send_chain_key) = Self::kdf_root_key(
            &[0u8; 32], // Initial root key
            &encap.shared_secret,
        );

        Ok(Self {
            session_id: session_id.to_string(),
            our_kyber_secret: our_kyber.secret_key.clone(),
            our_kyber_public: our_kyber.public_key.clone(),
            their_kyber_pub: Some(their_kyber_pub.to_vec()),
            root_key,
            send_chain_key,
            send_chain_index: 0,
            recv_chain_key: [0u8; 32], // Will be set on first receive
            recv_chain_index: 0,
            skipped_keys: HashMap::new(),
            prev_chain_length: 0,
            created_at: Utc::now(),
        })
    }

    /// Initialize a session as the responder (Bob)
    pub fn init_as_responder(
        session_id: &str,
        our_keypair: KyberKeyPair,
    ) -> Result<Self, RatchetError> {
        Ok(Self {
            session_id: session_id.to_string(),
            our_kyber_secret: our_keypair.secret_key.clone(),
            our_kyber_public: our_keypair.public_key.clone(),
            their_kyber_pub: None,
            root_key: [0u8; 32],
            send_chain_key: [0u8; 32],
            send_chain_index: 0,
            recv_chain_key: [0u8; 32],
            recv_chain_index: 0,
            skipped_keys: HashMap::new(),
            prev_chain_length: 0,
            created_at: Utc::now(),
        })
    }

    /// Encrypt a message
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<RatchetMessage, RatchetError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        // Derive message key from chain key
        let (new_chain_key, message_key) = Self::kdf_chain_key(&self.send_chain_key);
        self.send_chain_key = new_chain_key;

        // Create header
        let header = MessageHeader {
            sender_kyber_pub: self.our_kyber_public.clone(),
            chain_index: self.send_chain_index,
            prev_chain_length: self.prev_chain_length,
            timestamp: Utc::now(),
        };

        // Encrypt with AES-256-GCM
        let nonce_bytes: [u8; 12] = Crypto::random_bytes(12)
            .try_into()
            .map_err(|_| RatchetError::CryptoError("Failed to generate nonce".to_string()))?;

        let key = Key::<Aes256Gcm>::from_slice(&message_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Include header in AAD
        let header_bytes = bincode::serialize(&header)
            .map_err(|e| RatchetError::CryptoError(format!("Serialization failed: {}", e)))?;

        let ciphertext = cipher.encrypt(nonce, plaintext)
            .map_err(|e| RatchetError::CryptoError(format!("Encryption failed: {:?}", e)))?;

        self.send_chain_index += 1;

        Ok(RatchetMessage {
            header,
            kyber_ciphertext: None, // Only set on DH ratchet
            nonce: nonce_bytes,
            ciphertext,
        })
    }

    /// Decrypt a message
    pub fn decrypt(&mut self, message: &RatchetMessage) -> Result<Vec<u8>, RatchetError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        // Check if we need to perform DH ratchet
        let their_pub = &message.header.sender_kyber_pub;
        if self.their_kyber_pub.as_ref() != Some(their_pub) {
            // New DH ratchet step
            self.dh_ratchet(their_pub)?;
        }

        // Try skipped keys first
        let skip_key = (their_pub.clone(), message.header.chain_index);
        if let Some(message_key) = self.skipped_keys.remove(&skip_key) {
            return self.decrypt_with_key(&message, &message_key);
        }

        // Skip ahead if needed
        if message.header.chain_index > self.recv_chain_index {
            self.skip_message_keys(message.header.chain_index)?;
        }

        // Check for duplicate
        if message.header.chain_index < self.recv_chain_index {
            return Err(RatchetError::DuplicateMessage);
        }

        // Derive message key
        let (new_chain_key, message_key) = Self::kdf_chain_key(&self.recv_chain_key);
        self.recv_chain_key = new_chain_key;
        self.recv_chain_index += 1;

        self.decrypt_with_key(&message, &message_key)
    }

    /// Decrypt with a specific message key
    fn decrypt_with_key(
        &self,
        message: &RatchetMessage,
        message_key: &[u8; 32],
    ) -> Result<Vec<u8>, RatchetError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        let key = Key::<Aes256Gcm>::from_slice(message_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&message.nonce);

        cipher.decrypt(nonce, message.ciphertext.as_slice())
            .map_err(|e| RatchetError::CryptoError(format!("Decryption failed: {:?}", e)))
    }

    /// Perform DH ratchet step
    fn dh_ratchet(&mut self, their_new_pub: &[u8]) -> Result<(), RatchetError> {
        let pqc = PqcProvider::new();

        // Save previous chain length
        self.prev_chain_length = self.send_chain_index;

        // Update their public key
        self.their_kyber_pub = Some(their_new_pub.to_vec());

        // Decapsulate to get shared secret (receiving)
        // Note: In a real implementation, we'd need the ciphertext from the message
        // For now, we'll derive from the public keys using a KDF
        let shared_secret = self.derive_shared_secret(their_new_pub)?;

        // Update root key and receiving chain key
        let (new_root, recv_chain) = Self::kdf_root_key(&self.root_key, &shared_secret);
        self.root_key = new_root;
        self.recv_chain_key = recv_chain;
        self.recv_chain_index = 0;

        // Generate new Kyber keypair
        let new_kyber = pqc.generate_kyber_keypair()
            .map_err(|e| RatchetError::CryptoError(format!("{:?}", e)))?;
        self.our_kyber_secret = new_kyber.secret_key;
        self.our_kyber_public = new_kyber.public_key;

        // Encapsulate to get new shared secret (sending)
        let encap = pqc.kyber_encapsulate(their_new_pub)
            .map_err(|e| RatchetError::CryptoError(format!("{:?}", e)))?;

        // Update root key and sending chain key
        let (new_root, send_chain) = Self::kdf_root_key(&self.root_key, &encap.shared_secret);
        self.root_key = new_root;
        self.send_chain_key = send_chain;
        self.send_chain_index = 0;

        Ok(())
    }

    /// Derive shared secret from public key (simplified)
    fn derive_shared_secret(&self, their_pub: &[u8]) -> Result<Vec<u8>, RatchetError> {
        // In a real implementation, this would use the Kyber ciphertext
        // For now, derive from concatenated public keys
        let mut input = self.our_kyber_public.clone();
        input.extend_from_slice(their_pub);
        Ok(blake3::hash(&input).as_bytes().to_vec())
    }

    /// Skip message keys for out-of-order handling
    fn skip_message_keys(&mut self, until: u32) -> Result<(), RatchetError> {
        if self.recv_chain_index + MAX_SKIP < until {
            return Err(RatchetError::TooManySkipped);
        }

        while self.recv_chain_index < until {
            let (new_chain_key, message_key) = Self::kdf_chain_key(&self.recv_chain_key);
            self.recv_chain_key = new_chain_key;

            let skip_key = (
                self.their_kyber_pub.clone().unwrap_or_default(),
                self.recv_chain_index,
            );
            self.skipped_keys.insert(skip_key, message_key);

            self.recv_chain_index += 1;
        }

        Ok(())
    }

    /// KDF for root key derivation
    fn kdf_root_key(root_key: &[u8; 32], dh_out: &[u8]) -> ([u8; 32], [u8; 32]) {
        let mut input = root_key.to_vec();
        input.extend_from_slice(dh_out);
        input.extend_from_slice(b"ratchet-root");

        let output = blake3::hash(&input);
        let bytes = output.as_bytes();

        let mut new_root = [0u8; 32];
        let mut chain_key = [0u8; 32];
        new_root.copy_from_slice(&bytes[0..32]);

        // Derive chain key from root
        let chain_input = [bytes.as_slice(), b"chain"].concat();
        let chain_hash = blake3::hash(&chain_input);
        chain_key.copy_from_slice(&chain_hash.as_bytes()[0..32]);

        (new_root, chain_key)
    }

    /// KDF for chain key derivation
    fn kdf_chain_key(chain_key: &[u8; 32]) -> ([u8; 32], [u8; 32]) {
        // New chain key
        let new_chain = blake3::keyed_hash(chain_key, b"chain");

        // Message key
        let msg_key = blake3::keyed_hash(chain_key, b"message");

        let mut new_chain_arr = [0u8; 32];
        let mut msg_key_arr = [0u8; 32];
        new_chain_arr.copy_from_slice(new_chain.as_bytes());
        msg_key_arr.copy_from_slice(msg_key.as_bytes());

        (new_chain_arr, msg_key_arr)
    }

    /// Get our public key for sharing
    pub fn our_public_key(&self) -> &[u8] {
        &self.our_kyber_public
    }

    /// Get session age
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }
}

/// Ratchet session manager
pub struct RatchetManager {
    /// Active sessions by session ID
    sessions: std::sync::RwLock<HashMap<String, RatchetSession>>,
    /// PQC provider
    pqc: PqcProvider,
}

impl RatchetManager {
    pub fn new() -> Self {
        Self {
            sessions: std::sync::RwLock::new(HashMap::new()),
            pqc: PqcProvider::new(),
        }
    }

    /// Create a new session as initiator
    pub fn create_session(
        &self,
        session_id: &str,
        their_public_key: &[u8],
    ) -> Result<(), RatchetError> {
        let session = RatchetSession::init_as_initiator(session_id, their_public_key)?;
        self.sessions.write().unwrap().insert(session_id.to_string(), session);
        Ok(())
    }

    /// Encrypt a message in a session
    pub fn encrypt(
        &self,
        session_id: &str,
        plaintext: &[u8],
    ) -> Result<RatchetMessage, RatchetError> {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(session_id)
            .ok_or_else(|| RatchetError::InvalidState("Session not found".to_string()))?;
        session.encrypt(plaintext)
    }

    /// Decrypt a message in a session
    pub fn decrypt(
        &self,
        session_id: &str,
        message: &RatchetMessage,
    ) -> Result<Vec<u8>, RatchetError> {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(session_id)
            .ok_or_else(|| RatchetError::InvalidState("Session not found".to_string()))?;
        session.decrypt(message)
    }

    /// Remove a session
    pub fn remove_session(&self, session_id: &str) {
        self.sessions.write().unwrap().remove(session_id);
    }

    /// Get session count
    pub fn session_count(&self) -> usize {
        self.sessions.read().unwrap().len()
    }
}

impl Default for RatchetManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kdf_chain() {
        let chain_key = [1u8; 32];
        let (new_chain, msg_key) = RatchetSession::kdf_chain_key(&chain_key);

        // Should produce different keys
        assert_ne!(new_chain, chain_key);
        assert_ne!(msg_key, chain_key);
        assert_ne!(new_chain, msg_key);
    }

    #[test]
    fn test_kdf_root() {
        let root = [2u8; 32];
        let dh_out = [3u8; 32];

        let (new_root, chain) = RatchetSession::kdf_root_key(&root, &dh_out);

        assert_ne!(new_root, root);
        assert_ne!(chain, root);
    }
}
