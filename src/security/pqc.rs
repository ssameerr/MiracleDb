//! Post-Quantum Cryptography Provider
//!
//! Implements NIST-standardized PQC algorithms:
//! - Kyber-1024 for key encapsulation (key exchange)
//! - Dilithium for digital signatures

use tracing::{info, error};
use super::SecurityError;

/// Post-Quantum Cryptography provider
pub struct PqcProvider {
    /// Whether PQC is enabled
    enabled: bool,
}

impl PqcProvider {
    pub fn new() -> Self {
        Self { enabled: true }
    }

    /// Generate cryptographically secure random bytes using hardware RNG
    fn random_bytes(&self, len: usize) -> Vec<u8> {
        use ring::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        let mut bytes = vec![0u8; len];
        rng.fill(&mut bytes).expect("Failed to generate secure random bytes");
        bytes
    }

    /// Generate a Kyber key pair for key encapsulation
    pub fn generate_kyber_keypair(&self) -> Result<KyberKeyPair, SecurityError> {
        if !self.enabled {
            return Err(SecurityError::EncryptionError("PQC is disabled".to_string()));
        }

        use pqcrypto_traits::kem::{Ciphertext, SharedSecret, PublicKey, SecretKey};
        
        // Use real Kyber-1024 implementation
        let (pk, sk) = pqcrypto_kyber::kyber1024::keypair();
        
        info!("Generated Kyber-1024 key pair");
        
        Ok(KyberKeyPair {
            public_key: pk.as_bytes().to_vec(),
            secret_key: sk.as_bytes().to_vec(),
        })
    }

    /// Encapsulate a shared secret using recipient's public key
    pub fn kyber_encapsulate(&self, public_key: &[u8]) -> Result<KyberEncapsulation, SecurityError> {
        use pqcrypto_traits::kem::{Ciphertext, SharedSecret, PublicKey};
        
        let pk = pqcrypto_kyber::kyber1024::PublicKey::from_bytes(public_key)
            .map_err(|e| SecurityError::EncryptionError(format!("Invalid Kyber public key: {:?}", e)))?;
            
        let (ss, ct) = pqcrypto_kyber::kyber1024::encapsulate(&pk);
        
        Ok(KyberEncapsulation {
            shared_secret: ss.as_bytes().to_vec(),
            ciphertext: ct.as_bytes().to_vec(),
        })
    }

    /// Decapsulate to recover shared secret
    pub fn kyber_decapsulate(&self, ciphertext: &[u8], secret_key: &[u8]) -> Result<Vec<u8>, SecurityError> {
        use pqcrypto_traits::kem::{Ciphertext, SharedSecret, SecretKey};

        let sk = pqcrypto_kyber::kyber1024::SecretKey::from_bytes(secret_key)
            .map_err(|e| SecurityError::DecryptionError(format!("Invalid Kyber secret key: {:?}", e)))?;
            
        let ct = pqcrypto_kyber::kyber1024::Ciphertext::from_bytes(ciphertext)
            .map_err(|e| SecurityError::DecryptionError(format!("Invalid Kyber ciphertext: {:?}", e)))?;
            
        let ss = pqcrypto_kyber::kyber1024::decapsulate(&ct, &sk);
        
        Ok(ss.as_bytes().to_vec())
    }

    /// Generate a Dilithium key pair for signatures
    pub fn generate_dilithium_keypair(&self) -> Result<DilithiumKeyPair, SecurityError> {
        use pqcrypto_traits::sign::{PublicKey, SecretKey};
        
        let (pk, sk) = pqcrypto_dilithium::dilithium3::keypair();
        
        Ok(DilithiumKeyPair {
            public_key: pk.as_bytes().to_vec(),
            secret_key: sk.as_bytes().to_vec(),
        })
    }

    /// Sign a message using Dilithium
    pub fn dilithium_sign(&self, message: &[u8], secret_key: &[u8]) -> Result<Vec<u8>, SecurityError> {
        use pqcrypto_traits::sign::{SecretKey, SignedMessage};
        
        let sk = pqcrypto_dilithium::dilithium3::SecretKey::from_bytes(secret_key)
            .map_err(|e| SecurityError::EncryptionError(format!("Invalid Dilithium secret key: {:?}", e)))?;
            
        let signature = pqcrypto_dilithium::dilithium3::sign(message, &sk);
        
        Ok(signature.as_bytes().to_vec())
    }

    /// Verify a Dilithium signature
    pub fn dilithium_verify(&self, message: &[u8], signature: &[u8], public_key: &[u8]) -> Result<bool, SecurityError> {
        use pqcrypto_traits::sign::{PublicKey, SignedMessage, DetachedSignature};
        
        let pk = pqcrypto_dilithium::dilithium3::PublicKey::from_bytes(public_key)
            .map_err(|e| SecurityError::DecryptionError(format!("Invalid Dilithium public key: {:?}", e)))?;
            
        // Handle attached signature (which contains the message)
        let signed_msg = pqcrypto_dilithium::dilithium3::SignedMessage::from_bytes(signature)
            .map_err(|e| SecurityError::DecryptionError(format!("Invalid signature format: {:?}", e)))?;
            
        let open_res = pqcrypto_dilithium::dilithium3::open(&signed_msg, &pk);
        
        match open_res {
            Ok(opened_msg) => {
                // Verify the opened message matches what we expected
                Ok(opened_msg == message)
            }
            Err(_) => Ok(false)
        }
    }

    /// Hybrid encryption: Combine PQC with classical AES-GCM
    pub fn hybrid_encrypt(&self, plaintext: &[u8], pqc_pubkey: &[u8], _classical_pubkey: &[u8]) -> Result<HybridCiphertext, SecurityError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit}; // Use KeyInit trait
        
        // 1. Encapsulate with Kyber (PQC)
        let pqc_encap = self.kyber_encapsulate(pqc_pubkey)?;
        
        // 2. Use shared secret as AES key
        let key = Key::<Aes256Gcm>::from_slice(&pqc_encap.shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce_bytes = self.random_bytes(12);
        let nonce = Nonce::from_slice(&nonce_bytes);
        
        let encrypted_data = cipher.encrypt(nonce, plaintext)
            .map_err(|e| SecurityError::EncryptionError(format!("AES encryption failed: {:?}", e)))?;
            
        // Combine nonce + ciphertext
        let mut final_ciphertext = nonce_bytes;
        final_ciphertext.extend(encrypted_data);
        
        Ok(HybridCiphertext {
            pqc_ciphertext: pqc_encap.ciphertext,
            classical_ciphertext: vec![], 
            encrypted_data: final_ciphertext,
        })
    }
}

pub struct KyberKeyPair { pub public_key: Vec<u8>, pub secret_key: Vec<u8> }
pub struct KyberEncapsulation { pub shared_secret: Vec<u8>, pub ciphertext: Vec<u8> }
pub struct DilithiumKeyPair { pub public_key: Vec<u8>, pub secret_key: Vec<u8> }
pub struct HybridCiphertext { pub pqc_ciphertext: Vec<u8>, pub classical_ciphertext: Vec<u8>, pub encrypted_data: Vec<u8> }

impl Default for PqcProvider {
    fn default() -> Self { Self::new() }
}
