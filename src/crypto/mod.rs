//! Crypto Module - Cryptographic utilities

use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Sha512, Digest};
use hmac::{Hmac, Mac};

type HmacSha256 = Hmac<Sha256>;

/// Hash algorithms
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum HashAlgorithm {
    Sha256,
    Sha512,
    Blake3,
    Md5,
}

/// Crypto utilities
pub struct Crypto;

impl Crypto {
    /// Hash data with specified algorithm
    pub fn hash(data: &[u8], algorithm: HashAlgorithm) -> Vec<u8> {
        match algorithm {
            HashAlgorithm::Sha256 => Sha256::digest(data).to_vec(),
            HashAlgorithm::Sha512 => Sha512::digest(data).to_vec(),
            HashAlgorithm::Blake3 => blake3::hash(data).as_bytes().to_vec(),
            HashAlgorithm::Md5 => {
                md5::compute(data).0.to_vec()
            }
        }
    }

    /// Hash to hex string
    pub fn hash_hex(data: &[u8], algorithm: HashAlgorithm) -> String {
        hex::encode(Self::hash(data, algorithm))
    }

    /// HMAC-SHA256
    pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key)
            .expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Verify HMAC
    pub fn verify_hmac(key: &[u8], data: &[u8], signature: &[u8]) -> bool {
        let computed = Self::hmac_sha256(key, data);
        constant_time_eq(&computed, signature)
    }

    /// Generate random bytes
    pub fn random_bytes(len: usize) -> Vec<u8> {
        use ring::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        let mut bytes = vec![0u8; len];
        rng.fill(&mut bytes).expect("Failed to generate random bytes");
        bytes
    }

    /// Generate random hex string
    pub fn random_hex(len: usize) -> String {
        hex::encode(Self::random_bytes(len))
    }

    /// Generate UUID v4
    pub fn uuid() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Base64 encode
    pub fn base64_encode(data: &[u8]) -> String {
        use base64::{Engine, engine::general_purpose::STANDARD};
        STANDARD.encode(data)
    }

    /// Base64 decode
    pub fn base64_decode(data: &str) -> Result<Vec<u8>, String> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        STANDARD.decode(data).map_err(|e| e.to_string())
    }

    /// Derive key using PBKDF2
    pub fn derive_key(password: &[u8], salt: &[u8], iterations: u32, key_len: usize) -> Vec<u8> {
        use ring::pbkdf2;
        let mut key = vec![0u8; key_len];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            std::num::NonZeroU32::new(iterations).unwrap(),
            salt,
            password,
            &mut key,
        );
        key
    }

    /// Hash password for storage
    pub fn hash_password(password: &str) -> String {
        let salt = Self::random_bytes(16);
        let key = Self::derive_key(password.as_bytes(), &salt, 100_000, 32);
        format!("{}${}", hex::encode(&salt), hex::encode(&key))
    }

    /// Verify password against hash
    pub fn verify_password(password: &str, hash: &str) -> bool {
        let parts: Vec<&str> = hash.split('$').collect();
        if parts.len() != 2 {
            return false;
        }

        let salt = match hex::decode(parts[0]) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let stored_key = match hex::decode(parts[1]) {
            Ok(k) => k,
            Err(_) => return false,
        };

        let computed_key = Self::derive_key(password.as_bytes(), &salt, 100_000, 32);
        constant_time_eq(&computed_key, &stored_key)
    }
}

/// Constant-time comparison
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

/// JWT Token
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    pub sub: String,
    pub exp: i64,
    pub iat: i64,
    pub roles: Vec<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// JWT utilities
pub struct Jwt;

impl Jwt {
    /// Create a JWT token
    pub fn create(claims: &JwtClaims, secret: &[u8]) -> String {
        let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});
        
        let header_b64 = Crypto::base64_encode(header.to_string().as_bytes());
        let claims_b64 = Crypto::base64_encode(serde_json::to_string(claims).unwrap().as_bytes());
        
        let message = format!("{}.{}", header_b64, claims_b64);
        let signature = Crypto::hmac_sha256(secret, message.as_bytes());
        let signature_b64 = Crypto::base64_encode(&signature);
        
        format!("{}.{}", message, signature_b64)
    }

    /// Verify and decode a JWT token
    pub fn verify(token: &str, secret: &[u8]) -> Result<JwtClaims, String> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err("Invalid token format".to_string());
        }

        let message = format!("{}.{}", parts[0], parts[1]);
        let signature = Crypto::base64_decode(parts[2])?;
        
        if !Crypto::verify_hmac(secret, message.as_bytes(), &signature) {
            return Err("Invalid signature".to_string());
        }

        let claims_json = Crypto::base64_decode(parts[1])?;
        let claims: JwtClaims = serde_json::from_slice(&claims_json)
            .map_err(|e| format!("Invalid claims: {}", e))?;

        // Check expiration
        let now = chrono::Utc::now().timestamp();
        if claims.exp < now {
            return Err("Token expired".to_string());
        }

        Ok(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let data = b"hello world";
        let hash = Crypto::hash_hex(data, HashAlgorithm::Sha256);
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_password() {
        let password = "my_secret_password";
        let hash = Crypto::hash_password(password);
        assert!(Crypto::verify_password(password, &hash));
        assert!(!Crypto::verify_password("wrong_password", &hash));
    }

    #[test]
    fn test_jwt() {
        let secret = b"secret_key";
        let claims = JwtClaims {
            sub: "user123".to_string(),
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            roles: vec!["admin".to_string()],
            extra: HashMap::new(),
        };

        let token = Jwt::create(&claims, secret);
        let verified = Jwt::verify(&token, secret).unwrap();
        
        assert_eq!(verified.sub, "user123");
    }
}
