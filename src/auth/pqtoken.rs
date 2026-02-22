//! PQToken - Post-Quantum Token Implementation
//!
//! A quantum-resistant replacement for JWT using:
//! - Kyber-1024 for key encapsulation (payload encryption)
//! - Dilithium-3 for digital signatures
//! - AES-256-GCM for symmetric encryption

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::crypto::Crypto;
use crate::security::pqc::PqcProvider;

/// Magic bytes identifying a PQToken
pub const PQTOKEN_MAGIC: [u8; 4] = *b"PQTK";

/// Current protocol version
pub const PQTOKEN_VERSION: u16 = 1;

/// Token algorithm
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PqAlgorithm {
    /// Kyber-1024 + Dilithium-3 + AES-256-GCM
    Kyber1024Dilithium3 = 1,
    /// Signature only (no encryption) - Dilithium-3
    Dilithium3Only = 2,
}

impl Default for PqAlgorithm {
    fn default() -> Self {
        Self::Kyber1024Dilithium3
    }
}

/// Token type
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum TokenType {
    /// Short-lived access token (15 minutes)
    Access = 1,
    /// Long-lived refresh token (7 days)
    Refresh = 2,
    /// Session token for persistent connections (24 hours)
    Session = 3,
    /// Device trust token (30 days)
    Device = 4,
    /// Recovery token (1 year)
    Recovery = 5,
    /// API key (no expiry, manual revocation)
    ApiKey = 6,
    /// Ephemeral one-time token (5 minutes)
    Ephemeral = 7,
}

impl Default for TokenType {
    fn default() -> Self {
        Self::Access
    }
}

impl TokenType {
    /// Get default lifetime in seconds for this token type
    pub fn default_lifetime_secs(&self) -> Option<i64> {
        match self {
            TokenType::Access => Some(15 * 60),           // 15 minutes
            TokenType::Refresh => Some(7 * 24 * 60 * 60), // 7 days
            TokenType::Session => Some(24 * 60 * 60),     // 24 hours
            TokenType::Device => Some(30 * 24 * 60 * 60), // 30 days
            TokenType::Recovery => Some(365 * 24 * 60 * 60), // 1 year
            TokenType::ApiKey => None,                    // No expiry
            TokenType::Ephemeral => Some(5 * 60),         // 5 minutes
        }
    }
}

bitflags::bitflags! {
    /// Token flags
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct TokenFlags: u16 {
        /// Claims are encrypted (default)
        const ENCRYPTED_CLAIMS = 0b0000_0001;
        /// Token is bound to device
        const DEVICE_BOUND = 0b0000_0010;
        /// Contains ZK proof
        const HAS_ZK_PROOF = 0b0000_0100;
        /// Geo-fenced token
        const GEO_FENCED = 0b0000_1000;
        /// Requires MFA for sensitive operations
        const MFA_REQUIRED = 0b0001_0000;
        /// Token can be used offline
        const OFFLINE_CAPABLE = 0b0010_0000;
        /// Token is part of a refresh chain
        const REFRESH_CHAIN = 0b0100_0000;
    }
}

impl Serialize for TokenFlags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.bits().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TokenFlags {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bits = u16::deserialize(deserializer)?;
        TokenFlags::from_bits(bits).ok_or_else(|| {
            serde::de::Error::custom(format!("invalid token flags: {}", bits))
        })
    }
}

impl Default for TokenFlags {
    fn default() -> Self {
        Self::ENCRYPTED_CLAIMS | Self::DEVICE_BOUND
    }
}

/// PQToken Header (48 bytes)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PqTokenHeader {
    /// Magic bytes "PQTK"
    pub magic: [u8; 4],
    /// Protocol version
    pub version: u16,
    /// Algorithm used
    pub algorithm: PqAlgorithm,
    /// Token type
    pub token_type: TokenType,
    /// Feature flags
    pub flags: TokenFlags,
    /// AES-GCM nonce (12 bytes)
    pub nonce: [u8; 12],
    /// Reserved for future use
    pub reserved: [u8; 26],
}

impl Default for PqTokenHeader {
    fn default() -> Self {
        Self {
            magic: PQTOKEN_MAGIC,
            version: PQTOKEN_VERSION,
            algorithm: PqAlgorithm::default(),
            token_type: TokenType::default(),
            flags: TokenFlags::default(),
            nonce: [0u8; 12],
            reserved: [0u8; 26],
        }
    }
}

/// Geographic bounds for geo-fenced tokens
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoBounds {
    /// Allowed countries (ISO 3166-1 alpha-2)
    pub allowed_countries: Vec<String>,
    /// Allowed regions/states
    pub allowed_regions: Vec<String>,
    /// Center point latitude (for radius-based)
    pub center_lat: Option<f64>,
    /// Center point longitude
    pub center_lon: Option<f64>,
    /// Radius in kilometers
    pub radius_km: Option<f64>,
}

/// Zero-knowledge proof attachment
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZkProof {
    /// Proof type (membership, range, etc.)
    pub proof_type: String,
    /// The proof bytes
    pub proof: Vec<u8>,
    /// Public inputs
    pub public_inputs: Vec<Vec<u8>>,
}

/// Permission scope
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    /// Resource (e.g., "users", "orders")
    pub resource: String,
    /// Action (e.g., "read", "write", "delete")
    pub action: String,
    /// Optional constraints
    pub constraints: Option<HashMap<String, String>>,
}

impl Scope {
    pub fn new(resource: &str, action: &str) -> Self {
        Self {
            resource: resource.to_string(),
            action: action.to_string(),
            constraints: None,
        }
    }

    pub fn with_constraint(mut self, key: &str, value: &str) -> Self {
        self.constraints
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Parse from string like "read:users" or "write:orders:own"
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        match parts.as_slice() {
            [action, resource] => Some(Self::new(resource, action)),
            [action, resource, constraint] => {
                Some(Self::new(resource, action).with_constraint("scope", constraint))
            }
            _ => None,
        }
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match &self.constraints {
            Some(c) if c.contains_key("scope") => {
                format!("{}:{}:{}", self.action, self.resource, c.get("scope").unwrap())
            }
            _ => format!("{}:{}", self.action, self.resource),
        }
    }
}

/// Token claims (encrypted in token)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PqTokenClaims {
    // Standard claims
    /// Subject (user ID)
    pub sub: String,
    /// Issuer
    pub iss: String,
    /// Audience
    pub aud: Vec<String>,
    /// Issued at (Unix timestamp)
    pub iat: i64,
    /// Expiration (Unix timestamp)
    pub exp: i64,
    /// Not before (Unix timestamp)
    pub nbf: i64,
    /// Unique token ID (for revocation)
    pub jti: Uuid,

    // Security claims
    /// Permission scopes
    pub scopes: Vec<Scope>,
    /// Device fingerprint hash (BLAKE3)
    pub device_hash: [u8; 32],
    /// Key version (for rotation)
    pub key_version: u32,
    /// Trust score (0.0 - 1.0)
    pub trust_score: f32,

    // Optional advanced claims
    /// Geographic bounds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_bounds: Option<GeoBounds>,
    /// Zero-knowledge proof
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zk_proof: Option<ZkProof>,
    /// Parent token ID (for refresh chains)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_token: Option<Uuid>,

    // Custom claims
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

impl PqTokenClaims {
    /// Create new claims for a user
    pub fn new(user_id: &str, issuer: &str) -> Self {
        let now = Utc::now().timestamp();
        Self {
            sub: user_id.to_string(),
            iss: issuer.to_string(),
            aud: vec![],
            iat: now,
            exp: now + TokenType::Access.default_lifetime_secs().unwrap_or(900),
            nbf: now,
            jti: Uuid::new_v4(),
            scopes: vec![],
            device_hash: [0u8; 32],
            key_version: 1,
            trust_score: 1.0,
            geo_bounds: None,
            zk_proof: None,
            parent_token: None,
            custom: HashMap::new(),
        }
    }

    /// Set expiration based on token type
    pub fn with_token_type(mut self, token_type: TokenType) -> Self {
        if let Some(lifetime) = token_type.default_lifetime_secs() {
            self.exp = self.iat + lifetime;
        } else {
            // No expiry (e.g., API keys)
            self.exp = i64::MAX;
        }
        self
    }

    /// Add audience
    pub fn with_audience(mut self, audience: &str) -> Self {
        self.aud.push(audience.to_string());
        self
    }

    /// Add scope
    pub fn with_scope(mut self, scope: Scope) -> Self {
        self.scopes.push(scope);
        self
    }

    /// Add multiple scopes from strings
    pub fn with_scopes_str(mut self, scopes: &[&str]) -> Self {
        for s in scopes {
            if let Some(scope) = Scope::parse(s) {
                self.scopes.push(scope);
            }
        }
        self
    }

    /// Set device hash
    pub fn with_device_hash(mut self, device_fingerprint: &[u8]) -> Self {
        self.device_hash = blake3::hash(device_fingerprint).into();
        self
    }

    /// Set trust score
    pub fn with_trust_score(mut self, score: f32) -> Self {
        self.trust_score = score.clamp(0.0, 1.0);
        self
    }

    /// Set geo bounds
    pub fn with_geo_bounds(mut self, bounds: GeoBounds) -> Self {
        self.geo_bounds = Some(bounds);
        self
    }

    /// Add custom claim
    pub fn with_custom<T: Serialize>(mut self, key: &str, value: T) -> Self {
        if let Ok(v) = serde_json::to_value(value) {
            self.custom.insert(key.to_string(), v);
        }
        self
    }

    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        Utc::now().timestamp() > self.exp
    }

    /// Check if token is not yet valid
    pub fn is_not_yet_valid(&self) -> bool {
        Utc::now().timestamp() < self.nbf
    }

    /// Check if token has a specific scope
    pub fn has_scope(&self, resource: &str, action: &str) -> bool {
        self.scopes.iter().any(|s| s.resource == resource && s.action == action)
    }
}

/// Complete PQToken structure
#[derive(Clone, Debug)]
pub struct PqToken {
    /// Token header
    pub header: PqTokenHeader,
    /// Kyber ciphertext (encapsulated session key)
    pub kyber_ciphertext: Vec<u8>,
    /// AES-GCM encrypted claims
    pub encrypted_claims: Vec<u8>,
    /// Dilithium signature
    pub signature: Vec<u8>,
}

impl PqToken {
    /// Create a new PQToken
    pub fn new(
        claims: &PqTokenClaims,
        token_type: TokenType,
        kyber_public_key: &[u8],
        dilithium_secret_key: &[u8],
        pqc: &PqcProvider,
    ) -> Result<Self, TokenError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        // 1. Create header
        let nonce_bytes: [u8; 12] = Crypto::random_bytes(12)
            .try_into()
            .map_err(|_| TokenError::CryptoError("Failed to generate nonce".to_string()))?;

        let mut header = PqTokenHeader::default();
        header.token_type = token_type;
        header.nonce = nonce_bytes;

        // 2. Encapsulate with Kyber to get shared secret
        let encap = pqc.kyber_encapsulate(kyber_public_key)
            .map_err(|e| TokenError::CryptoError(format!("Kyber encapsulation failed: {:?}", e)))?;

        // 3. Serialize claims
        let claims_bytes = serde_json::to_vec(claims)
            .map_err(|e| TokenError::SerializationError(e.to_string()))?;

        // 4. Encrypt claims with AES-256-GCM using shared secret
        let key = Key::<Aes256Gcm>::from_slice(&encap.shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let encrypted_claims = cipher.encrypt(nonce, claims_bytes.as_slice())
            .map_err(|e| TokenError::CryptoError(format!("AES encryption failed: {:?}", e)))?;

        // 5. Sign header + kyber_ciphertext + encrypted_claims
        let mut sign_data = Vec::new();
        sign_data.extend_from_slice(&header.magic);
        sign_data.extend_from_slice(&header.version.to_le_bytes());
        sign_data.push(header.algorithm as u8);
        sign_data.push(header.token_type as u8);
        sign_data.extend_from_slice(&header.flags.bits().to_le_bytes());
        sign_data.extend_from_slice(&header.nonce);
        sign_data.extend_from_slice(&encap.ciphertext);
        sign_data.extend_from_slice(&encrypted_claims);

        let signature = pqc.dilithium_sign(&sign_data, dilithium_secret_key)
            .map_err(|e| TokenError::CryptoError(format!("Dilithium signing failed: {:?}", e)))?;

        Ok(Self {
            header,
            kyber_ciphertext: encap.ciphertext,
            encrypted_claims,
            signature,
        })
    }

    /// Verify and decrypt a PQToken
    pub fn verify_and_decrypt(
        &self,
        kyber_secret_key: &[u8],
        dilithium_public_key: &[u8],
        pqc: &PqcProvider,
    ) -> Result<PqTokenClaims, TokenError> {
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use aes_gcm::aead::{Aead, KeyInit};

        // 1. Verify magic
        if self.header.magic != PQTOKEN_MAGIC {
            return Err(TokenError::InvalidToken("Invalid magic bytes".to_string()));
        }

        // 2. Reconstruct signed data
        let mut sign_data = Vec::new();
        sign_data.extend_from_slice(&self.header.magic);
        sign_data.extend_from_slice(&self.header.version.to_le_bytes());
        sign_data.push(self.header.algorithm as u8);
        sign_data.push(self.header.token_type as u8);
        sign_data.extend_from_slice(&self.header.flags.bits().to_le_bytes());
        sign_data.extend_from_slice(&self.header.nonce);
        sign_data.extend_from_slice(&self.kyber_ciphertext);
        sign_data.extend_from_slice(&self.encrypted_claims);

        // 3. Verify Dilithium signature
        let valid = pqc.dilithium_verify(&sign_data, &self.signature, dilithium_public_key)
            .map_err(|e| TokenError::CryptoError(format!("Signature verification failed: {:?}", e)))?;

        if !valid {
            return Err(TokenError::InvalidSignature);
        }

        // 4. Decapsulate Kyber to get shared secret
        let shared_secret = pqc.kyber_decapsulate(&self.kyber_ciphertext, kyber_secret_key)
            .map_err(|e| TokenError::CryptoError(format!("Kyber decapsulation failed: {:?}", e)))?;

        // 5. Decrypt claims
        let key = Key::<Aes256Gcm>::from_slice(&shared_secret);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&self.header.nonce);

        let claims_bytes = cipher.decrypt(nonce, self.encrypted_claims.as_slice())
            .map_err(|e| TokenError::CryptoError(format!("AES decryption failed: {:?}", e)))?;

        // 6. Deserialize claims
        let claims: PqTokenClaims = serde_json::from_slice(&claims_bytes)
            .map_err(|e| TokenError::SerializationError(e.to_string()))?;

        // 7. Validate claims
        if claims.is_expired() {
            return Err(TokenError::Expired);
        }

        if claims.is_not_yet_valid() {
            return Err(TokenError::NotYetValid);
        }

        Ok(claims)
    }

    /// Serialize token to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Header (48 bytes)
        bytes.extend_from_slice(&self.header.magic);
        bytes.extend_from_slice(&self.header.version.to_le_bytes());
        bytes.push(self.header.algorithm as u8);
        bytes.push(self.header.token_type as u8);
        bytes.extend_from_slice(&self.header.flags.bits().to_le_bytes());
        bytes.extend_from_slice(&self.header.nonce);
        bytes.extend_from_slice(&self.header.reserved);

        // Lengths
        bytes.extend_from_slice(&(self.kyber_ciphertext.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.encrypted_claims.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.signature.len() as u32).to_le_bytes());

        // Data
        bytes.extend_from_slice(&self.kyber_ciphertext);
        bytes.extend_from_slice(&self.encrypted_claims);
        bytes.extend_from_slice(&self.signature);

        bytes
    }

    /// Deserialize token from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TokenError> {
        if bytes.len() < 60 {
            return Err(TokenError::InvalidToken("Token too short".to_string()));
        }

        // Parse header
        let magic: [u8; 4] = bytes[0..4].try_into().unwrap();
        if magic != PQTOKEN_MAGIC {
            return Err(TokenError::InvalidToken("Invalid magic bytes".to_string()));
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let algorithm = match bytes[6] {
            1 => PqAlgorithm::Kyber1024Dilithium3,
            2 => PqAlgorithm::Dilithium3Only,
            _ => return Err(TokenError::InvalidToken("Unknown algorithm".to_string())),
        };
        let token_type = match bytes[7] {
            1 => TokenType::Access,
            2 => TokenType::Refresh,
            3 => TokenType::Session,
            4 => TokenType::Device,
            5 => TokenType::Recovery,
            6 => TokenType::ApiKey,
            7 => TokenType::Ephemeral,
            _ => return Err(TokenError::InvalidToken("Unknown token type".to_string())),
        };
        let flags = TokenFlags::from_bits(u16::from_le_bytes(bytes[8..10].try_into().unwrap()))
            .unwrap_or(TokenFlags::default());
        let nonce: [u8; 12] = bytes[10..22].try_into().unwrap();
        let reserved: [u8; 26] = bytes[22..48].try_into().unwrap();

        // Parse lengths
        let kyber_len = u32::from_le_bytes(bytes[48..52].try_into().unwrap()) as usize;
        let claims_len = u32::from_le_bytes(bytes[52..56].try_into().unwrap()) as usize;
        let sig_len = u32::from_le_bytes(bytes[56..60].try_into().unwrap()) as usize;

        let expected_len = 60 + kyber_len + claims_len + sig_len;
        if bytes.len() != expected_len {
            return Err(TokenError::InvalidToken(format!(
                "Expected {} bytes, got {}",
                expected_len,
                bytes.len()
            )));
        }

        // Parse data
        let kyber_start = 60;
        let claims_start = kyber_start + kyber_len;
        let sig_start = claims_start + claims_len;

        Ok(Self {
            header: PqTokenHeader {
                magic,
                version,
                algorithm,
                token_type,
                flags,
                nonce,
                reserved,
            },
            kyber_ciphertext: bytes[kyber_start..claims_start].to_vec(),
            encrypted_claims: bytes[claims_start..sig_start].to_vec(),
            signature: bytes[sig_start..].to_vec(),
        })
    }

    /// Serialize to base64url for HTTP transport
    pub fn to_base64url(&self) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        URL_SAFE_NO_PAD.encode(self.to_bytes())
    }

    /// Deserialize from base64url
    pub fn from_base64url(s: &str) -> Result<Self, TokenError> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let bytes = URL_SAFE_NO_PAD.decode(s)
            .map_err(|e| TokenError::InvalidToken(format!("Base64 decode failed: {}", e)))?;
        Self::from_bytes(&bytes)
    }

    /// Get 64-bit fingerprint for fast cache lookup
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        // Use first 24 bytes of token for fingerprint
        let mut hasher = DefaultHasher::new();
        self.header.magic.hash(&mut hasher);
        self.header.version.hash(&mut hasher);
        (self.header.algorithm as u8).hash(&mut hasher);
        (self.header.token_type as u8).hash(&mut hasher);
        self.header.flags.bits().hash(&mut hasher);
        self.header.nonce.hash(&mut hasher);
        hasher.finish()
    }
}

/// Token errors
#[derive(Debug, Clone)]
pub enum TokenError {
    InvalidToken(String),
    InvalidSignature,
    Expired,
    NotYetValid,
    Revoked,
    DeviceMismatch,
    GeoFenceViolation,
    InsufficientTrustScore,
    CryptoError(String),
    SerializationError(String),
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::InvalidToken(msg) => write!(f, "Invalid token: {}", msg),
            TokenError::InvalidSignature => write!(f, "Invalid signature"),
            TokenError::Expired => write!(f, "Token expired"),
            TokenError::NotYetValid => write!(f, "Token not yet valid"),
            TokenError::Revoked => write!(f, "Token revoked"),
            TokenError::DeviceMismatch => write!(f, "Device mismatch"),
            TokenError::GeoFenceViolation => write!(f, "Geo-fence violation"),
            TokenError::InsufficientTrustScore => write!(f, "Insufficient trust score"),
            TokenError::CryptoError(msg) => write!(f, "Crypto error: {}", msg),
            TokenError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for TokenError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_parse() {
        let scope = Scope::parse("read:users").unwrap();
        assert_eq!(scope.resource, "users");
        assert_eq!(scope.action, "read");

        let scope = Scope::parse("write:orders:own").unwrap();
        assert_eq!(scope.resource, "orders");
        assert_eq!(scope.action, "write");
        assert_eq!(scope.constraints.as_ref().unwrap().get("scope"), Some(&"own".to_string()));
    }

    #[test]
    fn test_claims_builder() {
        let claims = PqTokenClaims::new("user123", "miracle-auth")
            .with_audience("api.example.com")
            .with_scopes_str(&["read:users", "write:orders"])
            .with_trust_score(0.95)
            .with_custom("tenant_id", "tenant_abc");

        assert_eq!(claims.sub, "user123");
        assert_eq!(claims.iss, "miracle-auth");
        assert_eq!(claims.aud, vec!["api.example.com"]);
        assert_eq!(claims.scopes.len(), 2);
        assert_eq!(claims.trust_score, 0.95);
        assert!(claims.custom.contains_key("tenant_id"));
    }

    #[test]
    fn test_token_type_lifetime() {
        assert_eq!(TokenType::Access.default_lifetime_secs(), Some(15 * 60));
        assert_eq!(TokenType::Refresh.default_lifetime_secs(), Some(7 * 24 * 60 * 60));
        assert_eq!(TokenType::ApiKey.default_lifetime_secs(), None);
    }
}
