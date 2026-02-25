//! MiracleAuth - Post-Quantum Cryptography Security Suite
//!
//! A comprehensive, quantum-resistant security system providing:
//! - PQToken: Quantum-safe JWT replacement (Kyber-1024 + Dilithium-3)
//! - Device binding and geo-fencing
//! - Token revocation with bloom filters
//! - Automatic key rotation
//!
//! # Example
//!
//! ```rust,ignore
//! use miracledb::auth::{TokenManager, Scope};
//!
//! // Create token manager
//! let manager = TokenManager::new("my-service")?;
//!
//! // Issue tokens
//! let bundle = manager.issue_tokens(
//!     "user123",
//!     vec![Scope::new("users", "read")],
//!     device_fingerprint,
//!     trust_score,
//! ).await?;
//!
//! // Verify token
//! let claims = manager.verify(&bundle.access_token).await?;
//!
//! // Refresh tokens
//! let new_bundle = manager.refresh(&bundle.refresh_token, device_fingerprint).await?;
//! ```

pub mod pqtoken;
pub mod token_manager;
pub mod miracle_auth;

// Re-exports
pub use pqtoken::{
    PqToken,
    PqTokenClaims,
    PqTokenHeader,
    PqAlgorithm,
    TokenType,
    TokenFlags,
    TokenError,
    Scope,
    GeoBounds,
    ZkProof,
    PQTOKEN_MAGIC,
    PQTOKEN_VERSION,
};

pub use token_manager::{
    TokenManager,
    TokenBundle,
    ServerKeys,
    PublicKeyBundle,
};

pub use miracle_auth::{
    MiracleAuth,
    MiracleAuthConfig,
    MiracleAuthError,
    EncryptedData,
};
