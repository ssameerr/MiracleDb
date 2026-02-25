//! Token Manager - Issue, verify, refresh, and revoke PQTokens

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use crate::security::pqc::{PqcProvider, KyberKeyPair, DilithiumKeyPair};
use super::pqtoken::{PqToken, PqTokenClaims, TokenType, TokenError, Scope};

/// Server key pair for signing/verifying tokens
pub struct ServerKeys {
    /// Kyber keypair for token encryption
    pub kyber: KyberKeyPair,
    /// Dilithium keypair for token signing
    pub dilithium: DilithiumKeyPair,
    /// Key version (increments on rotation)
    pub version: u32,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
}

impl ServerKeys {
    pub fn generate(pqc: &PqcProvider) -> Result<Self, TokenError> {
        let kyber = pqc.generate_kyber_keypair()
            .map_err(|e| TokenError::CryptoError(format!("Failed to generate Kyber keypair: {:?}", e)))?;
        let dilithium = pqc.generate_dilithium_keypair()
            .map_err(|e| TokenError::CryptoError(format!("Failed to generate Dilithium keypair: {:?}", e)))?;

        Ok(Self {
            kyber,
            dilithium,
            version: 1,
            created_at: Utc::now(),
        })
    }
}

/// Token bundle returned on login
#[derive(Clone, Debug)]
pub struct TokenBundle {
    /// Access token (short-lived)
    pub access_token: String,
    /// Refresh token (long-lived)
    pub refresh_token: String,
    /// Device token (optional, for trusted devices)
    pub device_token: Option<String>,
    /// Access token expiration
    pub expires_at: DateTime<Utc>,
    /// Token type
    pub token_type: String,
}

/// Revocation entry
#[derive(Clone, Debug)]
struct RevocationEntry {
    pub token_id: Uuid,
    pub revoked_at: DateTime<Utc>,
    pub reason: String,
    /// Original expiration (for cleanup)
    pub original_exp: DateTime<Utc>,
}

/// Refresh token tracking
#[derive(Clone, Debug)]
struct RefreshTokenEntry {
    pub token_id: Uuid,
    pub user_id: String,
    pub device_hash: [u8; 32],
    pub created_at: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub use_count: u32,
    /// Chain of tokens this refresh has spawned
    pub child_tokens: Vec<Uuid>,
}

/// Token Manager
pub struct TokenManager {
    /// PQC provider
    pqc: Arc<PqcProvider>,
    /// Current server keys
    current_keys: Arc<RwLock<ServerKeys>>,
    /// Previous server keys (for rotation grace period)
    previous_keys: Arc<RwLock<Option<ServerKeys>>>,
    /// Revocation list (in production, use Redis/database)
    revocations: Arc<RwLock<HashMap<Uuid, RevocationEntry>>>,
    /// Active refresh tokens
    refresh_tokens: Arc<RwLock<HashMap<Uuid, RefreshTokenEntry>>>,
    /// Issuer name
    issuer: String,
    /// Default audience
    default_audience: Vec<String>,
}

impl TokenManager {
    /// Create a new token manager
    pub fn new(issuer: &str) -> Result<Self, TokenError> {
        let pqc = Arc::new(PqcProvider::new());
        let keys = ServerKeys::generate(&pqc)?;

        Ok(Self {
            pqc,
            current_keys: Arc::new(RwLock::new(keys)),
            previous_keys: Arc::new(RwLock::new(None)),
            revocations: Arc::new(RwLock::new(HashMap::new())),
            refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
            issuer: issuer.to_string(),
            default_audience: vec![],
        })
    }

    /// Set default audience
    pub fn with_audience(mut self, audience: Vec<String>) -> Self {
        self.default_audience = audience;
        self
    }

    /// Issue a token bundle for a user
    pub async fn issue_tokens(
        &self,
        user_id: &str,
        scopes: Vec<Scope>,
        device_fingerprint: &[u8],
        trust_score: f32,
    ) -> Result<TokenBundle, TokenError> {
        let keys = self.current_keys.read().await;
        let device_hash: [u8; 32] = blake3::hash(device_fingerprint).into();

        // Create access token claims
        let access_claims = PqTokenClaims::new(user_id, &self.issuer)
            .with_token_type(TokenType::Access)
            .with_device_hash(device_fingerprint)
            .with_trust_score(trust_score);

        let mut access_claims = access_claims;
        for aud in &self.default_audience {
            access_claims = access_claims.with_audience(aud);
        }
        for scope in &scopes {
            access_claims = access_claims.with_scope(scope.clone());
        }

        // Create refresh token claims
        let refresh_claims = PqTokenClaims::new(user_id, &self.issuer)
            .with_token_type(TokenType::Refresh)
            .with_device_hash(device_fingerprint)
            .with_trust_score(trust_score);

        // Create tokens
        let access_token = PqToken::new(
            &access_claims,
            TokenType::Access,
            &keys.kyber.public_key,
            &keys.dilithium.secret_key,
            &self.pqc,
        )?;

        let refresh_token = PqToken::new(
            &refresh_claims,
            TokenType::Refresh,
            &keys.kyber.public_key,
            &keys.dilithium.secret_key,
            &self.pqc,
        )?;

        // Track refresh token
        {
            let mut refresh_tokens = self.refresh_tokens.write().await;
            refresh_tokens.insert(refresh_claims.jti, RefreshTokenEntry {
                token_id: refresh_claims.jti,
                user_id: user_id.to_string(),
                device_hash,
                created_at: Utc::now(),
                last_used: Utc::now(),
                use_count: 0,
                child_tokens: vec![access_claims.jti],
            });
        }

        let expires_at = DateTime::from_timestamp(access_claims.exp, 0)
            .unwrap_or_else(|| Utc::now());

        Ok(TokenBundle {
            access_token: access_token.to_base64url(),
            refresh_token: refresh_token.to_base64url(),
            device_token: None,
            expires_at,
            token_type: "Bearer".to_string(),
        })
    }

    /// Verify a token and return claims
    pub async fn verify(&self, token_str: &str) -> Result<PqTokenClaims, TokenError> {
        let token = PqToken::from_base64url(token_str)?;

        // Try current keys first
        let keys = self.current_keys.read().await;
        let result = token.verify_and_decrypt(
            &keys.kyber.secret_key,
            &keys.dilithium.public_key,
            &self.pqc,
        );

        let claims = match result {
            Ok(claims) => claims,
            Err(_) => {
                // Try previous keys (rotation grace period)
                drop(keys);
                let prev_keys = self.previous_keys.read().await;
                if let Some(ref prev) = *prev_keys {
                    token.verify_and_decrypt(
                        &prev.kyber.secret_key,
                        &prev.dilithium.public_key,
                        &self.pqc,
                    )?
                } else {
                    return Err(TokenError::InvalidSignature);
                }
            }
        };

        // Check revocation
        {
            let revocations = self.revocations.read().await;
            if revocations.contains_key(&claims.jti) {
                return Err(TokenError::Revoked);
            }
        }

        Ok(claims)
    }

    /// Verify token and check device binding
    pub async fn verify_with_device(
        &self,
        token_str: &str,
        device_fingerprint: &[u8],
    ) -> Result<PqTokenClaims, TokenError> {
        let claims = self.verify(token_str).await?;

        // Check device binding
        let device_hash: [u8; 32] = blake3::hash(device_fingerprint).into();
        if claims.device_hash != device_hash {
            return Err(TokenError::DeviceMismatch);
        }

        Ok(claims)
    }

    /// Refresh tokens using a refresh token
    pub async fn refresh(
        &self,
        refresh_token_str: &str,
        device_fingerprint: &[u8],
    ) -> Result<TokenBundle, TokenError> {
        // Verify refresh token
        let claims = self.verify_with_device(refresh_token_str, device_fingerprint).await?;

        // Check it's actually a refresh token
        let token = PqToken::from_base64url(refresh_token_str)?;
        if token.header.token_type != TokenType::Refresh {
            return Err(TokenError::InvalidToken("Not a refresh token".to_string()));
        }

        // Update refresh token tracking
        {
            let mut refresh_tokens = self.refresh_tokens.write().await;
            if let Some(entry) = refresh_tokens.get_mut(&claims.jti) {
                entry.last_used = Utc::now();
                entry.use_count += 1;

                // Implement refresh token rotation (optional: revoke old refresh token)
                // For now, we allow reuse but track it
            }
        }

        // Get scopes from original token
        let scopes = claims.scopes.clone();

        // Issue new tokens
        self.issue_tokens(
            &claims.sub,
            scopes,
            device_fingerprint,
            claims.trust_score,
        ).await
    }

    /// Revoke a token
    pub async fn revoke(&self, token_id: Uuid, reason: &str) -> Result<(), TokenError> {
        let mut revocations = self.revocations.write().await;
        revocations.insert(token_id, RevocationEntry {
            token_id,
            revoked_at: Utc::now(),
            reason: reason.to_string(),
            original_exp: Utc::now() + chrono::Duration::days(30), // Cleanup after 30 days
        });

        // Also revoke from refresh tokens if it's a refresh token
        let mut refresh_tokens = self.refresh_tokens.write().await;
        if let Some(entry) = refresh_tokens.remove(&token_id) {
            // Revoke all child tokens
            for child_id in entry.child_tokens {
                revocations.insert(child_id, RevocationEntry {
                    token_id: child_id,
                    revoked_at: Utc::now(),
                    reason: format!("Parent refresh token revoked: {}", reason),
                    original_exp: Utc::now() + chrono::Duration::days(1),
                });
            }
        }

        Ok(())
    }

    /// Revoke all tokens for a user
    pub async fn revoke_all_for_user(&self, user_id: &str, reason: &str) -> Result<u32, TokenError> {
        let mut refresh_tokens = self.refresh_tokens.write().await;
        let mut revocations = self.revocations.write().await;

        let mut count = 0;
        let tokens_to_remove: Vec<_> = refresh_tokens
            .iter()
            .filter(|(_, entry)| entry.user_id == user_id)
            .map(|(id, entry)| (*id, entry.child_tokens.clone()))
            .collect();

        for (token_id, child_tokens) in tokens_to_remove {
            refresh_tokens.remove(&token_id);
            revocations.insert(token_id, RevocationEntry {
                token_id,
                revoked_at: Utc::now(),
                reason: reason.to_string(),
                original_exp: Utc::now() + chrono::Duration::days(30),
            });
            count += 1;

            for child_id in child_tokens {
                revocations.insert(child_id, RevocationEntry {
                    token_id: child_id,
                    revoked_at: Utc::now(),
                    reason: format!("User tokens revoked: {}", reason),
                    original_exp: Utc::now() + chrono::Duration::days(1),
                });
                count += 1;
            }
        }

        Ok(count)
    }

    /// Check if a token ID is revoked
    pub async fn is_revoked(&self, token_id: &Uuid) -> bool {
        let revocations = self.revocations.read().await;
        revocations.contains_key(token_id)
    }

    /// Rotate server keys
    pub async fn rotate_keys(&self) -> Result<u32, TokenError> {
        let new_keys = ServerKeys::generate(&self.pqc)?;
        let new_version = {
            let keys = self.current_keys.read().await;
            keys.version + 1
        };

        // Move current to previous
        let mut prev = self.previous_keys.write().await;
        let mut curr = self.current_keys.write().await;

        *prev = Some(std::mem::replace(&mut *curr, ServerKeys {
            kyber: new_keys.kyber,
            dilithium: new_keys.dilithium,
            version: new_version,
            created_at: Utc::now(),
        }));

        Ok(new_version)
    }

    /// Cleanup expired revocations (call periodically)
    pub async fn cleanup_revocations(&self) -> u32 {
        let mut revocations = self.revocations.write().await;
        let now = Utc::now();

        let expired: Vec<_> = revocations
            .iter()
            .filter(|(_, entry)| entry.original_exp < now)
            .map(|(id, _)| *id)
            .collect();

        let count = expired.len() as u32;
        for id in expired {
            revocations.remove(&id);
        }

        count
    }

    /// Get current key version
    pub async fn key_version(&self) -> u32 {
        self.current_keys.read().await.version
    }

    /// Get public keys for clients
    pub async fn get_public_keys(&self) -> PublicKeyBundle {
        let keys = self.current_keys.read().await;
        PublicKeyBundle {
            kyber_public_key: keys.kyber.public_key.clone(),
            dilithium_public_key: keys.dilithium.public_key.clone(),
            version: keys.version,
            issued_at: keys.created_at,
        }
    }
}

/// Public key bundle for clients
#[derive(Clone, Debug)]
pub struct PublicKeyBundle {
    pub kyber_public_key: Vec<u8>,
    pub dilithium_public_key: Vec<u8>,
    pub version: u32,
    pub issued_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_issue_and_verify() {
        let manager = TokenManager::new("test-issuer").unwrap();

        let bundle = manager.issue_tokens(
            "user123",
            vec![Scope::new("users", "read")],
            b"device-fingerprint-123",
            0.95,
        ).await.unwrap();

        assert!(!bundle.access_token.is_empty());
        assert!(!bundle.refresh_token.is_empty());

        // Verify access token
        let claims = manager.verify(&bundle.access_token).await.unwrap();
        assert_eq!(claims.sub, "user123");
        assert_eq!(claims.iss, "test-issuer");
    }

    #[tokio::test]
    async fn test_device_binding() {
        let manager = TokenManager::new("test-issuer").unwrap();

        let bundle = manager.issue_tokens(
            "user123",
            vec![],
            b"device-fingerprint-123",
            1.0,
        ).await.unwrap();

        // Correct device
        let result = manager.verify_with_device(
            &bundle.access_token,
            b"device-fingerprint-123",
        ).await;
        assert!(result.is_ok());

        // Wrong device
        let result = manager.verify_with_device(
            &bundle.access_token,
            b"different-device",
        ).await;
        assert!(matches!(result, Err(TokenError::DeviceMismatch)));
    }

    #[tokio::test]
    async fn test_revocation() {
        let manager = TokenManager::new("test-issuer").unwrap();

        let bundle = manager.issue_tokens(
            "user123",
            vec![],
            b"device",
            1.0,
        ).await.unwrap();

        // Verify works before revocation
        let claims = manager.verify(&bundle.access_token).await.unwrap();

        // Revoke
        manager.revoke(claims.jti, "test revocation").await.unwrap();

        // Verify fails after revocation
        let result = manager.verify(&bundle.access_token).await;
        assert!(matches!(result, Err(TokenError::Revoked)));
    }
}
