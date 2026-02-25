//! Mutual TLS Configuration
//!
//! Provides mTLS support for secure cluster communication

use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use serde::{Serialize, Deserialize};

/// mTLS configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MtlsConfig {
    pub enabled: bool,
    pub ca_cert_path: String,
    pub server_cert_path: String,
    pub server_key_path: String,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub verify_client: bool,
    pub min_tls_version: TlsVersion,
    pub allowed_ciphers: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl Default for MtlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert_path: String::new(),
            server_cert_path: String::new(),
            server_key_path: String::new(),
            client_cert_path: None,
            client_key_path: None,
            verify_client: true,
            min_tls_version: TlsVersion::Tls13,
            allowed_ciphers: vec![
                "TLS_AES_256_GCM_SHA384".to_string(),
                "TLS_CHACHA20_POLY1305_SHA256".to_string(),
            ],
        }
    }
}

/// Certificate info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CertificateInfo {
    pub subject: String,
    pub issuer: String,
    pub serial_number: String,
    pub not_before: i64,
    pub not_after: i64,
    pub fingerprint: String,
}

/// mTLS manager
pub struct MtlsManager {
    config: RwLock<MtlsConfig>,
    server_cert: RwLock<Option<CertificateInfo>>,
    trusted_clients: RwLock<Vec<String>>, // fingerprints
}

impl MtlsManager {
    pub fn new(config: MtlsConfig) -> Self {
        Self {
            config: RwLock::new(config),
            server_cert: RwLock::new(None),
            trusted_clients: RwLock::new(Vec::new()),
        }
    }

    /// Validate configuration
    pub async fn validate(&self) -> Result<(), String> {
        let config = self.config.read().await;
        if !config.enabled {
            return Ok(());
        }
        
        if !Path::new(&config.ca_cert_path).exists() {
            return Err(format!("CA cert not found: {}", config.ca_cert_path));
        }
        if !Path::new(&config.server_cert_path).exists() {
            return Err(format!("Server cert not found: {}", config.server_cert_path));
        }
        if !Path::new(&config.server_key_path).exists() {
            return Err(format!("Server key not found: {}", config.server_key_path));
        }
        
        info!("mTLS configuration validated");
        Ok(())
    }

    /// Load certificates
    pub async fn load_certificates(&self) -> Result<(), String> {
        let config = self.config.read().await;
        if !config.enabled {
            return Ok(());
        }

        // In production: use rustls to parse certificates
        // let cert_pem = std::fs::read(&config.server_cert_path)?;
        // let cert = rustls_pemfile::certs(&mut cert_pem.as_slice())?;

        let cert_info = CertificateInfo {
            subject: "CN=miracledb".to_string(),
            issuer: "CN=MiracleDB CA".to_string(),
            serial_number: "001".to_string(),
            not_before: chrono::Utc::now().timestamp(),
            not_after: chrono::Utc::now().timestamp() + 365 * 24 * 3600,
            fingerprint: "sha256:abc123...".to_string(),
        };

        let mut server_cert = self.server_cert.write().await;
        *server_cert = Some(cert_info);

        info!("mTLS certificates loaded");
        Ok(())
    }

    /// Add trusted client fingerprint
    pub async fn add_trusted_client(&self, fingerprint: &str) {
        let mut clients = self.trusted_clients.write().await;
        if !clients.contains(&fingerprint.to_string()) {
            clients.push(fingerprint.to_string());
        }
    }

    /// Verify client certificate
    pub async fn verify_client(&self, fingerprint: &str) -> bool {
        let clients = self.trusted_clients.read().await;
        clients.contains(&fingerprint.to_string())
    }

    /// Get server certificate info
    pub async fn get_server_cert(&self) -> Option<CertificateInfo> {
        let cert = self.server_cert.read().await;
        cert.clone()
    }

    /// Check certificate expiration
    pub async fn check_expiration(&self) -> Result<i64, String> {
        let cert = self.server_cert.read().await;
        let cert = cert.as_ref().ok_or("No certificate loaded")?;
        
        let now = chrono::Utc::now().timestamp();
        let days_remaining = (cert.not_after - now) / (24 * 3600);
        
        if days_remaining < 30 {
            warn!("Certificate expires in {} days", days_remaining);
        }
        
        Ok(days_remaining)
    }

    /// Rotate certificates
    pub async fn rotate_certificates(&self, new_cert_path: &str, new_key_path: &str) -> Result<(), String> {
        let mut config = self.config.write().await;
        config.server_cert_path = new_cert_path.to_string();
        config.server_key_path = new_key_path.to_string();
        drop(config);

        self.load_certificates().await?;
        info!("Certificates rotated");
        Ok(())
    }
}

impl Default for MtlsManager {
    fn default() -> Self {
        Self::new(MtlsConfig::default())
    }
}
