//! Active Directory Authentication Provider
//!
//! Handles integration with LDAP/AD for enterprise authentication.
//! Note: Current implementation is a simulation for development.

use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AdProvider {
    config: Arc<RwLock<crate::config::AdConfig>>,
}

impl AdProvider {
    pub fn new(config: Arc<RwLock<crate::config::AdConfig>>) -> Self {
        Self { config }
    }

    pub async fn authenticate(&self, username: &str, password: &str) -> bool {
        let config = self.config.read().await;
        
        if !config.enabled {
            return false;
        }

        // Simulate AD authentication
        // In a real implementation, this would use the `ldap3` crate
        // to bind to the AD server.
        
        // Mock logic:
        // - user: "ad_admin" / pass: "ad_password" -> Success
        // - user starting with "corp_" -> Success if password is "password"
        
        tracing::info!("Attempting AD authentication for user: {}", username);
        
        if username == "ad_admin" && password == "ad_password" {
             return true;
        }
        
        if username.starts_with("corp_") && password == "password" {
            return true;
        }

        false
    }
}
