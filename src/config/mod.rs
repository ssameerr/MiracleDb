//! Config Module - Configuration management

use std::collections::HashMap;
use std::path::Path;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Main configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub security: SecurityConfig,
    pub ad: AdConfig,
    pub cluster: ClusterConfig,
    pub logging: LoggingConfig,
    pub features: FeatureFlags,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub request_timeout_ms: u64,
    pub keep_alive_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_dir: String,
    pub max_memory_mb: usize,
    pub checkpoint_interval_seconds: u64,
    pub compression: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub mtls_enabled: bool,
    pub auth_required: bool,
    pub encryption_at_rest: bool,
    pub audit_logging: bool,
    pub api_keys_enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub domain: String,
    pub use_tls: bool,
}

impl Default for AdConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "ad.example.com".to_string(),
            port: 389,
            domain: "example.com".to_string(),
            use_tls: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: String,
    pub seeds: Vec<String>,
    pub replication_factor: u8,
    pub heartbeat_interval_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub file: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FeatureFlags {
    pub vector_search: bool,
    pub graph_queries: bool,
    pub time_series: bool,
    pub ml_inference: bool,
    pub blockchain_audit: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 8080,
                max_connections: 1000,
                request_timeout_ms: 30000,
                keep_alive_seconds: 60,
            },
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                wal_dir: "./wal".to_string(),
                max_memory_mb: 4096,
                checkpoint_interval_seconds: 300,
                compression: true,
            },
            security: SecurityConfig {
                mtls_enabled: false,
                auth_required: true,
                encryption_at_rest: true,
                audit_logging: true,
                api_keys_enabled: true,
            },
            ad: AdConfig {
                enabled: false,
                host: "ad.example.com".to_string(),
                port: 389,
                domain: "example.com".to_string(),
                use_tls: true,
            },
            cluster: ClusterConfig {
                node_id: "node-1".to_string(),
                seeds: vec![],
                replication_factor: 3,
                heartbeat_interval_ms: 1000,
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: "json".to_string(),
                file: None,
            },
            features: FeatureFlags {
                vector_search: true,
                graph_queries: true,
                time_series: true,
                ml_inference: false,
                blockchain_audit: false,
            },
        }
    }
}

/// Configuration manager with hot reload
pub struct ConfigManager {
    config: RwLock<Config>,
    config_path: Option<String>,
    overrides: RwLock<HashMap<String, String>>,
}

impl ConfigManager {
    pub fn new() -> Self {
        Self {
            config: RwLock::new(Config::default()),
            config_path: None,
            overrides: RwLock::new(HashMap::new()),
        }
    }

    /// Load from file
    pub async fn load(&mut self, path: &str) -> Result<(), String> {
        let content = tokio::fs::read_to_string(path).await
            .map_err(|e| format!("Failed to read config: {}", e))?;
        
        let config: Config = if path.ends_with(".toml") {
            toml::from_str(&content).map_err(|e| format!("Invalid TOML: {}", e))?
        } else if path.ends_with(".json") {
            serde_json::from_str(&content).map_err(|e| format!("Invalid JSON: {}", e))?
        } else {
            return Err("Unsupported config format".to_string());
        };

        let mut cfg = self.config.write().await;
        *cfg = config;
        self.config_path = Some(path.to_string());

        Ok(())
    }

    /// Get current config
    pub async fn get(&self) -> Config {
        self.config.read().await.clone()
    }

    /// Update config value
    pub async fn set(&self, key: &str, value: &str) {
        let mut overrides = self.overrides.write().await;
        overrides.insert(key.to_string(), value.to_string());
    }

    /// Get specific value
    pub async fn get_value(&self, key: &str) -> Option<String> {
        let overrides = self.overrides.read().await;
        if let Some(v) = overrides.get(key) {
            return Some(v.clone());
        }

        let config = self.config.read().await;
        match key {
            "server.host" => Some(config.server.host.clone()),
            "server.port" => Some(config.server.port.to_string()),
            "storage.data_dir" => Some(config.storage.data_dir.clone()),
            "cluster.node_id" => Some(config.cluster.node_id.clone()),
            _ => None,
        }
    }

    /// Reload config from file
    pub async fn reload(&self) -> Result<(), String> {
        if let Some(path) = &self.config_path {
            let content = tokio::fs::read_to_string(path).await
                .map_err(|e| format!("Failed to read config: {}", e))?;
            
            let config: Config = if path.ends_with(".toml") {
                toml::from_str(&content).map_err(|e| format!("Invalid TOML: {}", e))?
            } else {
                serde_json::from_str(&content).map_err(|e| format!("Invalid JSON: {}", e))?
            };

            let mut cfg = self.config.write().await;
            *cfg = config;
        }
        Ok(())
    }

    /// Validate config
    pub async fn validate(&self) -> Result<(), Vec<String>> {
        let config = self.config.read().await;
        let mut errors = Vec::new();

        if config.server.port == 0 {
            errors.push("Invalid server port".to_string());
        }

        if config.storage.max_memory_mb < 256 {
            errors.push("max_memory_mb should be at least 256".to_string());
        }

        if config.cluster.replication_factor == 0 {
            errors.push("replication_factor must be > 0".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Export config as TOML
    pub async fn export_toml(&self) -> Result<String, String> {
        let config = self.config.read().await;
        toml::to_string_pretty(&*config)
            .map_err(|e| format!("Failed to serialize: {}", e))
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}
