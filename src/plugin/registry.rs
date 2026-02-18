//! Plugin Registry - Central catalog of available plugins

use std::collections::HashMap;
use super::sdk::{PluginManifest, PluginBuilder, ConfigFieldType};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub manifest: PluginManifest,
    pub download_url: Option<String>,
    pub checksum: Option<String>,
    pub install_count: u64,
    pub rating: f32,
}

pub struct PluginRegistry {
    entries: HashMap<String, RegistryEntry>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        let mut reg = Self { entries: HashMap::new() };
        reg.seed_builtin_plugins();
        reg
    }
    
    fn seed_builtin_plugins(&mut self) {
        // Audit logger plugin
        let audit = PluginBuilder::new("audit_logger")
            .version("1.0.0")
            .author("MiracleDB")
            .description("Logs all database operations to file or external service")
            .tag("security")
            .tag("compliance")
            .config_field("log_file", ConfigFieldType::String, false, "Path to audit log file")
            .config_field("include_data", ConfigFieldType::Boolean, false, "Whether to log row data")
            .build();
        self.register(audit, None, None);
        
        // Schema validator
        let validator = PluginBuilder::new("schema_validator")
            .version("1.0.0")
            .author("MiracleDB")
            .description("Validates inserted data against JSON Schema definitions")
            .tag("data-quality")
            .config_field("schema_dir", ConfigFieldType::String, true, "Directory containing JSON schema files")
            .build();
        self.register(validator, None, None);
        
        // PII masking
        let masking = PluginBuilder::new("pii_masking")
            .version("1.0.0")
            .author("MiracleDB")
            .description("Automatically masks PII fields in query results")
            .tag("security")
            .tag("privacy")
            .can_transform_data()
            .config_field("fields", ConfigFieldType::Array, true, "List of field names to mask")
            .config_field("mask_char", ConfigFieldType::String, false, "Character to use for masking (default *)")
            .build();
        self.register(masking, None, None);
        
        // Rate limiter
        let ratelimit = PluginBuilder::new("rate_limiter")
            .version("1.0.0")
            .author("MiracleDB")
            .description("Limits query rate per user/IP to prevent abuse")
            .tag("performance")
            .tag("security")
            .config_field("requests_per_second", ConfigFieldType::Integer, true, "Max requests per second per client")
            .config_field("burst_size", ConfigFieldType::Integer, false, "Burst allowance")
            .build();
        self.register(ratelimit, None, None);
        
        // Row-level security
        let rls = PluginBuilder::new("row_level_security")
            .version("1.0.0")
            .author("MiracleDB")
            .description("Adds WHERE clauses to queries based on user context")
            .tag("security")
            .tag("multitenancy")
            .config_field("tenant_field", ConfigFieldType::String, true, "Field name for tenant isolation")
            .build();
        self.register(rls, None, None);
    }
    
    pub fn register(&mut self, manifest: PluginManifest, url: Option<String>, checksum: Option<String>) {
        let name = manifest.name.clone();
        self.entries.insert(name, RegistryEntry {
            manifest,
            download_url: url,
            checksum,
            install_count: 0,
            rating: 0.0,
        });
    }
    
    pub fn find(&self, name: &str) -> Option<&RegistryEntry> {
        self.entries.get(name)
    }
    
    pub fn search(&self, query: &str) -> Vec<&RegistryEntry> {
        let q = query.to_lowercase();
        self.entries.values()
            .filter(|e| {
                e.manifest.name.to_lowercase().contains(&q)
                || e.manifest.description.to_lowercase().contains(&q)
                || e.manifest.tags.iter().any(|t| t.contains(&q))
            })
            .collect()
    }
    
    pub fn list_by_tag(&self, tag: &str) -> Vec<&RegistryEntry> {
        self.entries.values()
            .filter(|e| e.manifest.tags.iter().any(|t| t == tag))
            .collect()
    }
    
    pub fn all(&self) -> Vec<&RegistryEntry> {
        self.entries.values().collect()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
