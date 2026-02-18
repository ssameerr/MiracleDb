//! Plugin SDK - Interface for building MiracleDB plugins

use serde::{Serialize, Deserialize};

/// Plugin API version for compatibility checking
pub const PLUGIN_API_VERSION: &str = "1.0";

/// Plugin capabilities
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginCapabilities {
    pub can_modify_queries: bool,
    pub can_transform_data: bool,
    pub can_add_udfs: bool,
    pub can_access_storage: bool,
    pub requires_auth: bool,
}

impl Default for PluginCapabilities {
    fn default() -> Self {
        Self {
            can_modify_queries: false,
            can_transform_data: true,
            can_add_udfs: false,
            can_access_storage: false,
            requires_auth: false,
        }
    }
}

/// Plugin configuration schema
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigField {
    pub name: String,
    pub field_type: ConfigFieldType,
    pub required: bool,
    pub default: Option<serde_json::Value>,
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConfigFieldType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Object,
}

/// Plugin manifest (plugin.json / plugin.toml equivalent)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub license: String,
    pub api_version: String,
    pub capabilities: PluginCapabilities,
    pub config_schema: Vec<ConfigField>,
    pub tags: Vec<String>,
}

impl PluginManifest {
    pub fn new(name: &str, version: &str, author: &str, description: &str) -> Self {
        Self {
            name: name.to_string(),
            version: version.to_string(),
            author: author.to_string(),
            description: description.to_string(),
            license: "MIT".to_string(),
            api_version: PLUGIN_API_VERSION.to_string(),
            capabilities: PluginCapabilities::default(),
            config_schema: Vec::new(),
            tags: Vec::new(),
        }
    }
    
    pub fn is_compatible(&self) -> bool {
        self.api_version == PLUGIN_API_VERSION
    }
}

/// Plugin context passed to hook functions
#[derive(Clone, Debug)]
pub struct PluginContext {
    pub plugin_name: String,
    pub config: serde_json::Value,
    pub metadata: std::collections::HashMap<String, String>,
}

impl PluginContext {
    pub fn new(plugin_name: &str, config: serde_json::Value) -> Self {
        Self {
            plugin_name: plugin_name.to_string(),
            config,
            metadata: std::collections::HashMap::new(),
        }
    }
    
    pub fn get_config_str(&self, key: &str) -> Option<&str> {
        self.config.get(key)?.as_str()
    }
    
    pub fn get_config_bool(&self, key: &str) -> Option<bool> {
        self.config.get(key)?.as_bool()
    }
}

/// Plugin execution result
#[derive(Debug)]
pub enum PluginResult {
    Continue(serde_json::Value),  // Modified data, continue processing
    Block(String),                 // Block the operation with reason
    Passthrough,                   // No modification, pass through
}

/// Plugin lifecycle trait (in-process plugins implement this)
pub trait PluginLifecycle: Send + Sync {
    fn on_load(&self, ctx: &PluginContext) -> Result<(), String>;
    fn on_unload(&self) -> Result<(), String>;
    fn on_enable(&self) -> Result<(), String> { Ok(()) }
    fn on_disable(&self) -> Result<(), String> { Ok(()) }
}

/// Data transform plugin
pub trait DataTransformer: Send + Sync {
    fn transform(&self, data: &serde_json::Value, ctx: &PluginContext) -> PluginResult;
}

/// Query interceptor plugin
pub trait QueryInterceptor: Send + Sync {
    fn before_query(&self, sql: &str, ctx: &PluginContext) -> Result<String, String>;
    fn after_query(&self, sql: &str, result: &serde_json::Value, ctx: &PluginContext) -> PluginResult;
}

/// Template for creating a new plugin (builder pattern)
pub struct PluginBuilder {
    manifest: PluginManifest,
}

impl PluginBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            manifest: PluginManifest::new(name, "0.1.0", "Unknown", ""),
        }
    }
    
    pub fn version(mut self, v: &str) -> Self {
        self.manifest.version = v.to_string();
        self
    }
    
    pub fn author(mut self, a: &str) -> Self {
        self.manifest.author = a.to_string();
        self
    }
    
    pub fn description(mut self, d: &str) -> Self {
        self.manifest.description = d.to_string();
        self
    }
    
    pub fn tag(mut self, t: &str) -> Self {
        self.manifest.tags.push(t.to_string());
        self
    }
    
    pub fn can_transform_data(mut self) -> Self {
        self.manifest.capabilities.can_transform_data = true;
        self
    }
    
    pub fn config_field(mut self, name: &str, field_type: ConfigFieldType, required: bool, desc: &str) -> Self {
        self.manifest.config_schema.push(ConfigField {
            name: name.to_string(),
            field_type,
            required,
            default: None,
            description: desc.to_string(),
        });
        self
    }
    
    pub fn build(self) -> PluginManifest {
        self.manifest
    }
}
