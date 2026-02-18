//! Plugin Module - Extension system for custom functionality

pub mod sdk;
pub mod registry;
pub use sdk::{PluginManifest, PluginCapabilities, PluginContext, PluginResult, PluginBuilder};
pub use registry::PluginRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use libloading::{Library, Symbol};

/// Plugin metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub hooks: Vec<HookType>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HookType {
    BeforeQuery,
    AfterQuery,
    BeforeInsert,
    AfterInsert,
    OnConnect,
    OnDisconnect,
    Custom(String),
}

/// Plugin state
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PluginState {
    Loaded,
    Active,
    Disabled,
    Error(String),
}

/// Plugin instance
pub struct Plugin {
    pub metadata: PluginMetadata,
    pub state: PluginState,
    library: Option<Arc<Library>>,
}

/// Hook callback signature
pub type HookCallback = fn(&serde_json::Value) -> Result<serde_json::Value, String>;

/// Plugin manager
pub struct PluginManager {
    plugins: RwLock<HashMap<String, Plugin>>,
    hooks: RwLock<HashMap<String, Vec<String>>>, // hook_type -> [plugin_names]
    plugin_dir: String,
}

impl PluginManager {
    pub fn new(plugin_dir: &str) -> Self {
        Self {
            plugins: RwLock::new(HashMap::new()),
            hooks: RwLock::new(HashMap::new()),
            plugin_dir: plugin_dir.to_string(),
        }
    }

    /// Register a plugin from metadata (for built-in plugins)
    pub async fn register(&self, metadata: PluginMetadata) {
        let plugin_name = metadata.name.clone();
        let hooks = metadata.hooks.clone();

        let plugin = Plugin {
            metadata,
            state: PluginState::Loaded,
            library: None,
        };

        let mut plugins = self.plugins.write().await;
        plugins.insert(plugin_name.clone(), plugin);

        // Register hooks
        let mut hook_map = self.hooks.write().await;
        for hook in hooks {
            let hook_name = format!("{:?}", hook);
            hook_map.entry(hook_name)
                .or_insert_with(Vec::new)
                .push(plugin_name.clone());
        }
    }

    /// Load a dynamic plugin (.so/.dll)
    pub async fn load_dynamic(&self, path: &str) -> Result<(), String> {
        // In production: load shared library
        // unsafe {
        //     let lib = Library::new(path)
        //         .map_err(|e| format!("Failed to load plugin: {}", e))?;
        //     
        //     let get_metadata: Symbol<fn() -> PluginMetadata> = lib.get(b"get_metadata")
        //         .map_err(|e| format!("Missing get_metadata function: {}", e))?;
        //     
        //     let metadata = get_metadata();
        //     ...
        // }

        Ok(())
    }

    /// Enable a plugin
    pub async fn enable(&self, name: &str) -> Result<(), String> {
        let mut plugins = self.plugins.write().await;
        let plugin = plugins.get_mut(name)
            .ok_or("Plugin not found")?;
        plugin.state = PluginState::Active;
        Ok(())
    }

    /// Disable a plugin
    pub async fn disable(&self, name: &str) -> Result<(), String> {
        let mut plugins = self.plugins.write().await;
        let plugin = plugins.get_mut(name)
            .ok_or("Plugin not found")?;
        plugin.state = PluginState::Disabled;
        Ok(())
    }

    /// Trigger a hook
    pub async fn trigger_hook(&self, hook: HookType, data: &serde_json::Value) -> Vec<Result<serde_json::Value, String>> {
        let hook_name = format!("{:?}", hook);
        let hooks = self.hooks.read().await;
        let plugins = self.plugins.read().await;

        let mut results = Vec::new();

        if let Some(plugin_names) = hooks.get(&hook_name) {
            for plugin_name in plugin_names {
                if let Some(plugin) = plugins.get(plugin_name) {
                    if matches!(plugin.state, PluginState::Active) {
                        // In production: would call the actual hook function
                        results.push(Ok(data.clone()));
                    }
                }
            }
        }

        results
    }

    /// List all plugins
    pub async fn list(&self) -> Vec<(String, PluginMetadata, PluginState)> {
        let plugins = self.plugins.read().await;
        plugins.iter()
            .map(|(name, p)| (name.clone(), p.metadata.clone(), p.state.clone()))
            .collect()
    }

    /// Get plugin info
    pub async fn get(&self, name: &str) -> Option<(PluginMetadata, PluginState)> {
        let plugins = self.plugins.read().await;
        plugins.get(name).map(|p| (p.metadata.clone(), p.state.clone()))
    }

    /// Unload a plugin
    pub async fn unload(&self, name: &str) -> Result<(), String> {
        let mut plugins = self.plugins.write().await;
        plugins.remove(name)
            .ok_or("Plugin not found")?;

        // Remove from hooks
        let mut hooks = self.hooks.write().await;
        for plugin_list in hooks.values_mut() {
            plugin_list.retain(|n| n != name);
        }

        Ok(())
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new("./plugins")
    }
}

/// Example built-in plugin
pub fn create_audit_plugin() -> PluginMetadata {
    PluginMetadata {
        name: "audit_logger".to_string(),
        version: "1.0.0".to_string(),
        author: "MiracleDb".to_string(),
        description: "Logs all database operations".to_string(),
        hooks: vec![
            HookType::BeforeQuery,
            HookType::AfterQuery,
            HookType::BeforeInsert,
            HookType::AfterInsert,
        ],
    }
}

pub fn create_validation_plugin() -> PluginMetadata {
    PluginMetadata {
        name: "schema_validator".to_string(),
        version: "1.0.0".to_string(),
        author: "MiracleDb".to_string(),
        description: "Validates data against JSON Schema".to_string(),
        hooks: vec![HookType::BeforeInsert],
    }
}
