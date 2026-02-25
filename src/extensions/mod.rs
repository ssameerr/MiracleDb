//! Extensions Module - WASM plugins, A2A protocol, and hooks

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Extension metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Extension {
    pub id: String,
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub capabilities: Vec<String>,
    pub hooks: Vec<HookBinding>,
    pub enabled: bool,
    pub installed_at: i64,
}

/// Hook binding
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HookBinding {
    pub hook_point: HookPoint,
    pub function_name: String,
    pub priority: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HookPoint {
    BeforeQuery,
    AfterQuery,
    BeforeInsert,
    AfterInsert,
    BeforeUpdate,
    AfterUpdate,
    BeforeDelete,
    AfterDelete,
    OnConnect,
    OnDisconnect,
    OnError,
}

/// Extension manifest
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtensionManifest {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub main: String,
    pub capabilities: Vec<String>,
    pub hooks: Vec<HookBinding>,
    pub permissions: Vec<Permission>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Permission {
    ReadTable(String),
    WriteTable(String),
    Network,
    FileSystem,
    Subprocess,
}

/// Extension manager
pub struct ExtensionManager {
    extensions: RwLock<HashMap<String, Extension>>,
    wasm_modules: RwLock<HashMap<String, Vec<u8>>>,
    hook_registry: RwLock<HashMap<String, Vec<(String, String, i32)>>>, // hook -> [(ext_id, func, priority)]
}

impl ExtensionManager {
    pub fn new() -> Self {
        Self {
            extensions: RwLock::new(HashMap::new()),
            wasm_modules: RwLock::new(HashMap::new()),
            hook_registry: RwLock::new(HashMap::new()),
        }
    }

    /// Install extension from manifest and WASM bytes
    pub async fn install(&self, manifest: ExtensionManifest, wasm: Vec<u8>) -> Result<Extension, String> {
        let id = format!("{}@{}", manifest.name, manifest.version);

        let ext = Extension {
            id: id.clone(),
            name: manifest.name,
            version: manifest.version,
            author: manifest.author,
            description: manifest.description,
            capabilities: manifest.capabilities,
            hooks: manifest.hooks.clone(),
            enabled: true,
            installed_at: chrono::Utc::now().timestamp(),
        };

        // Register hooks
        let mut hook_registry = self.hook_registry.write().await;
        for hook in &manifest.hooks {
            let hook_name = format!("{:?}", hook.hook_point);
            hook_registry.entry(hook_name)
                .or_insert_with(Vec::new)
                .push((id.clone(), hook.function_name.clone(), hook.priority));
        }
        drop(hook_registry);

        // Sort hooks by priority
        let mut hook_registry = self.hook_registry.write().await;
        for hooks in hook_registry.values_mut() {
            hooks.sort_by_key(|(_, _, p)| *p);
        }
        drop(hook_registry);

        // Store extension and WASM
        let mut extensions = self.extensions.write().await;
        let mut wasm_modules = self.wasm_modules.write().await;
        
        extensions.insert(id.clone(), ext.clone());
        wasm_modules.insert(id, wasm);

        Ok(ext)
    }

    /// Uninstall extension
    pub async fn uninstall(&self, id: &str) -> Result<(), String> {
        let mut extensions = self.extensions.write().await;
        let ext = extensions.remove(id)
            .ok_or("Extension not found")?;

        let mut wasm_modules = self.wasm_modules.write().await;
        wasm_modules.remove(id);

        // Remove hooks
        let mut hook_registry = self.hook_registry.write().await;
        for hooks in hook_registry.values_mut() {
            hooks.retain(|(ext_id, _, _)| ext_id != id);
        }

        Ok(())
    }

    /// Enable/disable extension
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), String> {
        let mut extensions = self.extensions.write().await;
        let ext = extensions.get_mut(id)
            .ok_or("Extension not found")?;
        ext.enabled = enabled;
        Ok(())
    }

    /// Get hooks for a hook point
    pub async fn get_hooks(&self, hook_point: HookPoint) -> Vec<(String, String)> {
        let hook_name = format!("{:?}", hook_point);
        let hook_registry = self.hook_registry.read().await;
        let extensions = self.extensions.read().await;

        hook_registry.get(&hook_name)
            .map(|hooks| {
                hooks.iter()
                    .filter(|(ext_id, _, _)| {
                        extensions.get(ext_id).map(|e| e.enabled).unwrap_or(false)
                    })
                    .map(|(ext_id, func, _)| (ext_id.clone(), func.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List all extensions
    pub async fn list(&self) -> Vec<Extension> {
        let extensions = self.extensions.read().await;
        extensions.values().cloned().collect()
    }

    /// Get extension by ID
    pub async fn get(&self, id: &str) -> Option<Extension> {
        let extensions = self.extensions.read().await;
        extensions.get(id).cloned()
    }
}

impl Default for ExtensionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Agent-to-Agent protocol interface
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2ACapability {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
}

/// A2A message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2AMessage {
    pub id: String,
    pub from: String,
    pub to: String,
    pub capability: String,
    pub payload: serde_json::Value,
    pub timestamp: i64,
}

/// A2A response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct A2AResponse {
    pub message_id: String,
    pub success: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// A2A Interface for agent communication
pub struct A2AInterface {
    agent_id: String,
    capabilities: RwLock<Vec<A2ACapability>>,
    handlers: RwLock<HashMap<String, Arc<dyn Fn(serde_json::Value) -> serde_json::Value + Send + Sync>>>,
}

impl A2AInterface {
    pub fn new(agent_id: &str) -> Self {
        Self {
            agent_id: agent_id.to_string(),
            capabilities: RwLock::new(vec![]),
            handlers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a capability
    pub async fn register_capability<F>(&self, cap: A2ACapability, handler: F)
    where
        F: Fn(serde_json::Value) -> serde_json::Value + Send + Sync + 'static,
    {
        let mut caps = self.capabilities.write().await;
        let mut handlers = self.handlers.write().await;
        
        handlers.insert(cap.name.clone(), Arc::new(handler));
        caps.push(cap);
    }

    /// Get capabilities
    pub async fn get_capabilities(&self) -> Vec<A2ACapability> {
        let caps = self.capabilities.read().await;
        caps.clone()
    }

    /// Handle incoming message
    pub async fn handle_message(&self, msg: A2AMessage) -> A2AResponse {
        let handlers = self.handlers.read().await;
        
        if let Some(handler) = handlers.get(&msg.capability) {
            let result = handler(msg.payload);
            A2AResponse {
                message_id: msg.id,
                success: true,
                result: Some(result),
                error: None,
            }
        } else {
            A2AResponse {
                message_id: msg.id,
                success: false,
                result: None,
                error: Some("Unknown capability".to_string()),
            }
        }
    }

    /// Handshake response
    pub async fn handshake(&self) -> serde_json::Value {
        let caps = self.capabilities.read().await;
        serde_json::json!({
            "protocol": "a2a/v1",
            "agent_id": self.agent_id,
            "capabilities": caps.iter().map(|c| serde_json::json!({
                "name": c.name,
                "description": c.description
            })).collect::<Vec<_>>()
        })
    }
}

impl Default for A2AInterface {
    fn default() -> Self {
        Self::new("miracledb-agent")
    }
}
