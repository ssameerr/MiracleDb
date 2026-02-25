//! Portal Module - Portal and Cursor Management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Portal strategy
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum PortalStrategy {
    Select,
    Utility,
    Held,
}

/// Portal status
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum PortalStatus {
    New,
    Defined,
    Ready,
    Active,
    Done,
    Failed,
}

/// Portal definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Portal {
    pub name: String,
    pub strategy: PortalStrategy,
    pub status: PortalStatus,
    pub source_text: String,
    pub command_tag: String,
    pub created_at: i64,
    pub cursor_pos: u64,
    pub at_start: bool,
    pub at_end: bool,
    pub params: Vec<serde_json::Value>,
    pub result_desc: Option<TupleDesc>,
    pub hold_store: Option<Tuplestore>, // Encoded results if held
}

/// Tuple descriptor (simplified)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TupleDesc {
    pub attrs: Vec<AttrDef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttrDef {
    pub name: String,
    pub type_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tuplestore {
    pub tuples: Vec<Vec<serde_json::Value>>,
}

/// Portal manager
pub struct PortalManager {
    portals: RwLock<HashMap<String, Portal>>,
}

impl PortalManager {
    pub fn new() -> Self {
        Self {
            portals: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new portal
    pub async fn create(&self, name: &str, source: &str) -> Result<(), String> {
        let mut portals = self.portals.write().await;
        
        if portals.contains_key(name) && !name.is_empty() {
             return Err(format!("Portal {} already exists", name));
        }

        let portal = Portal {
            name: name.to_string(),
            strategy: PortalStrategy::Select,
            status: PortalStatus::New,
            source_text: source.to_string(),
            command_tag: "".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            cursor_pos: 0,
            at_start: true,
            at_end: false,
            params: vec![],
            result_desc: None,
            hold_store: None,
        };

        portals.insert(name.to_string(), portal);
        Ok(())
    }

    /// drop portal
    pub async fn drop(&self, name: &str) {
        let mut portals = self.portals.write().await;
        portals.remove(name);
    }

    /// Get portal
    pub async fn get(&self, name: &str) -> Option<Portal> {
        let portals = self.portals.read().await;
        portals.get(name).cloned()
    }

    /// Define portal (bind)
    pub async fn define(&self, name: &str, params: Vec<serde_json::Value>) -> Result<(), String> {
        let mut portals = self.portals.write().await;
        let portal = portals.get_mut(name)
            .ok_or_else(|| format!("Portal {} not found", name))?;

        portal.params = params;
        portal.status = PortalStatus::Defined;
        Ok(())
    }

    /// Set ready
    pub async fn ready(&self, name: &str) -> Result<(), String> {
        let mut portals = self.portals.write().await;
        let portal = portals.get_mut(name)
            .ok_or_else(|| format!("Portal {} not found", name))?;

        portal.status = PortalStatus::Ready;
        Ok(())
    }
}

impl Default for PortalManager {
    fn default() -> Self {
        Self::new()
    }
}
