//! Cast Module - Type casting definitions

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Cast context
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum CastContext {
    Implicit,
    Assignment,
    Explicit,
}

/// Cast method
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CastMethod {
    Function(String),
    InOut,
    Binary,
}

/// Cast definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cast {
    pub source_type: String,
    pub target_type: String,
    pub method: CastMethod,
    pub context: CastContext,
}

impl Cast {
    pub fn new(source: &str, target: &str, method: CastMethod, context: CastContext) -> Self {
        Self {
            source_type: source.to_string(),
            target_type: target.to_string(),
            method,
            context,
        }
    }

    pub fn implicit(source: &str, target: &str, function: &str) -> Self {
        Self::new(source, target, CastMethod::Function(function.to_string()), CastContext::Implicit)
    }

    pub fn assignment(source: &str, target: &str, function: &str) -> Self {
        Self::new(source, target, CastMethod::Function(function.to_string()), CastContext::Assignment)
    }

    pub fn explicit(source: &str, target: &str, function: &str) -> Self {
        Self::new(source, target, CastMethod::Function(function.to_string()), CastContext::Explicit)
    }

    pub fn key(&self) -> String {
        format!("{}::{}", self.source_type, self.target_type)
    }
}

/// Cast manager
pub struct CastManager {
    casts: RwLock<HashMap<String, Cast>>,
}

impl CastManager {
    pub fn new() -> Self {
        let mut manager = Self {
            casts: RwLock::new(HashMap::new()),
        };
        manager.register_builtins();
        manager
    }

    fn register_builtins(&mut self) {
        // Built-in casts would be registered here
        let builtins = vec![
            Cast::implicit("int4", "int8", "int48"),
            Cast::implicit("int2", "int4", "int24"),
            Cast::implicit("float4", "float8", "ftod"),
            Cast::assignment("int4", "int2", "int42"),
            Cast::assignment("int8", "int4", "int84"),
            Cast::explicit("text", "int4", "text_to_int4"),
            Cast::explicit("int4", "text", "int4_to_text"),
            Cast::implicit("varchar", "text", "varchartextcast"),
            Cast::implicit("text", "varchar", "textvarcharcast"),
        ];
        // Would be populated async
    }

    /// Create a cast
    pub async fn create(&self, cast: Cast) -> Result<(), String> {
        let mut casts = self.casts.write().await;
        let key = cast.key();

        if casts.contains_key(&key) {
            return Err(format!("Cast from {} to {} already exists", cast.source_type, cast.target_type));
        }

        casts.insert(key, cast);
        Ok(())
    }

    /// Drop a cast
    pub async fn drop(&self, source: &str, target: &str) -> Result<(), String> {
        let mut casts = self.casts.write().await;
        let key = format!("{}::{}", source, target);

        casts.remove(&key)
            .ok_or_else(|| format!("Cast from {} to {} not found", source, target))?;
        Ok(())
    }

    /// Get a cast
    pub async fn get(&self, source: &str, target: &str) -> Option<Cast> {
        let casts = self.casts.read().await;
        let key = format!("{}::{}", source, target);
        casts.get(&key).cloned()
    }

    /// Check if cast is allowed
    pub async fn can_cast(&self, source: &str, target: &str, context: CastContext) -> bool {
        if source == target {
            return true;
        }

        if let Some(cast) = self.get(source, target).await {
            match (cast.context, context) {
                (CastContext::Implicit, _) => true,
                (CastContext::Assignment, CastContext::Implicit) => false,
                (CastContext::Assignment, _) => true,
                (CastContext::Explicit, CastContext::Explicit) => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Find cast path (for multi-step casts)
    pub async fn find_cast_path(&self, source: &str, target: &str) -> Option<Vec<Cast>> {
        // Direct cast
        if let Some(cast) = self.get(source, target).await {
            return Some(vec![cast]);
        }

        // Two-step cast (simplified)
        let casts = self.casts.read().await;
        for (_, intermediate_cast) in casts.iter() {
            if intermediate_cast.source_type == source {
                let second_key = format!("{}::{}", intermediate_cast.target_type, target);
                if let Some(second_cast) = casts.get(&second_key) {
                    return Some(vec![intermediate_cast.clone(), second_cast.clone()]);
                }
            }
        }

        None
    }

    /// List all casts
    pub async fn list(&self) -> Vec<Cast> {
        let casts = self.casts.read().await;
        casts.values().cloned().collect()
    }

    /// List casts for a source type
    pub async fn list_from(&self, source: &str) -> Vec<Cast> {
        let casts = self.casts.read().await;
        casts.values()
            .filter(|c| c.source_type == source)
            .cloned()
            .collect()
    }
}

impl Default for CastManager {
    fn default() -> Self {
        Self::new()
    }
}
