//! Variable Module - Session and system variables

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Variable scope
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum VariableScope {
    System,
    Session,
    Transaction,
    Local,
}

/// Variable definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VariableDefinition {
    pub name: String,
    pub scope: VariableScope,
    pub var_type: VariableType,
    pub default_value: serde_json::Value,
    pub description: String,
    pub read_only: bool,
    pub restart_required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VariableType {
    Boolean,
    Integer { min: Option<i64>, max: Option<i64> },
    Float { min: Option<f64>, max: Option<f64> },
    String { max_length: Option<usize>, enum_values: Option<Vec<String>> },
    Duration,
    Size,
}

/// Variable value
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Variable {
    pub name: String,
    pub value: serde_json::Value,
    pub scope: VariableScope,
    pub modified_at: i64,
}

/// System variables
pub struct SystemVariables {
    definitions: HashMap<String, VariableDefinition>,
    values: RwLock<HashMap<String, serde_json::Value>>,
}

impl SystemVariables {
    pub fn new() -> Self {
        let mut sys = Self {
            definitions: HashMap::new(),
            values: RwLock::new(HashMap::new()),
        };
        sys.register_defaults();
        sys
    }

    fn register_defaults(&mut self) {
        // Memory settings
        self.define(VariableDefinition {
            name: "work_mem".to_string(),
            scope: VariableScope::Session,
            var_type: VariableType::Size,
            default_value: serde_json::json!("4MB"),
            description: "Memory for query operations".to_string(),
            read_only: false,
            restart_required: false,
        });

        self.define(VariableDefinition {
            name: "shared_buffers".to_string(),
            scope: VariableScope::System,
            var_type: VariableType::Size,
            default_value: serde_json::json!("128MB"),
            description: "Shared buffer pool size".to_string(),
            read_only: false,
            restart_required: true,
        });

        // Query settings
        self.define(VariableDefinition {
            name: "statement_timeout".to_string(),
            scope: VariableScope::Session,
            var_type: VariableType::Duration,
            default_value: serde_json::json!("0"),
            description: "Query timeout (0 = disabled)".to_string(),
            read_only: false,
            restart_required: false,
        });

        self.define(VariableDefinition {
            name: "search_path".to_string(),
            scope: VariableScope::Session,
            var_type: VariableType::String { max_length: None, enum_values: None },
            default_value: serde_json::json!("public"),
            description: "Schema search path".to_string(),
            read_only: false,
            restart_required: false,
        });

        // Logging
        self.define(VariableDefinition {
            name: "log_min_duration_statement".to_string(),
            scope: VariableScope::System,
            var_type: VariableType::Duration,
            default_value: serde_json::json!("-1"),
            description: "Log queries slower than this".to_string(),
            read_only: false,
            restart_required: false,
        });

        // Limits
        self.define(VariableDefinition {
            name: "max_connections".to_string(),
            scope: VariableScope::System,
            var_type: VariableType::Integer { min: Some(1), max: Some(10000) },
            default_value: serde_json::json!(100),
            description: "Maximum concurrent connections".to_string(),
            read_only: false,
            restart_required: true,
        });
    }

    /// Define a variable
    pub fn define(&mut self, def: VariableDefinition) {
        self.definitions.insert(def.name.clone(), def);
    }

    /// Get variable definition
    pub fn get_definition(&self, name: &str) -> Option<&VariableDefinition> {
        self.definitions.get(name)
    }

    /// Get variable value
    pub async fn get(&self, name: &str) -> Option<serde_json::Value> {
        let values = self.values.read().await;
        values.get(name).cloned()
            .or_else(|| self.definitions.get(name).map(|d| d.default_value.clone()))
    }

    /// Set variable value
    pub async fn set(&self, name: &str, value: serde_json::Value) -> Result<(), String> {
        let def = self.definitions.get(name)
            .ok_or_else(|| format!("Unknown variable: {}", name))?;

        if def.read_only {
            return Err(format!("Variable {} is read-only", name));
        }

        // Validate value
        self.validate(def, &value)?;

        let mut values = self.values.write().await;
        values.insert(name.to_string(), value);

        Ok(())
    }

    fn validate(&self, def: &VariableDefinition, value: &serde_json::Value) -> Result<(), String> {
        match &def.var_type {
            VariableType::Boolean => {
                if !value.is_boolean() && !value.is_string() {
                    return Err("Expected boolean".to_string());
                }
            }
            VariableType::Integer { min, max } => {
                if let Some(n) = value.as_i64() {
                    if let Some(m) = min {
                        if n < *m {
                            return Err(format!("Value must be >= {}", m));
                        }
                    }
                    if let Some(m) = max {
                        if n > *m {
                            return Err(format!("Value must be <= {}", m));
                        }
                    }
                } else {
                    return Err("Expected integer".to_string());
                }
            }
            VariableType::String { enum_values, .. } => {
                if let Some(enums) = enum_values {
                    if let Some(s) = value.as_str() {
                        if !enums.contains(&s.to_string()) {
                            return Err(format!("Value must be one of: {:?}", enums));
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// List all variables
    pub fn list(&self) -> Vec<&VariableDefinition> {
        self.definitions.values().collect()
    }

    /// Reset to default
    pub async fn reset(&self, name: &str) -> Result<(), String> {
        let def = self.definitions.get(name)
            .ok_or_else(|| format!("Unknown variable: {}", name))?;

        let mut values = self.values.write().await;
        values.insert(name.to_string(), def.default_value.clone());

        Ok(())
    }

    /// Reset all to defaults
    pub async fn reset_all(&self) {
        let mut values = self.values.write().await;
        values.clear();
    }
}

impl Default for SystemVariables {
    fn default() -> Self {
        Self::new()
    }
}

/// Session variables
pub struct SessionVariables {
    variables: RwLock<HashMap<String, serde_json::Value>>,
}

impl SessionVariables {
    pub fn new() -> Self {
        Self {
            variables: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, name: &str) -> Option<serde_json::Value> {
        let variables = self.variables.read().await;
        variables.get(name).cloned()
    }

    pub async fn set(&self, name: &str, value: serde_json::Value) {
        let mut variables = self.variables.write().await;
        variables.insert(name.to_string(), value);
    }

    pub async fn unset(&self, name: &str) {
        let mut variables = self.variables.write().await;
        variables.remove(name);
    }

    pub async fn list(&self) -> HashMap<String, serde_json::Value> {
        let variables = self.variables.read().await;
        variables.clone()
    }
}

impl Default for SessionVariables {
    fn default() -> Self {
        Self::new()
    }
}
