//! Routine Module - Stored procedures and functions

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Routine type
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum RoutineType {
    Function,
    Procedure,
    Aggregate,
    Window,
}

/// Routine parameter
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutineParam {
    pub name: String,
    pub data_type: String,
    pub mode: ParamMode,
    pub default_value: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ParamMode {
    In,
    Out,
    InOut,
    Variadic,
}

/// Routine definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Routine {
    pub name: String,
    pub schema: String,
    pub routine_type: RoutineType,
    pub language: String,
    pub parameters: Vec<RoutineParam>,
    pub return_type: Option<String>,
    pub body: String,
    pub volatility: Volatility,
    pub security: SecurityType,
    pub parallel: ParallelSafety,
    pub strict: bool,
    pub cost: f64,
    pub rows: Option<u64>,
    pub owner: String,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum Volatility {
    Immutable,
    Stable,
    #[default]
    Volatile,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum SecurityType {
    #[default]
    Invoker,
    Definer,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum ParallelSafety {
    Unsafe,
    Restricted,
    #[default]
    Safe,
}

impl Routine {
    pub fn new_function(schema: &str, name: &str, return_type: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            routine_type: RoutineType::Function,
            language: "sql".to_string(),
            parameters: vec![],
            return_type: Some(return_type.to_string()),
            body: String::new(),
            volatility: Volatility::default(),
            security: SecurityType::default(),
            parallel: ParallelSafety::default(),
            strict: false,
            cost: 100.0,
            rows: None,
            owner: "postgres".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn new_procedure(schema: &str, name: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            routine_type: RoutineType::Procedure,
            language: "sql".to_string(),
            parameters: vec![],
            return_type: None,
            body: String::new(),
            volatility: Volatility::Volatile,
            security: SecurityType::default(),
            parallel: ParallelSafety::Unsafe,
            strict: false,
            cost: 100.0,
            rows: None,
            owner: "postgres".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_param(mut self, name: &str, data_type: &str, mode: ParamMode) -> Self {
        self.parameters.push(RoutineParam {
            name: name.to_string(),
            data_type: data_type.to_string(),
            mode,
            default_value: None,
        });
        self
    }

    pub fn with_body(mut self, body: &str) -> Self {
        self.body = body.to_string();
        self
    }

    pub fn with_language(mut self, language: &str) -> Self {
        self.language = language.to_string();
        self
    }

    /// Get full qualified name
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Get signature for overload resolution
    pub fn signature(&self) -> String {
        let params: Vec<String> = self.parameters.iter()
            .filter(|p| p.mode == ParamMode::In || p.mode == ParamMode::InOut)
            .map(|p| p.data_type.clone())
            .collect();
        format!("{}({})", self.full_name(), params.join(", "))
    }
}

/// Routine manager
pub struct RoutineManager {
    routines: RwLock<HashMap<String, Vec<Routine>>>,
}

impl RoutineManager {
    pub fn new() -> Self {
        Self {
            routines: RwLock::new(HashMap::new()),
        }
    }

    /// Create or replace routine
    pub async fn create(&self, routine: Routine, replace: bool) -> Result<(), String> {
        let mut routines = self.routines.write().await;
        let key = routine.full_name();
        
        let overloads = routines.entry(key).or_insert_with(Vec::new);
        let signature = routine.signature();

        // Check for existing overload
        let existing = overloads.iter().position(|r| r.signature() == signature);

        match (existing, replace) {
            (Some(idx), true) => {
                overloads[idx] = routine;
            }
            (Some(_), false) => {
                return Err(format!("Routine {} already exists", signature));
            }
            (None, _) => {
                overloads.push(routine);
            }
        }

        Ok(())
    }

    /// Drop routine
    pub async fn drop(&self, schema: &str, name: &str, param_types: Option<Vec<String>>) -> Result<(), String> {
        let mut routines = self.routines.write().await;
        let key = format!("{}.{}", schema, name);

        let overloads = routines.get_mut(&key)
            .ok_or_else(|| format!("Routine {} not found", key))?;

        match param_types {
            Some(types) => {
                let signature = format!("{}({})", key, types.join(", "));
                let pos = overloads.iter().position(|r| r.signature() == signature)
                    .ok_or_else(|| format!("Routine {} not found", signature))?;
                overloads.remove(pos);
            }
            None => {
                if overloads.len() > 1 {
                    return Err(format!("Multiple overloads exist for {}", key));
                }
                overloads.clear();
            }
        }

        if overloads.is_empty() {
            routines.remove(&key);
        }

        Ok(())
    }

    /// Get routine
    pub async fn get(&self, schema: &str, name: &str, arg_types: &[&str]) -> Option<Routine> {
        let routines = self.routines.read().await;
        let key = format!("{}.{}", schema, name);

        routines.get(&key)?
            .iter()
            .find(|r| {
                let param_types: Vec<&str> = r.parameters.iter()
                    .filter(|p| p.mode == ParamMode::In || p.mode == ParamMode::InOut)
                    .map(|p| p.data_type.as_str())
                    .collect();
                param_types == arg_types
            })
            .cloned()
    }

    /// List all routines
    pub async fn list(&self, schema: Option<&str>, routine_type: Option<RoutineType>) -> Vec<Routine> {
        let routines = self.routines.read().await;
        routines.values()
            .flat_map(|v| v.iter())
            .filter(|r| {
                schema.map(|s| r.schema == s).unwrap_or(true) &&
                routine_type.map(|t| r.routine_type == t).unwrap_or(true)
            })
            .cloned()
            .collect()
    }
}

impl Default for RoutineManager {
    fn default() -> Self {
        Self::new()
    }
}
