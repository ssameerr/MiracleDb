//! Operator Module - Custom operators

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Operator definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operator {
    pub name: String,
    pub schema: String,
    pub left_type: Option<String>,
    pub right_type: Option<String>,
    pub result_type: String,
    pub procedure: String,
    pub commutator: Option<String>,
    pub negator: Option<String>,
    pub restrict: Option<String>,
    pub join: Option<String>,
    pub hashes: bool,
    pub merges: bool,
}

impl Operator {
    pub fn binary(schema: &str, name: &str, left: &str, right: &str, result: &str, proc: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            left_type: Some(left.to_string()),
            right_type: Some(right.to_string()),
            result_type: result.to_string(),
            procedure: proc.to_string(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            hashes: false,
            merges: false,
        }
    }

    pub fn prefix(schema: &str, name: &str, right: &str, result: &str, proc: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            left_type: None,
            right_type: Some(right.to_string()),
            result_type: result.to_string(),
            procedure: proc.to_string(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            hashes: false,
            merges: false,
        }
    }

    pub fn with_commutator(mut self, comm: &str) -> Self {
        self.commutator = Some(comm.to_string());
        self
    }

    pub fn with_negator(mut self, neg: &str) -> Self {
        self.negator = Some(neg.to_string());
        self
    }

    pub fn hashable(mut self) -> Self {
        self.hashes = true;
        self
    }

    pub fn mergeable(mut self) -> Self {
        self.merges = true;
        self
    }

    /// Get operator signature
    pub fn signature(&self) -> String {
        let left = self.left_type.as_deref().unwrap_or("NONE");
        let right = self.right_type.as_deref().unwrap_or("NONE");
        format!("{}.{}({}, {})", self.schema, self.name, left, right)
    }
}

/// Operator class for indexing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorClass {
    pub name: String,
    pub schema: String,
    pub index_method: String,
    pub data_type: String,
    pub default: bool,
    pub operators: Vec<OperatorClassMember>,
    pub functions: Vec<OperatorClassFunction>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorClassMember {
    pub strategy: u32,
    pub operator: String,
    pub recheck: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorClassFunction {
    pub support: u32,
    pub function: String,
}

impl OperatorClass {
    pub fn new(schema: &str, name: &str, index_method: &str, data_type: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            index_method: index_method.to_string(),
            data_type: data_type.to_string(),
            default: false,
            operators: vec![],
            functions: vec![],
        }
    }

    pub fn as_default(mut self) -> Self {
        self.default = true;
        self
    }

    pub fn add_operator(mut self, strategy: u32, operator: &str) -> Self {
        self.operators.push(OperatorClassMember {
            strategy,
            operator: operator.to_string(),
            recheck: false,
        });
        self
    }

    pub fn add_function(mut self, support: u32, function: &str) -> Self {
        self.functions.push(OperatorClassFunction {
            support,
            function: function.to_string(),
        });
        self
    }
}

/// Operator manager
pub struct OperatorManager {
    operators: RwLock<HashMap<String, Operator>>,
    operator_classes: RwLock<HashMap<String, OperatorClass>>,
}

impl OperatorManager {
    pub fn new() -> Self {
        Self {
            operators: RwLock::new(HashMap::new()),
            operator_classes: RwLock::new(HashMap::new()),
        }
    }

    /// Create an operator
    pub async fn create_operator(&self, operator: Operator) -> Result<(), String> {
        let mut operators = self.operators.write().await;
        let key = operator.signature();

        if operators.contains_key(&key) {
            return Err(format!("Operator {} already exists", key));
        }

        operators.insert(key, operator);
        Ok(())
    }

    /// Drop an operator
    pub async fn drop_operator(&self, schema: &str, name: &str, left: Option<&str>, right: Option<&str>) -> Result<(), String> {
        let mut operators = self.operators.write().await;
        let l = left.unwrap_or("NONE");
        let r = right.unwrap_or("NONE");
        let key = format!("{}.{}({}, {})", schema, name, l, r);

        operators.remove(&key)
            .ok_or_else(|| format!("Operator {} not found", key))?;
        Ok(())
    }

    /// Get an operator
    pub async fn get_operator(&self, schema: &str, name: &str, left: Option<&str>, right: Option<&str>) -> Option<Operator> {
        let operators = self.operators.read().await;
        let l = left.unwrap_or("NONE");
        let r = right.unwrap_or("NONE");
        let key = format!("{}.{}({}, {})", schema, name, l, r);
        operators.get(&key).cloned()
    }

    /// Find operator by types
    pub async fn resolve(&self, name: &str, left: &str, right: &str) -> Option<Operator> {
        let operators = self.operators.read().await;

        for op in operators.values() {
            if op.name == name {
                let matches_left = op.left_type.as_ref()
                    .map(|t| t == left || t == "any")
                    .unwrap_or(true);
                let matches_right = op.right_type.as_ref()
                    .map(|t| t == right || t == "any")
                    .unwrap_or(true);

                if matches_left && matches_right {
                    return Some(op.clone());
                }
            }
        }

        None
    }

    /// Create operator class
    pub async fn create_operator_class(&self, op_class: OperatorClass) -> Result<(), String> {
        let mut classes = self.operator_classes.write().await;
        let key = format!("{}.{}", op_class.schema, op_class.name);

        if classes.contains_key(&key) {
            return Err(format!("Operator class {} already exists", key));
        }

        classes.insert(key, op_class);
        Ok(())
    }

    /// List operators
    pub async fn list_operators(&self, schema: Option<&str>) -> Vec<Operator> {
        let operators = self.operators.read().await;
        operators.values()
            .filter(|o| schema.map(|s| o.schema == s).unwrap_or(true))
            .cloned()
            .collect()
    }
}

impl Default for OperatorManager {
    fn default() -> Self {
        Self::new()
    }
}
