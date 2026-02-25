//! Constraint Module - Database constraints management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Constraint type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConstraintType {
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
    Check {
        expression: String,
    },
    NotNull {
        column: String,
    },
    Default {
        column: String,
        value: serde_json::Value,
    },
    Exclusion {
        elements: Vec<ExclusionElement>,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExclusionElement {
    pub column: String,
    pub operator: String,
}

/// Constraint definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Constraint {
    pub name: String,
    pub table: String,
    pub constraint_type: ConstraintType,
    pub deferrable: bool,
    pub initially_deferred: bool,
    pub validated: bool,
    pub created_at: i64,
}

impl Constraint {
    pub fn primary_key(name: &str, table: &str, columns: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            table: table.to_string(),
            constraint_type: ConstraintType::PrimaryKey { columns },
            deferrable: false,
            initially_deferred: false,
            validated: true,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn unique(name: &str, table: &str, columns: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            table: table.to_string(),
            constraint_type: ConstraintType::Unique { columns },
            deferrable: false,
            initially_deferred: false,
            validated: true,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn foreign_key(
        name: &str,
        table: &str,
        columns: Vec<String>,
        ref_table: &str,
        ref_columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.to_string(),
            table: table.to_string(),
            constraint_type: ConstraintType::ForeignKey {
                columns,
                ref_table: ref_table.to_string(),
                ref_columns,
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            },
            deferrable: false,
            initially_deferred: false,
            validated: true,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn check(name: &str, table: &str, expression: &str) -> Self {
        Self {
            name: name.to_string(),
            table: table.to_string(),
            constraint_type: ConstraintType::Check {
                expression: expression.to_string(),
            },
            deferrable: false,
            initially_deferred: false,
            validated: true,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn not_null(table: &str, column: &str) -> Self {
        Self {
            name: format!("{}_nn", column),
            table: table.to_string(),
            constraint_type: ConstraintType::NotNull {
                column: column.to_string(),
            },
            deferrable: false,
            initially_deferred: false,
            validated: true,
            created_at: chrono::Utc::now().timestamp(),
        }
    }
}

/// Constraint violation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConstraintViolation {
    pub constraint_name: String,
    pub table: String,
    pub message: String,
    pub row_data: Option<serde_json::Value>,
}

/// Constraint manager
pub struct ConstraintManager {
    constraints: RwLock<HashMap<String, Vec<Constraint>>>,
}

impl ConstraintManager {
    pub fn new() -> Self {
        Self {
            constraints: RwLock::new(HashMap::new()),
        }
    }

    /// Add a constraint
    pub async fn add(&self, constraint: Constraint) -> Result<(), String> {
        let mut constraints = self.constraints.write().await;
        let table_constraints = constraints.entry(constraint.table.clone())
            .or_insert_with(Vec::new);

        // Check for duplicate name
        if table_constraints.iter().any(|c| c.name == constraint.name) {
            return Err(format!("Constraint {} already exists", constraint.name));
        }

        table_constraints.push(constraint);
        Ok(())
    }

    /// Remove a constraint
    pub async fn remove(&self, table: &str, name: &str) -> Result<(), String> {
        let mut constraints = self.constraints.write().await;
        let table_constraints = constraints.get_mut(table)
            .ok_or_else(|| format!("No constraints for table {}", table))?;

        let pos = table_constraints.iter().position(|c| c.name == name)
            .ok_or_else(|| format!("Constraint {} not found", name))?;

        table_constraints.remove(pos);
        Ok(())
    }

    /// Get constraints for table
    pub async fn get_table_constraints(&self, table: &str) -> Vec<Constraint> {
        let constraints = self.constraints.read().await;
        constraints.get(table).cloned().unwrap_or_default()
    }

    /// Validate row against constraints
    pub async fn validate(&self, table: &str, row: &serde_json::Value) -> Vec<ConstraintViolation> {
        let constraints = self.constraints.read().await;
        let mut violations = Vec::new();

        if let Some(table_constraints) = constraints.get(table) {
            for constraint in table_constraints {
                if let Some(violation) = self.check_constraint(constraint, row) {
                    violations.push(violation);
                }
            }
        }

        violations
    }

    fn check_constraint(&self, constraint: &Constraint, row: &serde_json::Value) -> Option<ConstraintViolation> {
        match &constraint.constraint_type {
            ConstraintType::NotNull { column } => {
                if row.get(column).map(|v| v.is_null()).unwrap_or(true) {
                    return Some(ConstraintViolation {
                        constraint_name: constraint.name.clone(),
                        table: constraint.table.clone(),
                        message: format!("Column {} cannot be null", column),
                        row_data: Some(row.clone()),
                    });
                }
            }
            ConstraintType::Check { expression } => {
                // In production: evaluate expression
                // For now, just check if it's a simple column != value check
                if expression.contains("!=") || expression.contains("<>") {
                    // Placeholder validation
                }
            }
            _ => {}
        }
        None
    }

    /// Get constraint by name
    pub async fn get(&self, table: &str, name: &str) -> Option<Constraint> {
        let constraints = self.constraints.read().await;
        constraints.get(table)?
            .iter()
            .find(|c| c.name == name)
            .cloned()
    }

    /// List all constraints
    pub async fn list_all(&self) -> Vec<Constraint> {
        let constraints = self.constraints.read().await;
        constraints.values().flat_map(|v| v.clone()).collect()
    }
}

impl Default for ConstraintManager {
    fn default() -> Self {
        Self::new()
    }
}
