//! Domain Module - Custom domain types with constraints

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Domain definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Domain {
    pub name: String,
    pub schema: String,
    pub base_type: String,
    pub default_value: Option<String>,
    pub not_null: bool,
    pub constraints: Vec<DomainConstraint>,
    pub owner: String,
    pub created_at: i64,
}

/// Domain constraint
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainConstraint {
    pub name: String,
    pub check_expression: String,
    pub validated: bool,
}

impl Domain {
    pub fn new(schema: &str, name: &str, base_type: &str) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.to_string(),
            base_type: base_type.to_string(),
            default_value: None,
            not_null: false,
            constraints: vec![],
            owner: "postgres".to_string(),
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn with_default(mut self, default: &str) -> Self {
        self.default_value = Some(default.to_string());
        self
    }

    pub fn with_not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    pub fn add_constraint(mut self, name: &str, expression: &str) -> Self {
        self.constraints.push(DomainConstraint {
            name: name.to_string(),
            check_expression: expression.to_string(),
            validated: true,
        });
        self
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Validate a value against domain constraints
    pub fn validate(&self, value: &serde_json::Value) -> Vec<String> {
        let mut errors = Vec::new();

        if self.not_null && value.is_null() {
            errors.push(format!("Domain {} does not allow NULL", self.name));
        }

        // In production: evaluate check constraints
        for constraint in &self.constraints {
            // Placeholder for constraint evaluation
            if constraint.check_expression.contains("VALUE > 0") {
                if let Some(n) = value.as_f64() {
                    if n <= 0.0 {
                        errors.push(format!("Constraint {} violated", constraint.name));
                    }
                }
            }
        }

        errors
    }
}

/// Domain manager
pub struct DomainManager {
    domains: RwLock<HashMap<String, Domain>>,
}

impl DomainManager {
    pub fn new() -> Self {
        Self {
            domains: RwLock::new(HashMap::new()),
        }
    }

    /// Create a domain
    pub async fn create(&self, domain: Domain) -> Result<(), String> {
        let mut domains = self.domains.write().await;
        let key = domain.full_name();

        if domains.contains_key(&key) {
            return Err(format!("Domain {} already exists", key));
        }

        domains.insert(key, domain);
        Ok(())
    }

    /// Drop a domain
    pub async fn drop(&self, schema: &str, name: &str) -> Result<(), String> {
        let mut domains = self.domains.write().await;
        let key = format!("{}.{}", schema, name);

        domains.remove(&key)
            .ok_or_else(|| format!("Domain {} not found", key))?;
        Ok(())
    }

    /// Get a domain
    pub async fn get(&self, schema: &str, name: &str) -> Option<Domain> {
        let domains = self.domains.read().await;
        let key = format!("{}.{}", schema, name);
        domains.get(&key).cloned()
    }

    /// List all domains
    pub async fn list(&self, schema: Option<&str>) -> Vec<Domain> {
        let domains = self.domains.read().await;
        domains.values()
            .filter(|d| schema.map(|s| d.schema == s).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Add constraint to domain
    pub async fn add_constraint(&self, schema: &str, name: &str, constraint: DomainConstraint) -> Result<(), String> {
        let mut domains = self.domains.write().await;
        let key = format!("{}.{}", schema, name);

        let domain = domains.get_mut(&key)
            .ok_or_else(|| format!("Domain {} not found", key))?;

        if domain.constraints.iter().any(|c| c.name == constraint.name) {
            return Err(format!("Constraint {} already exists", constraint.name));
        }

        domain.constraints.push(constraint);
        Ok(())
    }

    /// Drop constraint from domain
    pub async fn drop_constraint(&self, schema: &str, name: &str, constraint_name: &str) -> Result<(), String> {
        let mut domains = self.domains.write().await;
        let key = format!("{}.{}", schema, name);

        let domain = domains.get_mut(&key)
            .ok_or_else(|| format!("Domain {} not found", key))?;

        domain.constraints.retain(|c| c.name != constraint_name);
        Ok(())
    }

    /// Validate value
    pub async fn validate(&self, schema: &str, name: &str, value: &serde_json::Value) -> Vec<String> {
        if let Some(domain) = self.get(schema, name).await {
            domain.validate(value)
        } else {
            vec![]
        }
    }
}

impl Default for DomainManager {
    fn default() -> Self {
        Self::new()
    }
}
