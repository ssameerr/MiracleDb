//! Policy Module - Policy engine for access control

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Policy {
    pub id: String,
    pub name: String,
    pub description: String,
    pub effect: PolicyEffect,
    pub principals: Vec<Principal>,
    pub actions: Vec<String>,
    pub resources: Vec<Resource>,
    pub conditions: Vec<Condition>,
    pub priority: i32,
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

/// Principal - who the policy applies to
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Principal {
    User(String),
    Role(String),
    Group(String),
    Service(String),
    Any,
}

/// Resource - what the policy applies to
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Resource {
    Database(String),
    Schema(String),
    Table(String),
    Column(String, String), // table, column
    Function(String),
    Pattern(String),
    Any,
}

impl Resource {
    pub fn matches(&self, resource_type: &str, resource_name: &str) -> bool {
        match self {
            Resource::Any => true,
            Resource::Database(name) => resource_type == "database" && Self::pattern_match(name, resource_name),
            Resource::Schema(name) => resource_type == "schema" && Self::pattern_match(name, resource_name),
            Resource::Table(name) => resource_type == "table" && Self::pattern_match(name, resource_name),
            Resource::Function(name) => resource_type == "function" && Self::pattern_match(name, resource_name),
            Resource::Pattern(pattern) => Self::pattern_match(pattern, resource_name),
            _ => false,
        }
    }

    fn pattern_match(pattern: &str, value: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if pattern.ends_with('*') {
            return value.starts_with(&pattern[..pattern.len()-1]);
        }
        pattern == value
    }
}

/// Condition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Condition {
    pub operator: ConditionOperator,
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConditionOperator {
    Equals,
    NotEquals,
    In,
    NotIn,
    Contains,
    StartsWith,
    EndsWith,
    GreaterThan,
    LessThan,
    IpAddress,
    DateAfter,
    DateBefore,
}

impl Condition {
    pub fn evaluate(&self, context: &PolicyContext) -> bool {
        let actual = match context.get(&self.key) {
            Some(v) => v,
            None => return false,
        };

        match self.operator {
            ConditionOperator::Equals => actual == &self.value,
            ConditionOperator::NotEquals => actual != &self.value,
            ConditionOperator::In => {
                if let Some(arr) = self.value.as_array() {
                    arr.contains(actual)
                } else {
                    false
                }
            }
            ConditionOperator::Contains => {
                if let (Some(actual_str), Some(value_str)) = (actual.as_str(), self.value.as_str()) {
                    actual_str.contains(value_str)
                } else {
                    false
                }
            }
            ConditionOperator::StartsWith => {
                if let (Some(actual_str), Some(value_str)) = (actual.as_str(), self.value.as_str()) {
                    actual_str.starts_with(value_str)
                } else {
                    false
                }
            }
            _ => true,
        }
    }
}

/// Policy context
pub type PolicyContext = HashMap<String, serde_json::Value>;

/// Policy evaluation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyResult {
    pub allowed: bool,
    pub matched_policy: Option<String>,
    pub reason: String,
}

/// Policy engine
pub struct PolicyEngine {
    policies: RwLock<Vec<Policy>>,
}

impl PolicyEngine {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(Vec::new()),
        }
    }

    /// Add a policy
    pub async fn add_policy(&self, policy: Policy) {
        let mut policies = self.policies.write().await;
        policies.push(policy);
        policies.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove a policy
    pub async fn remove_policy(&self, policy_id: &str) {
        let mut policies = self.policies.write().await;
        policies.retain(|p| p.id != policy_id);
    }

    /// Evaluate policies
    pub async fn evaluate(
        &self,
        principal: &Principal,
        action: &str,
        resource_type: &str,
        resource_name: &str,
        context: &PolicyContext,
    ) -> PolicyResult {
        let policies = self.policies.read().await;

        // First check deny policies
        for policy in policies.iter().filter(|p| p.enabled && p.effect == PolicyEffect::Deny) {
            if self.matches_policy(policy, principal, action, resource_type, resource_name, context) {
                return PolicyResult {
                    allowed: false,
                    matched_policy: Some(policy.id.clone()),
                    reason: format!("Denied by policy: {}", policy.name),
                };
            }
        }

        // Then check allow policies
        for policy in policies.iter().filter(|p| p.enabled && p.effect == PolicyEffect::Allow) {
            if self.matches_policy(policy, principal, action, resource_type, resource_name, context) {
                return PolicyResult {
                    allowed: true,
                    matched_policy: Some(policy.id.clone()),
                    reason: format!("Allowed by policy: {}", policy.name),
                };
            }
        }

        // Default deny
        PolicyResult {
            allowed: false,
            matched_policy: None,
            reason: "No matching policy found".to_string(),
        }
    }

    fn matches_policy(
        &self,
        policy: &Policy,
        principal: &Principal,
        action: &str,
        resource_type: &str,
        resource_name: &str,
        context: &PolicyContext,
    ) -> bool {
        // Check principal
        let principal_match = policy.principals.iter().any(|p| match (p, principal) {
            (Principal::Any, _) => true,
            (Principal::User(a), Principal::User(b)) => a == b || a == "*",
            (Principal::Role(a), Principal::Role(b)) => a == b || a == "*",
            (Principal::Group(a), Principal::Group(b)) => a == b || a == "*",
            _ => false,
        });
        if !principal_match {
            return false;
        }

        // Check action
        let action_match = policy.actions.iter().any(|a| a == "*" || a == action);
        if !action_match {
            return false;
        }

        // Check resource
        let resource_match = policy.resources.iter().any(|r| r.matches(resource_type, resource_name));
        if !resource_match {
            return false;
        }

        // Check conditions
        policy.conditions.iter().all(|c| c.evaluate(context))
    }

    /// Get all policies
    pub async fn list_policies(&self) -> Vec<Policy> {
        let policies = self.policies.read().await;
        policies.clone()
    }
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}
