//! Quota Module - Resource quotas and limits

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Resource type
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceType {
    Cpu,
    Memory,
    Storage,
    Connections,
    QueriesPerSecond,
    RowsPerQuery,
    ResultSize,
    Tables,
    Indexes,
    Users,
    Databases,
    Custom(String),
}

/// Quota limit
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quota {
    pub resource: ResourceType,
    pub limit: u64,
    pub period_seconds: Option<u64>, // None = no time window
    pub hard: bool, // Hard limit blocks, soft limit warns
}

/// Quota usage
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QuotaUsage {
    pub current: u64,
    pub peak: u64,
    pub period_start: Option<i64>,
    pub last_updated: i64,
}

/// Quota policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaPolicy {
    pub name: String,
    pub quotas: Vec<Quota>,
    pub applies_to: QuotaTarget,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QuotaTarget {
    User(String),
    Role(String),
    Database(String),
    Tenant(String),
    Global,
}

/// Quota check result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QuotaResult {
    Allowed,
    Warning { resource: ResourceType, usage: f64, limit: u64 },
    Exceeded { resource: ResourceType, usage: u64, limit: u64 },
}

/// Quota manager
pub struct QuotaManager {
    policies: RwLock<HashMap<String, QuotaPolicy>>,
    usage: RwLock<HashMap<(String, ResourceType), QuotaUsage>>,
}

impl QuotaManager {
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
        }
    }

    /// Add quota policy
    pub async fn add_policy(&self, policy: QuotaPolicy) {
        let mut policies = self.policies.write().await;
        policies.insert(policy.name.clone(), policy);
    }

    /// Remove quota policy
    pub async fn remove_policy(&self, name: &str) {
        let mut policies = self.policies.write().await;
        policies.remove(name);
    }

    /// Check quota
    pub async fn check(&self, target: &str, resource: &ResourceType, amount: u64) -> QuotaResult {
        let policies = self.policies.read().await;
        let usage = self.usage.read().await;

        for policy in policies.values() {
            if !self.matches_target(&policy.applies_to, target) {
                continue;
            }

            for quota in &policy.quotas {
                if &quota.resource != resource {
                    continue;
                }

                let key = (target.to_string(), resource.clone());
                let current = usage.get(&key)
                    .map(|u| u.current)
                    .unwrap_or(0);

                let new_usage = current + amount;

                if new_usage > quota.limit {
                    if quota.hard {
                        return QuotaResult::Exceeded {
                            resource: resource.clone(),
                            usage: new_usage,
                            limit: quota.limit,
                        };
                    } else {
                        return QuotaResult::Warning {
                            resource: resource.clone(),
                            usage: new_usage as f64 / quota.limit as f64,
                            limit: quota.limit,
                        };
                    }
                }
            }
        }

        QuotaResult::Allowed
    }

    /// Record usage
    pub async fn record(&self, target: &str, resource: ResourceType, amount: u64) {
        let mut usage = self.usage.write().await;
        let key = (target.to_string(), resource);
        let now = chrono::Utc::now().timestamp();

        let entry = usage.entry(key).or_insert_with(|| QuotaUsage {
            period_start: Some(now),
            ..Default::default()
        });

        entry.current += amount;
        entry.peak = entry.peak.max(entry.current);
        entry.last_updated = now;
    }

    /// Decrement usage
    pub async fn release(&self, target: &str, resource: ResourceType, amount: u64) {
        let mut usage = self.usage.write().await;
        let key = (target.to_string(), resource);

        if let Some(entry) = usage.get_mut(&key) {
            entry.current = entry.current.saturating_sub(amount);
            entry.last_updated = chrono::Utc::now().timestamp();
        }
    }

    /// Reset usage for a period
    pub async fn reset(&self, target: &str, resource: ResourceType) {
        let mut usage = self.usage.write().await;
        let key = (target.to_string(), resource);
        usage.remove(&key);
    }

    /// Get usage
    pub async fn get_usage(&self, target: &str, resource: &ResourceType) -> Option<QuotaUsage> {
        let usage = self.usage.read().await;
        let key = (target.to_string(), resource.clone());
        usage.get(&key).cloned()
    }

    /// Get all usage for target
    pub async fn get_all_usage(&self, target: &str) -> HashMap<ResourceType, QuotaUsage> {
        let usage = self.usage.read().await;
        usage.iter()
            .filter(|((t, _), _)| t == target)
            .map(|((_, r), u)| (r.clone(), u.clone()))
            .collect()
    }

    fn matches_target(&self, policy_target: &QuotaTarget, target: &str) -> bool {
        match policy_target {
            QuotaTarget::Global => true,
            QuotaTarget::User(u) => u == target,
            QuotaTarget::Role(r) => r == target,
            QuotaTarget::Database(d) => d == target,
            QuotaTarget::Tenant(t) => t == target,
        }
    }

    /// Get quota status
    pub async fn get_status(&self, target: &str) -> QuotaStatus {
        let policies = self.policies.read().await;
        let usage = self.usage.read().await;

        let mut items = Vec::new();

        for policy in policies.values() {
            if !self.matches_target(&policy.applies_to, target) {
                continue;
            }

            for quota in &policy.quotas {
                let key = (target.to_string(), quota.resource.clone());
                let current = usage.get(&key).map(|u| u.current).unwrap_or(0);
                let percent = if quota.limit > 0 {
                    (current as f64 / quota.limit as f64) * 100.0
                } else {
                    0.0
                };

                items.push(QuotaStatusItem {
                    resource: quota.resource.clone(),
                    current,
                    limit: quota.limit,
                    percent_used: percent,
                    hard: quota.hard,
                });
            }
        }

        QuotaStatus { target: target.to_string(), items }
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Quota status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaStatus {
    pub target: String,
    pub items: Vec<QuotaStatusItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaStatusItem {
    pub resource: ResourceType,
    pub current: u64,
    pub limit: u64,
    pub percent_used: f64,
    pub hard: bool,
}
