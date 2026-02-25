//! Label Module - Resource labeling and tagging

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Label key-value pair
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Label {
    pub key: String,
    pub value: String,
}

impl Label {
    pub fn new(key: &str, value: &str) -> Self {
        Self {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.splitn(2, '=').collect();
        if parts.len() == 2 {
            Some(Self::new(parts[0], parts[1]))
        } else {
            None
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}={}", self.key, self.value)
    }
}

/// Label selector for filtering
#[derive(Clone, Debug)]
pub struct LabelSelector {
    pub requirements: Vec<Requirement>,
}

#[derive(Clone, Debug)]
pub struct Requirement {
    pub key: String,
    pub operator: SelectorOperator,
    pub values: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum SelectorOperator {
    Equals,
    NotEquals,
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

impl LabelSelector {
    pub fn new() -> Self {
        Self { requirements: vec![] }
    }

    pub fn equals(mut self, key: &str, value: &str) -> Self {
        self.requirements.push(Requirement {
            key: key.to_string(),
            operator: SelectorOperator::Equals,
            values: vec![value.to_string()],
        });
        self
    }

    pub fn not_equals(mut self, key: &str, value: &str) -> Self {
        self.requirements.push(Requirement {
            key: key.to_string(),
            operator: SelectorOperator::NotEquals,
            values: vec![value.to_string()],
        });
        self
    }

    pub fn in_values(mut self, key: &str, values: Vec<&str>) -> Self {
        self.requirements.push(Requirement {
            key: key.to_string(),
            operator: SelectorOperator::In,
            values: values.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    pub fn exists(mut self, key: &str) -> Self {
        self.requirements.push(Requirement {
            key: key.to_string(),
            operator: SelectorOperator::Exists,
            values: vec![],
        });
        self
    }

    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        self.requirements.iter().all(|req| {
            match req.operator {
                SelectorOperator::Equals => {
                    labels.get(&req.key) == req.values.first()
                }
                SelectorOperator::NotEquals => {
                    labels.get(&req.key) != req.values.first()
                }
                SelectorOperator::In => {
                    labels.get(&req.key)
                        .map(|v| req.values.contains(v))
                        .unwrap_or(false)
                }
                SelectorOperator::NotIn => {
                    labels.get(&req.key)
                        .map(|v| !req.values.contains(v))
                        .unwrap_or(true)
                }
                SelectorOperator::Exists => {
                    labels.contains_key(&req.key)
                }
                SelectorOperator::DoesNotExist => {
                    !labels.contains_key(&req.key)
                }
            }
        })
    }
}

impl Default for LabelSelector {
    fn default() -> Self {
        Self::new()
    }
}

/// Labeled resource
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabeledResource {
    pub resource_type: String,
    pub resource_id: String,
    pub labels: HashMap<String, String>,
}

/// Label manager
pub struct LabelManager {
    resources: RwLock<HashMap<String, LabeledResource>>,
}

impl LabelManager {
    pub fn new() -> Self {
        Self {
            resources: RwLock::new(HashMap::new()),
        }
    }

    fn resource_key(resource_type: &str, resource_id: &str) -> String {
        format!("{}:{}", resource_type, resource_id)
    }

    /// Set labels on resource
    pub async fn set_labels(&self, resource_type: &str, resource_id: &str, labels: HashMap<String, String>) {
        let key = Self::resource_key(resource_type, resource_id);
        let mut resources = self.resources.write().await;
        
        resources.insert(key, LabeledResource {
            resource_type: resource_type.to_string(),
            resource_id: resource_id.to_string(),
            labels,
        });
    }

    /// Add label to resource
    pub async fn add_label(&self, resource_type: &str, resource_id: &str, label: Label) {
        let key = Self::resource_key(resource_type, resource_id);
        let mut resources = self.resources.write().await;
        
        let resource = resources.entry(key).or_insert_with(|| LabeledResource {
            resource_type: resource_type.to_string(),
            resource_id: resource_id.to_string(),
            labels: HashMap::new(),
        });
        
        resource.labels.insert(label.key, label.value);
    }

    /// Remove label from resource
    pub async fn remove_label(&self, resource_type: &str, resource_id: &str, key: &str) {
        let res_key = Self::resource_key(resource_type, resource_id);
        let mut resources = self.resources.write().await;
        
        if let Some(resource) = resources.get_mut(&res_key) {
            resource.labels.remove(key);
        }
    }

    /// Get labels for resource
    pub async fn get_labels(&self, resource_type: &str, resource_id: &str) -> HashMap<String, String> {
        let key = Self::resource_key(resource_type, resource_id);
        let resources = self.resources.read().await;
        
        resources.get(&key)
            .map(|r| r.labels.clone())
            .unwrap_or_default()
    }

    /// Select resources by labels
    pub async fn select(&self, resource_type: &str, selector: &LabelSelector) -> Vec<LabeledResource> {
        let resources = self.resources.read().await;
        
        resources.values()
            .filter(|r| r.resource_type == resource_type && selector.matches(&r.labels))
            .cloned()
            .collect()
    }

    /// Get all label keys
    pub async fn get_all_keys(&self) -> HashSet<String> {
        let resources = self.resources.read().await;
        
        resources.values()
            .flat_map(|r| r.labels.keys().cloned())
            .collect()
    }

    /// Get all values for a key
    pub async fn get_values_for_key(&self, key: &str) -> HashSet<String> {
        let resources = self.resources.read().await;
        
        resources.values()
            .filter_map(|r| r.labels.get(key).cloned())
            .collect()
    }
}

impl Default for LabelManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_label_selector() {
        let manager = LabelManager::new();
        
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "production".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());
        
        manager.set_labels("table", "users", labels).await;
        
        let selector = LabelSelector::new()
            .equals("env", "production");
        
        let results = manager.select("table", &selector).await;
        assert_eq!(results.len(), 1);
    }
}
