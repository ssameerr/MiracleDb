use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeatureType { Numerical, Categorical, Text, Embedding(usize) }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDefinition {
    pub name: String,
    pub feature_type: FeatureType,
    pub source_table: String,
    pub source_column: String,
    pub transform: Option<String>, // e.g. "log", "normalize", "one_hot"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureGroup {
    pub name: String,
    pub features: Vec<FeatureDefinition>,
    pub entity_key: String,
}

pub struct FeatureStore {
    groups: Arc<RwLock<HashMap<String, FeatureGroup>>>,
}

impl FeatureStore {
    pub fn new() -> Self {
        Self { groups: Arc::new(RwLock::new(HashMap::new())) }
    }
    pub fn register_group(&self, group: FeatureGroup) {
        self.groups.write().unwrap().insert(group.name.clone(), group);
    }
    pub fn get_group(&self, name: &str) -> Option<FeatureGroup> {
        self.groups.read().unwrap().get(name).cloned()
    }
    pub fn list_groups(&self) -> Vec<String> {
        self.groups.read().unwrap().keys().cloned().collect()
    }
}
