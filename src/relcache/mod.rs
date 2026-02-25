//! Relcache Module - Relation Cache

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Relation kind
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum RelKind {
    OrdinaryTable,
    Index,
    Sequence,
    ToastTable,
    View,
    MaterializedView,
    CompositeType,
    ForeignTable,
    PartitionedTable,
    PartitionedIndex,
}

/// Cached relation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedRelation {
    pub oid: u64,
    pub name: String,
    pub namespace: u64,
    pub kind: RelKind,
    pub owner: u64,
    pub am: u64,
    pub filenode: u64,
    pub tablespace: u64,
    pub persistence: char,
    pub natts: u16,
    pub checks: u16,
    pub has_rules: bool,
    pub has_triggers: bool,
    pub has_subclass: bool,
    pub row_security: bool,
    pub is_partition: bool,
    pub is_populated: bool,
    pub attributes: Vec<CachedAttribute>,
    pub constraints: Vec<CachedConstraint>,
    pub indexes: Vec<u64>,
}

/// Cached attribute
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedAttribute {
    pub num: i16,
    pub name: String,
    pub type_oid: u64,
    pub type_mod: i32,
    pub len: i16,
    pub not_null: bool,
    pub has_default: bool,
    pub identity: char,
    pub generated: char,
}

/// Cached constraint
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedConstraint {
    pub oid: u64,
    pub name: String,
    pub contype: char,
    pub condeferrable: bool,
    pub condeferred: bool,
}

/// Relation cache
pub struct RelationCache {
    cache: RwLock<HashMap<u64, CachedRelation>>,
    name_index: RwLock<HashMap<(u64, String), u64>>,
    invalidated: RwLock<Vec<u64>>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            invalidated: RwLock::new(Vec::new()),
        }
    }

    /// Get relation by OID
    pub async fn get(&self, oid: u64) -> Option<CachedRelation> {
        let cache = self.cache.read().await;
        cache.get(&oid).cloned()
    }

    /// Get relation by namespace and name
    pub async fn get_by_name(&self, namespace: u64, name: &str) -> Option<CachedRelation> {
        let name_index = self.name_index.read().await;
        let oid = name_index.get(&(namespace, name.to_string()))?;
        self.get(*oid).await
    }

    /// Insert or update relation
    pub async fn insert(&self, rel: CachedRelation) {
        let oid = rel.oid;
        let namespace = rel.namespace;
        let name = rel.name.clone();

        let mut cache = self.cache.write().await;
        let mut name_index = self.name_index.write().await;

        cache.insert(oid, rel);
        name_index.insert((namespace, name), oid);
    }

    /// Remove relation
    pub async fn remove(&self, oid: u64) {
        let mut cache = self.cache.write().await;
        let mut name_index = self.name_index.write().await;

        if let Some(rel) = cache.remove(&oid) {
            name_index.remove(&(rel.namespace, rel.name));
        }
    }

    /// Invalidate relation
    pub async fn invalidate(&self, oid: u64) {
        let mut invalidated = self.invalidated.write().await;
        invalidated.push(oid);
    }

    /// Accept invalidations
    pub async fn accept_invalidations(&self) {
        let mut invalidated = self.invalidated.write().await;
        let mut cache = self.cache.write().await;
        let mut name_index = self.name_index.write().await;

        for oid in invalidated.drain(..) {
            if let Some(rel) = cache.remove(&oid) {
                name_index.remove(&(rel.namespace, rel.name));
            }
        }
    }

    /// Get all relations in namespace
    pub async fn get_namespace(&self, namespace: u64) -> Vec<CachedRelation> {
        let cache = self.cache.read().await;
        cache.values()
            .filter(|r| r.namespace == namespace)
            .cloned()
            .collect()
    }

    /// Get relation indexes
    pub async fn get_indexes(&self, oid: u64) -> Vec<u64> {
        let cache = self.cache.read().await;
        cache.get(&oid)
            .map(|r| r.indexes.clone())
            .unwrap_or_default()
    }

    /// Cache size
    pub async fn size(&self) -> usize {
        self.cache.read().await.len()
    }
}

impl Default for RelationCache {
    fn default() -> Self {
        Self::new()
    }
}
