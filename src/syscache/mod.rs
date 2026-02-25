//! Syscache Module - System Catalog Cache

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Syscache ID
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum SysCacheId {
    AttributeRelid,
    AttributeName,
    ClassOid,
    ClassName,
    TypeOid,
    TypeName,
    ProcOid,
    ProcName,
    IndexRelid,
    OperOid,
    OperName,
    AmOid,
    AmName,
    NamespaceOid,
    NamespaceName,
    AuthIdOid,
    AuthIdName,
}

/// Cached tuple
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedTuple {
    pub cache_id: SysCacheId,
    pub key: CacheKey,
    pub data: serde_json::Value,
    pub oid: u64,
    pub created_at: i64,
}

/// Cache key
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub enum CacheKey {
    Single(u64),
    Double(u64, String),
    Triple(u64, String, u64),
}

/// System catalog cache
pub struct SysCache {
    caches: RwLock<HashMap<SysCacheId, HashMap<CacheKey, CachedTuple>>>,
    oid_index: RwLock<HashMap<(SysCacheId, u64), CacheKey>>,
}

impl SysCache {
    pub fn new() -> Self {
        Self {
            caches: RwLock::new(HashMap::new()),
            oid_index: RwLock::new(HashMap::new()),
        }
    }

    /// Lookup by key
    pub async fn lookup(&self, cache_id: SysCacheId, key: &CacheKey) -> Option<CachedTuple> {
        let caches = self.caches.read().await;
        caches.get(&cache_id)?
            .get(key)
            .cloned()
    }

    /// Lookup by OID
    pub async fn lookup_oid(&self, cache_id: SysCacheId, oid: u64) -> Option<CachedTuple> {
        let oid_index = self.oid_index.read().await;
        let key = oid_index.get(&(cache_id, oid))?;
        self.lookup(cache_id, key).await
    }

    /// Insert tuple
    pub async fn insert(&self, tuple: CachedTuple) {
        let cache_id = tuple.cache_id;
        let key = tuple.key.clone();
        let oid = tuple.oid;

        let mut caches = self.caches.write().await;
        let mut oid_index = self.oid_index.write().await;

        let cache = caches.entry(cache_id).or_insert_with(HashMap::new);
        cache.insert(key.clone(), tuple);
        oid_index.insert((cache_id, oid), key);
    }

    /// Invalidate by OID
    pub async fn invalidate_oid(&self, cache_id: SysCacheId, oid: u64) {
        let key = {
            let oid_index = self.oid_index.read().await;
            oid_index.get(&(cache_id, oid)).cloned()
        };

        if let Some(key) = key {
            let mut caches = self.caches.write().await;
            let mut oid_index = self.oid_index.write().await;

            if let Some(cache) = caches.get_mut(&cache_id) {
                cache.remove(&key);
            }
            oid_index.remove(&(cache_id, oid));
        }
    }

    /// Invalidate all entries for a cache
    pub async fn invalidate_all(&self, cache_id: SysCacheId) {
        let mut caches = self.caches.write().await;
        let mut oid_index = self.oid_index.write().await;

        if let Some(cache) = caches.get_mut(&cache_id) {
            let oids: Vec<u64> = cache.values().map(|t| t.oid).collect();
            cache.clear();
            
            for oid in oids {
                oid_index.remove(&(cache_id, oid));
            }
        }
    }

    /// Reset all caches
    pub async fn reset(&self) {
        let mut caches = self.caches.write().await;
        let mut oid_index = self.oid_index.write().await;
        caches.clear();
        oid_index.clear();
    }

    /// Get cache size
    pub async fn size(&self, cache_id: SysCacheId) -> usize {
        let caches = self.caches.read().await;
        caches.get(&cache_id)
            .map(|c| c.len())
            .unwrap_or(0)
    }

    /// Get total cache size
    pub async fn total_size(&self) -> usize {
        let caches = self.caches.read().await;
        caches.values().map(|c| c.len()).sum()
    }

    // Helper methods for common lookups

    /// Get type by OID
    pub async fn get_type(&self, oid: u64) -> Option<serde_json::Value> {
        self.lookup_oid(SysCacheId::TypeOid, oid).await
            .map(|t| t.data)
    }

    /// Get type by name
    pub async fn get_type_by_name(&self, namespace: u64, name: &str) -> Option<serde_json::Value> {
        let key = CacheKey::Double(namespace, name.to_string());
        self.lookup(SysCacheId::TypeName, &key).await
            .map(|t| t.data)
    }

    /// Get relation by OID
    pub async fn get_class(&self, oid: u64) -> Option<serde_json::Value> {
        self.lookup_oid(SysCacheId::ClassOid, oid).await
            .map(|t| t.data)
    }

    /// Get procedure by OID
    pub async fn get_proc(&self, oid: u64) -> Option<serde_json::Value> {
        self.lookup_oid(SysCacheId::ProcOid, oid).await
            .map(|t| t.data)
    }
}

impl Default for SysCache {
    fn default() -> Self {
        Self::new()
    }
}
