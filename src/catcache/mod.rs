//! Catcache Module - Catalog Cache

use std::collections::{HashMap, VecDeque};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Catalog cache entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatCacheEntry {
    pub hash: u64,
    pub tuple: CatCacheTuple,
    pub ref_count: u32,
    pub dead: bool,
    pub negative: bool,
}

/// Catalog cache tuple
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatCacheTuple {
    pub rel_id: u64,
    pub data: serde_json::Value,
    pub created_at: i64,
}

/// Catalog cache
pub struct CatCache {
    id: u32,
    rel_id: u64,
    nkeys: u32,
    key_attrs: Vec<i16>,
    entries: RwLock<HashMap<u64, CatCacheEntry>>,
    lru: RwLock<VecDeque<u64>>,
    max_entries: usize,
    stats: RwLock<CatCacheStats>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CatCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub negative_hits: u64,
    pub invalidations: u64,
}

impl CatCache {
    pub fn new(id: u32, rel_id: u64, key_attrs: Vec<i16>) -> Self {
        Self {
            id,
            rel_id,
            nkeys: key_attrs.len() as u32,
            key_attrs,
            entries: RwLock::new(HashMap::new()),
            lru: RwLock::new(VecDeque::new()),
            max_entries: 1000,
            stats: RwLock::new(CatCacheStats::default()),
        }
    }

    fn compute_hash(&self, keys: &[serde_json::Value]) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        for key in keys {
            key.to_string().hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Search for an entry
    pub async fn search(&self, keys: &[serde_json::Value]) -> Option<CatCacheTuple> {
        let hash = self.compute_hash(keys);
        
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(&hash) {
            if !entry.dead {
                let mut stats = self.stats.write().await;
                if entry.negative {
                    stats.negative_hits += 1;
                    return None;
                } else {
                    stats.hits += 1;
                    return Some(entry.tuple.clone());
                }
            }
        }

        let mut stats = self.stats.write().await;
        stats.misses += 1;
        None
    }

    /// Insert an entry
    pub async fn insert(&self, keys: &[serde_json::Value], tuple: CatCacheTuple) {
        let hash = self.compute_hash(keys);
        
        let mut entries = self.entries.write().await;
        let mut lru = self.lru.write().await;

        // Evict if at capacity
        while entries.len() >= self.max_entries && !lru.is_empty() {
            if let Some(old_hash) = lru.pop_front() {
                if let Some(entry) = entries.get(&old_hash) {
                    if entry.ref_count == 0 {
                        entries.remove(&old_hash);
                    }
                }
            }
        }

        let entry = CatCacheEntry {
            hash,
            tuple,
            ref_count: 0,
            dead: false,
            negative: false,
        };

        entries.insert(hash, entry);
        lru.push_back(hash);
    }

    /// Insert negative entry (not found)
    pub async fn insert_negative(&self, keys: &[serde_json::Value]) {
        let hash = self.compute_hash(keys);
        
        let mut entries = self.entries.write().await;
        let mut lru = self.lru.write().await;

        let entry = CatCacheEntry {
            hash,
            tuple: CatCacheTuple {
                rel_id: self.rel_id,
                data: serde_json::Value::Null,
                created_at: chrono::Utc::now().timestamp(),
            },
            ref_count: 0,
            dead: false,
            negative: true,
        };

        entries.insert(hash, entry);
        lru.push_back(hash);
    }

    /// Invalidate entry
    pub async fn invalidate(&self, keys: &[serde_json::Value]) {
        let hash = self.compute_hash(keys);
        
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&hash) {
            entry.dead = true;
        }

        let mut stats = self.stats.write().await;
        stats.invalidations += 1;
    }

    /// Invalidate all entries
    pub async fn invalidate_all(&self) {
        let mut entries = self.entries.write().await;
        for entry in entries.values_mut() {
            entry.dead = true;
        }
    }

    /// Get statistics
    pub async fn get_stats(&self) -> CatCacheStats {
        self.stats.read().await.clone()
    }

    /// Get entry count
    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }
}

/// Catalog cache manager
pub struct CatCacheManager {
    caches: RwLock<HashMap<u32, CatCache>>,
}

impl CatCacheManager {
    pub fn new() -> Self {
        Self {
            caches: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_or_create(&self, id: u32, rel_id: u64, key_attrs: Vec<i16>) -> u32 {
        let mut caches = self.caches.write().await;
        if !caches.contains_key(&id) {
            caches.insert(id, CatCache::new(id, rel_id, key_attrs));
        }
        id
    }

    pub async fn search(&self, id: u32, keys: &[serde_json::Value]) -> Option<CatCacheTuple> {
        let caches = self.caches.read().await;
        if let Some(cache) = caches.get(&id) {
            return cache.search(keys).await;
        }
        None
    }

    pub async fn invalidate_relation(&self, rel_id: u64) {
        let caches = self.caches.read().await;
        for cache in caches.values() {
            if cache.rel_id == rel_id {
                cache.invalidate_all().await;
            }
        }
    }
}

impl Default for CatCacheManager {
    fn default() -> Self {
        Self::new()
    }
}
