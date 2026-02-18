//! Result Cache - Caches query results with automatic invalidation

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use lru::LruCache;
use std::num::NonZeroUsize;

/// Parse table names from a simple SQL query (heuristic)
pub fn extract_tables(sql: &str) -> Vec<String> {
    let sql_lower = sql.to_lowercase();
    let mut tables = Vec::new();

    // Find tables after FROM, JOIN keywords
    for keyword in &["from ", "join "] {
        let mut pos = 0;
        while let Some(idx) = sql_lower[pos..].find(keyword) {
            let start = pos + idx + keyword.len();
            // Skip whitespace
            let rest = sql_lower[start..].trim_start();
            // Take word until whitespace or comma or paren
            let end = rest
                .find(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')')
                .unwrap_or(rest.len());
            let table = rest[..end].trim().to_string();
            if !table.is_empty() && !table.starts_with('(') {
                tables.push(table);
            }
            pos = start + 1;
            if pos >= sql_lower.len() {
                break;
            }
        }
    }
    tables
}

#[derive(Clone)]
pub struct CachedResult {
    pub data: serde_json::Value,
    pub created_at: std::time::Instant,
    pub ttl_secs: u64,
}

impl CachedResult {
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed().as_secs() >= self.ttl_secs
    }
}

pub struct ResultCache {
    cache: RwLock<LruCache<u64, CachedResult>>,
    /// table_name -> set of cache keys that read from it
    table_keys: RwLock<HashMap<String, HashSet<u64>>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl ResultCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
            table_keys: RwLock::new(HashMap::new()),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn key(sql: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let mut h = DefaultHasher::new();
        sql.hash(&mut h);
        h.finish()
    }

    /// Check if a query should be cached (only SELECT statements)
    pub fn is_cacheable(sql: &str) -> bool {
        let s = sql.trim().to_lowercase();
        s.starts_with("select") && !s.contains("now()") && !s.contains("random()")
    }

    pub async fn get(&self, sql: &str) -> Option<serde_json::Value> {
        let k = Self::key(sql);
        let mut cache = self.cache.write().await;
        if let Some(entry) = cache.get(&k) {
            if entry.is_expired() {
                cache.pop(&k);
                self.misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return None;
            }
            self.hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(entry.data.clone())
        } else {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }

    pub async fn set(&self, sql: &str, data: serde_json::Value, ttl_secs: u64) {
        let k = Self::key(sql);
        let tables = extract_tables(sql);

        {
            let mut cache = self.cache.write().await;
            cache.put(
                k,
                CachedResult {
                    data,
                    created_at: std::time::Instant::now(),
                    ttl_secs,
                },
            );
        }

        let mut table_keys = self.table_keys.write().await;
        for table in tables {
            table_keys.entry(table).or_default().insert(k);
        }
    }

    /// Invalidate all cached results that read from a given table
    pub async fn invalidate_table(&self, table: &str) {
        let table_lower = table.to_lowercase();
        let keys_to_remove = {
            let table_keys = self.table_keys.read().await;
            table_keys.get(&table_lower).cloned().unwrap_or_default()
        };

        if !keys_to_remove.is_empty() {
            let mut cache = self.cache.write().await;
            for k in &keys_to_remove {
                cache.pop(k);
            }
            let mut table_keys = self.table_keys.write().await;
            table_keys.remove(&table_lower);
        }
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

impl Default for ResultCache {
    fn default() -> Self {
        Self::new(1000)
    }
}
