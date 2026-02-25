//! Advanced Caching System - Multi-tier, LLM, Semantic, and Adaptive Caching
//!
//! Implements cutting-edge caching architectures:
//! - Tiered Caching (L1 Hot / L2 Warm / L3 Cold)
//! - LLM Response Cache with prompt hashing
//! - Embedding/Vector Cache with similarity lookup
//! - Semantic Cache for approximate query matching
//! - Adaptive eviction using access patterns
//! - Write-through and Write-back policies

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::RwLock;
use lru::LruCache;
use std::num::NonZeroUsize;
use serde::{Serialize, Deserialize};

// =============================================================================
// Core Cache Entry
// =============================================================================

#[derive(Clone, Debug)]
pub struct CacheEntry<T> {
    pub value: T,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub access_count: u64,
    pub last_access: u64,
    pub size_bytes: usize,
}

impl<T: Clone> CacheEntry<T> {
    pub fn new(value: T, ttl_seconds: Option<i64>, size_bytes: usize) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            value,
            created_at: now,
            expires_at: ttl_seconds.map(|ttl| now + ttl),
            access_count: 1,
            last_access: now as u64,
            size_bytes,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            chrono::Utc::now().timestamp() > expires_at
        } else {
            false
        }
    }

    pub fn touch(&mut self) {
        self.access_count += 1;
        self.last_access = chrono::Utc::now().timestamp() as u64;
    }
}

// =============================================================================
// Cache Statistics
// =============================================================================

#[derive(Default)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub bytes_cached: AtomicUsize,
    pub items_cached: AtomicUsize,
}

impl CacheMetrics {
    pub fn hit(&self) { self.hits.fetch_add(1, Ordering::Relaxed); }
    pub fn miss(&self) { self.misses.fetch_add(1, Ordering::Relaxed); }
    pub fn evict(&self) { self.evictions.fetch_add(1, Ordering::Relaxed); }
    
    pub fn hit_rate(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed);
        let m = self.misses.load(Ordering::Relaxed);
        if h + m == 0 { 0.0 } else { h as f64 / (h + m) as f64 }
    }
}

// =============================================================================
// LRU-K Cache (Frequency + Recency)
// =============================================================================

/// LRU-K cache that considers both frequency and recency
/// More resistant to scan pollution than simple LRU
pub struct LruKCache<K: Hash + Eq + Clone, V: Clone> {
    data: RwLock<HashMap<K, CacheEntry<V>>>,
    history: RwLock<HashMap<K, Vec<u64>>>, // Access timestamps
    k: usize, // Number of accesses to track
    capacity: usize,
    metrics: CacheMetrics,
}

impl<K: Hash + Eq + Clone, V: Clone> LruKCache<K, V> {
    pub fn new(capacity: usize, k: usize) -> Self {
        Self {
            data: RwLock::new(HashMap::with_capacity(capacity)),
            history: RwLock::new(HashMap::new()),
            k,
            capacity,
            metrics: CacheMetrics::default(),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        let mut data = self.data.write().await;
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                drop(data);
                self.remove(key).await;
                self.metrics.miss();
                return None;
            }
            entry.touch();
            self.record_access(key).await;
            self.metrics.hit();
            Some(entry.value.clone())
        } else {
            self.metrics.miss();
            None
        }
    }

    pub async fn put(&self, key: K, value: V, ttl: Option<i64>) {
        let size = std::mem::size_of::<V>();
        let entry = CacheEntry::new(value, ttl, size);
        
        let mut data = self.data.write().await;
        if data.len() >= self.capacity {
            self.evict_lru_k(&mut data).await;
        }
        data.insert(key.clone(), entry);
        self.record_access(&key).await;
    }

    async fn record_access(&self, key: &K) {
        let now = chrono::Utc::now().timestamp() as u64;
        let mut history = self.history.write().await;
        let times = history.entry(key.clone()).or_insert_with(Vec::new);
        times.push(now);
        if times.len() > self.k {
            times.remove(0);
        }
    }

    async fn evict_lru_k(&self, data: &mut HashMap<K, CacheEntry<V>>) {
        let history = self.history.read().await;
        
        // Find entry with oldest k-th access (or oldest if < k accesses)
        let victim = data.keys()
            .min_by_key(|k| {
                history.get(*k)
                    .and_then(|times| times.first())
                    .copied()
                    .unwrap_or(0)
            })
            .cloned();
        
        if let Some(key) = victim {
            data.remove(&key);
            self.metrics.evict();
        }
    }

    async fn remove(&self, key: &K) {
        let mut data = self.data.write().await;
        data.remove(key);
    }
}

// =============================================================================
// Tiered Cache (L1/L2/L3)
// =============================================================================

/// Three-tier cache: L1 (hot), L2 (warm), L3 (cold)
/// Automatic promotion/demotion based on access patterns
pub struct TieredCache<K: Hash + Eq + Clone + Send + Sync, V: Clone + Send + Sync> {
    l1: RwLock<LruCache<K, CacheEntry<V>>>,  // Hot: Small, fast
    l2: RwLock<LruCache<K, CacheEntry<V>>>,  // Warm: Medium
    l3: RwLock<LruCache<K, CacheEntry<V>>>,  // Cold: Large, slower
    promotion_threshold: u64,  // Accesses needed to promote
    metrics: CacheMetrics,
}

impl<K: Hash + Eq + Clone + Send + Sync, V: Clone + Send + Sync> TieredCache<K, V> {
    pub fn new(l1_size: usize, l2_size: usize, l3_size: usize) -> Self {
        Self {
            l1: RwLock::new(LruCache::new(NonZeroUsize::new(l1_size).unwrap())),
            l2: RwLock::new(LruCache::new(NonZeroUsize::new(l2_size).unwrap())),
            l3: RwLock::new(LruCache::new(NonZeroUsize::new(l3_size).unwrap())),
            promotion_threshold: 3,
            metrics: CacheMetrics::default(),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        // Check L1 first
        {
            let mut l1 = self.l1.write().await;
            if let Some(entry) = l1.get_mut(key) {
                if !entry.is_expired() {
                    entry.touch();
                    self.metrics.hit();
                    return Some(entry.value.clone());
                }
            }
        }

        // Check L2
        let l2_result = {
            let mut l2 = self.l2.write().await;
            if let Some(entry) = l2.get_mut(key) {
                if !entry.is_expired() {
                    entry.touch();
                    let should_promote = entry.access_count >= self.promotion_threshold;
                    let result = Some((entry.value.clone(), entry.clone(), should_promote));
                    result
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        if let Some((value, entry_clone, should_promote)) = l2_result {
            if should_promote {
                self.promote_to_l1(key.clone(), entry_clone).await;
            }
            self.metrics.hit();
            return Some(value);
        }

        // Check L3
        let l3_result = {
            let mut l3 = self.l3.write().await;
            if let Some(entry) = l3.get_mut(key) {
                if !entry.is_expired() {
                    entry.touch();
                    let should_promote = entry.access_count >= 2;
                    let result = Some((entry.value.clone(), entry.clone(), should_promote));
                    result
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        if let Some((value, entry_clone, should_promote)) = l3_result {
            if should_promote {
                self.promote_to_l2(key.clone(), entry_clone).await;
            }
            self.metrics.hit();
            return Some(value);
        }

        self.metrics.miss();
        None
    }

    pub async fn put(&self, key: K, value: V, ttl: Option<i64>) {
        // New entries go to L3
        let entry = CacheEntry::new(value, ttl, 0);
        let mut l3 = self.l3.write().await;
        l3.put(key, entry);
    }

    async fn promote_to_l1(&self, key: K, entry: CacheEntry<V>) {
        let mut l1 = self.l1.write().await;
        l1.put(key, entry);
    }

    async fn promote_to_l2(&self, key: K, entry: CacheEntry<V>) {
        let mut l2 = self.l2.write().await;
        l2.put(key, entry);
    }
}

// =============================================================================
// LLM Response Cache
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LLMCacheEntry {
    pub prompt_hash: u64,
    pub response: String,
    pub model: String,
    pub tokens_used: u32,
    pub latency_ms: u32,
    pub created_at: i64,
}

/// Cache for LLM API responses
pub struct LLMCache {
    cache: RwLock<LruCache<u64, LLMCacheEntry>>,
    metrics: CacheMetrics,
}

impl LLMCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
            metrics: CacheMetrics::default(),
        }
    }

    pub fn hash_prompt(prompt: &str, model: &str, temperature: f32) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        prompt.hash(&mut hasher);
        model.hash(&mut hasher);
        ((temperature * 1000.0) as u32).hash(&mut hasher);
        hasher.finish()
    }

    pub async fn get(&self, prompt: &str, model: &str, temperature: f32) -> Option<String> {
        let hash = Self::hash_prompt(prompt, model, temperature);
        let mut cache = self.cache.write().await;
        
        if let Some(entry) = cache.get(&hash) {
            self.metrics.hit();
            Some(entry.response.clone())
        } else {
            self.metrics.miss();
            None
        }
    }

    pub async fn put(&self, prompt: &str, model: &str, temperature: f32, response: String, tokens: u32, latency_ms: u32) {
        let hash = Self::hash_prompt(prompt, model, temperature);
        let entry = LLMCacheEntry {
            prompt_hash: hash,
            response,
            model: model.to_string(),
            tokens_used: tokens,
            latency_ms,
            created_at: chrono::Utc::now().timestamp(),
        };
        
        let mut cache = self.cache.write().await;
        cache.put(hash, entry);
    }

    pub fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }
}

// =============================================================================
// Embedding/Vector Cache
// =============================================================================

#[derive(Clone, Debug)]
pub struct EmbeddingEntry {
    pub text_hash: u64,
    pub embedding: Vec<f32>,
    pub model: String,
    pub dimension: usize,
}

/// Cache for vector embeddings
pub struct EmbeddingCache {
    cache: RwLock<LruCache<u64, EmbeddingEntry>>,
    // Index for similarity search (simplified - production would use HNSW)
    vectors: RwLock<Vec<(u64, Vec<f32>)>>,
    metrics: CacheMetrics,
}

impl EmbeddingCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
            vectors: RwLock::new(Vec::with_capacity(capacity)),
            metrics: CacheMetrics::default(),
        }
    }

    pub fn hash_text(text: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        hasher.finish()
    }

    pub async fn get(&self, text: &str) -> Option<Vec<f32>> {
        let hash = Self::hash_text(text);
        let mut cache = self.cache.write().await;
        
        if let Some(entry) = cache.get(&hash) {
            self.metrics.hit();
            Some(entry.embedding.clone())
        } else {
            self.metrics.miss();
            None
        }
    }

    pub async fn put(&self, text: &str, embedding: Vec<f32>, model: &str) {
        let hash = Self::hash_text(text);
        let dim = embedding.len();
        
        let entry = EmbeddingEntry {
            text_hash: hash,
            embedding: embedding.clone(),
            model: model.to_string(),
            dimension: dim,
        };
        
        let mut cache = self.cache.write().await;
        cache.put(hash, entry);
        
        // Add to similarity index
        let mut vectors = self.vectors.write().await;
        vectors.push((hash, embedding));
        if vectors.len() > cache.cap().get() {
            vectors.remove(0);
        }
    }

    /// Find similar embeddings by cosine similarity
    pub async fn find_similar(&self, query: &[f32], top_k: usize, threshold: f32) -> Vec<(u64, f32)> {
        let vectors = self.vectors.read().await;
        let mut similarities: Vec<(u64, f32)> = vectors.iter()
            .map(|(hash, vec)| (*hash, cosine_similarity(query, vec)))
            .filter(|(_, sim)| *sim >= threshold)
            .collect();
        
        similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        similarities.truncate(top_k);
        similarities
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() { return 0.0; }
    
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a == 0.0 || norm_b == 0.0 { 0.0 } else { dot / (norm_a * norm_b) }
}

// =============================================================================
// Semantic Query Cache
// =============================================================================

/// Cache that matches semantically similar queries
pub struct SemanticCache {
    queries: RwLock<HashMap<u64, (String, Vec<f32>)>>,  // hash -> (query, embedding)
    results: RwLock<HashMap<u64, Vec<u8>>>,  // hash -> result
    similarity_threshold: f32,
    metrics: CacheMetrics,
}

impl SemanticCache {
    pub fn new(threshold: f32) -> Self {
        Self {
            queries: RwLock::new(HashMap::new()),
            results: RwLock::new(HashMap::new()),
            similarity_threshold: threshold,
            metrics: CacheMetrics::default(),
        }
    }

    /// Find cached result for semantically similar query
    pub async fn get_similar(&self, query_embedding: &[f32]) -> Option<Vec<u8>> {
        let queries = self.queries.read().await;
        
        for (hash, (_, embedding)) in queries.iter() {
            let sim = cosine_similarity(query_embedding, embedding);
            if sim >= self.similarity_threshold {
                let results = self.results.read().await;
                if let Some(result) = results.get(hash) {
                    self.metrics.hit();
                    return Some(result.clone());
                }
            }
        }
        
        self.metrics.miss();
        None
    }

    pub async fn put(&self, query: &str, embedding: Vec<f32>, result: Vec<u8>) {
        let hash = EmbeddingCache::hash_text(query);
        
        let mut queries = self.queries.write().await;
        queries.insert(hash, (query.to_string(), embedding));
        
        let mut results = self.results.write().await;
        results.insert(hash, result);
    }
}

// =============================================================================
// KV Store (Redis-like)
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KVValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    List(Vec<KVValue>),
    Hash(HashMap<String, KVValue>),
    Set(Vec<String>),
}

/// Redis-like key-value store with TTL
pub struct KVStore {
    data: RwLock<HashMap<String, CacheEntry<KVValue>>>,
    max_memory: usize,
    current_memory: AtomicUsize,
    metrics: CacheMetrics,
}

impl KVStore {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            max_memory: max_memory_bytes,
            current_memory: AtomicUsize::new(0),
            metrics: CacheMetrics::default(),
        }
    }

    pub async fn get(&self, key: &str) -> Option<KVValue> {
        let mut data = self.data.write().await;
        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                drop(data);
                self.del(key).await;
                self.metrics.miss();
                return None;
            }
            entry.touch();
            self.metrics.hit();
            Some(entry.value.clone())
        } else {
            self.metrics.miss();
            None
        }
    }

    pub async fn set(&self, key: &str, value: KVValue, ttl: Option<i64>) {
        let size = std::mem::size_of_val(&value);
        let entry = CacheEntry::new(value, ttl, size);
        
        // Evict if over memory limit
        while self.current_memory.load(Ordering::Relaxed) + size > self.max_memory {
            self.evict_one().await;
        }
        
        let mut data = self.data.write().await;
        if let Some(old) = data.insert(key.to_string(), entry) {
            self.current_memory.fetch_sub(old.size_bytes, Ordering::Relaxed);
        }
        self.current_memory.fetch_add(size, Ordering::Relaxed);
    }

    pub async fn del(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        if let Some(entry) = data.remove(key) {
            self.current_memory.fetch_sub(entry.size_bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub async fn incr(&self, key: &str, delta: i64) -> i64 {
        let mut data = self.data.write().await;
        let new_val = if let Some(entry) = data.get_mut(key) {
            match &mut entry.value {
                KVValue::Integer(v) => { *v += delta; *v }
                _ => delta,
            }
        } else {
            delta
        };
        
        if !data.contains_key(key) {
            let entry = CacheEntry::new(KVValue::Integer(new_val), None, 8);
            data.insert(key.to_string(), entry);
        }
        new_val
    }

    pub async fn lpush(&self, key: &str, values: Vec<KVValue>) {
        let mut data = self.data.write().await;
        if let Some(entry) = data.get_mut(key) {
            if let KVValue::List(list) = &mut entry.value {
                for v in values.into_iter().rev() {
                    list.insert(0, v);
                }
            }
        } else {
            let entry = CacheEntry::new(KVValue::List(values), None, 64);
            data.insert(key.to_string(), entry);
        }
    }

    pub async fn hset(&self, key: &str, field: &str, value: KVValue) {
        let mut data = self.data.write().await;
        if let Some(entry) = data.get_mut(key) {
            if let KVValue::Hash(hash) = &mut entry.value {
                hash.insert(field.to_string(), value);
            }
        } else {
            let mut hash = HashMap::new();
            hash.insert(field.to_string(), value);
            let entry = CacheEntry::new(KVValue::Hash(hash), None, 128);
            data.insert(key.to_string(), entry);
        }
    }

    pub async fn hget(&self, key: &str, field: &str) -> Option<KVValue> {
        let data = self.data.read().await;
        if let Some(entry) = data.get(key) {
            if let KVValue::Hash(hash) = &entry.value {
                return hash.get(field).cloned();
            }
        }
        None
    }

    async fn evict_one(&self) {
        let mut data = self.data.write().await;
        // Evict least recently used
        let victim = data.iter()
            .min_by_key(|(_, e)| e.last_access)
            .map(|(k, _)| k.clone());
        
        if let Some(key) = victim {
            if let Some(entry) = data.remove(&key) {
                self.current_memory.fetch_sub(entry.size_bytes, Ordering::Relaxed);
                self.metrics.evict();
            }
        }
    }

    pub async fn keys(&self, pattern: &str) -> Vec<String> {
        let data = self.data.read().await;
        let regex = regex::Regex::new(&pattern.replace("*", ".*")).unwrap_or_else(|_| regex::Regex::new(".*").unwrap());
        data.keys()
            .filter(|k| regex.is_match(k))
            .cloned()
            .collect()
    }

    pub async fn ttl(&self, key: &str) -> Option<i64> {
        let data = self.data.read().await;
        data.get(key).and_then(|e| {
            e.expires_at.map(|exp| exp - chrono::Utc::now().timestamp())
        })
    }
}

// =============================================================================
// Adaptive Cache (ML-based eviction)
// =============================================================================

/// Cache with adaptive eviction based on access patterns
pub struct AdaptiveCache<K: Hash + Eq + Clone, V: Clone> {
    data: RwLock<HashMap<K, CacheEntry<V>>>,
    access_history: RwLock<HashMap<K, AccessPattern>>,
    capacity: usize,
    metrics: CacheMetrics,
}

#[derive(Clone, Default)]
struct AccessPattern {
    frequency: u64,
    recency: u64,
    size: usize,
    access_times: Vec<u64>,  // Recent access timestamps
}

impl AccessPattern {
    fn score(&self, now: u64) -> f64 {
        // Combine frequency, recency, and size for eviction score
        // Higher score = more likely to keep
        let freq_score = (self.frequency as f64).ln().max(0.0);
        let recency_score = if now > self.recency { 
            1.0 / ((now - self.recency) as f64 + 1.0) 
        } else { 
            1.0 
        };
        let size_penalty = 1.0 / (self.size as f64 + 1.0);
        
        freq_score * 0.4 + recency_score * 0.5 + size_penalty * 0.1
    }
}

impl<K: Hash + Eq + Clone, V: Clone> AdaptiveCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: RwLock::new(HashMap::with_capacity(capacity)),
            access_history: RwLock::new(HashMap::new()),
            capacity,
            metrics: CacheMetrics::default(),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        let data = self.data.read().await;
        if let Some(entry) = data.get(key) {
            if entry.is_expired() {
                drop(data);
                self.remove(key).await;
                self.metrics.miss();
                return None;
            }
            
            // Update access pattern
            self.update_pattern(key, entry.size_bytes).await;
            self.metrics.hit();
            Some(entry.value.clone())
        } else {
            self.metrics.miss();
            None
        }
    }

    pub async fn put(&self, key: K, value: V, ttl: Option<i64>) {
        let size = std::mem::size_of::<V>();
        
        // Evict if needed
        let mut data = self.data.write().await;
        while data.len() >= self.capacity {
            self.evict_adaptive(&mut data).await;
        }
        
        let entry = CacheEntry::new(value, ttl, size);
        data.insert(key.clone(), entry);
        self.update_pattern(&key, size).await;
    }

    async fn update_pattern(&self, key: &K, size: usize) {
        let now = chrono::Utc::now().timestamp() as u64;
        let mut patterns = self.access_history.write().await;
        let pattern = patterns.entry(key.clone()).or_default();
        pattern.frequency += 1;
        pattern.recency = now;
        pattern.size = size;
        pattern.access_times.push(now);
        if pattern.access_times.len() > 10 {
            pattern.access_times.remove(0);
        }
    }

    async fn evict_adaptive(&self, data: &mut HashMap<K, CacheEntry<V>>) {
        let now = chrono::Utc::now().timestamp() as u64;
        let patterns = self.access_history.read().await;
        
        // Find entry with lowest adaptive score
        let victim = data.keys()
            .min_by(|a, b| {
                let score_a = patterns.get(*a).map(|p| p.score(now)).unwrap_or(0.0);
                let score_b = patterns.get(*b).map(|p| p.score(now)).unwrap_or(0.0);
                score_a.partial_cmp(&score_b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned();
        
        if let Some(key) = victim {
            data.remove(&key);
            self.metrics.evict();
        }
    }

    async fn remove(&self, key: &K) {
        let mut data = self.data.write().await;
        data.remove(key);
    }
}

// =============================================================================
// Write-Through / Write-Back Cache
// =============================================================================

pub enum WritePolicy {
    WriteThrough,  // Write to cache and backing store immediately
    WriteBack,     // Write to cache, lazy flush to backing store
}

/// Cache with configurable write policy
pub struct WriteCache<K: Hash + Eq + Clone + Send + Sync + 'static, V: Clone + Send + Sync + 'static> {
    cache: RwLock<HashMap<K, CacheEntry<V>>>,
    dirty: RwLock<Vec<K>>,  // Keys that need flushing (write-back)
    policy: WritePolicy,
    flush_threshold: usize,
    metrics: CacheMetrics,
}

impl<K: Hash + Eq + Clone + Send + Sync + 'static, V: Clone + Send + Sync + 'static> WriteCache<K, V> {
    pub fn new(policy: WritePolicy, flush_threshold: usize) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            dirty: RwLock::new(Vec::new()),
            policy,
            flush_threshold,
            metrics: CacheMetrics::default(),
        }
    }

    pub async fn get(&self, key: &K) -> Option<V> {
        let cache = self.cache.read().await;
        if let Some(entry) = cache.get(key) {
            self.metrics.hit();
            Some(entry.value.clone())
        } else {
            self.metrics.miss();
            None
        }
    }

    pub async fn put(&self, key: K, value: V) {
        let entry = CacheEntry::new(value, None, 0);
        
        match self.policy {
            WritePolicy::WriteThrough => {
                // In production: write to backing store here
                let mut cache = self.cache.write().await;
                cache.insert(key, entry);
            }
            WritePolicy::WriteBack => {
                let mut cache = self.cache.write().await;
                cache.insert(key.clone(), entry);
                drop(cache);
                
                let mut dirty = self.dirty.write().await;
                dirty.push(key);
                
                // Flush if threshold reached
                if dirty.len() >= self.flush_threshold {
                    drop(dirty);
                    self.flush().await;
                }
            }
        }
    }

    pub async fn flush(&self) {
        let mut dirty = self.dirty.write().await;
        let cache = self.cache.read().await;
        
        for key in dirty.iter() {
            if let Some(_entry) = cache.get(key) {
                // In production: write entry.value to backing store
            }
        }
        
        dirty.clear();
    }

    pub async fn dirty_count(&self) -> usize {
        self.dirty.read().await.len()
    }
}

// =============================================================================
// Unified Cache System
// =============================================================================

/// Complete caching system with all cache types
pub struct UnifiedCacheSystem {
    pub query_cache: TieredCache<String, Vec<u8>>,
    pub llm_cache: LLMCache,
    pub embedding_cache: EmbeddingCache,
    pub semantic_cache: SemanticCache,
    pub kv_store: KVStore,
    pub adaptive_cache: AdaptiveCache<String, Vec<u8>>,
}

impl UnifiedCacheSystem {
    pub fn new() -> Self {
        Self {
            query_cache: TieredCache::new(100, 1000, 10000),  // L1/L2/L3
            llm_cache: LLMCache::new(1000),
            embedding_cache: EmbeddingCache::new(10000),
            semantic_cache: SemanticCache::new(0.85),  // 85% similarity threshold
            kv_store: KVStore::new(100 * 1024 * 1024),  // 100MB
            adaptive_cache: AdaptiveCache::new(5000),
        }
    }

    pub fn metrics_summary(&self) -> HashMap<String, (u64, u64, f64)> {
        let mut summary = HashMap::new();
        
        let llm = &self.llm_cache.metrics;
        summary.insert("llm".to_string(), (
            llm.hits.load(Ordering::Relaxed),
            llm.misses.load(Ordering::Relaxed),
            llm.hit_rate()
        ));
        
        let emb = &self.embedding_cache.metrics;
        summary.insert("embedding".to_string(), (
            emb.hits.load(Ordering::Relaxed),
            emb.misses.load(Ordering::Relaxed),
            emb.hit_rate()
        ));
        
        summary
    }
}

impl Default for UnifiedCacheSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tiered_cache() {
        let cache: TieredCache<String, String> = TieredCache::new(10, 100, 1000);
        
        cache.put("key1".to_string(), "value1".to_string(), None).await;
        
        // Access multiple times to promote
        for _ in 0..5 {
            let val = cache.get(&"key1".to_string()).await;
            assert_eq!(val, Some("value1".to_string()));
        }
    }

    #[tokio::test]
    async fn test_kv_store() {
        let store = KVStore::new(1024 * 1024);
        
        store.set("name", KVValue::String("MiracleDb".to_string()), None).await;
        store.set("version", KVValue::Integer(1), None).await;
        
        let name = store.get("name").await;
        assert!(matches!(name, Some(KVValue::String(s)) if s == "MiracleDb"));
        
        let new_ver = store.incr("version", 1).await;
        assert_eq!(new_ver, 2);
    }

    #[tokio::test]
    async fn test_llm_cache() {
        let cache = LLMCache::new(100);
        
        cache.put("What is 2+2?", "gpt-4", 0.0, "4".to_string(), 10, 100).await;
        
        let result = cache.get("What is 2+2?", "gpt-4", 0.0).await;
        assert_eq!(result, Some("4".to_string()));
        
        // Different temp = cache miss
        let miss = cache.get("What is 2+2?", "gpt-4", 0.5).await;
        assert_eq!(miss, None);
    }
}
