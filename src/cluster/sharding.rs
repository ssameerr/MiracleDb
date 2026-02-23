//! Consistent hash ring sharding and query routing

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};

/// Consistent hash ring for shard assignment
pub struct ConsistentHashRing {
    num_shards: usize,
}

impl ConsistentHashRing {
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards: num_shards.max(1) }
    }

    pub fn get_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }
}

/// A routed query plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutedQuery {
    pub sql: String,
    pub target_shards: Vec<usize>,
    pub is_broadcast: bool,
}

/// Routes queries to appropriate shards
pub struct QueryRouter {
    ring: ConsistentHashRing,
    num_shards: usize,
}

impl QueryRouter {
    pub fn new(num_shards: usize) -> Self {
        Self {
            ring: ConsistentHashRing::new(num_shards),
            num_shards,
        }
    }

    pub fn route_query(&self, sql: &str) -> Result<RoutedQuery, String> {
        let shard_key = Self::extract_shard_key(sql);

        if let Some(key) = shard_key {
            let shard = self.ring.get_shard(&key);
            Ok(RoutedQuery {
                sql: sql.to_string(),
                target_shards: vec![shard],
                is_broadcast: false,
            })
        } else {
            // No shard key — broadcast to all shards (aggregates, full scans)
            Ok(RoutedQuery {
                sql: sql.to_string(),
                target_shards: (0..self.num_shards).collect(),
                is_broadcast: true,
            })
        }
    }

    /// Extract shard key value from WHERE id = 'value' or WHERE id = value
    fn extract_shard_key(sql: &str) -> Option<String> {
        let lower = sql.to_lowercase();
        let id_pos = lower.find(" id = ")?;
        let after = sql[id_pos + 6..].trim_start();
        if after.starts_with('\'') {
            let end = after[1..].find('\'')?;
            Some(after[1..=end].to_string())
        } else {
            let end = after.find(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
                .unwrap_or(after.len());
            if end > 0 { Some(after[..end].to_string()) } else { None }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_key_for_string() {
        let ring = ConsistentHashRing::new(3);
        let shard = ring.get_shard("user_123");
        assert!(shard < 3, "shard index should be < num_shards");
    }

    #[test]
    fn test_same_key_same_shard() {
        let ring = ConsistentHashRing::new(4);
        let s1 = ring.get_shard("stable_key");
        let s2 = ring.get_shard("stable_key");
        assert_eq!(s1, s2, "same key must map to same shard");
    }

    #[test]
    fn test_different_keys_distributed() {
        let ring = ConsistentHashRing::new(4);
        let shards: std::collections::HashSet<usize> = (0..100)
            .map(|i| ring.get_shard(&format!("key_{}", i)))
            .collect();
        assert!(shards.len() > 1, "keys should distribute across multiple shards");
    }

    #[test]
    fn test_query_router_select() {
        let router = QueryRouter::new(2);
        let plan = router.route_query("SELECT * FROM users WHERE id = '42'");
        assert!(plan.is_ok());
        let plan = plan.unwrap();
        assert!(!plan.target_shards.is_empty());
    }

    #[test]
    fn test_query_router_broadcast() {
        let router = QueryRouter::new(3);
        let plan = router.route_query("SELECT COUNT(*) FROM users").unwrap();
        assert_eq!(plan.target_shards.len(), 3, "aggregate without shard key should broadcast");
    }
}
