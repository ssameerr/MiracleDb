//! Rate Limiter - Token bucket and sliding window rate limiting

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Rate limit configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub burst_size: u32,
    pub window_seconds: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 100,
            burst_size: 200,
            window_seconds: 1,
        }
    }
}

/// Token bucket for rate limiting
struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u32, refill_rate: u32) -> Self {
        Self {
            tokens: capacity as f64,
            capacity: capacity as f64,
            refill_rate: refill_rate as f64,
            last_refill: Instant::now(),
        }
    }

    fn try_acquire(&mut self, tokens: f64) -> bool {
        self.refill();
        
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }
}

/// Sliding window entry
struct WindowEntry {
    count: u32,
    window_start: Instant,
}

/// Rate limiter
pub struct RateLimiter {
    buckets: Arc<RwLock<HashMap<String, TokenBucket>>>,
    windows: Arc<RwLock<HashMap<String, WindowEntry>>>,
    global_config: RateLimitConfig,
    user_configs: Arc<RwLock<HashMap<String, RateLimitConfig>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
            windows: Arc::new(RwLock::new(HashMap::new())),
            global_config: config,
            user_configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if request is allowed (token bucket)
    pub async fn check_token_bucket(&self, key: &str) -> bool {
        let config = self.get_config(key).await;
        
        let mut buckets = self.buckets.write().await;
        let bucket = buckets.entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(config.burst_size, config.requests_per_second));
        
        bucket.try_acquire(1.0)
    }

    /// Check if request is allowed (sliding window)
    pub async fn check_sliding_window(&self, key: &str) -> bool {
        let config = self.get_config(key).await;
        let window_duration = Duration::from_secs(config.window_seconds);
        let max_requests = config.requests_per_second * config.window_seconds as u32;

        let mut windows = self.windows.write().await;
        let now = Instant::now();

        let entry = windows.entry(key.to_string())
            .or_insert_with(|| WindowEntry {
                count: 0,
                window_start: now,
            });

        // Reset window if expired
        if now.duration_since(entry.window_start) > window_duration {
            entry.count = 0;
            entry.window_start = now;
        }

        if entry.count < max_requests {
            entry.count += 1;
            true
        } else {
            false
        }
    }

    /// Combined check using both methods
    pub async fn check(&self, key: &str) -> RateLimitResult {
        let bucket_allowed = self.check_token_bucket(key).await;
        let window_allowed = self.check_sliding_window(key).await;

        if bucket_allowed && window_allowed {
            RateLimitResult::Allowed
        } else {
            let config = self.get_config(key).await;
            RateLimitResult::Limited {
                retry_after_ms: (1000 / config.requests_per_second) as u64,
            }
        }
    }

    /// Set custom config for a user/key
    pub async fn set_config(&self, key: &str, config: RateLimitConfig) {
        let mut configs = self.user_configs.write().await;
        configs.insert(key.to_string(), config);
    }

    /// Get config for a key
    async fn get_config(&self, key: &str) -> RateLimitConfig {
        let configs = self.user_configs.read().await;
        configs.get(key).cloned().unwrap_or_else(|| self.global_config.clone())
    }

    /// Get current usage for a key
    pub async fn get_usage(&self, key: &str) -> RateLimitUsage {
        let buckets = self.buckets.read().await;
        let windows = self.windows.read().await;
        let config = self.get_config(key).await;

        let tokens_remaining = buckets.get(key)
            .map(|b| b.tokens as u32)
            .unwrap_or(config.burst_size);

        let requests_in_window = windows.get(key)
            .map(|w| w.count)
            .unwrap_or(0);

        RateLimitUsage {
            tokens_remaining,
            requests_in_window,
            max_requests: config.requests_per_second * config.window_seconds as u32,
        }
    }

    /// Reset limits for a key
    pub async fn reset(&self, key: &str) {
        let mut buckets = self.buckets.write().await;
        let mut windows = self.windows.write().await;
        buckets.remove(key);
        windows.remove(key);
    }

    /// Cleanup expired entries
    pub async fn cleanup(&self) {
        let now = Instant::now();
        let max_age = Duration::from_secs(3600); // 1 hour

        let mut windows = self.windows.write().await;
        windows.retain(|_, entry| now.duration_since(entry.window_start) < max_age);
    }
}

#[derive(Debug, Clone)]
pub enum RateLimitResult {
    Allowed,
    Limited { retry_after_ms: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitUsage {
    pub tokens_remaining: u32,
    pub requests_in_window: u32,
    pub max_requests: u32,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(RateLimitConfig {
            requests_per_second: 10,
            burst_size: 5,
            window_seconds: 1,
        });

        // Should allow burst
        for _ in 0..5 {
            assert!(matches!(limiter.check("user1").await, RateLimitResult::Allowed));
        }

        // Should be limited after burst
        assert!(matches!(limiter.check("user1").await, RateLimitResult::Limited { .. }));
    }

    #[tokio::test]
    async fn test_token_bucket_allows_up_to_burst() {
        let limiter = RateLimiter::new(RateLimitConfig {
            requests_per_second: 100,
            burst_size: 3,
            window_seconds: 1,
        });
        assert!(limiter.check_token_bucket("key").await);
        assert!(limiter.check_token_bucket("key").await);
        assert!(limiter.check_token_bucket("key").await);
        // 4th request exceeds burst
        assert!(!limiter.check_token_bucket("key").await);
    }

    #[tokio::test]
    async fn test_sliding_window_allows_within_limit() {
        let limiter = RateLimiter::new(RateLimitConfig {
            requests_per_second: 5,
            burst_size: 10,
            window_seconds: 1,
        });
        for _ in 0..5 {
            assert!(limiter.check_sliding_window("user").await);
        }
        assert!(!limiter.check_sliding_window("user").await);
    }

    #[tokio::test]
    async fn test_custom_config_per_user() {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        limiter.set_config("vip_user", RateLimitConfig {
            requests_per_second: 1000,
            burst_size: 1000,
            window_seconds: 1,
        }).await;
        // VIP user gets 1000 burst — first request always allowed
        assert!(limiter.check_token_bucket("vip_user").await);
    }

    #[tokio::test]
    async fn test_reset_clears_limits() {
        let limiter = RateLimiter::new(RateLimitConfig {
            requests_per_second: 1,
            burst_size: 1,
            window_seconds: 1,
        });
        // Exhaust burst
        let _ = limiter.check("user2").await;
        assert!(matches!(limiter.check("user2").await, RateLimitResult::Limited { .. }));
        // Reset and try again
        limiter.reset("user2").await;
        assert!(matches!(limiter.check("user2").await, RateLimitResult::Allowed));
    }

    #[tokio::test]
    async fn test_get_usage_returns_stats() {
        let limiter = RateLimiter::new(RateLimitConfig {
            requests_per_second: 10,
            burst_size: 10,
            window_seconds: 1,
        });
        limiter.check("usage_user").await;
        limiter.check("usage_user").await;
        let usage = limiter.get_usage("usage_user").await;
        assert!(usage.tokens_remaining <= 10);
        assert!(usage.requests_in_window <= 2);
    }
}
