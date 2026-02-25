//! Connection Pool - Database connection pooling and management

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, RwLock, OwnedSemaphorePermit};
use serde::{Serialize, Deserialize};

/// Pool configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub connection_timeout_ms: u64,
    pub idle_timeout_seconds: u64,
    pub max_lifetime_seconds: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 5,
            max_connections: 100,
            connection_timeout_ms: 5000,
            idle_timeout_seconds: 600,
            max_lifetime_seconds: 3600,
        }
    }
}

/// Pool statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PoolStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub idle_connections: usize,
    pub pending_requests: usize,
    pub total_checkouts: u64,
    pub total_timeouts: u64,
}

/// Pooled connection wrapper
pub struct PooledConnection<C: Send + Sync + 'static> {
    conn: Option<C>,
    created_at: Instant,
    last_used: Instant,
    _permit: OwnedSemaphorePermit,
    pool: Arc<ConnectionPoolInner<C>>,
}

impl<C: Send + Sync + 'static> Drop for PooledConnection<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = Arc::clone(&self.pool);
            tokio::spawn(async move {
                pool.return_connection(conn).await;
            });
        }
    }
}

impl<C: Send + Sync + 'static> std::ops::Deref for PooledConnection<C> {
    type Target = C;
    
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl<C: Send + Sync + 'static> std::ops::DerefMut for PooledConnection<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

/// Internal pool state
struct ConnectionPoolInner<C> {
    idle: RwLock<VecDeque<(C, Instant)>>,
    semaphore: Arc<Semaphore>,
    config: PoolConfig,
    stats: RwLock<PoolStats>,
    factory: Box<dyn Fn() -> C + Send + Sync>,
}

impl<C: Send + 'static> ConnectionPoolInner<C> {
    async fn return_connection(&self, conn: C) {
        let now = Instant::now();
        let mut idle = self.idle.write().await;
        idle.push_back((conn, now));

        let mut stats = self.stats.write().await;
        stats.active_connections -= 1;
        stats.idle_connections += 1;
    }
}

/// Connection pool
pub struct ConnectionPool<C> {
    inner: Arc<ConnectionPoolInner<C>>,
}

impl<C: Send + Sync + 'static> ConnectionPool<C> {
    pub fn new<F>(config: PoolConfig, factory: F) -> Self
    where
        F: Fn() -> C + Send + Sync + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        
        let inner = Arc::new(ConnectionPoolInner {
            idle: RwLock::new(VecDeque::new()),
            semaphore,
            config: config.clone(),
            stats: RwLock::new(PoolStats::default()),
            factory: Box::new(factory),
        });

        // Pre-create minimum connections
        let pool = Self { inner };
        
        let inner_clone = Arc::clone(&pool.inner);
        tokio::spawn(async move {
            for _ in 0..config.min_connections {
                let conn = (inner_clone.factory)();
                let mut idle = inner_clone.idle.write().await;
                idle.push_back((conn, Instant::now()));
                
                let mut stats = inner_clone.stats.write().await;
                stats.total_connections += 1;
                stats.idle_connections += 1;
            }
        });

        pool
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> Result<PooledConnection<C>, String> {
        let timeout = Duration::from_millis(self.inner.config.connection_timeout_ms);
        
        // Acquire permit with timeout
        let permit = tokio::time::timeout(timeout, self.inner.semaphore.clone().acquire_owned())
            .await
            .map_err(|_| {
                // Increment timeout count
                let mut stats = self.inner.stats.blocking_write();
                stats.total_timeouts += 1;
                "Connection timeout".to_string()
            })?
            .map_err(|_| "Semaphore closed".to_string())?;

        // Try to get idle connection
        let mut idle = self.inner.idle.write().await;
        let now = Instant::now();
        
        // Remove expired connections
        while let Some((_, created)) = idle.front() {
            if now.duration_since(*created).as_secs() > self.inner.config.idle_timeout_seconds {
                idle.pop_front();
                let mut stats = self.inner.stats.write().await;
                stats.total_connections -= 1;
                stats.idle_connections -= 1;
            } else {
                break;
            }
        }

        let (conn, created_at) = if let Some((conn, created)) = idle.pop_front() {
            let mut stats = self.inner.stats.write().await;
            stats.idle_connections -= 1;
            (conn, created)
        } else {
            let conn = (self.inner.factory)();
            let mut stats = self.inner.stats.write().await;
            stats.total_connections += 1;
            (conn, now)
        };

        let mut stats = self.inner.stats.write().await;
        stats.active_connections += 1;
        stats.total_checkouts += 1;
        drop(stats);

        Ok(PooledConnection {
            conn: Some(conn),
            created_at,
            last_used: now,
            _permit: permit,
            pool: Arc::clone(&self.inner),
        })
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let stats = self.inner.stats.read().await;
        stats.clone()
    }

    /// Clear all idle connections
    pub async fn clear(&self) {
        let mut idle = self.inner.idle.write().await;
        let count = idle.len();
        idle.clear();

        let mut stats = self.inner.stats.write().await;
        stats.idle_connections = 0;
        stats.total_connections -= count;
    }

    /// Get pool size
    pub async fn size(&self) -> (usize, usize) {
        let stats = self.inner.stats.read().await;
        (stats.idle_connections, stats.active_connections)
    }
}

/// Simple mock connection for testing
#[derive(Clone)]
pub struct MockConnection {
    pub id: u64,
}

impl MockConnection {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self {
            id: COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl Default for MockConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_pool() {
        let pool = ConnectionPool::new(
            PoolConfig {
                min_connections: 2,
                max_connections: 5,
                ..Default::default()
            },
            MockConnection::new,
        );

        // Wait for min connections to be created
        tokio::time::sleep(Duration::from_millis(100)).await;

        let conn1 = pool.get().await.unwrap();
        let conn2 = pool.get().await.unwrap();

        assert_ne!(conn1.id, conn2.id);

        let stats = pool.stats().await;
        assert_eq!(stats.active_connections, 2);
    }
}
