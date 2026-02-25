//! Health Module - System health monitoring and diagnostics

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Overall system health status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: HealthStatus,
    pub uptime_seconds: u64,
    pub version: String,
    pub components: HashMap<String, ComponentHealth>,
    pub timestamp: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Component health
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
    pub last_check: i64,
}

/// Resource metrics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceMetrics {
    pub cpu_usage_percent: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub open_connections: u64,
    pub active_queries: u64,
}

/// Health check function type
pub type HealthCheckFn = Box<dyn Fn() -> ComponentHealth + Send + Sync>;

/// Health monitor
pub struct HealthMonitor {
    start_time: Instant,
    checks: RwLock<HashMap<String, Arc<dyn Fn() -> ComponentHealth + Send + Sync>>>,
    last_results: RwLock<HashMap<String, ComponentHealth>>,
    metrics: RwLock<ResourceMetrics>,
}

impl HealthMonitor {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            checks: RwLock::new(HashMap::new()),
            last_results: RwLock::new(HashMap::new()),
            metrics: RwLock::new(ResourceMetrics::default()),
        }
    }

    /// Register a health check
    pub async fn register_check<F>(&self, name: &str, check: F)
    where
        F: Fn() -> ComponentHealth + Send + Sync + 'static,
    {
        let mut checks = self.checks.write().await;
        checks.insert(name.to_string(), Arc::new(check));
    }

    /// Run all health checks
    pub async fn check_all(&self) -> SystemHealth {
        let checks = self.checks.read().await;
        let mut components = HashMap::new();
        let mut overall_status = HealthStatus::Healthy;

        for (name, check_fn) in checks.iter() {
            let result = check_fn();
            
            if result.status == HealthStatus::Unhealthy {
                overall_status = HealthStatus::Unhealthy;
            } else if result.status == HealthStatus::Degraded && overall_status != HealthStatus::Unhealthy {
                overall_status = HealthStatus::Degraded;
            }

            components.insert(name.clone(), result.clone());
            
            let mut last_results = self.last_results.write().await;
            last_results.insert(name.clone(), result);
        }

        SystemHealth {
            status: overall_status,
            uptime_seconds: self.start_time.elapsed().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            components,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    /// Get specific component health
    pub async fn get_component(&self, name: &str) -> Option<ComponentHealth> {
        let results = self.last_results.read().await;
        results.get(name).cloned()
    }

    /// Update resource metrics
    pub async fn update_metrics(&self, metrics: ResourceMetrics) {
        let mut m = self.metrics.write().await;
        *m = metrics;
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> ResourceMetrics {
        let m = self.metrics.read().await;
        m.clone()
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Quick liveness check
    pub fn is_alive(&self) -> bool {
        true // Always alive if we can respond
    }

    /// Readiness check (can handle traffic)
    pub async fn is_ready(&self) -> bool {
        let health = self.check_all().await;
        health.status != HealthStatus::Unhealthy
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Create standard health checks
pub fn create_standard_checks() -> Vec<(&'static str, Box<dyn Fn() -> ComponentHealth + Send + Sync>)> {
    vec![
        ("storage", Box::new(|| ComponentHealth {
            name: "storage".to_string(),
            status: HealthStatus::Healthy,
            latency_ms: Some(1),
            message: None,
            last_check: chrono::Utc::now().timestamp(),
        })),
        ("query_engine", Box::new(|| ComponentHealth {
            name: "query_engine".to_string(),
            status: HealthStatus::Healthy,
            latency_ms: Some(1),
            message: None,
            last_check: chrono::Utc::now().timestamp(),
        })),
        ("cache", Box::new(|| ComponentHealth {
            name: "cache".to_string(),
            status: HealthStatus::Healthy,
            latency_ms: Some(0),
            message: None,
            last_check: chrono::Utc::now().timestamp(),
        })),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_monitor() {
        let monitor = HealthMonitor::new();
        
        monitor.register_check("test", || ComponentHealth {
            name: "test".to_string(),
            status: HealthStatus::Healthy,
            latency_ms: Some(5),
            message: None,
            last_check: chrono::Utc::now().timestamp(),
        }).await;

        let health = monitor.check_all().await;
        assert_eq!(health.status, HealthStatus::Healthy);
        assert!(health.components.contains_key("test"));
    }
}
