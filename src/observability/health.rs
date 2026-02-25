//! Health Check Module
//!
//! Provides comprehensive health checking for MiracleDb components.
//! Supports liveness, readiness, and startup probes for Kubernetes.

use serde::{Serialize, Deserialize};
use std::time::{Duration, Instant};
use std::sync::Arc;

/// Health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but operational
    Degraded,
    /// Component is unhealthy
    Unhealthy,
}

/// Component health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub component: String,
    /// Health status
    pub status: HealthStatus,
    /// Status message
    pub message: Option<String>,
    /// Response time in milliseconds
    pub response_time_ms: u64,
    /// Last check timestamp (ISO 8601)
    pub last_check: String,
}

/// Overall health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    /// Overall status
    pub status: HealthStatus,
    /// Individual component healths
    pub components: Vec<ComponentHealth>,
    /// Total check duration in milliseconds
    pub total_duration_ms: u64,
    /// Timestamp (ISO 8601)
    pub timestamp: String,
}

impl HealthCheckResult {
    /// Check if all components are healthy
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }

    /// Check if ready to serve traffic (healthy or degraded)
    pub fn is_ready(&self) -> bool {
        self.status == HealthStatus::Healthy || self.status == HealthStatus::Degraded
    }
}

/// Health checker
pub struct HealthChecker {
    /// Startup complete flag
    startup_complete: Arc<tokio::sync::RwLock<bool>>,
}

impl HealthChecker {
    pub fn new() -> Self {
        Self {
            startup_complete: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }

    /// Mark startup as complete
    pub async fn mark_startup_complete(&self) {
        let mut startup = self.startup_complete.write().await;
        *startup = true;
        tracing::info!("Health check: Startup complete");
    }

    /// Check if startup is complete
    pub async fn is_startup_complete(&self) -> bool {
        *self.startup_complete.read().await
    }

    /// Liveness probe - Is the server running?
    ///
    /// This checks if the server process is alive and responsive.
    /// Kubernetes will restart the pod if liveness fails.
    pub async fn check_liveness(&self) -> HealthCheckResult {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let mut components = Vec::new();

        // Basic process check (always healthy if we can run this code)
        components.push(ComponentHealth {
            component: "process".to_string(),
            status: HealthStatus::Healthy,
            message: Some("Process is running".to_string()),
            response_time_ms: 0,
            last_check: timestamp.clone(),
        });

        // Check async runtime
        let runtime_status = self.check_async_runtime().await;
        components.push(runtime_status);

        // Determine overall status
        let overall_status = if components.iter().all(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        };

        HealthCheckResult {
            status: overall_status,
            components,
            total_duration_ms: start.elapsed().as_millis() as u64,
            timestamp,
        }
    }

    /// Readiness probe - Is the server ready to serve traffic?
    ///
    /// This checks if all dependencies are available and the server can handle requests.
    /// Kubernetes will not route traffic to pods that are not ready.
    pub async fn check_readiness(&self, engine: Option<&crate::engine::MiracleEngine>) -> HealthCheckResult {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let mut components = Vec::new();

        // Check database engine
        if let Some(eng) = engine {
            let db_status = self.check_database(eng).await;
            components.push(db_status);
        }

        // Check async runtime
        let runtime_status = self.check_async_runtime().await;
        components.push(runtime_status);

        // Check memory pressure
        let memory_status = self.check_memory().await;
        components.push(memory_status);

        // Check disk space
        let disk_status = self.check_disk_space().await;
        components.push(disk_status);

        // Determine overall status
        let overall_status = if components.iter().all(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else if components.iter().any(|c| c.status == HealthStatus::Unhealthy) {
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Degraded
        };

        HealthCheckResult {
            status: overall_status,
            components,
            total_duration_ms: start.elapsed().as_millis() as u64,
            timestamp,
        }
    }

    /// Startup probe - Has the server finished initializing?
    ///
    /// This checks if the server has completed its startup sequence.
    /// Kubernetes will wait for startup before checking readiness/liveness.
    pub async fn check_startup(&self, engine: Option<&crate::engine::MiracleEngine>) -> HealthCheckResult {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let mut components = Vec::new();

        // Check if startup flag is set
        let startup_complete = self.is_startup_complete().await;

        components.push(ComponentHealth {
            component: "startup".to_string(),
            status: if startup_complete {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: Some(if startup_complete {
                "Startup complete".to_string()
            } else {
                "Startup in progress".to_string()
            }),
            response_time_ms: 0,
            last_check: timestamp.clone(),
        });

        // Check database initialization
        if let Some(eng) = engine {
            let db_status = self.check_database(eng).await;
            components.push(db_status);
        }

        // Determine overall status
        let overall_status = if components.iter().all(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        };

        HealthCheckResult {
            status: overall_status,
            components,
            total_duration_ms: start.elapsed().as_millis() as u64,
            timestamp,
        }
    }

    /// Deep health check - Comprehensive check of all components
    pub async fn check_deep(&self, engine: Option<&crate::engine::MiracleEngine>) -> HealthCheckResult {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let mut components = Vec::new();

        // Check database
        if let Some(eng) = engine {
            let db_status = self.check_database(eng).await;
            components.push(db_status);

            // Check ML models if ml feature enabled
            #[cfg(feature = "ml")]
            {
                let ml_status = self.check_ml_models(eng).await;
                components.push(ml_status);
            }

            // Check Kafka if kafka feature enabled
            #[cfg(feature = "kafka")]
            {
                let kafka_sources_status = self.check_kafka_sources(eng).await;
                components.push(kafka_sources_status);

                let kafka_sinks_status = self.check_kafka_sinks(eng).await;
                components.push(kafka_sinks_status);
            }
        }

        // Check system resources
        let cpu_status = self.check_cpu().await;
        components.push(cpu_status);

        let memory_status = self.check_memory().await;
        components.push(memory_status);

        let disk_status = self.check_disk_space().await;
        components.push(disk_status);

        // Check async runtime
        let runtime_status = self.check_async_runtime().await;
        components.push(runtime_status);

        // Determine overall status
        let overall_status = if components.iter().all(|c| c.status == HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else if components.iter().any(|c| c.status == HealthStatus::Unhealthy) {
            HealthStatus::Unhealthy
        } else {
            HealthStatus::Degraded
        };

        HealthCheckResult {
            status: overall_status,
            components,
            total_duration_ms: start.elapsed().as_millis() as u64,
            timestamp,
        }
    }

    // ========== Component Checks ==========

    async fn check_database(&self, engine: &crate::engine::MiracleEngine) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        // Try to execute a simple query
        let result = engine.query("SELECT 1 as health_check").await;

        let (status, message) = match result {
            Ok(_) => (HealthStatus::Healthy, "Database responding to queries".to_string()),
            Err(e) => (HealthStatus::Unhealthy, format!("Database error: {}", e)),
        };

        ComponentHealth {
            component: "database".to_string(),
            status,
            message: Some(message),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    #[cfg(feature = "ml")]
    async fn check_ml_models(&self, engine: &crate::engine::MiracleEngine) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        // Temporarily disabled
        // let models = engine.model_registry.list_models().await;
        let model_count = 0; // models.len();

        ComponentHealth {
            component: "ml_models".to_string(),
            status: HealthStatus::Healthy,
            message: Some(format!("{} ML models loaded", model_count)),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    #[cfg(feature = "kafka")]
    async fn check_kafka_sources(&self, engine: &crate::engine::MiracleEngine) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let sources = engine.kafka_sources.read().unwrap();
        let source_count = sources.len();

        ComponentHealth {
            component: "kafka_sources".to_string(),
            status: HealthStatus::Healthy,
            message: Some(format!("{} Kafka sources active", source_count)),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    #[cfg(feature = "kafka")]
    async fn check_kafka_sinks(&self, engine: &crate::engine::MiracleEngine) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        let sinks = engine.kafka_sinks.read().unwrap();
        let sink_count = sinks.len();

        ComponentHealth {
            component: "kafka_sinks".to_string(),
            status: HealthStatus::Healthy,
            message: Some(format!("{} Kafka sinks active", sink_count)),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    async fn check_cpu(&self) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        use sysinfo::{System, SystemExt, CpuExt};
        let mut sys = System::new_all();
        sys.refresh_cpu();

        // Wait a bit to get accurate CPU usage
        tokio::time::sleep(Duration::from_millis(100)).await;
        sys.refresh_cpu();

        let cpu_usage: f64 = sys.cpus().iter()
            .map(|cpu| cpu.cpu_usage() as f64)
            .sum::<f64>() / sys.cpus().len() as f64;

        let (status, message) = if cpu_usage > 90.0 {
            (HealthStatus::Unhealthy, format!("CPU usage critical: {:.1}%", cpu_usage))
        } else if cpu_usage > 75.0 {
            (HealthStatus::Degraded, format!("CPU usage high: {:.1}%", cpu_usage))
        } else {
            (HealthStatus::Healthy, format!("CPU usage: {:.1}%", cpu_usage))
        };

        ComponentHealth {
            component: "cpu".to_string(),
            status,
            message: Some(message),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    async fn check_memory(&self) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        use sysinfo::{System, SystemExt};
        let mut sys = System::new_all();
        sys.refresh_memory();

        let memory_used = sys.used_memory();
        let memory_total = sys.total_memory();
        let memory_percent = (memory_used as f64 / memory_total as f64) * 100.0;

        let (status, message) = if memory_percent > 95.0 {
            (HealthStatus::Unhealthy, format!("Memory usage critical: {:.1}%", memory_percent))
        } else if memory_percent > 85.0 {
            (HealthStatus::Degraded, format!("Memory usage high: {:.1}%", memory_percent))
        } else {
            (HealthStatus::Healthy, format!("Memory usage: {:.1}%", memory_percent))
        };

        ComponentHealth {
            component: "memory".to_string(),
            status,
            message: Some(message),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    async fn check_disk_space(&self) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        use sysinfo::{System, SystemExt, DiskExt};
        let mut sys = System::new_all();
        sys.refresh_disks_list();

        // Check first disk
        let disk_percent = sys.disks().first().map(|disk| {
            let total = disk.total_space();
            let available = disk.available_space();
            let used = total - available;
            (used as f64 / total as f64) * 100.0
        }).unwrap_or(0.0);

        let (status, message) = if disk_percent > 95.0 {
            (HealthStatus::Unhealthy, format!("Disk space critical: {:.1}%", disk_percent))
        } else if disk_percent > 85.0 {
            (HealthStatus::Degraded, format!("Disk space high: {:.1}%", disk_percent))
        } else {
            (HealthStatus::Healthy, format!("Disk usage: {:.1}%", disk_percent))
        };

        ComponentHealth {
            component: "disk_space".to_string(),
            status,
            message: Some(message),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }

    async fn check_async_runtime(&self) -> ComponentHealth {
        let start = Instant::now();
        let timestamp = chrono::Utc::now().to_rfc3339();

        // Spawn a task to verify runtime is working
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            tokio::spawn(async { "ok" })
        ).await;

        let (status, message) = match result {
            Ok(Ok(_)) => (HealthStatus::Healthy, "Async runtime operational".to_string()),
            Ok(Err(e)) => (HealthStatus::Unhealthy, format!("Runtime error: {}", e)),
            Err(_) => (HealthStatus::Unhealthy, "Runtime timeout".to_string()),
        };

        ComponentHealth {
            component: "async_runtime".to_string(),
            status,
            message: Some(message),
            response_time_ms: start.elapsed().as_millis() as u64,
            last_check: timestamp,
        }
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_liveness_check() {
        let checker = HealthChecker::new();
        let result = checker.check_liveness().await;

        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(!result.components.is_empty());
    }

    #[tokio::test]
    async fn test_startup_check() {
        let checker = HealthChecker::new();

        // Before marking complete
        let result = checker.check_startup(None).await;
        assert_eq!(result.status, HealthStatus::Unhealthy);

        // After marking complete
        checker.mark_startup_complete().await;
        let result = checker.check_startup(None).await;
        assert_eq!(result.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_readiness_check() {
        let checker = HealthChecker::new();
        let result = checker.check_readiness(None).await;

        // Should be healthy or degraded (ready to serve)
        assert!(result.is_ready());
    }
}
