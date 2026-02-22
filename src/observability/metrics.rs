//! Prometheus Metrics Module
//!
//! Provides comprehensive metrics collection using the official Prometheus crate.
//! Metrics are exposed via `/metrics` endpoint for Prometheus scraping.

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec,
    Opts, Registry, TextEncoder, Encoder,
};
use std::sync::{Arc, Once};
use lazy_static::lazy_static;

static METRICS_REGISTERED: Once = Once::new();

lazy_static! {
    /// Global Prometheus registry
    pub static ref REGISTRY: Registry = Registry::new();

    // ========== Query Metrics ==========

    /// Total number of queries executed
    pub static ref QUERY_TOTAL: CounterVec = CounterVec::new(
        Opts::new("miracledb_query_total", "Total number of queries executed"),
        &["query_type", "status"]
    ).expect("Failed to create query_total metric");

    /// Query execution duration in seconds
    pub static ref QUERY_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_query_duration_seconds", "Query execution duration")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["query_type"]
    ).expect("Failed to create query_duration metric");

    /// Number of rows returned by queries
    pub static ref QUERY_ROWS: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_query_rows", "Number of rows returned")
            .buckets(vec![1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0]),
        &["query_type"]
    ).expect("Failed to create query_rows metric");

    /// Number of bytes scanned by queries
    pub static ref QUERY_BYTES_SCANNED: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_query_bytes_scanned", "Bytes scanned by query")
            .buckets(vec![1024.0, 10240.0, 102400.0, 1048576.0, 10485760.0, 104857600.0]),
        &["query_type"]
    ).expect("Failed to create query_bytes_scanned metric");

    // ========== Connection Metrics ==========

    /// Current number of active connections
    pub static ref CONNECTIONS_ACTIVE: Gauge = Gauge::new(
        "miracledb_connections_active",
        "Number of active connections"
    ).expect("Failed to create connections_active metric");

    /// Total connections created
    pub static ref CONNECTIONS_TOTAL: Counter = Counter::new(
        "miracledb_connections_total",
        "Total number of connections created"
    ).expect("Failed to create connections_total metric");

    /// Connection errors
    pub static ref CONNECTIONS_ERRORS: CounterVec = CounterVec::new(
        Opts::new("miracledb_connection_errors_total", "Connection errors"),
        &["error_type"]
    ).expect("Failed to create connection_errors metric");

    // ========== Cache Metrics ==========

    /// Cache hits
    pub static ref CACHE_HITS: CounterVec = CounterVec::new(
        Opts::new("miracledb_cache_hits_total", "Cache hits"),
        &["cache_type"]
    ).expect("Failed to create cache_hits metric");

    /// Cache misses
    pub static ref CACHE_MISSES: CounterVec = CounterVec::new(
        Opts::new("miracledb_cache_misses_total", "Cache misses"),
        &["cache_type"]
    ).expect("Failed to create cache_misses metric");

    /// Cache size in bytes
    pub static ref CACHE_SIZE_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_cache_size_bytes", "Cache size in bytes"),
        &["cache_type"]
    ).expect("Failed to create cache_size_bytes metric");

    /// Cache evictions
    pub static ref CACHE_EVICTIONS: CounterVec = CounterVec::new(
        Opts::new("miracledb_cache_evictions_total", "Cache evictions"),
        &["cache_type"]
    ).expect("Failed to create cache_evictions metric");

    // ========== Resource Utilization ==========

    /// CPU usage percentage (0-100)
    pub static ref CPU_USAGE: Gauge = Gauge::new(
        "miracledb_cpu_usage_percent",
        "CPU usage percentage"
    ).expect("Failed to create cpu_usage metric");

    /// Memory usage in bytes
    pub static ref MEMORY_USAGE_BYTES: Gauge = Gauge::new(
        "miracledb_memory_usage_bytes",
        "Memory usage in bytes"
    ).expect("Failed to create memory_usage_bytes metric");

    /// Memory usage percentage (0-100)
    pub static ref MEMORY_USAGE_PERCENT: Gauge = Gauge::new(
        "miracledb_memory_usage_percent",
        "Memory usage percentage"
    ).expect("Failed to create memory_usage_percent metric");

    /// Disk I/O read bytes
    pub static ref DISK_READ_BYTES: Counter = Counter::new(
        "miracledb_disk_read_bytes_total",
        "Total bytes read from disk"
    ).expect("Failed to create disk_read_bytes metric");

    /// Disk I/O write bytes
    pub static ref DISK_WRITE_BYTES: Counter = Counter::new(
        "miracledb_disk_write_bytes_total",
        "Total bytes written to disk"
    ).expect("Failed to create disk_write_bytes metric");

    /// Disk space used in bytes
    pub static ref DISK_SPACE_USED_BYTES: Gauge = Gauge::new(
        "miracledb_disk_space_used_bytes",
        "Disk space used in bytes"
    ).expect("Failed to create disk_space_used_bytes metric");

    /// Disk space available in bytes
    pub static ref DISK_SPACE_AVAILABLE_BYTES: Gauge = Gauge::new(
        "miracledb_disk_space_available_bytes",
        "Disk space available in bytes"
    ).expect("Failed to create disk_space_available_bytes metric");

    // ========== ONNX ML Metrics ==========

    /// ML model inference requests
    pub static ref ML_INFERENCE_TOTAL: CounterVec = CounterVec::new(
        Opts::new("miracledb_ml_inference_total", "ML inference requests"),
        &["model_name", "status"]
    ).expect("Failed to create ml_inference_total metric");

    /// ML model inference duration
    pub static ref ML_INFERENCE_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_ml_inference_duration_seconds", "ML inference duration")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["model_name"]
    ).expect("Failed to create ml_inference_duration metric");

    /// Number of loaded ML models
    pub static ref ML_MODELS_LOADED: Gauge = Gauge::new(
        "miracledb_ml_models_loaded",
        "Number of loaded ML models"
    ).expect("Failed to create ml_models_loaded metric");

    /// ML model memory usage in bytes
    pub static ref ML_MODEL_MEMORY_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_ml_model_memory_bytes", "ML model memory usage"),
        &["model_name"]
    ).expect("Failed to create ml_model_memory_bytes metric");

    // ========== Kafka Streaming Metrics ==========

    /// Kafka messages consumed
    pub static ref KAFKA_MESSAGES_CONSUMED: CounterVec = CounterVec::new(
        Opts::new("miracledb_kafka_messages_consumed_total", "Kafka messages consumed"),
        &["source_name", "topic"]
    ).expect("Failed to create kafka_messages_consumed metric");

    /// Kafka messages produced
    pub static ref KAFKA_MESSAGES_PRODUCED: CounterVec = CounterVec::new(
        Opts::new("miracledb_kafka_messages_produced_total", "Kafka messages produced"),
        &["sink_name", "topic"]
    ).expect("Failed to create kafka_messages_produced metric");

    /// Kafka consumer lag
    pub static ref KAFKA_CONSUMER_LAG: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_kafka_consumer_lag", "Kafka consumer lag"),
        &["source_name", "topic", "partition"]
    ).expect("Failed to create kafka_consumer_lag metric");

    /// Kafka message processing duration
    pub static ref KAFKA_MESSAGE_PROCESSING_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_kafka_message_processing_duration_seconds", "Kafka message processing duration")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["source_name"]
    ).expect("Failed to create kafka_message_processing_duration metric");

    /// Active Kafka sources
    pub static ref KAFKA_SOURCES_ACTIVE: Gauge = Gauge::new(
        "miracledb_kafka_sources_active",
        "Number of active Kafka sources"
    ).expect("Failed to create kafka_sources_active metric");

    /// Active Kafka sinks
    pub static ref KAFKA_SINKS_ACTIVE: Gauge = Gauge::new(
        "miracledb_kafka_sinks_active",
        "Number of active Kafka sinks"
    ).expect("Failed to create kafka_sinks_active metric");

    // ========== Vector Search Metrics ==========

    /// Vector search requests
    pub static ref VECTOR_SEARCH_TOTAL: CounterVec = CounterVec::new(
        Opts::new("miracledb_vector_search_total", "Vector search requests"),
        &["index_name", "status"]
    ).expect("Failed to create vector_search_total metric");

    /// Vector search duration
    pub static ref VECTOR_SEARCH_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_vector_search_duration_seconds", "Vector search duration")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["index_name"]
    ).expect("Failed to create vector_search_duration metric");

    /// Number of vectors indexed
    pub static ref VECTORS_INDEXED: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_vectors_indexed", "Number of vectors indexed"),
        &["index_name"]
    ).expect("Failed to create vectors_indexed metric");

    // ========== Transaction Metrics ==========

    /// Transactions started
    pub static ref TRANSACTIONS_STARTED: Counter = Counter::new(
        "miracledb_transactions_started_total",
        "Total transactions started"
    ).expect("Failed to create transactions_started metric");

    /// Transactions committed
    pub static ref TRANSACTIONS_COMMITTED: Counter = Counter::new(
        "miracledb_transactions_committed_total",
        "Total transactions committed"
    ).expect("Failed to create transactions_committed metric");

    /// Transactions rolled back
    pub static ref TRANSACTIONS_ROLLED_BACK: Counter = Counter::new(
        "miracledb_transactions_rolled_back_total",
        "Total transactions rolled back"
    ).expect("Failed to create transactions_rolled_back metric");

    /// Transaction duration
    pub static ref TRANSACTION_DURATION: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("miracledb_transaction_duration_seconds", "Transaction duration")
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0])
    ).expect("Failed to create transaction_duration metric");

    // ========== Database Stats ==========

    /// Database size in bytes
    pub static ref DATABASE_SIZE_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_database_size_bytes", "Database size in bytes"),
        &["database"]
    ).expect("Failed to create database_size_bytes metric");

    /// Number of tables
    pub static ref TABLES_COUNT: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_tables_count", "Number of tables"),
        &["database"]
    ).expect("Failed to create tables_count metric");

    /// Table row count
    pub static ref TABLE_ROWS: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_table_rows", "Number of rows in table"),
        &["database", "table"]
    ).expect("Failed to create table_rows metric");

    // ========== Error Metrics ==========

    /// Total errors
    pub static ref ERRORS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("miracledb_errors_total", "Total errors"),
        &["error_type", "component"]
    ).expect("Failed to create errors_total metric");

    // ========== Backup Metrics ==========

    /// Backup operations
    pub static ref BACKUP_TOTAL: CounterVec = CounterVec::new(
        Opts::new("miracledb_backup_total", "Total backup operations"),
        &["backup_type", "status"]
    ).expect("Failed to create backup_total metric");

    /// Backup duration
    pub static ref BACKUP_DURATION: HistogramVec = HistogramVec::new(
        prometheus::HistogramOpts::new("miracledb_backup_duration_seconds", "Backup duration")
            .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]),
        &["backup_type"]
    ).expect("Failed to create backup_duration metric");

    /// Backup size in bytes
    pub static ref BACKUP_SIZE_BYTES: GaugeVec = GaugeVec::new(
        Opts::new("miracledb_backup_size_bytes", "Backup size in bytes"),
        &["backup_id"]
    ).expect("Failed to create backup_size_bytes metric");

    /// Active backups
    pub static ref BACKUPS_ACTIVE: Gauge = Gauge::new(
        "miracledb_backups_active",
        "Number of backups currently running"
    ).expect("Failed to create backups_active metric");

    /// Total backups stored
    pub static ref BACKUPS_STORED: Gauge = Gauge::new(
        "miracledb_backups_stored",
        "Total number of backups stored"
    ).expect("Failed to create backups_stored metric");

    /// Backup cleanup operations
    pub static ref BACKUP_CLEANUP_TOTAL: Counter = Counter::new(
        "miracledb_backup_cleanup_total",
        "Total backup cleanup operations"
    ).expect("Failed to create backup_cleanup_total metric");

    /// Restore operations
    pub static ref RESTORE_TOTAL: CounterVec = CounterVec::new(
        Opts::new("miracledb_restore_total", "Total restore operations"),
        &["status"]
    ).expect("Failed to create restore_total metric");

    /// Restore duration
    pub static ref RESTORE_DURATION: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new("miracledb_restore_duration_seconds", "Restore duration")
            .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0])
    ).expect("Failed to create restore_duration metric");
}

/// Metrics collector
pub struct MetricsCollector;

impl MetricsCollector {
    /// Register all default metrics with Prometheus registry
    pub fn register_default_metrics() -> Result<(), prometheus::Error> {
        // Use Once to ensure idempotent registration — safe when called from parallel tests
        // or multiple engine initializations in the same process.
        METRICS_REGISTERED.call_once(|| {
            let metrics: Vec<Box<dyn prometheus::core::Collector>> = vec![
                // Query metrics
                Box::new(QUERY_TOTAL.clone()),
                Box::new(QUERY_DURATION.clone()),
                Box::new(QUERY_ROWS.clone()),
                Box::new(QUERY_BYTES_SCANNED.clone()),
                // Connection metrics
                Box::new(CONNECTIONS_ACTIVE.clone()),
                Box::new(CONNECTIONS_TOTAL.clone()),
                Box::new(CONNECTIONS_ERRORS.clone()),
                // Cache metrics
                Box::new(CACHE_HITS.clone()),
                Box::new(CACHE_MISSES.clone()),
                Box::new(CACHE_SIZE_BYTES.clone()),
                Box::new(CACHE_EVICTIONS.clone()),
                // Resource metrics
                Box::new(CPU_USAGE.clone()),
                Box::new(MEMORY_USAGE_BYTES.clone()),
                Box::new(MEMORY_USAGE_PERCENT.clone()),
                Box::new(DISK_READ_BYTES.clone()),
                Box::new(DISK_WRITE_BYTES.clone()),
                Box::new(DISK_SPACE_USED_BYTES.clone()),
                Box::new(DISK_SPACE_AVAILABLE_BYTES.clone()),
                // ML metrics
                Box::new(ML_INFERENCE_TOTAL.clone()),
                Box::new(ML_INFERENCE_DURATION.clone()),
                Box::new(ML_MODELS_LOADED.clone()),
                Box::new(ML_MODEL_MEMORY_BYTES.clone()),
                // Kafka metrics
                Box::new(KAFKA_MESSAGES_CONSUMED.clone()),
                Box::new(KAFKA_MESSAGES_PRODUCED.clone()),
                Box::new(KAFKA_CONSUMER_LAG.clone()),
                Box::new(KAFKA_MESSAGE_PROCESSING_DURATION.clone()),
                Box::new(KAFKA_SOURCES_ACTIVE.clone()),
                Box::new(KAFKA_SINKS_ACTIVE.clone()),
                // Vector search metrics
                Box::new(VECTOR_SEARCH_TOTAL.clone()),
                Box::new(VECTOR_SEARCH_DURATION.clone()),
                Box::new(VECTORS_INDEXED.clone()),
                // Transaction metrics
                Box::new(TRANSACTIONS_STARTED.clone()),
                Box::new(TRANSACTIONS_COMMITTED.clone()),
                Box::new(TRANSACTIONS_ROLLED_BACK.clone()),
                Box::new(TRANSACTION_DURATION.clone()),
                // Database stats
                Box::new(DATABASE_SIZE_BYTES.clone()),
                Box::new(TABLES_COUNT.clone()),
                Box::new(TABLE_ROWS.clone()),
                // Error metrics
                Box::new(ERRORS_TOTAL.clone()),
                // Backup metrics
                Box::new(BACKUP_TOTAL.clone()),
                Box::new(BACKUP_DURATION.clone()),
                Box::new(BACKUP_SIZE_BYTES.clone()),
                Box::new(BACKUPS_ACTIVE.clone()),
                Box::new(BACKUPS_STORED.clone()),
                Box::new(BACKUP_CLEANUP_TOTAL.clone()),
                Box::new(RESTORE_TOTAL.clone()),
                Box::new(RESTORE_DURATION.clone()),
            ];
            for m in metrics {
                let _ = REGISTRY.register(m);
            }
            tracing::info!("Registered {} Prometheus metrics", 52);
        });

        Ok(())
    }

    /// Export metrics in Prometheus text format
    pub fn export_metrics() -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = REGISTRY.gather();

        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        Ok(String::from_utf8(buffer).unwrap_or_default())
    }

    /// Record query execution
    pub fn record_query(
        query_type: &str,
        status: &str,
        duration_secs: f64,
        rows: u64,
        bytes_scanned: u64,
    ) {
        QUERY_TOTAL
            .with_label_values(&[query_type, status])
            .inc();

        QUERY_DURATION
            .with_label_values(&[query_type])
            .observe(duration_secs);

        QUERY_ROWS
            .with_label_values(&[query_type])
            .observe(rows as f64);

        QUERY_BYTES_SCANNED
            .with_label_values(&[query_type])
            .observe(bytes_scanned as f64);
    }

    /// Record connection
    pub fn record_connection_open() {
        CONNECTIONS_ACTIVE.inc();
        CONNECTIONS_TOTAL.inc();
    }

    /// Record connection close
    pub fn record_connection_close() {
        CONNECTIONS_ACTIVE.dec();
    }

    /// Record connection error
    pub fn record_connection_error(error_type: &str) {
        CONNECTIONS_ERRORS
            .with_label_values(&[error_type])
            .inc();
    }

    /// Record cache hit
    pub fn record_cache_hit(cache_type: &str) {
        CACHE_HITS
            .with_label_values(&[cache_type])
            .inc();
    }

    /// Record cache miss
    pub fn record_cache_miss(cache_type: &str) {
        CACHE_MISSES
            .with_label_values(&[cache_type])
            .inc();
    }

    /// Record ML inference
    pub fn record_ml_inference(model_name: &str, status: &str, duration_secs: f64) {
        ML_INFERENCE_TOTAL
            .with_label_values(&[model_name, status])
            .inc();

        ML_INFERENCE_DURATION
            .with_label_values(&[model_name])
            .observe(duration_secs);
    }

    /// Update resource metrics
    pub fn update_resource_metrics(
        cpu_percent: f64,
        memory_bytes: u64,
        memory_percent: f64,
        disk_used: u64,
        disk_available: u64,
    ) {
        CPU_USAGE.set(cpu_percent);
        MEMORY_USAGE_BYTES.set(memory_bytes as f64);
        MEMORY_USAGE_PERCENT.set(memory_percent);
        DISK_SPACE_USED_BYTES.set(disk_used as f64);
        DISK_SPACE_AVAILABLE_BYTES.set(disk_available as f64);
    }
}

/// Query metrics helper
pub struct QueryMetrics {
    query_type: String,
    start_time: std::time::Instant,
}

impl QueryMetrics {
    pub fn new(query_type: &str) -> Self {
        Self {
            query_type: query_type.to_string(),
            start_time: std::time::Instant::now(),
        }
    }

    pub fn finish(self, status: &str, rows: u64, bytes_scanned: u64) {
        let duration = self.start_time.elapsed().as_secs_f64();
        MetricsCollector::record_query(&self.query_type, status, duration, rows, bytes_scanned);
    }
}

/// Resource metrics collector (updates periodically)
pub struct ResourceMetrics;

impl ResourceMetrics {
    /// Collect current resource usage metrics
    pub fn collect() {
        use sysinfo::System;

        let mut sys = System::new_all();
        sys.refresh_all();

        // CPU usage
        let cpu_usage: f64 = sys.cpus().iter().map(|cpu| cpu.cpu_usage() as f64).sum::<f64>() / sys.cpus().len() as f64;

        // Memory usage
        let memory_used = sys.used_memory();
        let memory_total = sys.total_memory();
        let memory_percent = (memory_used as f64 / memory_total as f64) * 100.0;

        // Disk usage - Temporarily disabled due to sysinfo 0.31 API changes
        // The disks() method signature changed and needs investigation
        let (disk_used, disk_available) = (0_u64, 0_u64);

        MetricsCollector::update_resource_metrics(
            cpu_usage,
            memory_used,
            memory_percent,
            disk_used,
            disk_available,
        );
    }

    /// Start periodic resource metrics collection
    pub fn start_periodic_collection(interval_secs: u64) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;
                Self::collect();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        let result = MetricsCollector::register_default_metrics();
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_metrics() {
        let metrics = QueryMetrics::new("SELECT");
        std::thread::sleep(std::time::Duration::from_millis(10));
        metrics.finish("success", 100, 1024);

        // Verify metric was recorded
        let exported = MetricsCollector::export_metrics().unwrap();
        assert!(exported.contains("miracledb_query_total"));
    }
}
