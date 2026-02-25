//! Observability Module - Metrics, Tracing, and Monitoring

pub mod metrics;
pub mod tracing;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Metric types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

/// A single metric
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub metric_type: MetricType,
    pub value: f64,
    pub labels: HashMap<String, String>,
    pub timestamp: i64,
}

/// Query execution span for tracing
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub operation_name: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub status: SpanStatus,
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SpanStatus {
    Ok,
    Error(String),
    InProgress,
}

/// Slow query entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowQuery {
    pub query: String,
    pub execution_time_ms: u64,
    pub timestamp: i64,
    pub user_id: Option<String>,
}

/// Counter metric
pub struct Counter {
    value: AtomicU64,
    name: String,
}

impl Counter {
    pub fn new(name: &str) -> Self {
        Self {
            value: AtomicU64::new(0),
            name: name.to_string(),
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Gauge metric
pub struct Gauge {
    value: AtomicU64,
    name: String,
}

impl Gauge {
    pub fn new(name: &str) -> Self {
        Self {
            value: AtomicU64::new(0),
            name: name.to_string(),
        }
    }

    pub fn set(&self, v: u64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Histogram for latency tracking
pub struct Histogram {
    buckets: Vec<AtomicU64>,
    bucket_bounds: Vec<f64>,
    sum: AtomicU64,
    count: AtomicU64,
    name: String,
}

impl Histogram {
    pub fn new(name: &str, bounds: Vec<f64>) -> Self {
        let buckets = bounds.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            buckets,
            bucket_bounds: bounds,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            name: name.to_string(),
        }
    }

    pub fn observe(&self, value: f64) {
        self.sum.fetch_add((value * 1000.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        for (i, bound) in self.bucket_bounds.iter().enumerate() {
            if value <= *bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }
}

/// Metrics registry
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Arc<Counter>>>,
    gauges: RwLock<HashMap<String, Arc<Gauge>>>,
    histograms: RwLock<HashMap<String, Arc<Histogram>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    pub async fn counter(&self, name: &str) -> Arc<Counter> {
        let mut counters = self.counters.write().await;
        counters.entry(name.to_string())
            .or_insert_with(|| Arc::new(Counter::new(name)))
            .clone()
    }

    pub async fn gauge(&self, name: &str) -> Arc<Gauge> {
        let mut gauges = self.gauges.write().await;
        gauges.entry(name.to_string())
            .or_insert_with(|| Arc::new(Gauge::new(name)))
            .clone()
    }

    pub async fn histogram(&self, name: &str) -> Arc<Histogram> {
        let bounds = vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        let mut histograms = self.histograms.write().await;
        histograms.entry(name.to_string())
            .or_insert_with(|| Arc::new(Histogram::new(name, bounds)))
            .clone()
    }

    /// Export metrics in Prometheus format
    pub async fn export_prometheus(&self) -> String {
        let mut output = String::new();

        let counters = self.counters.read().await;
        for (name, counter) in counters.iter() {
            output.push_str(&format!("# TYPE {} counter\n", name));
            output.push_str(&format!("{} {}\n", name, counter.get()));
        }

        let gauges = self.gauges.read().await;
        for (name, gauge) in gauges.iter() {
            output.push_str(&format!("# TYPE {} gauge\n", name));
            output.push_str(&format!("{} {}\n", name, gauge.get()));
        }

        output
    }
}

/// Distributed tracing
pub struct Tracer {
    spans: RwLock<HashMap<String, Span>>,
    slow_queries: RwLock<Vec<SlowQuery>>,
    slow_query_threshold_ms: u64,
}

impl Tracer {
    pub fn new(slow_query_threshold_ms: u64) -> Self {
        Self {
            spans: RwLock::new(HashMap::new()),
            slow_queries: RwLock::new(Vec::new()),
            slow_query_threshold_ms,
        }
    }

    pub async fn start_span(&self, trace_id: &str, operation: &str, parent: Option<&str>) -> String {
        let span_id = uuid::Uuid::new_v4().to_string();
        let span = Span {
            trace_id: trace_id.to_string(),
            span_id: span_id.clone(),
            parent_span_id: parent.map(|s| s.to_string()),
            operation_name: operation.to_string(),
            start_time: chrono::Utc::now().timestamp_millis(),
            end_time: None,
            status: SpanStatus::InProgress,
            attributes: HashMap::new(),
        };

        let mut spans = self.spans.write().await;
        spans.insert(span_id.clone(), span);
        span_id
    }

    pub async fn end_span(&self, span_id: &str, status: SpanStatus) {
        let mut spans = self.spans.write().await;
        if let Some(span) = spans.get_mut(span_id) {
            span.end_time = Some(chrono::Utc::now().timestamp_millis());
            span.status = status;
        }
    }

    pub async fn record_slow_query(&self, query: &str, execution_time_ms: u64, user_id: Option<&str>) {
        if execution_time_ms >= self.slow_query_threshold_ms {
            let mut slow_queries = self.slow_queries.write().await;
            slow_queries.push(SlowQuery {
                query: query.to_string(),
                execution_time_ms,
                timestamp: chrono::Utc::now().timestamp(),
                user_id: user_id.map(|s| s.to_string()),
            });

            // Keep only last 1000 slow queries
            if slow_queries.len() > 1000 {
                slow_queries.remove(0);
            }
        }
    }

    pub async fn get_slow_queries(&self, limit: usize) -> Vec<SlowQuery> {
        let queries = self.slow_queries.read().await;
        queries.iter().rev().take(limit).cloned().collect()
    }
}

/// Complete observability system
pub struct Observability {
    pub metrics: MetricsRegistry,
    pub tracer: Tracer,
}

impl Observability {
    pub fn new() -> Self {
        Self {
            metrics: MetricsRegistry::new(),
            tracer: Tracer::new(1000), // 1 second threshold
        }
    }
}

impl Default for Observability {
    fn default() -> Self {
        Self::new()
    }
}
