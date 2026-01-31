//! Metrics Module - System metrics collection and export

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Metric type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// Counter metric
pub struct Counter {
    name: String,
    value: AtomicU64,
    labels: HashMap<String, String>,
}

impl Counter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicU64::new(0),
            labels: HashMap::new(),
        }
    }

    pub fn with_labels(name: &str, labels: HashMap<String, String>) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicU64::new(0),
            labels,
        }
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, v: u64) {
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Gauge metric
pub struct Gauge {
    name: String,
    value: AtomicI64,
    labels: HashMap<String, String>,
}

impl Gauge {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            value: AtomicI64::new(0),
            labels: HashMap::new(),
        }
    }

    pub fn set(&self, v: i64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Histogram metric
pub struct Histogram {
    name: String,
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum: std::sync::atomic::AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new(name: &str, buckets: Vec<f64>) -> Self {
        let counts = buckets.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            name: name.to_string(),
            buckets,
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: f64) {
        let bits = value.to_bits();
        self.sum.fetch_add(bits, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        for (i, bucket) in self.buckets.iter().enumerate() {
            if value <= *bucket {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// Metrics registry
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Arc<Counter>>>,
    gauges: RwLock<HashMap<String, Arc<Gauge>>>,
    histograms: RwLock<HashMap<String, Arc<Histogram>>>,
    prefix: String,
}

impl MetricsRegistry {
    pub fn new(prefix: &str) -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            prefix: prefix.to_string(),
        }
    }

    /// Register or get a counter
    pub async fn counter(&self, name: &str) -> Arc<Counter> {
        let full_name = format!("{}_{}", self.prefix, name);
        let mut counters = self.counters.write().await;
        counters.entry(full_name.clone())
            .or_insert_with(|| Arc::new(Counter::new(&full_name)))
            .clone()
    }

    /// Register or get a gauge
    pub async fn gauge(&self, name: &str) -> Arc<Gauge> {
        let full_name = format!("{}_{}", self.prefix, name);
        let mut gauges = self.gauges.write().await;
        gauges.entry(full_name.clone())
            .or_insert_with(|| Arc::new(Gauge::new(&full_name)))
            .clone()
    }

    /// Register or get a histogram
    pub async fn histogram(&self, name: &str, buckets: Vec<f64>) -> Arc<Histogram> {
        let full_name = format!("{}_{}", self.prefix, name);
        let mut histograms = self.histograms.write().await;
        histograms.entry(full_name.clone())
            .or_insert_with(|| Arc::new(Histogram::new(&full_name, buckets)))
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

        let histograms = self.histograms.read().await;
        for (name, histogram) in histograms.iter() {
            output.push_str(&format!("# TYPE {} histogram\n", name));
            for (i, bucket) in histogram.buckets.iter().enumerate() {
                let count = histogram.counts[i].load(Ordering::Relaxed);
                output.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", name, bucket, count));
            }
            output.push_str(&format!("{}_count {}\n", name, histogram.get_count()));
        }

        output
    }

    /// Export metrics as JSON
    pub async fn export_json(&self) -> serde_json::Value {
        let counters = self.counters.read().await;
        let gauges = self.gauges.read().await;
        let histograms = self.histograms.read().await;

        serde_json::json!({
            "counters": counters.iter().map(|(k, v)| (k.clone(), v.get())).collect::<HashMap<_, _>>(),
            "gauges": gauges.iter().map(|(k, v)| (k.clone(), v.get())).collect::<HashMap<_, _>>(),
            "histograms": histograms.iter().map(|(k, v)| (k.clone(), v.get_count())).collect::<HashMap<_, _>>(),
        })
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new("miracledb")
    }
}

/// Standard database metrics
pub struct DatabaseMetrics {
    pub queries_total: Arc<Counter>,
    pub queries_errors: Arc<Counter>,
    pub active_connections: Arc<Gauge>,
    pub query_duration: Arc<Histogram>,
}

impl DatabaseMetrics {
    pub async fn new(registry: &MetricsRegistry) -> Self {
        let buckets = vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        Self {
            queries_total: registry.counter("queries_total").await,
            queries_errors: registry.counter("queries_errors_total").await,
            active_connections: registry.gauge("active_connections").await,
            query_duration: registry.histogram("query_duration_seconds", buckets).await,
        }
    }
}

/// ML-specific metrics
#[cfg(feature = "nlp")]
pub struct MLMetrics {
    pub model_load_duration: Arc<Histogram>,
    pub embedding_generation_duration: Arc<Histogram>,
    pub embeddings_generated_total: Arc<Counter>,
    pub embedding_cache_hits_total: Arc<Counter>,
    pub embedding_cache_misses_total: Arc<Counter>,
    pub model_errors_total: Arc<Counter>,
    pub models_loaded: Arc<Gauge>,
}

#[cfg(feature = "nlp")]
impl MLMetrics {
    pub async fn new(registry: &MetricsRegistry) -> Self {
        let buckets = vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        Self {
            model_load_duration: registry.histogram("ml_model_load_seconds", buckets.clone()).await,
            embedding_generation_duration: registry.histogram("ml_embedding_generation_seconds", buckets).await,
            embeddings_generated_total: registry.counter("ml_embeddings_generated_total").await,
            embedding_cache_hits_total: registry.counter("ml_embedding_cache_hits_total").await,
            embedding_cache_misses_total: registry.counter("ml_embedding_cache_misses_total").await,
            model_errors_total: registry.counter("ml_model_errors_total").await,
            models_loaded: registry.gauge("ml_models_loaded").await,
        }
    }
}
