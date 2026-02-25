//! OpenTelemetry Tracing Module
//!
//! Provides distributed tracing support using OpenTelemetry.
//! Traces are exported to Jaeger, Zipkin, or any OTLP-compatible backend.

use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::{
    runtime,
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
use opentelemetry_otlp::WithExportConfig;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Trace context for propagating trace IDs across operations
#[derive(Debug, Clone)]
pub struct TraceContext {
    /// Trace ID
    pub trace_id: String,
    /// Span ID
    pub span_id: String,
}

impl TraceContext {
    /// Create a new trace context
    pub fn new() -> Self {
        Self {
            trace_id: uuid::Uuid::new_v4().to_string(),
            span_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Create from existing IDs
    pub fn from_ids(trace_id: String, span_id: String) -> Self {
        Self { trace_id, span_id }
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize OpenTelemetry tracing
///
/// This sets up tracing with the OpenTelemetry SDK and exports traces
/// to the configured OTLP endpoint (defaults to localhost:4317 for Jaeger).
pub fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    // Get OTLP endpoint from environment or use default
    let otlp_endpoint = std::env::var("OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    // Create OTLP exporter
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&otlp_endpoint)
        .with_timeout(Duration::from_secs(3));

    // Create trace provider
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_max_events_per_span(64)
                .with_max_attributes_per_span(32)
                .with_resource(Resource::new(vec![
                    KeyValue::new("service.name", "miracledb"),
                    KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                ])),
        )
        .install_batch(runtime::Tokio)?;

    // Set as global tracer
    global::set_tracer_provider(tracer.provider().unwrap().clone());

    // Initialize tracing subscriber
    // Temporarily disabled - tracing_opentelemetry not in dependencies
    // let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        // .with(telemetry)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()?;

    tracing::info!("OpenTelemetry tracing initialized (endpoint: {})", otlp_endpoint);

    Ok(())
}

/// Shutdown tracing and flush remaining spans
pub fn shutdown_tracing() {
    global::shutdown_tracer_provider();
    tracing::info!("OpenTelemetry tracing shutdown");
}

/// Macro to create a traced span
#[macro_export]
macro_rules! traced_span {
    ($name:expr) => {
        tracing::info_span!($name)
    };
    ($name:expr, $($key:tt = $value:expr),+) => {
        tracing::info_span!($name, $($key = $value),+)
    };
}

/// Helper to record query execution trace
pub fn trace_query(query: &str, query_type: &str) -> tracing::Span {
    tracing::info_span!(
        "query_execution",
        query_type = query_type,
        query = %query,
        otel.kind = "internal",
    )
}

/// Helper to record ML inference trace
pub fn trace_ml_inference(model_name: &str, batch_size: usize) -> tracing::Span {
    tracing::info_span!(
        "ml_inference",
        model_name = model_name,
        batch_size = batch_size,
        otel.kind = "internal",
    )
}

/// Helper to record Kafka message processing trace
pub fn trace_kafka_message(topic: &str, partition: i32, offset: i64) -> tracing::Span {
    tracing::info_span!(
        "kafka_message",
        topic = topic,
        partition = partition,
        offset = offset,
        otel.kind = "consumer",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context() {
        let ctx = TraceContext::new();
        assert!(!ctx.trace_id.is_empty());
        assert!(!ctx.span_id.is_empty());
    }

    #[test]
    fn test_trace_context_from_ids() {
        let ctx = TraceContext::from_ids("trace-123".to_string(), "span-456".to_string());
        assert_eq!(ctx.trace_id, "trace-123");
        assert_eq!(ctx.span_id, "span-456");
    }
}
