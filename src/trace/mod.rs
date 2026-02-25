//! Trace Module - Distributed tracing for queries

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Trace span
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub parent_id: Option<String>,
    pub operation: String,
    pub service: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub status: SpanStatus,
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: i64,
    pub attributes: HashMap<String, String>,
}

/// Trace
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trace {
    pub trace_id: String,
    pub spans: Vec<Span>,
    pub start_time: i64,
    pub end_time: Option<i64>,
}

impl Trace {
    pub fn duration_ms(&self) -> Option<i64> {
        self.end_time.map(|end| end - self.start_time)
    }

    pub fn root_span(&self) -> Option<&Span> {
        self.spans.iter().find(|s| s.parent_id.is_none())
    }
}

/// Span builder
pub struct SpanBuilder {
    span: Span,
}

impl SpanBuilder {
    pub fn new(trace_id: &str, operation: &str) -> Self {
        Self {
            span: Span {
                trace_id: trace_id.to_string(),
                span_id: uuid::Uuid::new_v4().to_string(),
                parent_id: None,
                operation: operation.to_string(),
                service: "miracledb".to_string(),
                start_time: chrono::Utc::now().timestamp_millis(),
                end_time: None,
                status: SpanStatus::Unset,
                attributes: HashMap::new(),
                events: vec![],
            },
        }
    }

    pub fn parent(mut self, parent_id: &str) -> Self {
        self.span.parent_id = Some(parent_id.to_string());
        self
    }

    pub fn service(mut self, service: &str) -> Self {
        self.span.service = service.to_string();
        self
    }

    pub fn attribute(mut self, key: &str, value: &str) -> Self {
        self.span.attributes.insert(key.to_string(), value.to_string());
        self
    }

    pub fn build(self) -> Span {
        self.span
    }
}

/// Tracer
pub struct Tracer {
    traces: RwLock<HashMap<String, Trace>>,
    current_spans: RwLock<HashMap<String, Span>>,
    max_traces: usize,
}

impl Tracer {
    pub fn new(max_traces: usize) -> Self {
        Self {
            traces: RwLock::new(HashMap::new()),
            current_spans: RwLock::new(HashMap::new()),
            max_traces,
        }
    }

    /// Start a new trace
    pub async fn start_trace(&self, operation: &str) -> String {
        let trace_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis();

        let trace = Trace {
            trace_id: trace_id.clone(),
            spans: vec![],
            start_time: now,
            end_time: None,
        };

        let span = SpanBuilder::new(&trace_id, operation).build();

        let mut traces = self.traces.write().await;
        let mut spans = self.current_spans.write().await;

        // Trim old traces
        if traces.len() >= self.max_traces {
            let oldest: Vec<String> = traces.iter()
                .filter(|(_, t)| t.end_time.is_some())
                .take(self.max_traces / 4)
                .map(|(id, _)| id.clone())
                .collect();
            for id in oldest {
                traces.remove(&id);
            }
        }

        traces.insert(trace_id.clone(), trace);
        spans.insert(span.span_id.clone(), span);

        trace_id
    }

    /// Start a new span within a trace
    pub async fn start_span(&self, trace_id: &str, operation: &str, parent_id: Option<&str>) -> String {
        let span = match parent_id {
            Some(pid) => SpanBuilder::new(trace_id, operation).parent(pid).build(),
            None => SpanBuilder::new(trace_id, operation).build(),
        };

        let span_id = span.span_id.clone();

        let mut spans = self.current_spans.write().await;
        spans.insert(span_id.clone(), span);

        span_id
    }

    /// End a span
    pub async fn end_span(&self, span_id: &str, status: SpanStatus) {
        let mut spans = self.current_spans.write().await;
        
        if let Some(mut span) = spans.remove(span_id) {
            span.end_time = Some(chrono::Utc::now().timestamp_millis());
            span.status = status;

            drop(spans);

            let mut traces = self.traces.write().await;
            if let Some(trace) = traces.get_mut(&span.trace_id) {
                trace.spans.push(span);
            }
        }
    }

    /// End a trace
    pub async fn end_trace(&self, trace_id: &str) {
        let mut traces = self.traces.write().await;
        if let Some(trace) = traces.get_mut(trace_id) {
            trace.end_time = Some(chrono::Utc::now().timestamp_millis());
        }
    }

    /// Add event to span
    pub async fn add_event(&self, span_id: &str, name: &str, attributes: HashMap<String, String>) {
        let mut spans = self.current_spans.write().await;
        if let Some(span) = spans.get_mut(span_id) {
            span.events.push(SpanEvent {
                name: name.to_string(),
                timestamp: chrono::Utc::now().timestamp_millis(),
                attributes,
            });
        }
    }

    /// Add attribute to span
    pub async fn add_attribute(&self, span_id: &str, key: &str, value: &str) {
        let mut spans = self.current_spans.write().await;
        if let Some(span) = spans.get_mut(span_id) {
            span.attributes.insert(key.to_string(), value.to_string());
        }
    }

    /// Get trace
    pub async fn get_trace(&self, trace_id: &str) -> Option<Trace> {
        let traces = self.traces.read().await;
        traces.get(trace_id).cloned()
    }

    /// Get recent traces
    pub async fn get_recent_traces(&self, limit: usize) -> Vec<Trace> {
        let traces = self.traces.read().await;
        let mut all: Vec<_> = traces.values().cloned().collect();
        all.sort_by(|a, b| b.start_time.cmp(&a.start_time));
        all.into_iter().take(limit).collect()
    }

    /// Export traces in Jaeger format
    pub async fn export_jaeger(&self, trace_id: &str) -> Option<serde_json::Value> {
        let trace = self.get_trace(trace_id).await?;

        let spans: Vec<_> = trace.spans.iter().map(|s| {
            serde_json::json!({
                "traceID": s.trace_id,
                "spanID": s.span_id,
                "parentSpanID": s.parent_id,
                "operationName": s.operation,
                "serviceName": s.service,
                "startTime": s.start_time * 1000, // microseconds
                "duration": s.end_time.map(|e| (e - s.start_time) * 1000).unwrap_or(0),
                "tags": s.attributes,
            })
        }).collect();

        Some(serde_json::json!({
            "traceID": trace.trace_id,
            "spans": spans,
        }))
    }
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new(10000)
    }
}
