# MiracleDb Observability Guide

**Version:** 1.0
**Date:** 2026-01-25
**Phase:** 2 - Week 6-7

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Prometheus Metrics](#prometheus-metrics)
4. [Health Checks](#health-checks)
5. [OpenTelemetry Tracing](#opentelemetry-tracing)
6. [Grafana Dashboards](#grafana-dashboards)
7. [Kubernetes Integration](#kubernetes-integration)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## Overview

MiracleDb provides comprehensive observability features for production monitoring and debugging:

- **40+ Prometheus Metrics** - Query performance, resource utilization, ML inference, Kafka streaming
- **Health Check Endpoints** - Kubernetes liveness, readiness, and startup probes
- **OpenTelemetry Tracing** - Distributed tracing with Jaeger/Zipkin support
- **Grafana Dashboards** - Pre-built dashboards for visualization
- **Automatic Resource Monitoring** - CPU, memory, disk metrics collected every 15 seconds

### Key Features

✅ **Production-Ready Metrics**
- Query latency (p50, p95, p99)
- Throughput (queries/sec, messages/sec)
- Error rates and types
- Resource utilization

✅ **Kubernetes-Native Health Checks**
- Liveness probe (is server alive?)
- Readiness probe (ready for traffic?)
- Startup probe (initialization complete?)

✅ **Distributed Tracing**
- Query execution traces
- ML inference traces
- Kafka message processing traces
- Cross-service trace propagation

---

## Quick Start

### 1. Enable Observability

Initialize observability in your application:

```rust
use miracledb::observability;

#[tokio::main]
async fn main() {
    // Initialize observability
    observability::init_observability().expect("Failed to initialize observability");

    // Start periodic resource metrics collection (every 15 seconds)
    observability::ResourceMetrics::start_periodic_collection(15);

    // Create engine...
    let engine = MiracleEngine::new().await.unwrap();

    // Mark startup complete for health checks
    health_checker.mark_startup_complete().await;

    // ... start server ...
}
```

### 2. Add Observability Endpoints

Integrate observability endpoints into your REST API:

```rust
use miracledb::observability::api::create_observability_router;
use std::sync::Arc;

// Create health checker
let health_checker = Arc::new(HealthChecker::new());

// Create observability router
let obs_router = create_observability_router(
    health_checker.clone(),
    Some(Arc::new(engine)),
);

// Mount under /api/v1
let app = Router::new()
    .nest("/api/v1", obs_router)
    // ... other routes ...
```

### 3. Access Endpoints

Observability endpoints are now available:

```bash
# Prometheus metrics
curl http://localhost:8080/api/v1/metrics

# Health checks
curl http://localhost:8080/api/v1/health/live    # Liveness
curl http://localhost:8080/api/v1/health/ready   # Readiness
curl http://localhost:8080/api/v1/health/startup # Startup
curl http://localhost:8080/api/v1/health         # Deep health check
```

### 4. Setup Prometheus Scraping

Configure Prometheus to scrape MiracleDb metrics:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'miracledb'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/api/v1/metrics'
    scrape_interval: 15s
```

### 5. Import Grafana Dashboard

Import the pre-built dashboard:

```bash
# Copy dashboard template
cp grafana/miracledb-dashboard.json /path/to/grafana/dashboards/

# Or import via Grafana UI:
# Dashboards → Import → Upload JSON file
```

---

## Prometheus Metrics

### Metric Categories

MiracleDb exposes **40+ Prometheus metrics** across 10 categories:

#### 1. Query Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_query_total` | Counter | Total queries executed (labels: query_type, status) |
| `miracledb_query_duration_seconds` | Histogram | Query execution duration (labels: query_type) |
| `miracledb_query_rows` | Histogram | Number of rows returned |
| `miracledb_query_bytes_scanned` | Histogram | Bytes scanned by query |

**Example Usage:**
```rust
use miracledb::observability::{QueryMetrics, MetricsCollector};

// Record query execution
let metrics = QueryMetrics::new("SELECT");
// ... execute query ...
metrics.finish("success", rows_returned, bytes_scanned);
```

**PromQL Examples:**
```promql
# Query rate
rate(miracledb_query_total[5m])

# P95 query latency
histogram_quantile(0.95, rate(miracledb_query_duration_seconds_bucket[5m]))

# Success rate
rate(miracledb_query_total{status="success"}[5m]) / rate(miracledb_query_total[5m])
```

#### 2. Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_connections_active` | Gauge | Current active connections |
| `miracledb_connections_total` | Counter | Total connections created |
| `miracledb_connection_errors_total` | Counter | Connection errors (labels: error_type) |

**Example Usage:**
```rust
// Record connection lifecycle
MetricsCollector::record_connection_open();
// ... use connection ...
MetricsCollector::record_connection_close();

// Record connection error
MetricsCollector::record_connection_error("timeout");
```

**PromQL Examples:**
```promql
# Active connections
miracledb_connections_active

# Connection error rate
rate(miracledb_connection_errors_total[5m])

# Connection churn rate
rate(miracledb_connections_total[5m])
```

#### 3. Cache Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_cache_hits_total` | Counter | Cache hits (labels: cache_type) |
| `miracledb_cache_misses_total` | Counter | Cache misses (labels: cache_type) |
| `miracledb_cache_size_bytes` | Gauge | Cache size in bytes (labels: cache_type) |
| `miracledb_cache_evictions_total` | Counter | Cache evictions (labels: cache_type) |

**Example Usage:**
```rust
// Record cache hit/miss
MetricsCollector::record_cache_hit("query_cache");
MetricsCollector::record_cache_miss("query_cache");
```

**PromQL Examples:**
```promql
# Cache hit rate
rate(miracledb_cache_hits_total[5m]) /
(rate(miracledb_cache_hits_total[5m]) + rate(miracledb_cache_misses_total[5m]))

# Cache eviction rate
rate(miracledb_cache_evictions_total[5m])
```

#### 4. Resource Utilization Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_cpu_usage_percent` | Gauge | CPU usage percentage (0-100) |
| `miracledb_memory_usage_bytes` | Gauge | Memory usage in bytes |
| `miracledb_memory_usage_percent` | Gauge | Memory usage percentage (0-100) |
| `miracledb_disk_read_bytes_total` | Counter | Total bytes read from disk |
| `miracledb_disk_write_bytes_total` | Counter | Total bytes written to disk |
| `miracledb_disk_space_used_bytes` | Gauge | Disk space used |
| `miracledb_disk_space_available_bytes` | Gauge | Disk space available |

**Automatic Collection:**

Resource metrics are automatically collected every 15 seconds:

```rust
// Start periodic collection
ResourceMetrics::start_periodic_collection(15);
```

**PromQL Examples:**
```promql
# CPU usage
miracledb_cpu_usage_percent

# Memory usage
miracledb_memory_usage_percent

# Disk I/O rate
rate(miracledb_disk_read_bytes_total[5m])
rate(miracledb_disk_write_bytes_total[5m])

# Disk space percentage
miracledb_disk_space_used_bytes /
(miracledb_disk_space_used_bytes + miracledb_disk_space_available_bytes) * 100
```

#### 5. ML Inference Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_ml_inference_total` | Counter | ML inference requests (labels: model_name, status) |
| `miracledb_ml_inference_duration_seconds` | Histogram | ML inference duration (labels: model_name) |
| `miracledb_ml_models_loaded` | Gauge | Number of loaded models |
| `miracledb_ml_model_memory_bytes` | Gauge | Model memory usage (labels: model_name) |

**Example Usage:**
```rust
let start = std::time::Instant::now();
// ... run inference ...
let duration = start.elapsed().as_secs_f64();

MetricsCollector::record_ml_inference("fraud_model", "success", duration);
```

**PromQL Examples:**
```promql
# Inference rate by model
rate(miracledb_ml_inference_total[5m])

# P95 inference latency
histogram_quantile(0.95, rate(miracledb_ml_inference_duration_seconds_bucket[5m]))

# Model success rate
rate(miracledb_ml_inference_total{status="success"}[5m]) /
rate(miracledb_ml_inference_total[5m])
```

#### 6. Kafka Streaming Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_kafka_messages_consumed_total` | Counter | Messages consumed (labels: source_name, topic) |
| `miracledb_kafka_messages_produced_total` | Counter | Messages produced (labels: sink_name, topic) |
| `miracledb_kafka_consumer_lag` | Gauge | Consumer lag (labels: source_name, topic, partition) |
| `miracledb_kafka_message_processing_duration_seconds` | Histogram | Message processing duration (labels: source_name) |

**PromQL Examples:**
```promql
# Message consumption rate
rate(miracledb_kafka_messages_consumed_total[5m])

# Consumer lag
miracledb_kafka_consumer_lag

# Message processing latency
histogram_quantile(0.95, rate(miracledb_kafka_message_processing_duration_seconds_bucket[5m]))
```

#### 7. Vector Search Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_vector_search_total` | Counter | Vector search requests (labels: index_name, status) |
| `miracledb_vector_search_duration_seconds` | Histogram | Vector search duration (labels: index_name) |
| `miracledb_vectors_indexed` | Gauge | Number of vectors indexed (labels: index_name) |

**PromQL Examples:**
```promql
# Vector search rate
rate(miracledb_vector_search_total[5m])

# Search latency
histogram_quantile(0.95, rate(miracledb_vector_search_duration_seconds_bucket[5m]))
```

#### 8. Transaction Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_transactions_started_total` | Counter | Transactions started |
| `miracledb_transactions_committed_total` | Counter | Transactions committed |
| `miracledb_transactions_rolled_back_total` | Counter | Transactions rolled back |
| `miracledb_transaction_duration_seconds` | Histogram | Transaction duration |

**PromQL Examples:**
```promql
# Transaction rate
rate(miracledb_transactions_started_total[5m])

# Commit success rate
rate(miracledb_transactions_committed_total[5m]) /
rate(miracledb_transactions_started_total[5m])

# Rollback rate
rate(miracledb_transactions_rolled_back_total[5m])
```

#### 9. Database Stats

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_database_size_bytes` | Gauge | Database size (labels: database) |
| `miracledb_tables_count` | Gauge | Number of tables (labels: database) |
| `miracledb_table_rows` | Gauge | Table row count (labels: database, table) |

#### 10. Error Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `miracledb_errors_total` | Counter | Total errors (labels: error_type, component) |

**PromQL Examples:**
```promql
# Error rate
rate(miracledb_errors_total[5m])

# Errors by component
sum(rate(miracledb_errors_total[5m])) by (component)
```

---

## Health Checks

### Health Check Types

MiracleDb provides 4 types of health checks:

#### 1. Liveness Probe (`/health/live`)

**Purpose:** Is the server process running?

**Kubernetes Usage:**
```yaml
livenessProbe:
  httpGet:
    path: /api/v1/health/live
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

**Response:**
```json
{
  "status": "healthy",
  "components": [
    {
      "component": "process",
      "status": "healthy",
      "message": "Process is running",
      "response_time_ms": 0,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "async_runtime",
      "status": "healthy",
      "message": "Async runtime operational",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    }
  ],
  "total_duration_ms": 2,
  "timestamp": "2026-01-25T10:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Server is alive
- `503 Service Unavailable` - Server is unhealthy (Kubernetes will restart pod)

#### 2. Readiness Probe (`/health/ready`)

**Purpose:** Is the server ready to serve traffic?

**Kubernetes Usage:**
```yaml
readinessProbe:
  httpGet:
    path: /api/v1/health/ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
  successThreshold: 1
```

**Checks:**
- Database connectivity
- Async runtime operational
- Memory usage < 95%
- Disk space < 95%

**Response:**
```json
{
  "status": "healthy",
  "components": [
    {
      "component": "database",
      "status": "healthy",
      "message": "Database responding to queries",
      "response_time_ms": 5,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "memory",
      "status": "healthy",
      "message": "Memory usage: 45.2%",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "disk_space",
      "status": "healthy",
      "message": "Disk usage: 62.1%",
      "response_time_ms": 2,
      "last_check": "2026-01-25T10:30:00Z"
    }
  ],
  "total_duration_ms": 10,
  "timestamp": "2026-01-25T10:30:00Z"
}
```

**Status Codes:**
- `200 OK` - Ready to serve traffic
- `503 Service Unavailable` - Not ready (Kubernetes won't route traffic)

#### 3. Startup Probe (`/health/startup`)

**Purpose:** Has the server finished initializing?

**Kubernetes Usage:**
```yaml
startupProbe:
  httpGet:
    path: /api/v1/health/startup
    port: 8080
  initialDelaySeconds: 0
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 30  # Allow 5 minutes for startup
  successThreshold: 1
```

**Usage in Code:**
```rust
// At the end of initialization
health_checker.mark_startup_complete().await;
```

**Status Codes:**
- `200 OK` - Startup complete
- `503 Service Unavailable` - Still initializing

#### 4. Deep Health Check (`/health`)

**Purpose:** Comprehensive check of all components.

**Checks:**
- Database
- ML models (if ml feature enabled)
- Kafka sources/sinks (if kafka feature enabled)
- CPU usage
- Memory usage
- Disk space
- Async runtime

**Response:**
```json
{
  "status": "healthy",
  "components": [
    {
      "component": "database",
      "status": "healthy",
      "message": "Database responding to queries",
      "response_time_ms": 5,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "ml_models",
      "status": "healthy",
      "message": "3 ML models loaded",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "kafka_sources",
      "status": "healthy",
      "message": "2 Kafka sources active",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "kafka_sinks",
      "status": "healthy",
      "message": "1 Kafka sinks active",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "cpu",
      "status": "healthy",
      "message": "CPU usage: 42.5%",
      "response_time_ms": 102,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "memory",
      "status": "healthy",
      "message": "Memory usage: 45.2%",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "disk_space",
      "status": "healthy",
      "message": "Disk usage: 62.1%",
      "response_time_ms": 2,
      "last_check": "2026-01-25T10:30:00Z"
    },
    {
      "component": "async_runtime",
      "status": "healthy",
      "message": "Async runtime operational",
      "response_time_ms": 1,
      "last_check": "2026-01-25T10:30:00Z"
    }
  ],
  "total_duration_ms": 115,
  "timestamp": "2026-01-25T10:30:00Z"
}
```

### Health Status Values

| Status | Meaning | HTTP Code |
|--------|---------|-----------|
| `healthy` | Component is operating normally | 200 |
| `degraded` | Component is operational but degraded | 200 |
| `unhealthy` | Component is not operational | 503 |

### Thresholds

| Resource | Degraded | Unhealthy |
|----------|----------|-----------|
| CPU | > 75% | > 90% |
| Memory | > 85% | > 95% |
| Disk | > 85% | > 95% |

---

## OpenTelemetry Tracing

### Setup

#### 1. Enable OpenTelemetry

Set the `OTLP_ENDPOINT` environment variable to enable distributed tracing:

```bash
export OTLP_ENDPOINT=http://localhost:4317  # Jaeger OTLP gRPC endpoint
```

#### 2. Run Jaeger (Development)

```bash
docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 4317:4317 \
  -p 16686:16686 \
  jaegertracing/all-in-one:latest

# Access Jaeger UI at http://localhost:16686
```

#### 3. Initialize Tracing

```rust
use miracledb::observability;

// Initialize (reads OTLP_ENDPOINT from environment)
observability::init_observability().expect("Failed to initialize observability");
```

### Trace Helpers

#### Query Tracing

```rust
use miracledb::observability::tracing::trace_query;

let span = trace_query(&sql, "SELECT");
let _enter = span.enter();

// ... execute query ...

drop(_enter);  // End span
```

#### ML Inference Tracing

```rust
use miracledb::observability::tracing::trace_ml_inference;

let span = trace_ml_inference("fraud_model", batch_size);
let _enter = span.enter();

// ... run inference ...

drop(_enter);
```

#### Kafka Message Tracing

```rust
use miracledb::observability::tracing::trace_kafka_message;

let span = trace_kafka_message(topic, partition, offset);
let _enter = span.enter();

// ... process message ...

drop(_enter);
```

### Trace Context Propagation

```rust
use miracledb::observability::TraceContext;

// Create trace context
let ctx = TraceContext::new();

// Pass to downstream services
let headers = vec![
    ("x-trace-id", ctx.trace_id.clone()),
    ("x-span-id", ctx.span_id.clone()),
];
```

---

## Grafana Dashboards

### Import Pre-Built Dashboard

1. **Copy dashboard JSON:**
   ```bash
   cp grafana/miracledb-dashboard.json /path/to/grafana/dashboards/
   ```

2. **Or import via Grafana UI:**
   - Navigate to Dashboards → Import
   - Click "Upload JSON file"
   - Select `grafana/miracledb-dashboard.json`
   - Click "Import"

### Dashboard Panels

The pre-built dashboard includes 14 panels:

1. **Query Rate** - Queries per second by type and status
2. **Query Latency (p95)** - 95th and 99th percentile query latency
3. **Active Connections** - Current active database connections
4. **CPU Usage** - CPU usage percentage with thresholds
5. **Memory Usage** - Memory usage percentage with thresholds
6. **Disk Space Used** - Disk space percentage with thresholds
7. **Cache Hit Rate** - Cache hit percentage by cache type
8. **ML Inference Rate** - Inferences per second by model and status
9. **ML Inference Latency (p95)** - 95th percentile inference latency
10. **Kafka Message Rate** - Messages consumed/produced per second
11. **Kafka Consumer Lag** - Consumer lag by source and topic
12. **Vector Search Rate** - Vector searches per second
13. **Transaction Rate** - Transactions started/committed/rolled back per second
14. **Error Rate** - Errors per second with alert configured

### Alerts

The dashboard includes a pre-configured alert:

**High Error Rate Alert:**
- Triggers when error rate > 10 errors/sec
- Frequency: Check every 1 minute
- Can be integrated with notification channels

---

## Kubernetes Integration

### Complete Deployment Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: miracledb
spec:
  replicas: 3
  selector:
    matchLabels:
      app: miracledb
  template:
    metadata:
      labels:
        app: miracledb
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/api/v1/metrics"
    spec:
      containers:
      - name: miracledb
        image: miracledb:latest
        ports:
        - containerPort: 8080
          name: http
        env:
        - name: OTLP_ENDPOINT
          value: "http://jaeger-collector:4317"

        # Liveness probe
        livenessProbe:
          httpGet:
            path: /api/v1/health/live
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3

        # Readiness probe
        readinessProbe:
          httpGet:
            path: /api/v1/health/ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
          successThreshold: 1

        # Startup probe
        startupProbe:
          httpGet:
            path: /api/v1/health/startup
            port: 8080
          initialDelaySeconds: 0
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 30  # 5 minutes max startup time
          successThreshold: 1

        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
---
apiVersion: v1
kind: Service
metadata:
  name: miracledb
  labels:
    app: miracledb
spec:
  selector:
    app: miracledb
  ports:
  - port: 8080
    targetPort: 8080
    name: http
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: miracledb
spec:
  selector:
    matchLabels:
      app: miracledb
  endpoints:
  - port: http
    path: /api/v1/metrics
    interval: 15s
```

### Prometheus Operator

If using Prometheus Operator, create a ServiceMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: miracledb
  namespace: monitoring
spec:
  selector:
    matchLabels:
      app: miracledb
  endpoints:
  - port: http
    path: /api/v1/metrics
    interval: 15s
    scrapeTimeout: 10s
```

---

## Best Practices

### 1. Metrics Collection

**DO:**
- ✅ Use periodic resource collection (every 15 seconds)
- ✅ Record metrics for all queries
- ✅ Use appropriate metric types (Counter, Gauge, Histogram)
- ✅ Keep label cardinality low (< 10 unique values per label)

**DON'T:**
- ❌ Record metrics synchronously in hot paths (use buffering)
- ❌ Create metrics with high-cardinality labels (user IDs, timestamps)
- ❌ Record every single event (sample if necessary)

### 2. Health Checks

**DO:**
- ✅ Mark startup complete after full initialization
- ✅ Use appropriate timeouts (5s for probes)
- ✅ Configure failure thresholds properly (3-5 failures)
- ✅ Test health checks in staging before production

**DON'T:**
- ❌ Make health checks expensive (< 100ms response time)
- ❌ Return 200 when components are unhealthy
- ❌ Forget to mark startup complete

### 3. Tracing

**DO:**
- ✅ Use sampling in production (sample 1-10% of traces)
- ✅ Propagate trace context across services
- ✅ Add meaningful span attributes
- ✅ Use helper functions for common operations

**DON'T:**
- ❌ Trace every single request (use sampling)
- ❌ Create spans in tight loops
- ❌ Forget to close spans

### 4. Alerting

**DO:**
- ✅ Alert on symptoms, not causes
- ✅ Set appropriate thresholds
- ✅ Include runbooks in alerts
- ✅ Test alerts regularly

**Example Alerts:**

```yaml
# prometheus-alerts.yml
groups:
- name: miracledb
  interval: 1m
  rules:

  # High query latency
  - alert: HighQueryLatency
    expr: histogram_quantile(0.95, rate(miracledb_query_duration_seconds_bucket[5m])) > 1.0
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High query latency (p95 > 1s)"
      description: "Query latency is high: {{ $value }}s"

  # High error rate
  - alert: HighErrorRate
    expr: rate(miracledb_errors_total[5m]) > 10
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "High error rate (> 10 errors/sec)"
      description: "Error rate is {{ $value }} errors/sec"

  # High memory usage
  - alert: HighMemoryUsage
    expr: miracledb_memory_usage_percent > 90
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High memory usage (> 90%)"
      description: "Memory usage is {{ $value }}%"

  # High consumer lag
  - alert: HighKafkaConsumerLag
    expr: miracledb_kafka_consumer_lag > 10000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High Kafka consumer lag"
      description: "Consumer lag is {{ $value }} messages"
```

---

## Troubleshooting

### Issue: Metrics not appearing in Prometheus

**Symptoms:**
- `/metrics` endpoint returns empty or minimal metrics
- Prometheus shows "0 targets up"

**Solutions:**

1. **Verify metrics registration:**
   ```rust
   MetricsCollector::register_default_metrics()?;
   ```

2. **Check Prometheus configuration:**
   ```yaml
   scrape_configs:
     - job_name: 'miracledb'
       metrics_path: '/api/v1/metrics'  # Correct path
       static_configs:
         - targets: ['localhost:8080']
   ```

3. **Test metrics endpoint:**
   ```bash
   curl http://localhost:8080/api/v1/metrics
   ```

---

### Issue: Health checks failing

**Symptoms:**
- Kubernetes keeps restarting pods
- Health endpoints return 503

**Solutions:**

1. **Check if startup is marked complete:**
   ```rust
   health_checker.mark_startup_complete().await;
   ```

2. **Verify database connectivity:**
   ```bash
   curl http://localhost:8080/api/v1/health/ready
   ```

3. **Check resource thresholds:**
   - CPU > 90% causes unhealthy status
   - Memory > 95% causes unhealthy status
   - Disk > 95% causes unhealthy status

4. **Adjust Kubernetes probe settings:**
   ```yaml
   startupProbe:
     failureThreshold: 60  # Allow more time for startup
     periodSeconds: 5
   ```

---

### Issue: Traces not appearing in Jaeger

**Symptoms:**
- No traces visible in Jaeger UI
- OpenTelemetry errors in logs

**Solutions:**

1. **Verify OTLP_ENDPOINT is set:**
   ```bash
   echo $OTLP_ENDPOINT  # Should be http://localhost:4317
   ```

2. **Check Jaeger is running:**
   ```bash
   docker ps | grep jaeger
   curl http://localhost:16686  # Jaeger UI
   ```

3. **Test trace export:**
   ```bash
   # Check Jaeger collector logs
   docker logs jaeger
   ```

4. **Verify network connectivity:**
   ```bash
   telnet localhost 4317
   ```

---

### Issue: High metric cardinality

**Symptoms:**
- Prometheus memory usage growing rapidly
- Slow PromQL queries
- "series churn" warnings

**Solutions:**

1. **Audit label cardinality:**
   ```promql
   # Check unique label combinations
   count by (__name__, job) (miracledb_query_total)
   ```

2. **Remove high-cardinality labels:**
   ```rust
   // DON'T do this (user_id has high cardinality)
   QUERY_TOTAL.with_label_values(&[query_type, status, &user_id]).inc();

   // DO this instead
   QUERY_TOTAL.with_label_values(&[query_type, status]).inc();
   ```

3. **Use recording rules to aggregate:**
   ```yaml
   # prometheus-rules.yml
   groups:
   - name: miracledb_aggregations
     interval: 1m
     rules:
     - record: miracledb:query_rate:5m
       expr: rate(miracledb_query_total[5m])
   ```

---

## Conclusion

MiracleDb's observability features provide comprehensive monitoring and debugging capabilities for production deployments:

- ✅ **40+ Prometheus metrics** for deep insights
- ✅ **Kubernetes-native health checks** for reliable deployments
- ✅ **OpenTelemetry tracing** for distributed debugging
- ✅ **Pre-built Grafana dashboards** for visualization
- ✅ **Automatic resource monitoring** with no configuration

**Next Steps:**
1. Set up Prometheus and Grafana
2. Import MiracleDb dashboard
3. Configure Kubernetes health probes
4. Enable OpenTelemetry tracing (optional)
5. Set up alerts for critical metrics

For questions or issues, please visit:
- GitHub: https://github.com/miracledb/miracledb
- Documentation: https://docs.miracledb.com
- Community: https://discord.gg/miracledb

---

**Prepared by:** MiracleDb Development Team
**Date:** January 25, 2026
**Version:** 1.0
