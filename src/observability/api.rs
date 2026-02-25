//! REST API endpoints for Observability
//!
//! Provides HTTP endpoints for metrics and health checks.
//! These endpoints are designed for Prometheus scraping and Kubernetes probes.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use std::sync::Arc;

use super::health::{HealthChecker, HealthCheckResult};
use super::metrics::MetricsCollector;

/// Observability API state
#[derive(Clone)]
pub struct ObservabilityState {
    pub health_checker: Arc<HealthChecker>,
    pub engine: Option<Arc<crate::engine::MiracleEngine>>,
}

/// Create observability router
pub fn create_observability_router(
    health_checker: Arc<HealthChecker>,
    engine: Option<Arc<crate::engine::MiracleEngine>>,
) -> Router {
    let state = ObservabilityState {
        health_checker,
        engine,
    };

    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_deep_handler))
        .route("/health/live", get(health_liveness_handler))
        .route("/health/ready", get(health_readiness_handler))
        .route("/health/startup", get(health_startup_handler))
        .with_state(state)
}

/// GET /metrics - Prometheus metrics endpoint
///
/// Returns metrics in Prometheus text format for scraping.
async fn metrics_handler() -> impl IntoResponse {
    match MetricsCollector::export_metrics() {
        Ok(metrics) => (StatusCode::OK, metrics).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to export metrics: {}", e),
        )
            .into_response(),
    }
}

/// GET /health - Deep health check
///
/// Comprehensive health check of all components.
/// Returns JSON with detailed component status.
async fn health_deep_handler(State(state): State<ObservabilityState>) -> impl IntoResponse {
    let result = state
        .health_checker
        .check_deep(state.engine.as_deref())
        .await;

    let status = if result.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(result))
}

/// GET /health/live - Liveness probe
///
/// Kubernetes liveness probe endpoint.
/// Returns 200 if server is alive, 503 if unhealthy.
async fn health_liveness_handler(State(state): State<ObservabilityState>) -> impl IntoResponse {
    let result = state.health_checker.check_liveness().await;

    let status = if result.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(result))
}

/// GET /health/ready - Readiness probe
///
/// Kubernetes readiness probe endpoint.
/// Returns 200 if ready to serve traffic, 503 if not ready.
async fn health_readiness_handler(
    State(state): State<ObservabilityState>,
) -> impl IntoResponse {
    let result = state
        .health_checker
        .check_readiness(state.engine.as_deref())
        .await;

    let status = if result.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(result))
}

/// GET /health/startup - Startup probe
///
/// Kubernetes startup probe endpoint.
/// Returns 200 if startup complete, 503 if still initializing.
async fn health_startup_handler(State(state): State<ObservabilityState>) -> impl IntoResponse {
    let result = state
        .health_checker
        .check_startup(state.engine.as_deref())
        .await;

    let status = if result.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let health_checker = Arc::new(HealthChecker::new());
        let router = create_observability_router(health_checker, None);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_liveness_endpoint() {
        let health_checker = Arc::new(HealthChecker::new());
        let router = create_observability_router(health_checker, None);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_startup_endpoint() {
        let health_checker = Arc::new(HealthChecker::new());
        let router = create_observability_router(health_checker.clone(), None);

        // Before startup complete
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/startup")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // Mark startup complete
        health_checker.mark_startup_complete().await;

        // After startup complete
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health/startup")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
