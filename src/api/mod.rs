//! API Layer - REST, GraphQL, WebSocket, and SSE
//!
//! Provides comprehensive API access to MiracleDb

pub mod rest;
pub mod graphql;
pub mod websocket;
pub mod sse;
pub mod mcp;
pub mod middleware;
pub mod dashboard;
pub mod dashboard_v2;
pub mod nucleus;
pub mod docs_api;
pub mod auto;
pub mod udf;
#[cfg(feature = "ml")]
pub mod onnx;
#[cfg(feature = "nlp")]
pub mod ml;
pub mod spatial;
pub mod backup;
pub mod vector_index;
pub mod fulltext_index;
pub mod bulk_index;
pub mod notifications;

use std::sync::Arc;
use axum::{Router, routing::{get, post}, Extension};
use tower_http::cors::{CorsLayer, Any};
use tower_http::trace::TraceLayer;
use tracing::info;
use crate::graph::GraphDb;
use crate::engine::MiracleEngine;
use crate::nucleus::NucleusSystem;
use crate::logs::LogEngine;
use crate::vector::VectorIndexManager;
use crate::fulltext::FullTextIndexManager;

/// Create the main API router
pub fn router(
    engine: Arc<MiracleEngine>,
    sse_manager: Arc<sse::SSEManager>,
    nucleus_system: Arc<NucleusSystem>,
    graph_db: Arc<GraphDb>,
    rlm_agent: Arc<crate::rlm::RecursiveAgent>,
    security_mgr: Arc<crate::security::SecurityManager>,
    log_engine: Option<Arc<LogEngine>>,
    backup_api_state: Option<backup::BackupApiState>,
    vector_manager: Arc<VectorIndexManager>,
    fulltext_manager: Arc<FullTextIndexManager>,
    notification_manager: Arc<crate::notification::NotificationManager>,
) -> Router {
    let rate_limiter = Arc::new(middleware::RateLimiter::new(100, 60)); // 100 req per minute
    let dashboard_state = Arc::new(dashboard::DashboardState::new());

    // Initialize auto-generation registry
    let auto_registry = Arc::new(auto::ApiRegistry::new(engine.clone()));
    let auto_router = auto_registry.create_router();

    // Discover and register all tables at startup
    tokio::spawn(async move {
        if let Err(e) = auto_registry.discover_and_register_all().await {
            tracing::warn!("Failed to auto-register tables: {}", e);
        }
    });

    let mut app = Router::new()
        // Health check (Public, no rate limit?) - actually usually good to rate limit too, but skip auth
        .route("/health", get(health_check))

        // Web SQL IDE
        .route("/ide", get(sql_ide_handler))

        // RLM Agent
        .route("/api/v1/agent/run", post(agent_run_handler))

        // REST API
        .nest("/api/v1", rest::routes())

        // Spatial Index API
        .nest("/api/v1/spatial", spatial::routes())

        // Auto-Generated Table APIs (separate namespace to avoid conflicts)
        .nest("/api/v1/auto", auto_router)

        // WASM UDF Management
        .nest("/api/v1/udf", udf::udf_router(engine.clone()));

    // ML Model Management (conditional)
    #[cfg(feature = "nlp")]
    {
        app = app.nest("/api/v1/ml", ml::routes(engine.clone()));
    }

    // Backup & Recovery API (conditional)
    if let Some(backup_state) = backup_api_state {
        app = app.nest("/api/v1", backup::routes(backup_state));
    }

    // Vector Index Management API
    let vector_state = vector_index::VectorIndexState::new(vector_manager.clone());
    app = app.nest("/api/v1/vector/index", vector_index::routes(vector_state));

    // Full-Text Index Management API
    let fulltext_state = fulltext_index::FullTextIndexState::new(fulltext_manager.clone());
    app = app.nest("/api/v1/fulltext/index", fulltext_index::routes(fulltext_state));

    // Bulk Indexing API
    let bulk_state = bulk_index::BulkIndexState::new(
        engine.clone(),
        vector_manager.clone(),
        fulltext_manager.clone(),
    );
    app = app.nest("/api/v1/bulk", bulk_index::routes(bulk_state));

    // Notification API
    let notification_state = notifications::NotificationApiState::new(notification_manager);
    app = app.nest("/api/v1/notifications", notifications::routes(notification_state));

    app
        // Documentation
        .route("/api/docs/openapi.json", get(docs_api::openapi_json))
        .route("/api/docs", get(docs_api::swagger_ui))
        
        // CLI Support
        
        // CLI Support
        .route("/api/v1/sql", post(sql_handler))
        .route("/api/v1/metadata", get(metadata_handler))
        
        // GraphQL
        .nest("/graphql", graphql::routes(engine.clone()))
        
        // WebSocket for CDC
        .route("/ws/events", get(websocket::handler))
        
        // Server-Sent Events
        .route("/events/stream", get(sse::handler))
        
        // Model Context Protocol
        .nest("/mcp", mcp::routes())
        
        // Admin Dashboard
        .nest("/admin", dashboard::routes(dashboard_state.clone()))
        .nest("/admin/v2", dashboard_v2::routes(
            dashboard_state, 
            nucleus_system.clone(),
            graph_db,
            log_engine,
        ))
        
        // Nucleus Reactive Data API
        .nest("/nucleus", nucleus::routes(nucleus_system))
        .layer(axum::middleware::from_fn(middleware::auth_middleware))

        // Timeout Layer (30s per request - prevents runaway queries)
        .layer(axum::middleware::from_fn(middleware::timeout_middleware))

        // Rate Limit Layer (Applied to all above)
        .layer(axum::middleware::from_fn(middleware::rate_limit_middleware))

        // Request ID tracking (for debugging and audit)
        .layer(axum::middleware::from_fn(middleware::request_id_middleware))
        
        // Metrics (Public)
        .route("/metrics", get(metrics_handler))
        
        // Global Extensions
        .layer(Extension(engine))
        .layer(Extension(sse_manager))
        .layer(Extension(rlm_agent))
        .layer(Extension(security_mgr))
        .layer(Extension(rate_limiter)) // Provide the rate limiter instance for extraction
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
        )
}

async fn health_check() -> &'static str {
    "OK"
}

async fn sql_ide_handler() -> axum::response::Response<axum::body::Body> {
    use axum::response::IntoResponse;
    use axum::http::header;
    let html = include_str!("sql_ide.html");
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
}

async fn metrics_handler() -> String {
    // Prometheus-format metrics
    let mut output = String::new();
    output.push_str("# HELP miracledb_up MiracleDb is running\n");
    output.push_str("# TYPE miracledb_up gauge\n");
    output.push_str("miracledb_up 1\n");
    output
}


#[derive(serde::Deserialize)]
struct AgentRunRequest {
    goal: String,
}

async fn agent_run_handler(
    Extension(agent): Extension<Arc<crate::rlm::RecursiveAgent>>,
    axum::Json(req): axum::Json<AgentRunRequest>,
) -> axum::Json<serde_json::Value> {
    let result = agent.run(&req.goal).await;
    axum::Json(serde_json::json!({
        "status": "success",
        "result": result
    }))
}

// --- CLI Support Endpoints ---

#[derive(serde::Deserialize)]
pub struct SqlRequest {
    pub query: String,
}

#[derive(serde::Serialize)]
pub struct SqlResponse {
    pub status: String,
    pub data: Option<Vec<serde_json::Value>>,
    pub error: Option<String>,
}

async fn sql_handler(
    Extension(engine): Extension<Arc<MiracleEngine>>,
    Extension(security_mgr): Extension<Arc<crate::security::SecurityManager>>,
    claims: Option<Extension<middleware::Claims>>,
    axum::Json(req): axum::Json<SqlRequest>,
) -> axum::Json<SqlResponse> {
    info!("Executing SQL via API: {}", req.query);

    // 1. RBAC Check
    let user_id = claims.map(|Extension(c)| c.sub.clone()).unwrap_or_else(|| "anonymous".to_string());
    
    // Check permission - "database" resource, Execute action
    if !security_mgr.check_access(&user_id, "database", crate::security::Action::Execute).await {
         return axum::Json(SqlResponse {
            status: "error".to_string(),
            data: None,
            error: Some("Access denied".to_string()),
        });
    }
    
    // 2. Execution
    match engine.query(&req.query).await {
        Ok(df) => {
             // 3. Format Response
             // Collect results
             match df.collect().await {
                 Ok(batches) => {
                     let mut rows = Vec::new();
                     for batch in batches {
                        for row_idx in 0..batch.num_rows() {
                            let mut row = serde_json::Map::new();
                            let schema = batch.schema();
                            for col_idx in 0..batch.num_columns() {
                                let col_name = schema.field(col_idx).name();
                                let val_str = format!("{:?}", batch.column(col_idx)); 
                                // Simplified value extraction (debug format usually includes surrounding 'StringArray' etc, 
                                // strictly we should map arrow types to json types properly. 
                                // For MVP: Using debug format is readable enough for CLI but not perfect JSON.)
                                row.insert(col_name.clone(), serde_json::Value::String(val_str));
                            }
                            rows.push(serde_json::Value::Object(row));
                        }
                     }
                     
                     axum::Json(SqlResponse {
                        status: "success".to_string(),
                        data: Some(rows),
                        error: None,
                    })
                 }
                 Err(e) => {
                     axum::Json(SqlResponse {
                        status: "error".to_string(),
                        data: None,
                        error: Some(format!("Execution failed: {}", e)),
                    })
                 }
             }
        }
        Err(e) => {
            axum::Json(SqlResponse {
                status: "error".to_string(),
                data: None,
                error: Some(format!("Query error: {}", e)),
            })
        }
    }
}

#[derive(serde::Serialize)]
pub struct MetadataResponse {
    pub tables: Vec<String>,
    pub columns: Vec<String>, // Simplified for now
}

async fn metadata_handler(
    Extension(engine): Extension<Arc<MiracleEngine>>,
) -> axum::Json<MetadataResponse> {
    // Get real tables from engine
    let tables = engine.list_tables().await;
    
    // Collect columns from all tables (for autocompletion)
    let mut all_columns = std::collections::HashSet::new();
    for table_name in &tables {
        // Get schema from table provider to extract column names
        if let Some(schema) = engine.get_table_schema(table_name).await {
            for field in schema.fields() {
                all_columns.insert(field.name().clone());
            }
        }
    }
    
    // If no columns found, provide common defaults for CLI usability
    let columns: Vec<String> = if all_columns.is_empty() {
        vec!["id".to_string(), "name".to_string(), "created_at".to_string(), "status".to_string()]
    } else {
        all_columns.into_iter().collect()
    };
    
    axum::Json(MetadataResponse {
        tables,
        columns,
    })
}
