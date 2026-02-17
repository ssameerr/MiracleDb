//! API Registry - Manages auto-generated endpoints for tables
//!
//! The registry:
//! - Discovers table schemas from the database
//! - Generates CRUD and search routes dynamically
//! - Updates OpenAPI documentation automatically
//! - Maintains a mapping of tables to their generated APIs

use std::collections::HashMap;
use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post, put, delete, patch},
    extract::{Path, Query, State},
    Json,
    http::StatusCode,
};
use serde_json::{Value, json};
use tokio::sync::RwLock;

use super::schema::{TableSchema, discover_table_schema};
use super::crud::{CrudHandlers, PaginationParams, SortParams, FilterParams, ErrorResponse};
use super::search::{SearchRequest, SearchResponse, handle_search,
    BatchVectorSearchRequest, BatchVectorSearchResponse, SearchHit};
use crate::engine::MiracleEngine;

/// State passed to router handlers
#[derive(Clone)]
pub struct RouterState {
    pub tables: Arc<RwLock<HashMap<String, Arc<TableSchema>>>>,
    pub engine: Arc<MiracleEngine>,
}

/// Global registry of auto-generated APIs
pub struct ApiRegistry {
    /// Map of table names to their schemas
    tables: Arc<RwLock<HashMap<String, Arc<TableSchema>>>>,
    /// Database engine reference
    engine: Arc<MiracleEngine>,
}

impl ApiRegistry {
    /// Create a new API registry
    pub fn new(engine: Arc<MiracleEngine>) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            engine,
        }
    }

    /// Register a table and generate its API endpoints
    pub async fn register_table(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Discover table schema from database
        let schema = discover_table_schema(table_name, &self.engine).await?;
        let schema_arc = Arc::new(schema);

        // Store in registry
        let mut tables = self.tables.write().await;
        tables.insert(table_name.to_string(), schema_arc.clone());

        tracing::info!("Registered table '{}' with auto-generated API", table_name);

        Ok(())
    }

    /// Unregister a table (when table is dropped)
    pub async fn unregister_table(&self, table_name: &str) {
        let mut tables = self.tables.write().await;
        tables.remove(table_name);
        tracing::info!("Unregistered table '{}' from API registry", table_name);
    }

    /// Get schema for a table
    pub async fn get_schema(&self, table_name: &str) -> Option<Arc<TableSchema>> {
        let tables = self.tables.read().await;
        tables.get(table_name).cloned()
    }

    /// List all registered tables
    pub async fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().await;
        tables.keys().cloned().collect()
    }

    /// Discover and register all tables from the database
    /// This should be called during server initialization
    pub async fn discover_and_register_all(&self) -> Result<usize, Box<dyn std::error::Error>> {
        tracing::info!("Discovering tables from database...");

        // Query actual database for table list
        let discovered_tables = self.engine.list_tables().await;

        tracing::info!("Found {} tables in database", discovered_tables.len());

        let mut registered_count = 0;
        for table_name in discovered_tables {
            match self.register_table(&table_name).await {
                Ok(_) => {
                    registered_count += 1;
                    tracing::info!("Auto-registered table: {}", table_name);
                }
                Err(e) => {
                    tracing::warn!("Failed to register table {}: {}", table_name, e);
                }
            }
        }

        tracing::info!("Auto-registered {} tables", registered_count);
        Ok(registered_count)
    }

    /// Generate Axum router with all auto-generated endpoints
    pub fn create_router(&self) -> Router {
        let state = RouterState {
            tables: self.tables.clone(),
            engine: self.engine.clone(),
        };

        Router::new()
            // Dynamic table routes - /:table pattern
            .route("/tables/:table", get(list_handler))
            .route("/tables/:table", post(create_handler))
            .route("/tables/:table/:id", get(get_by_id_handler))
            .route("/tables/:table/:id", put(update_handler))
            .route("/tables/:table/:id", patch(patch_handler))
            .route("/tables/:table/:id", delete(delete_handler))
            .route("/tables/:table/search", post(search_handler))
            .route("/tables/:table/search/batch", post(batch_vector_search_handler))
            .route("/tables/:table/bulk", post(bulk_create_handler))
            .route("/tables/:table/bulk", put(bulk_update_handler))
            .route("/tables/:table/bulk", delete(bulk_delete_handler))
            // Metadata routes
            .route("/tables", get(list_tables_handler))
            .route("/tables/:table/schema", get(get_schema_handler))
            .with_state(state)
    }

    /// Generate OpenAPI documentation for all registered tables
    pub async fn generate_openapi(&self) -> Value {
        let tables = self.tables.read().await;
        let mut paths = serde_json::Map::new();

        for (table_name, schema) in tables.iter() {
            // Generate paths for this table
            let table_paths = generate_table_openapi_paths(table_name, schema);
            for (path, operations) in table_paths {
                paths.insert(path, operations);
            }
        }

        serde_json::json!({
            "openapi": "3.0.0",
            "info": {
                "title": "MiracleDb Auto-Generated API",
                "version": "1.0.0",
                "description": "Automatically generated REST API for database tables"
            },
            "paths": paths
        })
    }
}

// Handler functions that receive table name from path and look up schema

async fn list_handler(
    Path(table): Path<String>,
    pagination: Query<PaginationParams>,
    sort: Query<SortParams>,
    filters: Query<FilterParams>,
    State(state): State<RouterState>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let _schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    // Build and execute SQL query
    let page = pagination.page.max(1);
    let limit = pagination.limit.min(100).max(1);
    let offset = (page - 1) * limit;

    // Build SELECT query
    let mut sql = format!("SELECT * FROM {}", table);

    // Add ORDER BY if specified
    if let Some(ref sort_field) = sort.sort_by {
        let order = &sort.order;
        sql.push_str(&format!(" ORDER BY {} {}", sort_field, order));
    }

    // Add LIMIT and OFFSET
    sql.push_str(&format!(" LIMIT {} OFFSET {}", limit, offset));

    tracing::debug!("Executing query: {}", sql);

    // Execute query
    let result = state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Query execution failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    // Collect results
    let batches = result.collect().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to collect results".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    // Convert Arrow batches to JSON
    let mut rows = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let col_name = schema.field(col_idx).name().clone();
                // Simple string conversion for now
                let val_str = format!("{:?}", batch.column(col_idx));
                row.insert(col_name, Value::String(val_str));
            }
            rows.push(Value::Object(row));
        }
    }

    let total = rows.len() as u32;

    Ok(Json(json!({
        "data": rows,
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total as f32 / limit as f32).ceil() as u32,
    })))
}

async fn get_by_id_handler(
    Path((table, id)): Path<(String, String)>,
    State(state): State<RouterState>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    // Find primary key column (default to 'id')
    let pk_column = schema.primary_key.first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    // Build SELECT query with WHERE clause
    let sql = format!("SELECT * FROM {} WHERE {} = {}", table, pk_column, id);

    tracing::debug!("Executing query: {}", sql);

    // Execute query
    let result = state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Query execution failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    // Collect results
    let batches = result.collect().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to collect results".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    // Convert Arrow batches to JSON
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Record with id '{}' not found", id),
                details: None,
            }),
        ));
    }

    let batch = &batches[0];
    let schema_ref = batch.schema();
    let mut row = serde_json::Map::new();

    for col_idx in 0..batch.num_columns() {
        let col_name = schema_ref.field(col_idx).name().clone();
        let val_str = format!("{:?}", batch.column(col_idx));
        row.insert(col_name, Value::String(val_str));
    }

    Ok(Json(json!({
        "data": Value::Object(row)
    })))
}

async fn create_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
    Json(data): Json<Value>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let _schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    // Extract columns and values from JSON
    let obj = data.as_object().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Request body must be a JSON object".to_string(),
                details: None,
            }),
        )
    })?;

    let mut columns = Vec::new();
    let mut values = Vec::new();

    for (key, value) in obj {
        columns.push(key.clone());
        // Format value based on type
        let formatted_value = match value {
            Value::String(s) => format!("'{}'", s.replace("'", "''")),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            _ => format!("'{}'", value.to_string().replace("'", "''")),
        };
        values.push(formatted_value);
    }

    // Build INSERT statement
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        columns.join(", "),
        values.join(", ")
    );

    tracing::debug!("Executing query: {}", sql);

    // Execute insert
    state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Insert failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "data": data,
            "message": "Record created successfully"
        })),
    ))
}

async fn update_handler(
    Path((table, id)): Path<(String, String)>,
    State(state): State<RouterState>,
    Json(data): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    // Extract columns and values from JSON
    let obj = data.as_object().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Request body must be a JSON object".to_string(),
                details: None,
            }),
        )
    })?;

    let mut set_clauses = Vec::new();

    for (key, value) in obj {
        // Format value based on type
        let formatted_value = match value {
            Value::String(s) => format!("'{}'", s.replace("'", "''")),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            _ => format!("'{}'", value.to_string().replace("'", "''")),
        };
        set_clauses.push(format!("{} = {}", key, formatted_value));
    }

    // Find primary key column
    let pk_column = schema.primary_key.first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    // Build UPDATE statement
    let sql = format!(
        "UPDATE {} SET {} WHERE {} = {}",
        table,
        set_clauses.join(", "),
        pk_column,
        id
    );

    tracing::debug!("Executing query: {}", sql);

    // Execute update
    state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Update failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    Ok(Json(json!({
        "data": data,
        "id": id,
        "message": "Record updated successfully"
    })))
}

async fn patch_handler(
    Path((table, id)): Path<(String, String)>,
    State(state): State<RouterState>,
    Json(data): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    // Extract columns and values from JSON (same as update)
    let obj = data.as_object().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Request body must be a JSON object".to_string(),
                details: None,
            }),
        )
    })?;

    if obj.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "No fields to update".to_string(),
                details: None,
            }),
        ));
    }

    let mut set_clauses = Vec::new();

    for (key, value) in obj {
        let formatted_value = match value {
            Value::String(s) => format!("'{}'", s.replace("'", "''")),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            _ => format!("'{}'", value.to_string().replace("'", "''")),
        };
        set_clauses.push(format!("{} = {}", key, formatted_value));
    }

    // Find primary key column
    let pk_column = schema.primary_key.first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    // Build UPDATE statement (PATCH is partial update, same as UPDATE)
    let sql = format!(
        "UPDATE {} SET {} WHERE {} = {}",
        table,
        set_clauses.join(", "),
        pk_column,
        id
    );

    tracing::debug!("Executing query: {}", sql);

    // Execute update
    state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Patch failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    Ok(Json(json!({
        "data": data,
        "id": id,
        "message": "Record partially updated successfully"
    })))
}

async fn delete_handler(
    Path((table, id)): Path<(String, String)>,
    State(state): State<RouterState>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    // Find primary key column
    let pk_column = schema.primary_key.first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    // Build DELETE statement
    let sql = format!("DELETE FROM {} WHERE {} = {}", table, pk_column, id);

    tracing::debug!("Executing query: {}", sql);

    // Execute delete
    state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Delete failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    Ok(StatusCode::NO_CONTENT)
}

async fn search_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    handle_search(schema.clone(), state.engine.clone(), Json(request)).await
}

async fn bulk_create_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
    Json(data): Json<Vec<Value>>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let _schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    if data.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Empty data array".to_string(),
                details: None,
            }),
        ));
    }

    // Build multiple INSERT statements
    let mut insert_count = 0;
    for record in &data {
        let obj = record.as_object().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Each record must be a JSON object".to_string(),
                    details: None,
                }),
            )
        })?;

        let mut columns = Vec::new();
        let mut values = Vec::new();

        for (key, value) in obj {
            columns.push(key.clone());
            let formatted_value = match value {
                Value::String(s) => format!("'{}'", s.replace("'", "''")),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => format!("'{}'", value.to_string().replace("'", "''")),
            };
            values.push(formatted_value);
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table,
            columns.join(", "),
            values.join(", ")
        );

        tracing::debug!("Executing bulk insert: {}", sql);

        state.engine.query(&sql).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Bulk insert failed at record {}", insert_count + 1),
                    details: Some(e.to_string()),
                }),
            )
        })?;

        insert_count += 1;
    }

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "created": insert_count,
            "total": data.len(),
            "message": format!("Successfully created {} records", insert_count)
        })),
    ))
}

async fn bulk_update_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
    Json(data): Json<Vec<Value>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    if data.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Empty data array".to_string(),
                details: None,
            }),
        ));
    }

    let pk_column = schema.primary_key.first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    let mut update_count = 0;
    for record in &data {
        let obj = record.as_object().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Each record must be a JSON object".to_string(),
                    details: None,
                }),
            )
        })?;

        // Extract ID from the record
        let id_value = obj.get(pk_column).ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Record missing primary key '{}'", pk_column),
                    details: None,
                }),
            )
        })?;

        let id_str = match id_value {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => id_value.to_string(),
        };

        let mut set_clauses = Vec::new();
        for (key, value) in obj {
            if key == pk_column {
                continue; // Skip primary key in SET clause
            }
            let formatted_value = match value {
                Value::String(s) => format!("'{}'", s.replace("'", "''")),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => format!("'{}'", value.to_string().replace("'", "''")),
            };
            set_clauses.push(format!("{} = {}", key, formatted_value));
        }

        if set_clauses.is_empty() {
            continue; // Skip if no fields to update
        }

        let sql = format!(
            "UPDATE {} SET {} WHERE {} = {}",
            table,
            set_clauses.join(", "),
            pk_column,
            id_str
        );

        tracing::debug!("Executing bulk update: {}", sql);

        state.engine.query(&sql).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Bulk update failed at record {}", update_count + 1),
                    details: Some(e.to_string()),
                }),
            )
        })?;

        update_count += 1;
    }

    Ok(Json(json!({
        "updated": update_count,
        "total": data.len(),
        "message": format!("Successfully updated {} records", update_count)
    })))
}

async fn bulk_delete_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
    Json(ids): Json<Vec<String>>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    if ids.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Empty IDs array".to_string(),
                details: None,
            }),
        ));
    }

    let pk_column = schema.primary_key.first()
        .map(|s| s.as_str())
        .unwrap_or("id");

    // Build DELETE with IN clause
    let id_list = ids.iter()
        .map(|id| format!("'{}'", id.replace("'", "''")))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "DELETE FROM {} WHERE {} IN ({})",
        table,
        pk_column,
        id_list
    );

    tracing::debug!("Executing bulk delete: {}", sql);

    state.engine.query(&sql).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Bulk delete failed".to_string(),
                details: Some(e.to_string()),
            }),
        )
    })?;

    Ok(Json(json!({
        "deleted": ids.len(),
        "message": format!("Successfully deleted {} records", ids.len())
    })))
}

/// Batch vector search handler
///
/// POST /api/v1/auto/tables/:table/search/batch
///
/// Accepts multiple query vectors and returns k-NN results for each query
/// simultaneously, enabling efficient batched similarity search.
async fn batch_vector_search_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
    Json(req): Json<BatchVectorSearchRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    if req.queries.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "queries array must not be empty".to_string(),
                details: None,
            }),
        ).into_response();
    }

    if req.k == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "k must be greater than 0".to_string(),
                details: None,
            }),
        ).into_response();
    }

    let vector_mgr = crate::vector::VectorIndexManager::new("./data/vectors");

    // Verify the dataset exists before processing the batch
    if !vector_mgr.dataset_exists(&table).await {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Vector dataset for table '{}' not found", table),
                details: Some(format!(
                    "Please ensure Lance dataset exists at ./data/vectors/{}",
                    table
                )),
            }),
        ).into_response();
    }

    let mut all_results: Vec<Vec<SearchHit>> = Vec::with_capacity(req.queries.len());

    for (query_idx, query_vec) in req.queries.iter().enumerate() {
        match vector_mgr.search(&table, &req.field, query_vec, req.k, None).await {
            Ok(hits) => {
                let search_hits: Vec<SearchHit> = hits
                    .into_iter()
                    .map(|h| SearchHit {
                        id: h.id,
                        // Prefer score if present, fall back to distance, default to 0.0
                        score: h.score.or(h.distance).unwrap_or(0.0),
                    })
                    .collect();
                all_results.push(search_hits);
            }
            Err(e) => {
                tracing::error!(
                    "Batch vector search failed for query index {} on {}.{}: {}",
                    query_idx, table, req.field, e
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "Vector search failed",
                        "details": e.to_string(),
                        "query_index": query_idx,
                    })),
                ).into_response();
            }
        }
    }

    tracing::info!(
        "Batch vector search on {}.{}: {} queries, k={}, total hits={}",
        table,
        req.field,
        req.queries.len(),
        req.k,
        all_results.iter().map(|r| r.len()).sum::<usize>(),
    );

    Json(BatchVectorSearchResponse { results: all_results }).into_response()
}

// Metadata handlers

async fn list_tables_handler(
    State(state): State<RouterState>,
) -> Json<Value> {
    let tables = state.tables.read().await;
    let table_list: Vec<String> = tables.keys().cloned().collect();
    Json(serde_json::json!({
        "tables": table_list,
        "count": table_list.len()
    }))
}

async fn get_schema_handler(
    Path(table): Path<String>,
    State(state): State<RouterState>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tables = state.tables.read().await;
    let schema = tables.get(&table).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Table '{}' not found", table),
                details: None,
            }),
        )
    })?;

    Ok(Json(serde_json::to_value(&**schema).unwrap_or_default()))
}

// Helper function to generate OpenAPI paths for a table
fn generate_table_openapi_paths(table_name: &str, schema: &TableSchema) -> Vec<(String, Value)> {
    let mut paths = Vec::new();

    // List/Create endpoint
    paths.push((
        format!("/api/v1/tables/{}", table_name),
        serde_json::json!({
            "get": {
                "summary": format!("List {} records", table_name),
                "tags": [table_name],
                "parameters": [
                    {"name": "page", "in": "query", "schema": {"type": "integer"}},
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {"name": "sort", "in": "query", "schema": {"type": "string"}},
                ],
                "responses": {
                    "200": {
                        "description": "List of records",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {"type": "array"},
                                        "total": {"type": "integer"},
                                        "page": {"type": "integer"},
                                        "limit": {"type": "integer"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "post": {
                "summary": format!("Create {} record", table_name),
                "tags": [table_name],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": schema.to_openapi_schema()
                        }
                    }
                },
                "responses": {
                    "201": {"description": "Record created"}
                }
            }
        }),
    ));

    // Get/Update/Delete by ID endpoint
    paths.push((
        format!("/api/v1/tables/{}/{{id}}", table_name),
        serde_json::json!({
            "get": {
                "summary": format!("Get {} by ID", table_name),
                "tags": [table_name],
                "parameters": [
                    {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}
                ],
                "responses": {
                    "200": {"description": "Record details"}
                }
            },
            "put": {
                "summary": format!("Update {} record", table_name),
                "tags": [table_name],
                "parameters": [
                    {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}
                ],
                "responses": {
                    "200": {"description": "Record updated"}
                }
            },
            "delete": {
                "summary": format!("Delete {} record", table_name),
                "tags": [table_name],
                "parameters": [
                    {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}}
                ],
                "responses": {
                    "204": {"description": "Record deleted"}
                }
            }
        }),
    ));

    // Search endpoint
    paths.push((
        format!("/api/v1/tables/{}/search", table_name),
        serde_json::json!({
            "post": {
                "summary": format!("Search {} records", table_name),
                "tags": [table_name],
                "description": "Multi-type search with vector, text, semantic, and other search capabilities",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "searchTypes": {"type": "array"},
                                    "combine": {"type": "string", "enum": ["union", "intersection"]},
                                    "pagination": {"type": "object"},
                                    "sort": {"type": "array"}
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Search results",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "results": {"type": "array"},
                                        "total": {"type": "integer"},
                                        "took_ms": {"type": "integer"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }),
    ));

    paths
}
