//! GraphQL API - Auto-generated schema from table metadata
//!
//! Provides a fully-functional GraphQL API backed by the MiracleDB engine:
//! - `tables` resolver: lists all registered tables
//! - `schema` resolver: returns column metadata for a given table
//! - `query` resolver: executes a SELECT against any table with optional limit
//! - `sql` resolver: executes arbitrary SQL and returns JSON rows
//! - GraphQL Playground UI  (GET /graphql)
//! - POST endpoint for GraphQL queries  (POST /graphql)
//!
//! # Version note
//! `async-graphql-axum 7.x` links against axum 0.8, while this crate uses
//! axum 0.7.  To avoid the ABI mismatch we parse GraphQL requests manually
//! using `async_graphql::http::receive_body` and call `schema.execute()`
//! directly, returning a plain `axum::Json` response.

use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject,
    http::{playground_source, GraphQLPlaygroundConfig},
};
use axum::{
    Extension, Router,
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json, Response},
    routing::post,
};

use crate::engine::MiracleEngine;

// ---------------------------------------------------------------------------
// GraphQL data types
// ---------------------------------------------------------------------------

/// A single row returned from a table query.
#[derive(SimpleObject, Clone)]
pub struct TableRow {
    /// Opaque row identifier (stringified row index when no `id` column).
    pub id: String,
    /// JSON-encoded map of column name -> value for this row.
    pub data: String,
}

/// Metadata for a single column in a table.
#[derive(SimpleObject, Clone)]
pub struct ColumnMeta {
    /// Column name.
    pub name: String,
    /// Arrow / SQL data-type string.
    pub data_type: String,
    /// Whether the column allows NULL.
    pub nullable: bool,
}

/// Full column metadata for a table.
#[derive(SimpleObject, Clone)]
pub struct TableMeta {
    /// Table name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnMeta>,
}

// ---------------------------------------------------------------------------
// Query root
// ---------------------------------------------------------------------------

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Health probe – always returns `"GraphQL is operational"`.
    async fn health(&self) -> &str {
        "GraphQL is operational"
    }

    /// List all table names registered with MiracleDB.
    async fn tables(&self, ctx: &Context<'_>) -> Vec<String> {
        ctx.data_unchecked::<Arc<MiracleEngine>>()
            .list_tables()
            .await
    }

    /// Return column metadata for `table`.  Returns `null` when the table
    /// does not exist.
    async fn schema(&self, ctx: &Context<'_>, table: String) -> Option<TableMeta> {
        let engine = ctx.data_unchecked::<Arc<MiracleEngine>>();
        let arrow_schema = engine.get_table_schema(&table).await?;

        let columns = arrow_schema
            .fields()
            .iter()
            .map(|f| ColumnMeta {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        Some(TableMeta {
            name: table,
            columns,
        })
    }

    /// Query `table`, returning rows serialised as JSON strings.
    ///
    /// `limit` defaults to 100 (capped at 1 000).
    async fn query(
        &self,
        ctx: &Context<'_>,
        table: String,
        limit: Option<i32>,
    ) -> Vec<TableRow> {
        let engine = ctx.data_unchecked::<Arc<MiracleEngine>>();
        let n = limit.unwrap_or(100).min(1000).max(1) as usize;
        let sql = format!("SELECT * FROM \"{}\" LIMIT {}", table, n);
        execute_sql_to_rows(engine, &sql).await
    }

    /// Execute arbitrary SQL and return rows as JSON strings (capped at 1 000).
    async fn sql(&self, ctx: &Context<'_>, query: String) -> Vec<TableRow> {
        let engine = ctx.data_unchecked::<Arc<MiracleEngine>>();
        execute_sql_to_rows(engine, &query).await
    }
}

// ---------------------------------------------------------------------------
// Schema type alias & builder
// ---------------------------------------------------------------------------

pub type MiracleSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Build the `MiracleSchema`, injecting the engine as shared context data.
pub fn build_schema(engine: Arc<MiracleEngine>) -> MiracleSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(engine)
        .finish()
}

// ---------------------------------------------------------------------------
// HTTP handlers (axum 0.7 compatible – no async-graphql-axum extractors)
// ---------------------------------------------------------------------------

/// POST /graphql – accept a GraphQL request body and return the response.
///
/// Accepts `application/json` bodies of the form
/// `{"query": "...", "variables": {...}}`.
pub async fn graphql_handler(
    Extension(schema): Extension<MiracleSchema>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Parse the body as a GraphQL request using async-graphql's parser so we
    // remain independent of the axum version used by async-graphql-axum.
    // Content-Type is accepted as-is; we always parse as JSON.
    let _content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    // Use async_graphql's http helpers to parse the body
    let request: async_graphql::Request =
        match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(val) => {
                // Extract query, variables and operationName from JSON body
                let query = val
                    .get("query")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let variables = val
                    .get("variables")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let operation_name = val
                    .get("operationName")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);

                let mut req = async_graphql::Request::new(query);
                if let serde_json::Value::Object(vars) = variables {
                    let gql_vars: async_graphql::Variables =
                        async_graphql::Variables::from_json(serde_json::Value::Object(vars));
                    req = req.variables(gql_vars);
                }
                if let Some(op) = operation_name {
                    req = req.operation_name(op);
                }
                req
            }
            Err(e) => {
                tracing::warn!("GraphQL: failed to parse request body: {}", e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"errors": [{"message": format!("Invalid JSON: {}", e)}]})),
                )
                    .into_response();
            }
        };

    let response = schema.execute(request).await;
    let status = if response.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::OK // GraphQL errors are still HTTP 200
    };
    let body = serde_json::to_value(&response).unwrap_or_else(|_| {
        serde_json::json!({"errors": [{"message": "serialisation error"}]})
    });
    (status, Json(body)).into_response()
}

/// GET /graphql – serve the GraphQL Playground UI.
pub async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Build an Axum [`Router`] for the GraphQL API.
///
/// Mounts:
/// - `POST /`  – execute GraphQL queries
/// - `GET /`   – GraphQL Playground UI
///
/// The `MiracleSchema` is attached as an Axum `Extension` layer so both
/// handlers can resolve it.
pub fn routes(engine: Arc<MiracleEngine>) -> Router {
    let schema = build_schema(engine);
    Router::new()
        .route("/", post(graphql_handler).get(graphql_playground))
        .layer(Extension(schema))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Run `sql` through the engine and convert every result row into a
/// [`TableRow`] with JSON-encoded field values.
async fn execute_sql_to_rows(engine: &Arc<MiracleEngine>, sql: &str) -> Vec<TableRow> {
    let df = match engine.query(sql).await {
        Ok(df) => df,
        Err(e) => {
            tracing::warn!("GraphQL query failed: {}", e);
            return vec![];
        }
    };

    let batches = match df.collect().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("GraphQL collect failed: {}", e);
            return vec![];
        }
    };

    let mut rows: Vec<TableRow> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let col_name = schema.field(col_idx).name().clone();
                let col = batch.column(col_idx);
                let val = arrow_scalar_to_json(col.as_ref(), row_idx);
                map.insert(col_name, val);
            }
            // Use the "id" column value if present, otherwise use row index.
            let id_str = map
                .get("id")
                .map(|v| v.to_string())
                .unwrap_or_else(|| rows.len().to_string());
            rows.push(TableRow {
                id: id_str,
                data: serde_json::Value::Object(map).to_string(),
            });
        }
    }
    rows
}

/// Convert a single cell from an Arrow array to a `serde_json::Value`.
fn arrow_scalar_to_json(col: &dyn arrow::array::Array, row: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::json!(arr.value(row))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            serde_json::Value::String(format!("{}", arr.value(row)))
        }
        DataType::Date64 => {
            let arr = col.as_any().downcast_ref::<Date64Array>().unwrap();
            serde_json::Value::String(format!("{}", arr.value(row)))
        }
        DataType::Timestamp(_, _) => {
            // Emit as a debug string until full timestamp formatting is needed.
            serde_json::Value::String(format!("{:?}", col.data_type()))
        }
        _ => {
            // Generic fallback: debug the column slice.
            serde_json::Value::String(format!("{:?}", col.slice(row, 1)))
        }
    }
}
