//! Spatial Index REST API
//!
//! Provides HTTP endpoints for creating, querying, and managing R-tree spatial indexes.

use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

use crate::engine::MiracleEngine;
use crate::geospatial::{SpatialIndexConfig, SpatialBounds};

// ==================== Request/Response DTOs ====================

/// Request to create a spatial index
#[derive(Debug, Deserialize)]
pub struct CreateIndexRequest {
    /// Table name
    pub table: String,
    /// X coordinate column name (e.g., "longitude")
    pub column_x: String,
    /// Y coordinate column name (e.g., "latitude")
    pub column_y: String,
    /// Optional configuration
    #[serde(default)]
    pub config: Option<SpatialIndexConfig>,
}

/// Response for successful index creation
#[derive(Debug, Serialize)]
pub struct CreateIndexResponse {
    pub status: String,
    pub table: String,
    pub columns: Vec<String>,
    pub point_count: usize,
    pub index_path: String,
}

/// Request to search spatial index
#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    /// Table name
    pub table: String,
    /// X column name
    pub column_x: String,
    /// Y column name
    pub column_y: String,
    /// Search type: "box" or "nearest"
    pub search_type: String,
    /// Minimum X coordinate (for box search)
    pub min_x: Option<f64>,
    /// Minimum Y coordinate (for box search)
    pub min_y: Option<f64>,
    /// Maximum X coordinate (for box search)
    pub max_x: Option<f64>,
    /// Maximum Y coordinate (for box search)
    pub max_y: Option<f64>,
    /// X coordinate (for nearest neighbor search)
    pub x: Option<f64>,
    /// Y coordinate (for nearest neighbor search)
    pub y: Option<f64>,
    /// Result limit
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Search result entry
#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub row_id: u64,
    pub x: f64,
    pub y: f64,
}

/// Response for search query
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResult>,
    pub count: usize,
    pub execution_time_ms: f64,
}

/// Response for index info query
#[derive(Debug, Serialize)]
pub struct IndexInfoResponse {
    pub table: String,
    pub column_x: String,
    pub column_y: String,
    pub point_count: usize,
    pub bounds: SpatialBounds,
    pub index_path: String,
    pub created_at: String,
}

/// Generic success response
#[derive(Debug, Serialize)]
pub struct SuccessResponse {
    pub status: String,
    pub message: String,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub details: Option<String>,
}

// ==================== API Routes ====================

/// Create router with all spatial index endpoints
pub fn routes() -> Router {
    Router::new()
        .route("/indexes", post(create_index))
        .route("/indexes/:table/:column_x/:column_y", get(get_index_info))
        .route("/indexes/:table/:column_x/:column_y", delete(drop_index))
        .route("/search", post(search_index))
}

// ==================== Handlers ====================

/// Create a new spatial index
///
/// POST /api/v1/spatial/indexes
async fn create_index(
    Extension(engine): Extension<Arc<MiracleEngine>>,
    Json(req): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    info!(
        "Creating spatial index for table '{}' on columns ({}, {})",
        req.table, req.column_x, req.column_y
    );

    let start = std::time::Instant::now();

    // Use provided config or default
    let config = req.config.unwrap_or_default();

    // Get table schema to validate columns exist
    let schema = match engine.get_table_schema(&req.table).await {
        Some(schema) => schema,
        None => {
            error!("Table '{}' not found", req.table);
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("Table '{}' not found", req.table),
                    details: None,
                }),
            )
                .into_response();
        }
    };

    // Validate columns exist in schema
    if !schema.fields().iter().any(|f| f.name() == &req.column_x) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Column '{}' not found in table", req.column_x),
                details: None,
            }),
        )
            .into_response();
    }

    if !schema.fields().iter().any(|f| f.name() == &req.column_y) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Column '{}' not found in table", req.column_y),
                details: None,
            }),
        )
            .into_response();
    }

    // Query table to get all coordinate points
    let sql = format!(
        "SELECT {}, {} FROM {} WHERE {} IS NOT NULL AND {} IS NOT NULL",
        req.column_x, req.column_y, req.table, req.column_x, req.column_y
    );

    let df = match engine.query(&sql).await {
        Ok(df) => df,
        Err(e) => {
            error!("Failed to query table: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to query table for coordinates".to_string(),
                    details: Some(e.to_string()),
                }),
            )
                .into_response();
        }
    };

    let batches = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            error!("Failed to collect results: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to collect coordinate data".to_string(),
                    details: Some(e.to_string()),
                }),
            )
                .into_response();
        }
    };

    // Create empty index first
    match engine
        .spatial_index_manager
        .create_index(&req.table, &req.column_x, &req.column_y, config)
        .await
    {
        Ok(metadata) => {
            // Now bulk insert points
            let mut point_count = 0;
            for (batch_idx, batch) in batches.iter().enumerate() {
                let x_col = batch.column(0);
                let y_col = batch.column(1);

                for row_idx in 0..batch.num_rows() {
                    // Extract x coordinate
                    let x = match x_col.data_type() {
                        arrow::datatypes::DataType::Float64 => {
                            let array = x_col
                                .as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .unwrap();
                            array.value(row_idx)
                        }
                        arrow::datatypes::DataType::Float32 => {
                            let array = x_col
                                .as_any()
                                .downcast_ref::<arrow::array::Float32Array>()
                                .unwrap();
                            array.value(row_idx) as f64
                        }
                        _ => continue, // Skip non-numeric values
                    };

                    // Extract y coordinate
                    let y = match y_col.data_type() {
                        arrow::datatypes::DataType::Float64 => {
                            let array = y_col
                                .as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .unwrap();
                            array.value(row_idx)
                        }
                        arrow::datatypes::DataType::Float32 => {
                            let array = y_col
                                .as_any()
                                .downcast_ref::<arrow::array::Float32Array>()
                                .unwrap();
                            array.value(row_idx) as f64
                        }
                        _ => continue, // Skip non-numeric values
                    };

                    // Generate synthetic row_id from batch and row indices
                    let row_id = (batch_idx * batch.num_rows() + row_idx) as u64;

                    // Insert point into index
                    if let Err(e) = engine.spatial_index_manager.insert_point(
                        &req.table,
                        &req.column_x,
                        &req.column_y,
                        x,
                        y,
                        row_id,
                    ) {
                        error!("Failed to insert point: {}", e);
                        // Continue with other points
                    } else {
                        point_count += 1;
                    }
                }
            }

            if point_count == 0 {
                // Drop the empty index
                let _ = engine
                    .spatial_index_manager
                    .drop_index(&req.table, &req.column_x, &req.column_y);

                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "No valid coordinate points found in table".to_string(),
                        details: Some("Table contains no non-null numeric coordinate pairs".to_string()),
                    }),
                )
                    .into_response();
            }

            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            info!(
                "Created spatial index for '{}' with {} points in {:.2}ms",
                req.table, point_count, elapsed
            );

            (
                StatusCode::CREATED,
                Json(CreateIndexResponse {
                    status: "created".to_string(),
                    table: req.table,
                    columns: vec![req.column_x, req.column_y],
                    point_count,
                    index_path: metadata.index_path.to_string_lossy().to_string(),
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!("Failed to create spatial index: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to create spatial index".to_string(),
                    details: Some(e.to_string()),
                }),
            )
                .into_response()
        }
    }
}

/// Get information about a spatial index
///
/// GET /api/v1/spatial/indexes/:table/:column_x/:column_y
async fn get_index_info(
    Extension(engine): Extension<Arc<MiracleEngine>>,
    Path((table, column_x, column_y)): Path<(String, String, String)>,
) -> impl IntoResponse {
    match engine
        .spatial_index_manager
        .get_metadata(&table, &column_x, &column_y)
    {
        Ok(metadata) => (
            StatusCode::OK,
            Json(IndexInfoResponse {
                table: table.clone(),
                column_x: column_x.clone(),
                column_y: column_y.clone(),
                point_count: metadata.point_count,
                bounds: metadata.bounds.clone(),
                index_path: metadata.index_path.to_string_lossy().to_string(),
                created_at: metadata.created_at.to_rfc3339(),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!(
                    "Spatial index not found for table '{}' on columns ({}, {})",
                    table, column_x, column_y
                ),
                details: None,
            }),
        )
            .into_response(),
    }
}

/// Drop a spatial index
///
/// DELETE /api/v1/spatial/indexes/:table/:column_x/:column_y
async fn drop_index(
    Extension(engine): Extension<Arc<MiracleEngine>>,
    Path((table, column_x, column_y)): Path<(String, String, String)>,
) -> impl IntoResponse {
    info!(
        "Dropping spatial index for table '{}' on columns ({}, {})",
        table, column_x, column_y
    );

    match engine
        .spatial_index_manager
        .drop_index(&table, &column_x, &column_y)
    {
        Ok(()) => {
            info!(
                "Successfully dropped spatial index for '{}' on ({}, {})",
                table, column_x, column_y
            );
            (
                StatusCode::OK,
                Json(SuccessResponse {
                    status: "success".to_string(),
                    message: format!(
                        "Spatial index dropped for table '{}' on columns ({}, {})",
                        table, column_x, column_y
                    ),
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!("Failed to drop spatial index: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to drop spatial index".to_string(),
                    details: Some(e.to_string()),
                }),
            )
                .into_response()
        }
    }
}

/// Search a spatial index
///
/// POST /api/v1/spatial/search
async fn search_index(
    Extension(engine): Extension<Arc<MiracleEngine>>,
    Json(req): Json<SearchRequest>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();

    // Execute search based on type
    let row_ids = match req.search_type.as_str() {
        "box" => {
            let (min_x, min_y, max_x, max_y) = match (req.min_x, req.min_y, req.max_x, req.max_y)
            {
                (Some(min_x), Some(min_y), Some(max_x), Some(max_y)) => {
                    (min_x, min_y, max_x, max_y)
                }
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: "Box search requires min_x, min_y, max_x, max_y".to_string(),
                            details: None,
                        }),
                    )
                        .into_response();
                }
            };

            match engine.spatial_index_manager.search_box(
                &req.table,
                &req.column_x,
                &req.column_y,
                min_x,
                min_y,
                max_x,
                max_y,
            ) {
                Ok(ids) => ids,
                Err(e) => {
                    error!("Search failed: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Search failed".to_string(),
                            details: Some(e.to_string()),
                        }),
                    )
                        .into_response();
                }
            }
        }
        "nearest" => {
            let (x, y) = match (req.x, req.y) {
                (Some(x), Some(y)) => (x, y),
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: "Nearest search requires x and y coordinates".to_string(),
                            details: None,
                        }),
                    )
                        .into_response();
                }
            };

            match engine.spatial_index_manager.nearest_neighbor(
                &req.table,
                &req.column_x,
                &req.column_y,
                x,
                y,
            ) {
                Ok(Some(id)) => vec![id],
                Ok(None) => vec![],
                Err(e) => {
                    error!("Nearest neighbor search failed: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Nearest neighbor search failed".to_string(),
                            details: Some(e.to_string()),
                        }),
                    )
                        .into_response();
                }
            }
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid search type: '{}'", req.search_type),
                    details: Some("Valid types are 'box' or 'nearest'".to_string()),
                }),
            )
                .into_response();
        }
    };

    // Limit results
    let row_ids: Vec<u64> = row_ids.into_iter().take(req.limit).collect();

    // For now, return mock results with row_ids
    // In production, we'd query the actual rows and return full data
    let results: Vec<SearchResult> = row_ids
        .iter()
        .map(|&row_id| SearchResult {
            row_id,
            x: 0.0, // Would be populated from actual query
            y: 0.0, // Would be populated from actual query
        })
        .collect();

    let elapsed = start.elapsed().as_secs_f64() * 1000.0;

    (
        StatusCode::OK,
        Json(SearchResponse {
            count: results.len(),
            results,
            execution_time_ms: elapsed,
        }),
    )
        .into_response()
}
