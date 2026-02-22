//! Vector Index Management API
//!
//! REST API endpoints for managing vector indexes on Lance datasets.
//! Provides functionality to create indexes and retrieve statistics.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::vector::{VectorIndexConfig, VectorIndexManager};

/// Application state holding the vector index manager
#[derive(Clone)]
pub struct VectorIndexState {
    pub vector_manager: Arc<VectorIndexManager>,
}

impl VectorIndexState {
    pub fn new(vector_manager: Arc<VectorIndexManager>) -> Self {
        Self { vector_manager }
    }
}

/// Request body for creating a vector index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVectorIndexRequest {
    /// Name of the table/dataset
    pub table_name: String,
    /// Name of the vector column to index
    pub column: String,
    /// Index configuration (flattened into request body)
    #[serde(flatten)]
    pub config: VectorIndexConfig,
}

/// Response for index creation
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateVectorIndexResponse {
    pub success: bool,
    pub message: String,
    pub error: Option<String>,
}

/// Response for index statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexStatsResponse {
    pub table_name: String,
    pub exists: bool,
    pub row_count: Option<usize>,
    pub version: Option<u64>,
}

/// Error response format
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub details: Option<String>,
}

/// Custom error type for API responses
#[derive(Debug)]
pub enum ApiError {
    NotFound(String),
    BadRequest(String),
    InternalError(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = Json(ErrorResponse {
            error: error_message.clone(),
            details: None,
        });

        (status, body).into_response()
    }
}

/// Create a vector index on a table column
///
/// POST /api/v1/vector/index/create
async fn create_index(
    State(state): State<VectorIndexState>,
    Json(req): Json<CreateVectorIndexRequest>,
) -> Json<CreateVectorIndexResponse> {
    // Validate input
    if req.table_name.is_empty() {
        return Json(CreateVectorIndexResponse {
            success: false,
            message: "Failed to create vector index".to_string(),
            error: Some("Table name cannot be empty".to_string()),
        });
    }
    if req.column.is_empty() {
        return Json(CreateVectorIndexResponse {
            success: false,
            message: "Failed to create vector index".to_string(),
            error: Some("Column name cannot be empty".to_string()),
        });
    }

    // Check if dataset exists
    if !state.vector_manager.dataset_exists(&req.table_name).await {
        return Json(CreateVectorIndexResponse {
            success: false,
            message: "Failed to create vector index".to_string(),
            error: Some(format!("Table '{}' does not exist", req.table_name)),
        });
    }

    // Create the index
    match state
        .vector_manager
        .create_index(&req.table_name, &req.column, req.config.clone())
        .await
    {
        Ok(_) => {
            tracing::info!(
                "Created vector index on {}.{}",
                req.table_name,
                req.column
            );

            Json(CreateVectorIndexResponse {
                success: true,
                message: format!(
                    "Vector index created on {}.{}",
                    req.table_name, req.column
                ),
                error: None,
            })
        }
        Err(e) => Json(CreateVectorIndexResponse {
            success: false,
            message: "Failed to create vector index".to_string(),
            error: Some(e.to_string()),
        }),
    }
}

/// Get statistics for a table
///
/// GET /api/v1/vector/index/stats/:table
async fn get_index_stats(
    State(state): State<VectorIndexState>,
    Path(table_name): Path<String>,
) -> Json<IndexStatsResponse> {
    // Check if dataset exists
    let exists = state.vector_manager.dataset_exists(&table_name).await;

    if !exists {
        // Return graceful response with exists: false
        return Json(IndexStatsResponse {
            table_name,
            exists: false,
            row_count: None,
            version: None,
        });
    }

    // Get statistics
    match state.vector_manager.get_stats(&table_name).await {
        Ok(stats) => Json(IndexStatsResponse {
            table_name,
            exists: true,
            row_count: Some(stats.row_count),
            version: Some(stats.version),
        }),
        Err(_) => {
            // If stats retrieval fails, return exists but no data
            Json(IndexStatsResponse {
                table_name,
                exists: true,
                row_count: None,
                version: None,
            })
        }
    }
}

/// Create the router for vector index endpoints
pub fn routes(state: VectorIndexState) -> Router {
    Router::new()
        .route("/create", post(create_index))
        .route("/stats/:table", get(get_index_stats))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::VectorIndexManager;
    use arrow::array::{Float32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatchIterator;
    use lance::dataset::{Dataset, WriteParams};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Helper function to create a test Lance dataset with vector data
    async fn create_test_dataset(
        base_path: &str,
        table_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uri = format!("{}/{}", base_path, table_name);

        // Create schema with id and embedding columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    128,
                ),
                true,
            ),
        ]));

        // Create sample data
        let ids = StringArray::from(vec!["doc1", "doc2", "doc3"]);

        // Create embeddings (128-dimensional vectors)
        let mut embedding_values = Vec::new();
        for _ in 0..3 {
            for i in 0..128 {
                embedding_values.push(i as f32 * 0.01);
            }
        }

        let embedding_array = Arc::new(Float32Array::from(embedding_values));
        let field = Arc::new(arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Float32, true));
        let embedding_list = arrow::array::FixedSizeListArray::try_new(
            field,
            128,
            embedding_array,
            None,
        )?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(embedding_list)],
        )?;

        // Write to Lance dataset using RecordBatchIterator
        let reader = arrow::record_batch::RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            schema.clone(),
        );
        Dataset::write(
            reader,
            &uri,
            Some(WriteParams {
                mode: lance::dataset::WriteMode::Create,
                ..Default::default()
            }),
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_index_success() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "test_vectors";

        // Create test dataset
        create_test_dataset(base_path, table_name)
            .await
            .expect("Failed to create test dataset");

        // Create state
        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Create request
        let request = CreateVectorIndexRequest {
            table_name: table_name.to_string(),
            column: "embedding".to_string(),
            config: VectorIndexConfig::default(),
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful success response
        assert_eq!(response.success, true);
        assert!(response.message.contains(table_name));
        assert!(response.message.contains("embedding"));
        assert_eq!(response.error, None);
    }

    #[tokio::test]
    async fn test_create_index_empty_table_name() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Create request with empty table name
        let request = CreateVectorIndexRequest {
            table_name: "".to_string(),
            column: "embedding".to_string(),
            config: VectorIndexConfig::default(),
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful error response with success: false
        assert_eq!(response.success, false);
        assert_eq!(response.message, "Failed to create vector index");
        assert!(response.error.is_some());
        assert!(response.0.error.as_deref().unwrap().contains("Table name cannot be empty"));
    }

    #[tokio::test]
    async fn test_create_index_empty_column_name() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Create request with empty column name
        let request = CreateVectorIndexRequest {
            table_name: "test_table".to_string(),
            column: "".to_string(),
            config: VectorIndexConfig::default(),
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful error response with success: false
        assert_eq!(response.success, false);
        assert_eq!(response.message, "Failed to create vector index");
        assert!(response.error.is_some());
        assert!(response.0.error.as_deref().unwrap().contains("Column name cannot be empty"));
    }

    #[tokio::test]
    async fn test_create_index_table_not_found() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Create request for non-existent table
        let request = CreateVectorIndexRequest {
            table_name: "nonexistent_table".to_string(),
            column: "embedding".to_string(),
            config: VectorIndexConfig::default(),
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful error response with success: false
        assert_eq!(response.success, false);
        assert_eq!(response.message, "Failed to create vector index");
        assert!(response.error.is_some());
        assert!(response.0.error.as_deref().unwrap().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_get_index_stats_success() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "test_stats";

        // Create test dataset
        create_test_dataset(base_path, table_name)
            .await
            .expect("Failed to create test dataset");

        // Create state
        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Execute
        let response = get_index_stats(State(state), Path(table_name.to_string())).await;

        // Assert
        assert_eq!(response.table_name, table_name);
        assert_eq!(response.exists, true);
        assert_eq!(response.row_count, Some(3));
        assert!(response.version.is_some());
        assert!(response.version.unwrap() > 0);
    }

    #[tokio::test]
    async fn test_get_index_stats_empty_table_name() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Execute with empty table name - should return gracefully with exists: false
        let response = get_index_stats(State(state), Path("".to_string())).await;

        // Assert - empty table name returns graceful response
        assert_eq!(response.table_name, "");
        assert_eq!(response.exists, false);
        assert_eq!(response.row_count, None);
        assert_eq!(response.version, None);
    }

    #[tokio::test]
    async fn test_get_index_stats_table_not_found() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Execute with non-existent table
        let response = get_index_stats(State(state), Path("nonexistent_table".to_string())).await;

        // Assert - non-existent table returns graceful response with exists: false
        assert_eq!(response.table_name, "nonexistent_table");
        assert_eq!(response.exists, false);
        assert_eq!(response.row_count, None);
        assert_eq!(response.version, None);
    }

    #[tokio::test]
    async fn test_vector_index_state_creation() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager.clone());

        // Assert
        assert!(Arc::ptr_eq(&state.vector_manager, &vector_manager));
    }

    #[tokio::test]
    async fn test_create_index_with_custom_config() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "test_custom_config";

        // Create test dataset
        create_test_dataset(base_path, table_name)
            .await
            .expect("Failed to create test dataset");

        // Create state
        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Create request with custom config
        let request = CreateVectorIndexRequest {
            table_name: table_name.to_string(),
            column: "embedding".to_string(),
            config: VectorIndexConfig {
                metric_type: "Cosine".to_string(),
                num_partitions: Some(128),
                num_sub_vectors: Some(8),
                max_iters: Some(100),
            },
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful success response
        assert_eq!(response.success, true);
        assert!(response.message.contains("created"));
        assert_eq!(response.error, None);
    }
}
