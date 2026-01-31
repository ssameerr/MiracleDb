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
    Json(request): Json<CreateVectorIndexRequest>,
) -> Result<(StatusCode, Json<CreateVectorIndexResponse>), ApiError> {
    // Validate input
    if request.table_name.is_empty() {
        return Err(ApiError::BadRequest("Table name cannot be empty".to_string()));
    }
    if request.column.is_empty() {
        return Err(ApiError::BadRequest("Column name cannot be empty".to_string()));
    }

    // Check if dataset exists
    if !state.vector_manager.dataset_exists(&request.table_name).await {
        return Err(ApiError::NotFound(format!(
            "Table '{}' does not exist",
            request.table_name
        )));
    }

    // Create the index
    state
        .vector_manager
        .create_index(&request.table_name, &request.column, request.config.clone())
        .await
        .map_err(|e| ApiError::InternalError(format!("Failed to create index: {}", e)))?;

    tracing::info!(
        "Created vector index on {}.{}",
        request.table_name,
        request.column
    );

    let response = CreateVectorIndexResponse {
        success: true,
        message: format!(
            "Vector index created on {}.{}",
            request.table_name, request.column
        ),
        error: None,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

/// Get statistics for a table
///
/// GET /api/v1/vector/index/stats/:table
async fn get_index_stats(
    State(state): State<VectorIndexState>,
    Path(table): Path<String>,
) -> Result<Json<IndexStatsResponse>, ApiError> {
    // Validate input
    if table.is_empty() {
        return Err(ApiError::BadRequest("Table name cannot be empty".to_string()));
    }

    // Check if dataset exists
    let exists = state.vector_manager.dataset_exists(&table).await;

    if !exists {
        return Err(ApiError::NotFound(format!("Table '{}' does not exist", table)));
    }

    // Get statistics
    let stats = state
        .vector_manager
        .get_stats(&table)
        .await
        .map_err(|e| ApiError::InternalError(format!("Failed to get stats: {}", e)))?;

    let response = IndexStatsResponse {
        exists: true,
        row_count: Some(stats.row_count),
        version: Some(stats.version),
    };

    Ok(Json(response))
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

        let embedding_array = Float32Array::from(embedding_values);
        let embedding_list = arrow::array::FixedSizeListArray::try_new_from_values(
            embedding_array,
            128,
        )?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(embedding_list)],
        )?;

        // Write to Lance dataset
        Dataset::write(
            vec![batch],
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
        let result = create_index(State(state), Json(request)).await;

        // Assert
        assert!(result.is_ok());
        let (status, response) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);
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
        let result = create_index(State(state), Json(request)).await;

        // Assert
        assert!(result.is_err());
        match result {
            Err(ApiError::BadRequest(msg)) => {
                assert!(msg.contains("Table name cannot be empty"));
            }
            _ => panic!("Expected BadRequest error"),
        }
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
        let result = create_index(State(state), Json(request)).await;

        // Assert
        assert!(result.is_err());
        match result {
            Err(ApiError::BadRequest(msg)) => {
                assert!(msg.contains("Column name cannot be empty"));
            }
            _ => panic!("Expected BadRequest error"),
        }
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
        let result = create_index(State(state), Json(request)).await;

        // Assert
        assert!(result.is_err());
        match result {
            Err(ApiError::NotFound(msg)) => {
                assert!(msg.contains("does not exist"));
            }
            _ => panic!("Expected NotFound error"),
        }
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
        let result = get_index_stats(State(state), Path(table_name.to_string())).await;

        // Assert
        assert!(result.is_ok());
        let response = result.unwrap();
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

        // Execute with empty table name
        let result = get_index_stats(State(state), Path("".to_string())).await;

        // Assert
        assert!(result.is_err());
        match result {
            Err(ApiError::BadRequest(msg)) => {
                assert!(msg.contains("Table name cannot be empty"));
            }
            _ => panic!("Expected BadRequest error"),
        }
    }

    #[tokio::test]
    async fn test_get_index_stats_table_not_found() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let vector_manager = Arc::new(VectorIndexManager::new(base_path));
        let state = VectorIndexState::new(vector_manager);

        // Execute with non-existent table
        let result = get_index_stats(State(state), Path("nonexistent_table".to_string())).await;

        // Assert
        assert!(result.is_err());
        match result {
            Err(ApiError::NotFound(msg)) => {
                assert!(msg.contains("does not exist"));
            }
            _ => panic!("Expected NotFound error"),
        }
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
        let result = create_index(State(state), Json(request)).await;

        // Assert
        assert!(result.is_ok());
        let (status, response) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(response.success, true);
        assert!(response.message.contains("created"));
    }
}
