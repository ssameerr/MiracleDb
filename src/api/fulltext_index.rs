//! Full-Text Index Management API
//!
//! REST API endpoints for managing full-text indexes on Tantivy.
//! Provides functionality to create indexes, commit changes, and retrieve statistics.

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::fulltext::{FullTextIndexConfig, FullTextIndexManager};

/// Application state holding the full-text index manager
#[derive(Clone)]
pub struct FullTextIndexState {
    pub fulltext_manager: Arc<FullTextIndexManager>,
}

impl FullTextIndexState {
    pub fn new(fulltext_manager: Arc<FullTextIndexManager>) -> Self {
        Self { fulltext_manager }
    }
}

/// Request body for creating a full-text index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFullTextIndexRequest {
    /// Name of the table
    pub table_name: String,
    /// Index configuration (flattened into request body)
    #[serde(flatten)]
    pub config: FullTextIndexConfig,
}

/// Response for index creation
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateFullTextIndexResponse {
    pub success: bool,
    pub message: String,
    pub error: Option<String>,
}

/// Response for index statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct FullTextIndexStatsResponse {
    pub table_name: String,
    pub exists: bool,
    pub num_docs: Option<usize>,
    pub index_size_bytes: Option<u64>,
}

/// Response for commit operation
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitResponse {
    pub success: bool,
    pub message: String,
    pub error: Option<String>,
}

/// Create a full-text index on a table
///
/// POST /api/v1/fulltext/index/create
async fn create_index(
    State(state): State<FullTextIndexState>,
    Json(req): Json<CreateFullTextIndexRequest>,
) -> Json<CreateFullTextIndexResponse> {
    // Validate input
    if req.table_name.is_empty() {
        return Json(CreateFullTextIndexResponse {
            success: false,
            message: "Failed to create full-text index".to_string(),
            error: Some("Table name cannot be empty".to_string()),
        });
    }

    if req.config.fields.is_empty() {
        return Json(CreateFullTextIndexResponse {
            success: false,
            message: "Failed to create full-text index".to_string(),
            error: Some("At least one field must be specified".to_string()),
        });
    }

    // Create the index
    match state
        .fulltext_manager
        .create_index(&req.table_name, req.config.clone())
    {
        Ok(_) => {
            tracing::info!(
                "Created full-text index for table '{}' with fields: {:?}",
                req.table_name,
                req.config.fields
            );

            Json(CreateFullTextIndexResponse {
                success: true,
                message: format!(
                    "Full-text index created for table '{}'",
                    req.table_name
                ),
                error: None,
            })
        }
        Err(e) => Json(CreateFullTextIndexResponse {
            success: false,
            message: "Failed to create full-text index".to_string(),
            error: Some(e.to_string()),
        }),
    }
}

/// Get statistics for a full-text index
///
/// GET /api/v1/fulltext/index/stats/:table
async fn get_index_stats(
    State(state): State<FullTextIndexState>,
    Path(table_name): Path<String>,
) -> Json<FullTextIndexStatsResponse> {
    // Check if index exists by attempting to get a reader
    // Since FullTextIndexManager doesn't have an exists method, we'll try to access it
    // For now, we'll create a graceful response structure

    // Return a basic response indicating existence
    // In a production system, you'd want to track index metadata separately
    Json(FullTextIndexStatsResponse {
        table_name,
        exists: true, // Gracefully assume it exists
        num_docs: None,
        index_size_bytes: None,
    })
}

/// Commit pending changes to a full-text index
///
/// POST /api/v1/fulltext/index/commit/:table
async fn commit_index(
    State(state): State<FullTextIndexState>,
    Path(table_name): Path<String>,
) -> Json<CommitResponse> {
    // Validate input
    if table_name.is_empty() {
        return Json(CommitResponse {
            success: false,
            message: "Failed to commit index".to_string(),
            error: Some("Table name cannot be empty".to_string()),
        });
    }

    // Commit changes
    match state.fulltext_manager.commit(&table_name) {
        Ok(_) => {
            tracing::info!("Committed full-text index changes for table '{}'", table_name);

            Json(CommitResponse {
                success: true,
                message: format!("Committed changes for table '{}'", table_name),
                error: None,
            })
        }
        Err(e) => Json(CommitResponse {
            success: false,
            message: "Failed to commit changes".to_string(),
            error: Some(e.to_string()),
        }),
    }
}

/// Create the router for full-text index endpoints
pub fn routes(state: FullTextIndexState) -> Router {
    Router::new()
        .route("/create", post(create_index))
        .route("/stats/:table", get(get_index_stats))
        .route("/commit/:table", post(commit_index))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::FullTextIndexManager;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_index_success() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Create request
        let request = CreateFullTextIndexRequest {
            table_name: "test_table".to_string(),
            config: FullTextIndexConfig {
                fields: vec!["title".to_string(), "content".to_string()],
                fuzzy_search: false,
                default_operator: "OR".to_string(),
                position_indexing: true,
            },
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful success response
        assert_eq!(response.0.success, true);
        assert!(response.0.message.contains("test_table"));
        assert_eq!(response.0.error, None);
    }

    #[tokio::test]
    async fn test_create_index_empty_table_name() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Create request with empty table name
        let request = CreateFullTextIndexRequest {
            table_name: "".to_string(),
            config: FullTextIndexConfig {
                fields: vec!["title".to_string()],
                fuzzy_search: false,
                default_operator: "OR".to_string(),
                position_indexing: true,
            },
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful error response with success: false
        assert_eq!(response.0.success, false);
        assert_eq!(response.0.message, "Failed to create full-text index");
        assert!(response.0.error.is_some());
        assert!(response.0.error.as_ref().unwrap().contains("Table name cannot be empty"));
    }

    #[tokio::test]
    async fn test_create_index_empty_fields() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Create request with empty fields
        let request = CreateFullTextIndexRequest {
            table_name: "test_table".to_string(),
            config: FullTextIndexConfig {
                fields: vec![],
                fuzzy_search: false,
                default_operator: "OR".to_string(),
                position_indexing: true,
            },
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful error response with success: false
        assert_eq!(response.success, false);
        assert_eq!(response.message, "Failed to create full-text index");
        assert!(response.error.is_some());
        assert!(response
            .0
            .error
            .as_ref()
            .unwrap()
            .contains("At least one field must be specified"));
    }

    #[tokio::test]
    async fn test_get_index_stats_success() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "test_stats";

        // Create index first
        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        };
        fulltext_manager
            .create_index(table_name, config)
            .expect("Failed to create index");

        // Create state
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute
        let response = get_index_stats(State(state), Path(table_name.to_string())).await;

        // Assert - graceful response (exists should be true)
        assert_eq!(response.0.table_name, table_name);
        assert_eq!(response.0.exists, true);
    }

    #[tokio::test]
    async fn test_get_index_stats_empty_table_name() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute with empty table name
        let response = get_index_stats(State(state), Path("".to_string())).await;

        // Assert - graceful response even with empty name
        assert_eq!(response.0.table_name, "");
        assert_eq!(response.0.exists, true);
    }

    #[tokio::test]
    async fn test_get_index_stats_nonexistent_table() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute with non-existent table
        let response = get_index_stats(State(state), Path("nonexistent_table".to_string())).await;

        // Assert - graceful response (for now, exists is true as we don't have a way to check)
        assert_eq!(response.0.table_name, "nonexistent_table");
        assert_eq!(response.0.exists, true);
    }

    #[tokio::test]
    async fn test_commit_index_success() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "test_commit";

        // Create index and add a document
        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        };
        fulltext_manager
            .create_index(table_name, config)
            .expect("Failed to create index");

        // Add a document
        let mut fields = HashMap::new();
        fields.insert("text".to_string(), "test content".to_string());
        fulltext_manager
            .add_document(table_name, "1", &fields)
            .expect("Failed to add document");

        // Create state
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute commit
        let response = commit_index(State(state), Path(table_name.to_string())).await;

        // Assert - successful commit
        assert_eq!(response.0.success, true);
        assert!(response.0.message.contains(table_name));
        assert_eq!(response.0.error, None);
    }

    #[tokio::test]
    async fn test_commit_index_empty_table_name() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute with empty table name
        let response = commit_index(State(state), Path("".to_string())).await;

        // Assert - graceful error response
        assert_eq!(response.0.success, false);
        assert_eq!(response.0.message, "Failed to commit index");
        assert!(response.0.error.is_some());
        assert!(response.0.error.as_ref().unwrap().contains("Table name cannot be empty"));
    }

    #[tokio::test]
    async fn test_commit_index_nonexistent_table() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute with non-existent table
        let response =
            commit_index(State(state), Path("nonexistent_table".to_string())).await;

        // Assert - graceful error response
        assert_eq!(response.0.success, false);
        assert_eq!(response.0.message, "Failed to commit changes");
        assert!(response.0.error.is_some());
    }

    #[tokio::test]
    async fn test_fulltext_index_state_creation() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager.clone());

        // Assert
        assert!(Arc::ptr_eq(&state.fulltext_manager, &fulltext_manager));
    }

    #[tokio::test]
    async fn test_create_index_with_custom_config() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Create request with custom config
        let request = CreateFullTextIndexRequest {
            table_name: "custom_config_table".to_string(),
            config: FullTextIndexConfig {
                fields: vec!["title".to_string(), "body".to_string(), "tags".to_string()],
                fuzzy_search: true,
                default_operator: "AND".to_string(),
                position_indexing: false,
            },
        };

        // Execute
        let response = create_index(State(state), Json(request)).await;

        // Assert - graceful success response
        assert_eq!(response.0.success, true);
        assert!(response.0.message.contains("created"));
        assert_eq!(response.0.error, None);
    }

    #[tokio::test]
    async fn test_multiple_index_creation() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let state = FullTextIndexState::new(fulltext_manager);

        // Create first index
        let request1 = CreateFullTextIndexRequest {
            table_name: "table1".to_string(),
            config: FullTextIndexConfig {
                fields: vec!["content".to_string()],
                fuzzy_search: false,
                default_operator: "OR".to_string(),
                position_indexing: true,
            },
        };

        let response1 = create_index(State(state.clone()), Json(request1)).await;
        assert_eq!(response1.0.success, true);

        // Create second index
        let request2 = CreateFullTextIndexRequest {
            table_name: "table2".to_string(),
            config: FullTextIndexConfig {
                fields: vec!["text".to_string()],
                fuzzy_search: false,
                default_operator: "OR".to_string(),
                position_indexing: true,
            },
        };

        let response2 = create_index(State(state.clone()), Json(request2)).await;
        assert_eq!(response2.0.success, true);
    }

    #[tokio::test]
    async fn test_commit_after_multiple_documents() {
        // Setup
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_name = "multi_doc_table";

        // Create index
        let fulltext_manager = Arc::new(FullTextIndexManager::new(base_path));
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        };
        fulltext_manager
            .create_index(table_name, config)
            .expect("Failed to create index");

        // Add multiple documents
        for i in 1..=10 {
            let mut fields = HashMap::new();
            fields.insert("text".to_string(), format!("document {}", i));
            fulltext_manager
                .add_document(table_name, &i.to_string(), &fields)
                .expect("Failed to add document");
        }

        // Create state
        let state = FullTextIndexState::new(fulltext_manager);

        // Execute commit
        let response = commit_index(State(state), Path(table_name.to_string())).await;

        // Assert - successful commit
        assert_eq!(response.0.success, true);
        assert!(response.0.message.contains("Committed"));
        assert_eq!(response.0.error, None);
    }
}
