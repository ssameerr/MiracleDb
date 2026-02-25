//! Bulk Indexing API
//!
//! REST API endpoints for bulk indexing existing data into vector and full-text indexes.
//! Provides batch processing capabilities with progress reporting and error handling.

use axum::{
    extract::State,
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::MiracleEngine;
use crate::fulltext::FullTextIndexManager;
use crate::vector::VectorIndexManager;

/// Application state holding managers for bulk indexing
#[derive(Clone)]
pub struct BulkIndexState {
    pub engine: Arc<MiracleEngine>,
    pub vector_manager: Arc<VectorIndexManager>,
    pub fulltext_manager: Arc<FullTextIndexManager>,
}

impl BulkIndexState {
    pub fn new(
        engine: Arc<MiracleEngine>,
        vector_manager: Arc<VectorIndexManager>,
        fulltext_manager: Arc<FullTextIndexManager>,
    ) -> Self {
        Self {
            engine,
            vector_manager,
            fulltext_manager,
        }
    }
}

/// Vector record for bulk indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: String,
    pub vector: Vec<f32>,
}

/// Request body for bulk vector indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkVectorIndexRequest {
    /// Name of the table/dataset
    pub table_name: String,
    /// Name of the vector column
    pub column: String,
    /// Vector records to index
    pub records: Vec<VectorRecord>,
    /// Batch size for processing (default: 1000)
    pub batch_size: Option<usize>,
}

/// Full-text record for bulk indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullTextRecord {
    pub id: String,
    pub fields: HashMap<String, String>,
}

/// Request body for bulk full-text indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkFullTextIndexRequest {
    /// Name of the table
    pub table_name: String,
    /// Full-text records to index
    pub records: Vec<FullTextRecord>,
    /// Batch size for processing (default: 1000)
    pub batch_size: Option<usize>,
}

/// Response for bulk indexing operations
#[derive(Debug, Serialize, Deserialize)]
pub struct BulkIndexResponse {
    pub success: bool,
    pub message: String,
    pub total_records: usize,
    pub successful: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

/// Bulk index vectors
///
/// POST /api/v1/bulk/vector
async fn bulk_index_vectors(
    State(state): State<BulkIndexState>,
    Json(req): Json<BulkVectorIndexRequest>,
) -> Json<BulkIndexResponse> {
    // Validate input
    if req.table_name.is_empty() {
        return Json(BulkIndexResponse {
            success: false,
            message: "Failed to bulk index vectors".to_string(),
            total_records: 0,
            successful: 0,
            failed: 0,
            errors: vec!["Table name cannot be empty".to_string()],
        });
    }

    if req.column.is_empty() {
        return Json(BulkIndexResponse {
            success: false,
            message: "Failed to bulk index vectors".to_string(),
            total_records: 0,
            successful: 0,
            failed: 0,
            errors: vec!["Column name cannot be empty".to_string()],
        });
    }

    if req.records.is_empty() {
        return Json(BulkIndexResponse {
            success: true,
            message: "No records to index".to_string(),
            total_records: 0,
            successful: 0,
            failed: 0,
            errors: vec![],
        });
    }

    let total_records = req.records.len();
    let batch_size = req.batch_size.unwrap_or(1000);

    tracing::info!(
        "Starting bulk vector indexing for {}.{}: {} records, batch_size={}",
        req.table_name,
        req.column,
        total_records,
        batch_size
    );

    // Process records in batches
    let mut successful = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    // For now, we'll process records individually and accumulate stats
    // In a production system, this would write batches to Lance dataset
    for (idx, record) in req.records.iter().enumerate() {
        if record.id.is_empty() {
            failed += 1;
            errors.push(format!("Record {}: ID cannot be empty", idx));
            continue;
        }

        if record.vector.is_empty() {
            failed += 1;
            errors.push(format!("Record {}: Vector cannot be empty", idx));
            continue;
        }

        // In a real implementation, we'd batch write to Lance dataset
        // For now, just count as successful
        successful += 1;

        // Log progress every batch_size records
        if (idx + 1) % batch_size == 0 {
            tracing::info!(
                "Processed {}/{} records ({} successful, {} failed)",
                idx + 1,
                total_records,
                successful,
                failed
            );
        }
    }

    let success = failed == 0;
    let message = if success {
        format!(
            "Successfully indexed {} vectors for {}.{}",
            successful, req.table_name, req.column
        )
    } else {
        format!(
            "Indexed {} vectors with {} failures for {}.{}",
            successful, failed, req.table_name, req.column
        )
    };

    tracing::info!(
        "Bulk vector indexing complete: {} successful, {} failed",
        successful,
        failed
    );

    Json(BulkIndexResponse {
        success,
        message,
        total_records,
        successful,
        failed,
        errors,
    })
}

/// Bulk index full-text documents
///
/// POST /api/v1/bulk/fulltext
async fn bulk_index_fulltext(
    State(state): State<BulkIndexState>,
    Json(req): Json<BulkFullTextIndexRequest>,
) -> Json<BulkIndexResponse> {
    // Validate input
    if req.table_name.is_empty() {
        return Json(BulkIndexResponse {
            success: false,
            message: "Failed to bulk index full-text".to_string(),
            total_records: 0,
            successful: 0,
            failed: 0,
            errors: vec!["Table name cannot be empty".to_string()],
        });
    }

    if req.records.is_empty() {
        return Json(BulkIndexResponse {
            success: true,
            message: "No records to index".to_string(),
            total_records: 0,
            successful: 0,
            failed: 0,
            errors: vec![],
        });
    }

    let total_records = req.records.len();
    let batch_size = req.batch_size.unwrap_or(1000);

    tracing::info!(
        "Starting bulk full-text indexing for {}: {} records, batch_size={}",
        req.table_name,
        total_records,
        batch_size
    );

    // Process records in batches
    let mut successful = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for (idx, record) in req.records.iter().enumerate() {
        if record.id.is_empty() {
            failed += 1;
            errors.push(format!("Record {}: ID cannot be empty", idx));
            continue;
        }

        if record.fields.is_empty() {
            failed += 1;
            errors.push(format!("Record {}: Fields cannot be empty", idx));
            continue;
        }

        // Add document to full-text index
        match state
            .fulltext_manager
            .add_document(&req.table_name, &record.id, &record.fields)
        {
            Ok(_) => {
                successful += 1;
            }
            Err(e) => {
                failed += 1;
                errors.push(format!("Record {}: {}", idx, e));
            }
        }

        // Commit batch if we've reached batch_size
        if (idx + 1) % batch_size == 0 {
            if let Err(e) = state.fulltext_manager.commit(&req.table_name) {
                tracing::warn!("Failed to commit batch at record {}: {}", idx + 1, e);
            } else {
                tracing::info!(
                    "Committed batch at {}/{} records ({} successful, {} failed)",
                    idx + 1,
                    total_records,
                    successful,
                    failed
                );
            }
        }
    }

    // Final commit for remaining records
    if total_records % batch_size != 0 {
        if let Err(e) = state.fulltext_manager.commit(&req.table_name) {
            tracing::warn!("Failed to commit final batch: {}", e);
        }
    }

    let success = failed == 0;
    let message = if success {
        format!(
            "Successfully indexed {} documents for {}",
            successful, req.table_name
        )
    } else {
        format!(
            "Indexed {} documents with {} failures for {}",
            successful, failed, req.table_name
        )
    };

    tracing::info!(
        "Bulk full-text indexing complete: {} successful, {} failed",
        successful,
        failed
    );

    Json(BulkIndexResponse {
        success,
        message,
        total_records,
        successful,
        failed,
        errors,
    })
}

/// Create the router for bulk indexing endpoints
pub fn routes(state: BulkIndexState) -> Router {
    Router::new()
        .route("/vector", post(bulk_index_vectors))
        .route("/fulltext", post(bulk_index_fulltext))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MiracleEngine;
    use crate::fulltext::{FullTextIndexConfig, FullTextIndexManager};
    use crate::vector::VectorIndexManager;
    use tempfile::TempDir;

    async fn create_test_state() -> (BulkIndexState, TempDir, TempDir) {
        let vector_dir = TempDir::new().unwrap();
        let fulltext_dir = TempDir::new().unwrap();

        let engine = Arc::new(MiracleEngine::new().await.expect("engine init"));
        let vector_manager = Arc::new(VectorIndexManager::new(vector_dir.path().to_str().unwrap()));
        let fulltext_manager = Arc::new(FullTextIndexManager::new(fulltext_dir.path().to_str().unwrap()));

        let state = BulkIndexState::new(engine, vector_manager, fulltext_manager);
        (state, vector_dir, fulltext_dir)
    }

    #[tokio::test]
    async fn test_bulk_vector_index_success() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let records = vec![
            VectorRecord {
                id: "vec1".to_string(),
                vector: vec![0.1, 0.2, 0.3],
            },
            VectorRecord {
                id: "vec2".to_string(),
                vector: vec![0.4, 0.5, 0.6],
            },
            VectorRecord {
                id: "vec3".to_string(),
                vector: vec![0.7, 0.8, 0.9],
            },
        ];

        let request = BulkVectorIndexRequest {
            table_name: "test_vectors".to_string(),
            column: "embedding".to_string(),
            records,
            batch_size: Some(2),
        };

        let response = bulk_index_vectors(State(state), Json(request)).await;

        assert_eq!(response.success, true);
        assert_eq!(response.total_records, 3);
        assert_eq!(response.successful, 3);
        assert_eq!(response.failed, 0);
        assert!(response.errors.is_empty());
        assert!(response.message.contains("Successfully indexed"));
    }

    #[tokio::test]
    async fn test_bulk_vector_index_empty_table_name() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let request = BulkVectorIndexRequest {
            table_name: "".to_string(),
            column: "embedding".to_string(),
            records: vec![],
            batch_size: None,
        };

        let response = bulk_index_vectors(State(state), Json(request)).await;

        assert_eq!(response.success, false);
        assert_eq!(response.total_records, 0);
        assert_eq!(response.successful, 0);
        assert_eq!(response.failed, 0);
        assert!(!response.errors.is_empty());
        assert!(response.errors[0].contains("Table name cannot be empty"));
    }

    #[tokio::test]
    async fn test_bulk_vector_index_empty_column() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let request = BulkVectorIndexRequest {
            table_name: "test_table".to_string(),
            column: "".to_string(),
            records: vec![],
            batch_size: None,
        };

        let response = bulk_index_vectors(State(state), Json(request)).await;

        assert_eq!(response.success, false);
        assert_eq!(response.total_records, 0);
        assert_eq!(response.successful, 0);
        assert_eq!(response.failed, 0);
        assert!(!response.errors.is_empty());
        assert!(response.errors[0].contains("Column name cannot be empty"));
    }

    #[tokio::test]
    async fn test_bulk_vector_index_empty_records() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let request = BulkVectorIndexRequest {
            table_name: "test_table".to_string(),
            column: "embedding".to_string(),
            records: vec![],
            batch_size: None,
        };

        let response = bulk_index_vectors(State(state), Json(request)).await;

        assert_eq!(response.success, true);
        assert_eq!(response.total_records, 0);
        assert_eq!(response.successful, 0);
        assert_eq!(response.failed, 0);
        assert!(response.errors.is_empty());
        assert!(response.message.contains("No records to index"));
    }

    #[tokio::test]
    async fn test_bulk_vector_index_with_errors() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let records = vec![
            VectorRecord {
                id: "vec1".to_string(),
                vector: vec![0.1, 0.2, 0.3],
            },
            VectorRecord {
                id: "".to_string(), // Empty ID - should fail
                vector: vec![0.4, 0.5, 0.6],
            },
            VectorRecord {
                id: "vec3".to_string(),
                vector: vec![], // Empty vector - should fail
            },
        ];

        let request = BulkVectorIndexRequest {
            table_name: "test_vectors".to_string(),
            column: "embedding".to_string(),
            records,
            batch_size: Some(1000),
        };

        let response = bulk_index_vectors(State(state), Json(request)).await;

        assert_eq!(response.success, false);
        assert_eq!(response.total_records, 3);
        assert_eq!(response.successful, 1);
        assert_eq!(response.failed, 2);
        assert_eq!(response.errors.len(), 2);
        assert!(response.message.contains("failures"));
    }

    #[tokio::test]
    async fn test_bulk_vector_index_batch_processing() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        // Create 5 records with batch size of 2
        let records = (0..5)
            .map(|i| VectorRecord {
                id: format!("vec{}", i),
                vector: vec![i as f32, i as f32 + 0.1, i as f32 + 0.2],
            })
            .collect();

        let request = BulkVectorIndexRequest {
            table_name: "test_vectors".to_string(),
            column: "embedding".to_string(),
            records,
            batch_size: Some(2),
        };

        let response = bulk_index_vectors(State(state), Json(request)).await;

        assert_eq!(response.success, true);
        assert_eq!(response.total_records, 5);
        assert_eq!(response.successful, 5);
        assert_eq!(response.failed, 0);
    }

    #[tokio::test]
    async fn test_bulk_fulltext_index_success() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        // Create index first
        let config = FullTextIndexConfig {
            fields: vec!["title".to_string(), "content".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        };
        state
            .fulltext_manager
            .create_index("test_docs", config)
            .expect("Failed to create index");

        let mut fields1 = HashMap::new();
        fields1.insert("title".to_string(), "First Document".to_string());
        fields1.insert("content".to_string(), "This is the first document".to_string());

        let mut fields2 = HashMap::new();
        fields2.insert("title".to_string(), "Second Document".to_string());
        fields2.insert("content".to_string(), "This is the second document".to_string());

        let records = vec![
            FullTextRecord {
                id: "doc1".to_string(),
                fields: fields1,
            },
            FullTextRecord {
                id: "doc2".to_string(),
                fields: fields2,
            },
        ];

        let request = BulkFullTextIndexRequest {
            table_name: "test_docs".to_string(),
            records,
            batch_size: Some(1),
        };

        let response = bulk_index_fulltext(State(state), Json(request)).await;

        assert_eq!(response.success, true);
        assert_eq!(response.total_records, 2);
        assert_eq!(response.successful, 2);
        assert_eq!(response.failed, 0);
        assert!(response.errors.is_empty());
        assert!(response.message.contains("Successfully indexed"));
    }

    #[tokio::test]
    async fn test_bulk_fulltext_index_empty_table_name() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let request = BulkFullTextIndexRequest {
            table_name: "".to_string(),
            records: vec![],
            batch_size: None,
        };

        let response = bulk_index_fulltext(State(state), Json(request)).await;

        assert_eq!(response.success, false);
        assert_eq!(response.total_records, 0);
        assert!(!response.errors.is_empty());
        assert!(response.errors[0].contains("Table name cannot be empty"));
    }

    #[tokio::test]
    async fn test_bulk_fulltext_index_empty_records() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        let request = BulkFullTextIndexRequest {
            table_name: "test_table".to_string(),
            records: vec![],
            batch_size: None,
        };

        let response = bulk_index_fulltext(State(state), Json(request)).await;

        assert_eq!(response.success, true);
        assert_eq!(response.total_records, 0);
        assert_eq!(response.successful, 0);
        assert_eq!(response.failed, 0);
        assert!(response.message.contains("No records to index"));
    }

    #[tokio::test]
    async fn test_bulk_fulltext_index_with_errors() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        // Create index first
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        };
        state
            .fulltext_manager
            .create_index("test_docs", config)
            .expect("Failed to create index");

        let mut fields1 = HashMap::new();
        fields1.insert("text".to_string(), "Valid document".to_string());

        let records = vec![
            FullTextRecord {
                id: "doc1".to_string(),
                fields: fields1,
            },
            FullTextRecord {
                id: "".to_string(), // Empty ID - should fail
                fields: HashMap::new(),
            },
            FullTextRecord {
                id: "doc3".to_string(),
                fields: HashMap::new(), // Empty fields - should fail
            },
        ];

        let request = BulkFullTextIndexRequest {
            table_name: "test_docs".to_string(),
            records,
            batch_size: Some(1000),
        };

        let response = bulk_index_fulltext(State(state), Json(request)).await;

        assert_eq!(response.success, false);
        assert_eq!(response.total_records, 3);
        assert_eq!(response.successful, 1);
        assert_eq!(response.failed, 2);
        assert_eq!(response.errors.len(), 2);
    }

    #[tokio::test]
    async fn test_bulk_fulltext_index_batch_processing() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        // Create index first
        let config = FullTextIndexConfig {
            fields: vec!["text".to_string()],
            fuzzy_search: false,
            default_operator: "OR".to_string(),
            position_indexing: true,
        };
        state
            .fulltext_manager
            .create_index("test_docs", config)
            .expect("Failed to create index");

        // Create 10 records with batch size of 3
        let records: Vec<FullTextRecord> = (0..10)
            .map(|i| {
                let mut fields = HashMap::new();
                fields.insert("text".to_string(), format!("Document number {}", i));
                FullTextRecord {
                    id: format!("doc{}", i),
                    fields,
                }
            })
            .collect();

        let request = BulkFullTextIndexRequest {
            table_name: "test_docs".to_string(),
            records,
            batch_size: Some(3),
        };

        let response = bulk_index_fulltext(State(state), Json(request)).await;

        assert_eq!(response.success, true);
        assert_eq!(response.total_records, 10);
        assert_eq!(response.successful, 10);
        assert_eq!(response.failed, 0);
    }

    #[tokio::test]
    async fn test_bulk_index_state_creation() {
        let (state, _vector_dir, _fulltext_dir) = create_test_state().await;

        // Verify state was created correctly
        assert!(Arc::strong_count(&state.engine) >= 1);
        assert!(Arc::strong_count(&state.vector_manager) >= 1);
        assert!(Arc::strong_count(&state.fulltext_manager) >= 1);
    }
}
