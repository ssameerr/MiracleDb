use std::sync::Arc;
use lance::dataset::{Dataset, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance_index::{IndexType, DatasetIndexExt};
use lance_linalg::distance::MetricType;
use arrow::array::{RecordBatch, FixedSizeListArray, StringArray, Float32Array};
use arrow::datatypes::{Schema, Field, DataType};
use serde::{Deserialize, Serialize};
use futures::stream::TryStreamExt;

pub mod quantization;
pub mod embeddings;

/// Configuration for vector index creation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Distance metric (L2, Cosine, Dot)
    pub metric_type: String,
    /// Number of partitions for IVF index
    pub num_partitions: Option<usize>,
    /// Number of sub-vectors for PQ
    pub num_sub_vectors: Option<usize>,
    /// Max iterations for training
    pub max_iters: Option<usize>,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            metric_type: "L2".to_string(),
            num_partitions: Some(256),
            num_sub_vectors: Some(16),
            max_iters: Some(50),
        }
    }
}

pub struct VectorIndexManager {
    base_path: String,
}

impl VectorIndexManager {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    /// Create a vector index on a column in a Lance dataset
    pub async fn create_index(
        &self,
        table_name: &str,
        column: &str,
        config: VectorIndexConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uri = format!("{}/{}", self.base_path, table_name);

        tracing::info!("Creating vector index on {}.{} at {}", table_name, column, uri);

        // Open the Lance dataset
        let mut dataset = Dataset::open(&uri).await.map_err(|e| {
            format!("Failed to open Lance dataset at {}: {}", uri, e)
        })?;

        // Parse metric type
        let metric = match config.metric_type.to_uppercase().as_str() {
            "L2" => MetricType::L2,
            "COSINE" => MetricType::Cosine,
            "DOT" => MetricType::Dot,
            _ => {
                tracing::warn!("Unknown metric type {}, defaulting to L2", config.metric_type);
                MetricType::L2
            }
        };

        // IVF-PQ training requires at least num_sub_vectors * 16 rows.
        // For tiny datasets, skip physical index creation (sequential scan is fast enough).
        let row_count = dataset.count_rows(None).await.unwrap_or(0);
        let num_sub_vectors = config.num_sub_vectors.unwrap_or(16);
        let min_rows_for_pq = num_sub_vectors * 16;

        if row_count < min_rows_for_pq {
            tracing::info!(
                "Dataset {}.{} has {} rows (< {} required for IVF-PQ); skipping physical index build",
                table_name, column, row_count, min_rows_for_pq
            );
            return Ok(());
        }

        let requested_partitions = config.num_partitions.unwrap_or(256);
        let max_iters = config.max_iters.unwrap_or(50);
        // Cap partitions to half the row count so k-means training has enough data
        let num_partitions = (requested_partitions).min(row_count / 2).max(1);

        let params = VectorIndexParams::ivf_pq(
            num_partitions,
            8,  // num_bits for PQ (8-bit quantization)
            num_sub_vectors,
            metric,
            max_iters,
        );

        // Create the vector index (Lance 0.19: IndexType::Vector = legacy IvfPq alias)
        dataset.create_index(
            &[column],
            IndexType::Vector,
            None,
            &params,
            true,
        ).await.map_err(|e| {
            format!("Failed to create vector index on {}.{}: {}", table_name, column, e)
        })?;

        tracing::info!(
            "Successfully created IVF-PQ vector index on {}.{} (metric: {:?}, partitions: {})",
            table_name, column, metric, num_partitions
        );

        Ok(())
    }

    /// Search for k nearest neighbors
    pub async fn search(
        &self,
        table_name: &str,
        column: &str,
        vector: &[f32],
        k: usize,
        threshold: Option<f32>,
    ) -> Result<Vec<VectorSearchResult>, Box<dyn std::error::Error>> {
        let uri = format!("{}/{}", self.base_path, table_name);

        tracing::debug!(
            "Vector search on {}.{}: k={}, vector_len={}, threshold={:?}",
            table_name,
            column,
            k,
            vector.len(),
            threshold
        );

        // Open the Lance dataset
        let dataset = Dataset::open(&uri).await.map_err(|e| {
            format!("Failed to open Lance dataset at {}: {}", uri, e)
        })?;

        // Convert vector to Float32Array
        let vector_array = Float32Array::from(vector.to_vec());

        // Create a scanner with vector search
        let mut scanner = dataset.scan();
        scanner.nearest(column, &vector_array, k)?;

        // Add distance threshold filter if specified
        if let Some(_thresh) = threshold {
            // Note: Lance returns results sorted by distance, so we filter after
            scanner.limit(Some(k as i64), None)?;
        }

        // Execute the query and collect results
        let batches = scanner.try_into_stream().await?
            .try_collect::<Vec<RecordBatch>>()
            .await?;

        let mut results = Vec::new();

        for batch in batches {
            let schema = batch.schema();

            // Find the ID column (assume it's named "id")
            let id_col_idx = schema.fields().iter()
                .position(|f| f.name() == "id")
                .ok_or("No 'id' column found in Lance dataset")?;

            // Find the distance column (Lance adds "_distance" column)
            let distance_col_name = format!("{}_distance", column);
            let distance_col_idx = schema.fields().iter()
                .position(|f| f.name() == &distance_col_name);

            // Extract results from batch
            for row_idx in 0..batch.num_rows() {
                let id_array = batch.column(id_col_idx);
                let id = match id_array.data_type() {
                    DataType::Utf8 => {
                        let string_array = id_array.as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or("Failed to cast id column to StringArray")?;
                        string_array.value(row_idx).to_string()
                    }
                    DataType::Int32 | DataType::Int64 => {
                        format!("{:?}", id_array.slice(row_idx, 1))
                    }
                    _ => {
                        format!("{:?}", id_array.slice(row_idx, 1))
                    }
                };

                // Get distance if available
                let distance = if let Some(dist_idx) = distance_col_idx {
                    let dist_array = batch.column(dist_idx);
                    if let Some(float_array) = dist_array.as_any().downcast_ref::<Float32Array>() {
                        Some(float_array.value(row_idx))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Apply threshold filter if specified
                if let (Some(thresh), Some(dist)) = (threshold, distance) {
                    if dist > thresh {
                        continue;
                    }
                }

                results.push(VectorSearchResult {
                    id,
                    distance,
                    score: distance.map(|d| 1.0 / (1.0 + d)), // Convert distance to similarity score
                });
            }
        }

        tracing::debug!("Vector search returned {} results", results.len());

        Ok(results)
    }

    /// Check if a dataset exists
    pub async fn dataset_exists(&self, table_name: &str) -> bool {
        let uri = format!("{}/{}", self.base_path, table_name);
        Dataset::open(&uri).await.is_ok()
    }

    /// Get dataset statistics
    pub async fn get_stats(&self, table_name: &str) -> Result<DatasetStats, Box<dyn std::error::Error>> {
        let uri = format!("{}/{}", self.base_path, table_name);
        let dataset = Dataset::open(&uri).await?;

        let count = dataset.count_rows(None).await?;
        let version = dataset.version().version;

        Ok(DatasetStats {
            row_count: count,
            version,
        })
    }
}

/// Result from vector search
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorSearchResult {
    pub id: String,
    pub distance: Option<f32>,
    pub score: Option<f32>,
}

/// Dataset statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatasetStats {
    pub row_count: usize,
    pub version: u64,
}
