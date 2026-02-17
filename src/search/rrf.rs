//! Reciprocal Rank Fusion (RRF) for Hybrid Search
//!
//! RRF combines multiple ranked result sets into a single unified ranking.
//! This is the "killer feature" that enables true hybrid search combining:
//! - Vector similarity search (Lance)
//! - Full-text search (Tantivy)
//! - SQL WHERE clauses
//! - Geospatial queries
//! - Time-series queries
//!
//! Algorithm: score = sum(1 / (k + rank)) for each list where item appears
//! Where k is typically 60 (constant)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

/// RRF scoring parameter (typically 60)
const DEFAULT_K: f64 = 60.0;

/// A search result from any search type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Unique identifier for this result (could be row ID, document ID, etc.)
    pub id: String,

    /// Original rank in the source list (0-based)
    pub rank: usize,

    /// Original score from source search (optional, for debugging)
    pub original_score: Option<f64>,

    /// Source of this result (for debugging/explanation)
    pub source: String,

    /// Result data (generic JSON)
    pub data: serde_json::Value,
}

/// Combined result after RRF scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RRFResult {
    /// Result ID
    pub id: String,

    /// RRF combined score
    pub rrf_score: f64,

    /// Number of sources this result appeared in
    pub source_count: usize,

    /// Sources and their ranks
    pub source_ranks: HashMap<String, usize>,

    /// Original scores from each source
    pub source_scores: HashMap<String, f64>,

    /// Result data
    pub data: serde_json::Value,
}

/// RRF Engine
pub struct RRFEngine {
    k: f64,
}

impl RRFEngine {
    /// Create new RRF engine with default k=60
    pub fn new() -> Self {
        Self { k: DEFAULT_K }
    }

    /// Create RRF engine with custom k parameter
    pub fn with_k(k: f64) -> Self {
        Self { k }
    }

    /// Combine multiple result sets using RRF
    ///
    /// # Arguments
    /// * `result_sets` - Vec of (source_name, Vec<SearchResult>)
    ///
    /// # Returns
    /// * Vec of RRFResult sorted by RRF score (descending)
    pub fn combine(
        &self,
        result_sets: Vec<(String, Vec<SearchResult>)>,
    ) -> Vec<RRFResult> {
        info!(
            "RRF: Combining {} result sets with k={}",
            result_sets.len(),
            self.k
        );

        // Track RRF scores for each unique ID
        let mut rrf_scores: HashMap<String, f64> = HashMap::new();
        let mut source_ranks: HashMap<String, HashMap<String, usize>> = HashMap::new();
        let mut source_scores: HashMap<String, HashMap<String, f64>> = HashMap::new();
        let mut result_data: HashMap<String, serde_json::Value> = HashMap::new();

        // Process each result set
        for (source_name, results) in result_sets {
            debug!("RRF: Processing {} results from {}", results.len(), source_name);

            for result in results {
                let id = result.id.clone();

                // Calculate RRF contribution: 1 / (k + rank)
                let rrf_contribution = 1.0 / (self.k + result.rank as f64);

                // Add to cumulative score
                *rrf_scores.entry(id.clone()).or_insert(0.0) += rrf_contribution;

                // Track source rank
                source_ranks
                    .entry(id.clone())
                    .or_insert_with(HashMap::new)
                    .insert(source_name.clone(), result.rank);

                // Track source score
                if let Some(score) = result.original_score {
                    source_scores
                        .entry(id.clone())
                        .or_insert_with(HashMap::new)
                        .insert(source_name.clone(), score);
                }

                // Store result data (use first occurrence)
                result_data.entry(id).or_insert(result.data);
            }
        }

        // Convert to RRFResult vec
        let mut combined_results: Vec<RRFResult> = rrf_scores
            .into_iter()
            .map(|(id, rrf_score)| {
                let ranks = source_ranks.get(&id).cloned().unwrap_or_default();
                let scores = source_scores.get(&id).cloned().unwrap_or_default();
                let source_count = ranks.len();
                let data = result_data
                    .get(&id)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);

                RRFResult {
                    id,
                    rrf_score,
                    source_count,
                    source_ranks: ranks,
                    source_scores: scores,
                    data,
                }
            })
            .collect();

        // Sort by RRF score (descending)
        combined_results.sort_by(|a, b| {
            b.rrf_score
                .partial_cmp(&a.rrf_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        info!(
            "RRF: Combined into {} unique results",
            combined_results.len()
        );

        combined_results
    }

    /// Combine with intersection strategy
    /// Only return results that appear in ALL source sets
    pub fn combine_intersect(
        &self,
        result_sets: Vec<(String, Vec<SearchResult>)>,
        min_sources: usize,
    ) -> Vec<RRFResult> {
        let combined = self.combine(result_sets);

        // Filter to only results appearing in min_sources
        let filtered: Vec<RRFResult> = combined
            .into_iter()
            .filter(|r| r.source_count >= min_sources)
            .collect();

        info!(
            "RRF Intersect: {} results appear in {} or more sources",
            filtered.len(),
            min_sources
        );

        filtered
    }
}

impl Default for RRFEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rrf_basic() {
        let engine = RRFEngine::new();

        // Create test result sets
        let vector_results = vec![
            SearchResult {
                id: "doc1".to_string(),
                rank: 0,
                original_score: Some(0.95),
                source: "vector".to_string(),
                data: serde_json::json!({"title": "Doc 1"}),
            },
            SearchResult {
                id: "doc2".to_string(),
                rank: 1,
                original_score: Some(0.85),
                source: "vector".to_string(),
                data: serde_json::json!({"title": "Doc 2"}),
            },
        ];

        let text_results = vec![
            SearchResult {
                id: "doc2".to_string(), // Appears in both!
                rank: 0,
                original_score: Some(10.5),
                source: "text".to_string(),
                data: serde_json::json!({"title": "Doc 2"}),
            },
            SearchResult {
                id: "doc3".to_string(),
                rank: 1,
                original_score: Some(8.3),
                source: "text".to_string(),
                data: serde_json::json!({"title": "Doc 3"}),
            },
        ];

        let result_sets = vec![
            ("vector".to_string(), vector_results),
            ("text".to_string(), text_results),
        ];

        let combined = engine.combine(result_sets);

        // doc2 should rank highest (appears in both with good ranks)
        assert_eq!(combined.len(), 3);
        assert_eq!(combined[0].id, "doc2");
        assert_eq!(combined[0].source_count, 2);

        // Verify RRF score for doc2:
        // vector: 1/(60+0) = 0.01667
        // text:   1/(60+0) = 0.01667
        // total:  0.03334
        assert!((combined[0].rrf_score - 0.03334).abs() < 0.001);
    }

    #[test]
    fn test_rrf_intersect() {
        let engine = RRFEngine::new();

        let set1 = vec![
            SearchResult {
                id: "doc1".to_string(),
                rank: 0,
                original_score: Some(1.0),
                source: "set1".to_string(),
                data: serde_json::Value::Null,
            },
            SearchResult {
                id: "doc2".to_string(),
                rank: 1,
                original_score: Some(0.9),
                source: "set1".to_string(),
                data: serde_json::Value::Null,
            },
        ];

        let set2 = vec![
            SearchResult {
                id: "doc2".to_string(), // Only doc2 in both
                rank: 0,
                original_score: Some(1.0),
                source: "set2".to_string(),
                data: serde_json::Value::Null,
            },
        ];

        let results = engine.combine_intersect(
            vec![("set1".to_string(), set1), ("set2".to_string(), set2)],
            2, // Must appear in 2 sources
        );

        // Only doc2 should be returned
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "doc2");
    }
}
