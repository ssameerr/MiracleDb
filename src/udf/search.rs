//! Search UDFs - Reciprocal Rank Fusion and Hybrid Search
//!
//! This module provides User-Defined Functions for advanced search ranking,
//! particularly Reciprocal Rank Fusion (RRF) which combines multiple search
//! result rankings into a unified score.
//!
//! RRF is useful for:
//! - Combining vector search (semantic) with text search (keyword)
//! - Merging results from multiple ranking algorithms
//! - Creating hybrid search systems

use datafusion::arrow::array::{Array, ArrayRef, Float32Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Default RRF k constant (typically 60)
/// This constant controls the impact of rank position
const DEFAULT_RRF_K: f64 = 60.0;

/// Return type function for RRF
fn rrf_return_type(_: &[DataType]) -> Result<Arc<DataType>> {
    Ok(Arc::new(DataType::Float64))
}

/// Create RRF (Reciprocal Rank Fusion) UDF
///
/// RRF combines multiple ranked result lists into a single ranking.
/// Formula: RRF_score = Σ (1 / (k + rank_i))
///
/// Usage:
///   SELECT id, rrf(vector_rank, text_rank) as hybrid_score
///   FROM (
///     SELECT id,
///            ROW_NUMBER() OVER (ORDER BY vector_score DESC) as vector_rank,
///            ROW_NUMBER() OVER (ORDER BY text_score DESC) as text_rank
///     FROM results
///   )
///   ORDER BY hybrid_score DESC;
///
/// Arguments:
///   - rank1, rank2, ..., rankN: Integer rank positions (1-based)
///   - Optional k parameter (defaults to 60)
///
/// Returns: Float64 - Combined RRF score (higher is better)
pub fn create_rrf_udf() -> ScalarUDF {
    let rrf = make_scalar_function(move |args: &[ArrayRef]| {
        if args.is_empty() {
            return Err(DataFusionError::Execution(
                "rrf() requires at least 1 rank argument".to_string(),
            ));
        }

        let num_rows = args[0].len();
        let num_ranks = args.len();

        debug!("RRF calculation: {} rows, {} rank inputs", num_rows, num_ranks);

        // RRF constant k (default 60)
        let k = DEFAULT_RRF_K;

        let mut rrf_scores: Vec<f64> = vec![0.0; num_rows];

        // For each rank input
        for rank_array in args {
            // Support both Int64 and Float64 ranks
            let ranks: Vec<Option<i64>> = if let Some(int_arr) = rank_array.as_any().downcast_ref::<Int64Array>() {
                (0..num_rows).map(|i| {
                    if !int_arr.is_null(i) {
                        Some(int_arr.value(i))
                    } else {
                        None
                    }
                }).collect()
            } else if let Some(float_arr) = rank_array.as_any().downcast_ref::<Float64Array>() {
                (0..num_rows).map(|i| {
                    if !float_arr.is_null(i) {
                        Some(float_arr.value(i) as i64)
                    } else {
                        None
                    }
                }).collect()
            } else {
                return Err(DataFusionError::Execution(
                    "rrf() rank arguments must be INT or FLOAT".to_string(),
                ));
            };

            // Add RRF contribution from this rank list
            for (row_idx, rank_opt) in ranks.iter().enumerate() {
                if let Some(rank) = rank_opt {
                    if *rank > 0 {
                        // RRF formula: 1 / (k + rank)
                        let contribution = 1.0 / (k + (*rank as f64));
                        rrf_scores[row_idx] += contribution;
                    }
                }
            }
        }

        debug!("RRF scores computed for {} rows", num_rows);

        // Return as Float64Array
        Ok(Arc::new(Float64Array::from(rrf_scores)) as ArrayRef)
    });

    ScalarUDF::new(
        "rrf",
        &Signature::one_of(
            vec![
                TypeSignature::Variadic(vec![DataType::Int64]),
                TypeSignature::Variadic(vec![DataType::Float64]),
                TypeSignature::VariadicAny,
            ],
            Volatility::Immutable,
        ),
        &(Arc::new(rrf_return_type) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &rrf,
    )
}

/// Create RRF with custom k parameter
///
/// Usage:
///   SELECT id, rrf_k(vector_rank, text_rank, 30.0) as hybrid_score
///   FROM ...
///
/// Arguments:
///   - rank1, rank2, ..., rankN-1: Integer rank positions
///   - k: Float64 constant (last argument)
pub fn create_rrf_k_udf() -> ScalarUDF {
    let rrf_k = make_scalar_function(move |args: &[ArrayRef]| {
        if args.len() < 2 {
            return Err(DataFusionError::Execution(
                "rrf_k() requires at least 1 rank argument and k parameter".to_string(),
            ));
        }

        let num_rows = args[0].len();
        let num_rank_inputs = args.len() - 1;

        // Last argument is k parameter
        let k_array = &args[args.len() - 1];
        let k = if let Some(float_arr) = k_array.as_any().downcast_ref::<Float64Array>() {
            float_arr.value(0)
        } else if let Some(float_arr) = k_array.as_any().downcast_ref::<Float32Array>() {
            float_arr.value(0) as f64
        } else {
            return Err(DataFusionError::Execution(
                "rrf_k() last argument (k) must be FLOAT".to_string(),
            ));
        };

        debug!("RRF_K calculation: k={}, {} rank inputs", k, num_rank_inputs);

        let mut rrf_scores: Vec<f64> = vec![0.0; num_rows];

        // Process rank inputs (all except last which is k)
        for rank_array in &args[..num_rank_inputs] {
            let ranks: Vec<Option<i64>> = if let Some(int_arr) = rank_array.as_any().downcast_ref::<Int64Array>() {
                (0..int_arr.len()).map(|i| {
                    if !int_arr.is_null(i) {
                        Some(int_arr.value(i))
                    } else {
                        None
                    }
                }).collect()
            } else if let Some(float_arr) = rank_array.as_any().downcast_ref::<Float64Array>() {
                (0..float_arr.len()).map(|i| {
                    if !float_arr.is_null(i) {
                        Some(float_arr.value(i) as i64)
                    } else {
                        None
                    }
                }).collect()
            } else {
                return Err(DataFusionError::Execution(
                    "rrf_k() rank arguments must be INT or FLOAT".to_string(),
                ));
            };

            for (row_idx, rank_opt) in ranks.iter().enumerate() {
                if let Some(rank) = rank_opt {
                    if *rank > 0 {
                        let contribution = 1.0 / (k + (*rank as f64));
                        rrf_scores[row_idx] += contribution;
                    }
                }
            }
        }

        Ok(Arc::new(Float64Array::from(rrf_scores)) as ArrayRef)
    });

    ScalarUDF::new(
        "rrf_k",
        &Signature::variadic_any(Volatility::Immutable),
        &(Arc::new(rrf_return_type) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &rrf_k,
    )
}

/// Hybrid search UDF - Combines vector and text scores using RRF
///
/// This is a convenience function that wraps RRF specifically for hybrid search.
///
/// Usage:
///   SELECT
///     id,
///     title,
///     hybrid_search(vector_score, text_score) as relevance
///   FROM documents
///   ORDER BY relevance DESC
///   LIMIT 10;
///
/// Arguments:
///   - vector_score: Float64 - Cosine similarity or distance (higher = more similar)
///   - text_score: Float64 - BM25 or text relevance score (higher = more relevant)
///   - Optional weights: (w1, w2) to weight vector vs text importance
///
/// Returns: Float64 - Combined relevance score
pub fn create_hybrid_search_udf() -> ScalarUDF {
    let hybrid = make_scalar_function(move |args: &[ArrayRef]| {
        if args.len() < 2 {
            return Err(DataFusionError::Execution(
                "hybrid_search() requires vector_score and text_score".to_string(),
            ));
        }

        let num_rows = args[0].len();

        // Extract vector scores
        let vector_scores = args[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("First argument must be vector_score (FLOAT)".to_string())
            })?;

        // Extract text scores
        let text_scores = args[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("Second argument must be text_score (FLOAT)".to_string())
            })?;

        // Optional weights (default: equal weighting)
        let (w_vector, w_text) = if args.len() >= 4 {
            let w1 = args[2]
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(0))
                .unwrap_or(0.5);
            let w2 = args[3]
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(0))
                .unwrap_or(0.5);
            (w1, w2)
        } else {
            (0.5, 0.5)
        };

        debug!(
            "Hybrid search: {} rows, weights=(vector:{}, text:{})",
            num_rows, w_vector, w_text
        );

        // Compute hybrid scores
        let mut hybrid_scores: Vec<f64> = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let vector_score = if !vector_scores.is_null(i) {
                vector_scores.value(i)
            } else {
                0.0
            };

            let text_score = if !text_scores.is_null(i) {
                text_scores.value(i)
            } else {
                0.0
            };

            // Weighted combination
            let hybrid_score = (w_vector * vector_score) + (w_text * text_score);
            hybrid_scores.push(hybrid_score);
        }

        Ok(Arc::new(Float64Array::from(hybrid_scores)) as ArrayRef)
    });

    ScalarUDF::new(
        "hybrid_search",
        &Signature::variadic_any(Volatility::Immutable),
        &(Arc::new(rrf_return_type) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &hybrid,
    )
}

/// Register all search UDFs
pub fn register_search_functions(ctx: &datafusion::prelude::SessionContext) {
    // RRF functions
    ctx.register_udf(create_rrf_udf());
    ctx.register_udf(create_rrf_k_udf());

    // Hybrid search
    ctx.register_udf(create_hybrid_search_udf());

    info!("Registered search UDFs: rrf, rrf_k, hybrid_search");
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;

    #[test]
    fn test_rrf_basic() {
        // Test RRF with simple ranks
        let rank1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let rank2 = Arc::new(Int64Array::from(vec![2, 1, 3])) as ArrayRef;

        let udf = create_rrf_udf();
        let result = (udf.fun())(&[rank1, rank2]).unwrap();

        let scores = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Document 0: rank 1 in list 1, rank 2 in list 2
        // RRF = 1/(60+1) + 1/(60+2) = 0.01639 + 0.01613 = 0.03252
        assert!((scores.value(0) - 0.03252).abs() < 0.001);

        // Document 1: rank 2 in list 1, rank 1 in list 2
        // RRF = 1/(60+2) + 1/(60+1) = 0.01613 + 0.01639 = 0.03252
        assert!((scores.value(1) - 0.03252).abs() < 0.001);

        // Document 2: rank 3 in both lists
        // RRF = 1/(60+3) + 1/(60+3) = 0.01587 * 2 = 0.03175
        assert!((scores.value(2) - 0.03175).abs() < 0.001);
    }

    #[test]
    fn test_rrf_with_nulls() {
        // Test RRF with missing ranks (nulls)
        let rank1 = Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef;
        let rank2 = Arc::new(Int64Array::from(vec![Some(2), None, Some(1)])) as ArrayRef;

        let udf = create_rrf_udf();
        let result = (udf.fun())(&[rank1, rank2]).unwrap();

        let scores = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Document 0: rank 1 + rank 2 = both present
        assert!(scores.value(0) > 0.03);

        // Document 1: only rank 2 present (rank1=2)
        assert!(scores.value(1) > 0.015 && scores.value(1) < 0.02);

        // Document 2: only rank 2 present (rank=1)
        assert!(scores.value(2) > 0.015 && scores.value(2) < 0.02);
    }

    #[test]
    fn test_hybrid_search_equal_weights() {
        let vector_scores = Arc::new(Float64Array::from(vec![0.9, 0.7, 0.5])) as ArrayRef;
        let text_scores = Arc::new(Float64Array::from(vec![0.6, 0.8, 0.9])) as ArrayRef;

        let udf = create_hybrid_search_udf();
        let result = (udf.fun())(&[vector_scores, text_scores]).unwrap();

        let scores = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Equal weights: 0.5 * vector + 0.5 * text
        assert!((scores.value(0) - 0.75).abs() < 0.01); // 0.5*0.9 + 0.5*0.6 = 0.75
        assert!((scores.value(1) - 0.75).abs() < 0.01); // 0.5*0.7 + 0.5*0.8 = 0.75
        assert!((scores.value(2) - 0.70).abs() < 0.01); // 0.5*0.5 + 0.5*0.9 = 0.70
    }
}
