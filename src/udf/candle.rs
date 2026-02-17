//! Candle ML UDFs - SQL functions for ML inference
//!
//! This module provides User-Defined Functions for ML inference using Candle:
//! - candle_embed(text) - Generate embeddings from text
//! - candle_predict(model, features) - Run inference on a model
//!
//! Candle provides native Rust ML inference without C++ dependencies.

use datafusion::arrow::array::{Array, ArrayRef, Float32Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::ml::candle_inference::block_on_candle;
use crate::ml::CandleEngine;

/// Create candle_embed UDF
///
/// Generates text embeddings using a loaded BERT model.
///
/// Usage:
///   SELECT id, title, candle_embed(title) as embedding
///   FROM documents;
///
///   -- With specific model
///   SELECT candle_embed('bert-base-uncased', 'Hello world') as embedding;
///
/// Arguments:
///   - text: VARCHAR - Text to embed (single argument)
///   - model_name, text: VARCHAR - Model name and text (two arguments)
///
/// Returns: ARRAY<FLOAT> - Embedding vector (typically 768 dimensions for BERT)
pub fn create_candle_embed_udf(engine: Arc<CandleEngine>) -> ScalarUDF {
    let engine_clone = engine.clone();

    let candle_embed = make_scalar_function(move |args: &[ArrayRef]| {
        if args.is_empty() || args.len() > 2 {
            return Err(DataFusionError::Execution(
                "candle_embed() requires 1 or 2 arguments: (text) or (model_name, text)".to_string(),
            ));
        }

        let num_rows = args[0].len();

        // Parse arguments
        let (model_name, text_array) = if args.len() == 1 {
            // Single argument: use default model
            ("default".to_string(), &args[0])
        } else {
            // Two arguments: model_name and text
            let model_array = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("First argument (model_name) must be VARCHAR".to_string())
                })?;

            let model = if !model_array.is_null(0) {
                model_array.value(0).to_string()
            } else {
                "default".to_string()
            };

            (model, &args[1])
        };

        // Extract text strings
        let texts = text_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("Text argument must be VARCHAR".to_string())
            })?;

        debug!("Candle embed: generating embeddings for {} texts using model '{}'", num_rows, model_name);

        // Generate embeddings for each text
        let embeddings: Vec<Vec<f32>> = texts
            .iter()
            .map(|text_opt| {
                if let Some(text) = text_opt {
                    // Generate embedding (sync call)
                    match engine_clone.generate_embedding(&model_name, text) {
                        Ok(emb) => emb,
                        Err(e) => {
                            warn!("Failed to generate embedding for '{}': {}", text, e);
                            // Return zero vector on error
                            // Note: Dimension depends on model (384 for MiniLM, 768 for BERT-base)
                            vec![0.0; 384]
                        }
                    }
                } else {
                    // Null text -> zero vector
                    vec![0.0; 384]
                }
            })
            .collect();

        debug!("Generated {} embeddings", embeddings.len());

        // Convert to Arrow Array
        // For now, flatten to single Float32Array
        // TODO: Return proper List<Float32> array for structured embeddings
        let flattened: Vec<f32> = embeddings.into_iter().flatten().collect();
        Ok(Arc::new(Float32Array::from(flattened)) as ArrayRef)
    });

    ScalarUDF::new(
        "candle_embed",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Volatile, // Volatile because ML inference is not deterministic
        ),
        &(Arc::new(|_args: &[DataType]| {
            // Return type: List<Float32> (embedding vector)
            // For simplicity, returning Float32Array for now
            Ok(Arc::new(DataType::Float32))
        }) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &candle_embed,
    )
}

/// Create candle_predict UDF
///
/// Runs inference on a loaded model with given features.
///
/// Usage:
///   SELECT id, candle_predict('classifier', ARRAY[1.2, 3.4, 5.6]) as prediction
///   FROM data;
///
/// Arguments:
///   - model_name: VARCHAR - Name of loaded model
///   - features: ARRAY<FLOAT> - Input features
///
/// Returns: FLOAT - Prediction result
pub fn create_candle_predict_udf(engine: Arc<CandleEngine>) -> ScalarUDF {
    let _engine_clone = engine.clone();

    let candle_predict = make_scalar_function(move |args: &[ArrayRef]| {
        if args.len() != 2 {
            return Err(DataFusionError::Execution(
                "candle_predict() requires 2 arguments: (model_name, features)".to_string(),
            ));
        }

        debug!("Candle predict: running inference");

        // Parse model_name
        let model_array = args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("First argument (model_name) must be VARCHAR".to_string())
            })?;

        let model_name = if !model_array.is_null(0) {
            model_array.value(0).to_string()
        } else {
            return Err(DataFusionError::Execution("Model name cannot be NULL".to_string()));
        };

        // Parse features (for now, expect Float32Array)
        let features = args[1]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("Second argument (features) must be ARRAY<FLOAT>".to_string())
            })?;

        // Note: Candle inference for prediction is not yet implemented
        // This would require a different model type (classifier, not just embeddings)
        warn!("candle_predict() called for model '{}' but prediction models not yet supported", model_name);

        let num_rows = features.len();
        let predictions: Vec<f32> = vec![0.0; num_rows];

        Ok(Arc::new(Float32Array::from(predictions)) as ArrayRef)
    });

    ScalarUDF::new(
        "candle_predict",
        &Signature::exact(
            vec![DataType::Utf8, DataType::Float32],
            Volatility::Volatile,
        ),
        &(Arc::new(|_args: &[DataType]| {
            Ok(Arc::new(DataType::Float32))
        }) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &candle_predict,
    )
}

/// Create candle_load_model UDF
///
/// Downloads and loads a model from HuggingFace Hub synchronously inside a
/// DataFusion UDF.  The underlying `download_model_from_hf` method is `async`;
/// this UDF bridges that gap using `block_on_candle()`.
///
/// Usage:
///   SELECT candle_load_model('sentence-transformers/all-MiniLM-L6-v2', 'minilm', '/tmp/models');
///
/// Arguments:
///   - hf_model_id: VARCHAR  - HuggingFace model identifier
///   - model_name:  VARCHAR  - Local alias for the loaded model
///   - cache_dir:   VARCHAR  - Directory to cache downloaded files
///
/// Returns: VARCHAR - Status message ("ok" or an error description)
pub fn create_candle_load_model_udf(engine: Arc<CandleEngine>) -> ScalarUDF {
    let engine_clone = engine.clone();

    let load_model_fn = make_scalar_function(move |args: &[ArrayRef]| {
        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "candle_load_model() requires 3 arguments: (hf_model_id, model_name, cache_dir)"
                    .to_string(),
            ));
        }

        let hf_model_id_arr = args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "First argument (hf_model_id) must be VARCHAR".to_string(),
                )
            })?;

        let model_name_arr = args[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Second argument (model_name) must be VARCHAR".to_string(),
                )
            })?;

        let cache_dir_arr = args[2]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Third argument (cache_dir) must be VARCHAR".to_string(),
                )
            })?;

        let num_rows = hf_model_id_arr.len();
        let mut results: Vec<Option<String>> = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            if hf_model_id_arr.is_null(i)
                || model_name_arr.is_null(i)
                || cache_dir_arr.is_null(i)
            {
                results.push(Some("error: NULL argument".to_string()));
                continue;
            }

            let hf_id = hf_model_id_arr.value(i);
            let model_name = model_name_arr.value(i);
            let cache_dir = cache_dir_arr.value(i);

            info!(
                "candle_load_model: downloading '{}' as '{}' into '{}'",
                hf_id, model_name, cache_dir
            );

            // Use the async-to-sync bridge to drive the async download.
            let result = block_on_candle(
                engine_clone.download_model_from_hf(hf_id, model_name, cache_dir),
            );

            match result {
                Ok(()) => {
                    info!("candle_load_model: '{}' loaded successfully", model_name);
                    results.push(Some("ok".to_string()));
                }
                Err(e) => {
                    warn!("candle_load_model: failed to load '{}': {}", model_name, e);
                    results.push(Some(format!("error: {}", e)));
                }
            }
        }

        Ok(Arc::new(StringArray::from(results)) as ArrayRef)
    });

    ScalarUDF::new(
        "candle_load_model",
        &Signature::exact(
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
            Volatility::Volatile,
        ),
        &(Arc::new(|_args: &[DataType]| Ok(Arc::new(DataType::Utf8)))
            as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &load_model_fn,
    )
}

/// Register Candle ML UDFs
///
/// This function registers all Candle-based ML UDFs with the DataFusion context.
/// Requires the 'nlp' feature to be enabled.
pub fn register_candle_functions(ctx: &datafusion::prelude::SessionContext, engine: Arc<CandleEngine>) {
    // Register embedding function
    ctx.register_udf(create_candle_embed_udf(engine.clone()));

    // Register prediction function
    ctx.register_udf(create_candle_predict_udf(engine.clone()));

    // Register model-loading function (uses async-to-sync bridge internally)
    ctx.register_udf(create_candle_load_model_udf(engine.clone()));

    info!("Registered Candle ML UDFs: candle_embed, candle_predict, candle_load_model");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candle_engine_creation() {
        // Test that CandleEngine can be created
        let engine = CandleEngine::new();
        assert!(engine.is_ok());
    }

    #[test]
    fn test_udf_signatures() {
        // Test that UDFs can be created with proper signatures
        let engine = Arc::new(CandleEngine::new().unwrap());
        let embed_udf = create_candle_embed_udf(engine.clone());
        assert_eq!(embed_udf.name(), "candle_embed");

        let predict_udf = create_candle_predict_udf(engine);
        assert_eq!(predict_udf.name(), "candle_predict");
    }
}
