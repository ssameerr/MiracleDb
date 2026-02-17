use datafusion::arrow::array::{Array, ArrayRef, Float32Array, Float64Array, StringArray, ListArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

#[cfg(feature = "ml")]
use ort::{
    session::{builder::GraphOptimizationLevel, Session},
    value::Tensor,
};

/// Model registry for loaded ONNX models
pub struct ModelRegistry {
    #[cfg(feature = "ml")]
    models: RwLock<HashMap<String, Arc<Mutex<Session>>>>,
    #[cfg(not(feature = "ml"))]
    _phantom: std::marker::PhantomData<()>,
}

impl ModelRegistry {
    pub fn new() -> Result<Self> {
        #[cfg(feature = "ml")]
        {
            info!("Initialized ONNX Runtime environment");

            Ok(Self {
                models: RwLock::new(HashMap::new()),
            })
        }

        #[cfg(not(feature = "ml"))]
        {
            warn!("ONNX Runtime not available - compile with 'ml' feature");
            Ok(Self {
                _phantom: std::marker::PhantomData,
            })
        }
    }

    /// Load model from path
    pub fn load_model(&self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        #[cfg(feature = "ml")]
        {
            let path = path.as_ref();
            info!("Loading ONNX model '{}' from {:?}", name, path);

            if !path.exists() {
                return Err(DataFusionError::External(
                    format!("Model file not found: {:?}", path).into(),
                ));
            }

            // Build session with optimizations (ort 2.0 API)
            let session = Session::builder()
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .with_optimization_level(GraphOptimizationLevel::Level3)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
                .commit_from_file(path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Get model metadata - ort 2.0 API uses methods
            let inputs = session.inputs().len();
            let outputs = session.outputs().len();
            info!(
                "Loaded model '{}': {} inputs, {} outputs",
                name, inputs, outputs
            );

            // Store in registry (wrapped in Mutex for interior mutability)
            let mut models = self.models.write().unwrap();
            models.insert(name.to_string(), Arc::new(Mutex::new(session)));

            Ok(())
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX Runtime not available - compile with 'ml' feature".to_string(),
            ))
        }
    }

    /// Load model from URL (S3, HTTP, etc.)
    pub async fn load_model_from_url(&self, name: &str, url: &str) -> Result<()> {
        #[cfg(feature = "ml")]
        {
            info!("Downloading ONNX model '{}' from {}", name, url);

            // Download to temporary file
            let response = reqwest::get(url)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            if !response.status().is_success() {
                return Err(DataFusionError::External(
                    format!("Failed to download model: HTTP {}", response.status()).into(),
                ));
            }

            let bytes = response
                .bytes()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Create temporary file
            let temp_dir = std::env::temp_dir();
            let temp_path = temp_dir.join(format!("miracledb_model_{}_{}.onnx", name, uuid::Uuid::new_v4()));

            std::fs::write(&temp_path, bytes)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Load from temporary file
            self.load_model(name, &temp_path)?;

            // Clean up (keep file for caching, or delete after load)
            // std::fs::remove_file(&temp_path)?;

            Ok(())
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX Runtime not available - compile with 'ml' feature".to_string(),
            ))
        }
    }

    /// Get loaded model
    #[cfg(feature = "ml")]
    pub fn get_model(&self, name: &str) -> Option<Arc<Mutex<Session>>> {
        let models = self.models.read().unwrap();
        models.get(name).cloned()
    }

    #[cfg(not(feature = "ml"))]
    pub fn get_model(&self, _name: &str) -> Option<()> {
        None
    }

    /// List all loaded models
    pub fn list_models(&self) -> Vec<String> {
        #[cfg(feature = "ml")]
        {
            let models = self.models.read().unwrap();
            models.keys().cloned().collect()
        }

        #[cfg(not(feature = "ml"))]
        {
            vec![]
        }
    }

    /// Unload model
    pub fn unload_model(&self, name: &str) -> Result<()> {
        #[cfg(feature = "ml")]
        {
            let mut models = self.models.write().unwrap();
            if models.remove(name).is_some() {
                info!("Unloaded model '{}'", name);
                Ok(())
            } else {
                Err(DataFusionError::Execution(format!(
                    "Model '{}' not found",
                    name
                )))
            }
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX Runtime not available".to_string(),
            ))
        }
    }
}

/// Create `predict()` UDF for ONNX model inference
pub fn create_predict_udf(registry: Arc<ModelRegistry>) -> ScalarUDF {
    let predict = make_scalar_function(move |args: &[ArrayRef]| {
        #[cfg(feature = "ml")]
        {
            if args.is_empty() {
                return Err(DataFusionError::Execution(
                    "predict() requires at least 1 argument (model name)".to_string(),
                ));
            }

            // First argument is model name
            let model_name_arr = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("First argument must be model name (string)".to_string())
                })?;

            if model_name_arr.len() == 0 {
                return Err(DataFusionError::Execution("Empty model name array".to_string()));
            }

            let model_name = model_name_arr.value(0);

            // Get model from registry
            let session = registry
                .get_model(model_name)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("Model '{}' not found. Use load_model() first.", model_name))
                })?;

            // Extract input features from remaining arguments
            let num_rows = args[1].len();
            let num_features = args.len() - 1;

            debug!(
                "predict() inference: model={}, rows={}, features={}",
                model_name, num_rows, num_features
            );

            // Prepare input tensor (batch_size x num_features)
            let mut predictions = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                let mut input_features: Vec<f32> = Vec::with_capacity(num_features);

                // Collect features for this row
                for arg in &args[1..] {
                    let value = if let Some(float_arr) = arg.as_any().downcast_ref::<Float32Array>() {
                        float_arr.value(row_idx)
                    } else if let Some(float_arr) = arg.as_any().downcast_ref::<Float64Array>() {
                        float_arr.value(row_idx) as f32
                    } else {
                        return Err(DataFusionError::Execution(
                            "Input features must be numeric (Float32 or Float64)".to_string(),
                        ));
                    };

                    input_features.push(value);
                }

                // Create input tensor (1 x num_features) - ort 2.0 API
                let input_tensor = Tensor::from_array(([1, num_features], input_features))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Run inference - ort 2.0 API (assuming first input is named "input")
                // Lock the mutex to get mutable access to session
                let mut session_guard = session.lock().unwrap();
                let outputs = session_guard
                    .run(ort::inputs!["input" => input_tensor])
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Extract output (assuming first output, first element) - ort 2.0 API
                // try_extract_tensor returns (shape, data)
                let (_shape, output_data) = outputs[0]
                    .try_extract_tensor::<f32>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let prediction = output_data[0];

                predictions.push(prediction);
            }

            // Return predictions as Float32Array
            Ok(Arc::new(Float32Array::from(predictions)) as ArrayRef)
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX Runtime not available - compile with 'ml' feature".to_string(),
            ))
        }
    });

    ScalarUDF::new(
        "predict",
        &Signature::one_of(
            vec![
                TypeSignature::Variadic(vec![DataType::Utf8]),
                TypeSignature::VariadicAny,
            ],
            Volatility::Stable,
        ),
        &(Arc::new(|_args: &[DataType]| {
            Ok(Arc::new(DataType::Float32))
        }) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &predict,
    )
}

/// Create `onnx_predict()` UDF for ONNX model inference with array features
/// Syntax: onnx_predict('model_name', ARRAY[1.0, 2.0, 3.0])
pub fn create_onnx_predict_udf(registry: Arc<ModelRegistry>) -> ScalarUDF {
    let onnx_predict = make_scalar_function(move |args: &[ArrayRef]| {
        #[cfg(feature = "ml")]
        {
            if args.len() != 2 {
                return Err(DataFusionError::Execution(
                    "onnx_predict() requires exactly 2 arguments: model_name (string) and features (array of floats)".to_string(),
                ));
            }

            // First argument is model name
            let model_name_arr = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("First argument must be model name (string)".to_string())
                })?;

            // Second argument is features array
            let features_list = args[1]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("Second argument must be an array of floats".to_string())
                })?;

            let num_rows = model_name_arr.len();
            let mut predictions = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                // Handle NULL model names
                if model_name_arr.is_null(row_idx) {
                    predictions.push(None);
                    continue;
                }

                let model_name = model_name_arr.value(row_idx);

                // Handle NULL features
                if features_list.is_null(row_idx) {
                    predictions.push(None);
                    continue;
                }

                // Get model from registry
                let session = registry
                    .get_model(model_name)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Model '{}' not found. Use CREATE MODEL first.",
                            model_name
                        ))
                    })?;

                // Extract features from list array
                let features_array = features_list.value(row_idx);

                let input_features: Vec<f32> = if let Some(float32_arr) = features_array.as_any().downcast_ref::<Float32Array>() {
                    // Extract f32 values
                    (0..float32_arr.len())
                        .map(|i| float32_arr.value(i))
                        .collect()
                } else if let Some(float64_arr) = features_array.as_any().downcast_ref::<Float64Array>() {
                    // Extract f64 values and convert to f32
                    (0..float64_arr.len())
                        .map(|i| float64_arr.value(i) as f32)
                        .collect()
                } else {
                    return Err(DataFusionError::Execution(
                        "Features array must contain Float32 or Float64 values".to_string(),
                    ));
                };

                let num_features = input_features.len();

                debug!(
                    "onnx_predict() inference: model={}, features={}",
                    model_name, num_features
                );

                // Create input tensor (1 x num_features) - ort 2.0 API
                let input_tensor = Tensor::from_array(([1, num_features], input_features))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Run inference - ort 2.0 API
                let mut session_guard = session.lock().unwrap();
                let outputs = session_guard
                    .run(ort::inputs!["input" => input_tensor])
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Extract output (assuming first output, first element) - ort 2.0 API
                let (_shape, output_data) = outputs[0]
                    .try_extract_tensor::<f32>()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let prediction = output_data[0];
                predictions.push(Some(prediction));
            }

            // Return predictions as Float32Array (with nulls for NULL inputs)
            let prediction_values: Vec<Option<f32>> = predictions;
            Ok(Arc::new(Float32Array::from(prediction_values)) as ArrayRef)
        }

        #[cfg(not(feature = "ml"))]
        {
            Err(DataFusionError::NotImplemented(
                "ONNX Runtime not available - compile with 'ml' feature".to_string(),
            ))
        }
    });

    ScalarUDF::new(
        "onnx_predict",
        &Signature::exact(
            vec![
                DataType::Utf8, // model_name
                DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                    "item",
                    DataType::Float32,
                    true,
                ))), // features array
            ],
            Volatility::Stable,
        ),
        &(Arc::new(|_args: &[DataType]| {
            Ok(Arc::new(DataType::Float32))
        }) as Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>),
        &onnx_predict,
    )
}

/// Batch prediction for better throughput
#[cfg(feature = "ml")]
pub fn predict_batch(
    session: &mut Session,
    inputs: Vec<Vec<f32>>,
) -> Result<Vec<f32>> {
    let batch_size = inputs.len();
    if batch_size == 0 {
        return Ok(vec![]);
    }

    let feature_size = inputs[0].len();

    debug!(
        "Batch inference: batch_size={}, feature_size={}",
        batch_size, feature_size
    );

    // Flatten batch into single tensor
    let flat_inputs: Vec<f32> = inputs.into_iter().flatten().collect();

    // Create input tensor (batch_size x feature_size) - ort 2.0 API
    let input_tensor = Tensor::from_array(([batch_size, feature_size], flat_inputs))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Run inference - ort 2.0 API (assuming first input is named "input")
    let outputs = session
        .run(ort::inputs!["input" => input_tensor])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Extract outputs - ort 2.0 API
    // try_extract_tensor returns (shape, data)
    let (_shape, output_data) = outputs[0]
        .try_extract_tensor::<f32>()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(output_data.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_registry_creation() {
        let registry = ModelRegistry::new();
        assert!(registry.is_ok());
    }

    #[test]
    #[cfg(feature = "ml")]
    fn test_list_models_empty() {
        let registry = ModelRegistry::new().unwrap();
        assert_eq!(registry.list_models().len(), 0);
    }

    // Note: Integration tests with actual ONNX models should be in tests/
}
