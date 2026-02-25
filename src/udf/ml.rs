//! ML UDF Engine - ONNX model inference

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelMetadata {
    pub name: String,
    pub model_type: ModelType,
    pub input_shape: Vec<usize>,
    pub output_shape: Vec<usize>,
    pub loaded_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ModelType {
    Classification,
    Regression,
    Embedding,
    ObjectDetection,
    Custom,
}

pub struct MlEngine {
    models: Arc<RwLock<HashMap<String, (Vec<u8>, ModelMetadata)>>>,
}

impl MlEngine {
    pub fn new() -> Self {
        Self {
            models: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn load_model(&self, name: &str, model_bytes: Vec<u8>, metadata: ModelMetadata) -> Result<(), String> {
        let mut models = self.models.write().await;
        models.insert(name.to_string(), (model_bytes, metadata));
        Ok(())
    }

    /// Run inference on a model (supports SmartCore Logistic Regression)
    pub async fn predict(&self, model_name: &str, input: &[f32]) -> Result<Vec<f32>, String> {
        let models = self.models.read().await;
        let (model_bytes, _metadata) = models.get(model_name)
            .ok_or_else(|| format!("Model {} not found", model_name))?;

        // 1. Try to deserialize as SmartCore Logistic Regression
        // We use bincode for serialization typically with SmartCore
        if let Ok(lr) = bincode::deserialize::<smartcore::linear::logistic_regression::LogisticRegression<f32, i32, smartcore::linalg::basic::matrix::DenseMatrix<f32>, Vec<i32>>>(&model_bytes) {
             use smartcore::api::Predictor;
             // Reshape input to Matrix (1 row)
             let matrix_input = smartcore::linalg::basic::matrix::DenseMatrix::from_2d_array(&[input]);
             
             match lr.predict(&matrix_input) {
                 Ok(preds) => {
                     // Cast i32 labels to f32
                     return Ok(preds.iter().map(|&x| x as f32).collect());
                 }
                 Err(e) => return Err(format!("SmartCore prediction failed: {:?}", e)),
             }
        }
        
        // 2. If deserialization fails, return error (No simulations!)
        Err("Failed to deserialize model: Invalid SmartCore format".to_string())
    }
    
    pub async fn list_models(&self) -> Vec<String> {
        let models = self.models.read().await;
        models.keys().cloned().collect()
    }
    
    pub async fn get_metadata(&self, model_name: &str) -> Option<ModelMetadata> {
        let models = self.models.read().await;
        models.get(model_name).map(|(_, meta)| meta.clone())
    }

    pub async fn unload_model(&self, model_name: &str) -> Result<(), String> {
        let mut models = self.models.write().await;
        models.remove(model_name)
            .ok_or_else(|| format!("Model {} not found", model_name))?;
        Ok(())
    }
}

impl Default for MlEngine {
    fn default() -> Self { Self::new() }
}
