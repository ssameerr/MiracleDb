//! Candle ML Inference Engine
//!
//! Provides ML inference using Candle (Rust-native ML framework)
//! Alternative to ONNX Runtime with better Rust integration
//!
//! Features:
//! - Text embedding generation for vector search
//! - Model inference for predictions
//! - Batch processing support
//! - CPU and CUDA support
//! - Async-to-sync bridge for DataFusion UDF compatibility

use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokenizers::Tokenizer;
use tokio::runtime::Runtime;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Async-to-sync bridge
// ---------------------------------------------------------------------------

lazy_static! {
    /// Dedicated Tokio runtime for Candle inference operations.
    ///
    /// DataFusion UDFs are called from a synchronous context, but some Candle
    /// operations (e.g. model downloads via `download_model_from_hf`) are async.
    /// This static runtime lets UDF implementations call `block_on_candle()` to
    /// drive those futures to completion without requiring an outer async context.
    static ref CANDLE_RUNTIME: Runtime = Runtime::new()
        .expect("Failed to create Tokio runtime for Candle UDFs");
}

/// Synchronously runs an async Candle operation from a sync context (e.g., DataFusion UDF).
///
/// # Example
/// ```ignore
/// let embedding = block_on_candle(engine.download_model_from_hf(id, name, dir));
/// ```
pub fn block_on_candle<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CANDLE_RUNTIME.block_on(future)
}

/// ML Model type
#[derive(Debug, Clone)]
pub enum ModelType {
    /// Sentence embedding model (e.g., all-MiniLM-L6-v2)
    SentenceEmbedding {
        config_path: PathBuf,
        weights_path: PathBuf,
        tokenizer_path: PathBuf,
    },
    /// Custom BERT-like model
    Bert {
        config_path: PathBuf,
        weights_path: PathBuf,
        tokenizer_path: PathBuf,
    },
}

/// Loaded model with tokenizer
pub struct LoadedModel {
    model_type: ModelType,
    bert_model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
}

/// Candle ML Engine
pub struct CandleEngine {
    models: Arc<RwLock<HashMap<String, Arc<LoadedModel>>>>,
    device: Device,
}

impl CandleEngine {
    /// Create new Candle engine
    /// Automatically selects CUDA if available, otherwise CPU
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let device = if candle_core::utils::cuda_is_available() {
            info!("CUDA available - using GPU for inference");
            Device::new_cuda(0)?
        } else {
            info!("CUDA not available - using CPU for inference");
            Device::Cpu
        };

        Ok(Self {
            models: Arc::new(RwLock::new(HashMap::new())),
            device,
        })
    }

    /// Load a model
    pub fn load_model(
        &self,
        name: &str,
        model_type: ModelType,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Loading model: {}", name);

        let loaded_model = match &model_type {
            ModelType::SentenceEmbedding {
                config_path,
                weights_path,
                tokenizer_path,
            }
            | ModelType::Bert {
                config_path,
                weights_path,
                tokenizer_path,
            } => {
                // Load config
                let config_json = std::fs::read_to_string(config_path)?;
                let config: BertConfig = serde_json::from_str(&config_json)?;

                // Load weights
                let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[weights_path.clone()], DType::F32, &self.device)? };
                let bert_model = BertModel::load(vb, &config)?;

                // Load tokenizer
                let tokenizer = Tokenizer::from_file(tokenizer_path)
                    .map_err(|e| format!("Failed to load tokenizer: {:?}", e))?;

                LoadedModel {
                    model_type: model_type.clone(),
                    bert_model,
                    tokenizer,
                    device: self.device.clone(),
                }
            }
        };

        let mut models = self.models.write().unwrap();
        models.insert(name.to_string(), Arc::new(loaded_model));

        info!("Model loaded successfully: {}", name);
        Ok(())
    }

    /// Generate embeddings for a single text
    pub fn generate_embedding(
        &self,
        model_name: &str,
        text: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        let models = self.models.read().unwrap();
        let model = models
            .get(model_name)
            .ok_or_else(|| format!("Model not found: {}", model_name))?;

        debug!("Generating embedding for text: {}", text);

        // Tokenize
        let encoding = model
            .tokenizer
            .encode(text, true)
            .map_err(|e| format!("Tokenization failed: {:?}", e))?;

        let token_ids = encoding.get_ids();
        let attention_mask = encoding.get_attention_mask();

        // Convert to tensors
        let token_ids_tensor = Tensor::new(token_ids, &model.device)?
            .unsqueeze(0)?; // Add batch dimension

        let attention_mask_tensor = Tensor::new(attention_mask, &model.device)?
            .unsqueeze(0)?;

        // Forward pass
        let outputs = model
            .bert_model
            .forward(&token_ids_tensor, &attention_mask_tensor, None)?;

        // Mean pooling over sequence dimension
        let embedding = self.mean_pooling(&outputs, &attention_mask_tensor)?;

        // Convert to Vec<f32>
        let embedding_vec = embedding
            .squeeze(0)?
            .to_vec1::<f32>()?;

        debug!("Generated embedding with {} dimensions", embedding_vec.len());

        Ok(embedding_vec)
    }

    /// Generate embeddings for multiple texts (batch)
    pub fn generate_embeddings_batch(
        &self,
        model_name: &str,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        info!("Generating embeddings for {} texts", texts.len());

        let mut embeddings = Vec::new();
        for text in texts {
            let embedding = self.generate_embedding(model_name, text)?;
            embeddings.push(embedding);
        }

        Ok(embeddings)
    }

    /// Mean pooling helper
    fn mean_pooling(
        &self,
        token_embeddings: &Tensor,
        attention_mask: &Tensor,
    ) -> Result<Tensor, Box<dyn std::error::Error>> {
        // Expand attention mask to match embeddings shape
        let attention_mask_expanded = attention_mask
            .unsqueeze(2)?
            .expand(token_embeddings.shape())?
            .to_dtype(token_embeddings.dtype())?;

        // Multiply embeddings by attention mask
        let masked_embeddings = token_embeddings.mul(&attention_mask_expanded)?;

        // Sum over sequence dimension
        let sum_embeddings = masked_embeddings.sum(1)?;

        // Sum attention mask
        let sum_mask = attention_mask_expanded
            .sum(1)?
            .clamp(1e-9, f32::MAX as f64)?; // Avoid division by zero

        // Divide to get mean
        let mean_pooled = sum_embeddings.div(&sum_mask)?;

        Ok(mean_pooled)
    }

    /// Download model from HuggingFace Hub
    ///
    /// Downloads config.json, model.safetensors, and tokenizer.json from HuggingFace
    ///
    /// # Arguments
    /// * `model_id` - HuggingFace model ID (e.g., "sentence-transformers/all-MiniLM-L6-v2")
    /// * `model_name` - Local name to use for this model
    /// * `cache_dir` - Directory to store downloaded models
    ///
    /// # Returns
    /// * `Ok(())` if successful
    pub async fn download_model_from_hf(
        &self,
        model_id: &str,
        model_name: &str,
        cache_dir: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Downloading model '{}' from HuggingFace: {}", model_name, model_id);

        let cache_path = cache_dir.as_ref();
        let model_dir = cache_path.join(model_name);

        // Create model directory
        std::fs::create_dir_all(&model_dir)?;

        // HuggingFace CDN base URL
        let base_url = format!("https://huggingface.co/{}/resolve/main", model_id);

        // Files to download
        let files = vec![
            ("config.json", "config.json"),
            ("model.safetensors", "model.safetensors"),
            ("tokenizer.json", "tokenizer.json"),
        ];

        // Download each file
        for (filename, local_filename) in files {
            let url = format!("{}/{}", base_url, filename);
            let local_path = model_dir.join(local_filename);

            // Skip if already exists
            if local_path.exists() {
                info!("File already exists, skipping: {}", filename);
                continue;
            }

            info!("Downloading: {} -> {:?}", url, local_path);

            let response = reqwest::get(&url).await?;

            if !response.status().is_success() {
                return Err(format!(
                    "Failed to download {}: HTTP {}",
                    filename,
                    response.status()
                )
                .into());
            }

            let bytes = response.bytes().await?;
            std::fs::write(&local_path, bytes)?;

            info!("Downloaded: {} ({} bytes)", filename, local_path.metadata()?.len());
        }

        info!("Model download complete: {}", model_name);

        // Automatically load the model
        let model_type = ModelType::Bert {
            config_path: model_dir.join("config.json"),
            weights_path: model_dir.join("model.safetensors"),
            tokenizer_path: model_dir.join("tokenizer.json"),
        };

        self.load_model(model_name, model_type)?;

        Ok(())
    }

    /// List loaded models
    pub fn list_models(&self) -> Vec<String> {
        let models = self.models.read().unwrap();
        models.keys().cloned().collect()
    }

    /// Unload a model
    pub fn unload_model(&self, name: &str) -> bool {
        let mut models = self.models.write().unwrap();
        models.remove(name).is_some()
    }
}

impl Default for CandleEngine {
    fn default() -> Self {
        Self::new().expect("Failed to create Candle engine")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candle_engine_creation() {
        let engine = CandleEngine::new();
        assert!(engine.is_ok());
    }

    #[test]
    fn test_list_models_empty() {
        let engine = CandleEngine::new().unwrap();
        assert_eq!(engine.list_models().len(), 0);
    }

    // Note: Full embedding tests require model files
    // These would be integration tests with actual models
}
