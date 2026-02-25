//! ML Model Management API
//!
//! Provides REST endpoints for managing ML models:
//! - Download models from HuggingFace
//! - List loaded models
//! - Get model information
//! - Unload models
//! - Test embedding generation

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
    response::IntoResponse,
    routing::{delete, get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

/// Create ML API router
pub fn routes(engine: Arc<crate::engine::MiracleEngine>) -> Router {
    Router::new()
        .route("/models/download", post(download_model))
        .route("/models", get(list_models))
        .route("/models/:name", get(get_model_info))
        .route("/models/:name", delete(unload_model))
        .route("/embed", post(embed))
        .with_state(engine)
}

/// Request to download a model from HuggingFace
#[derive(Debug, Deserialize)]
pub struct DownloadModelRequest {
    /// HuggingFace model ID (e.g., "sentence-transformers/all-MiniLM-L6-v2")
    pub model_id: String,
    /// Local name to use for the model
    pub name: String,
    /// Optional cache directory (defaults to ./data/models)
    #[serde(default = "default_cache_dir")]
    pub cache_dir: String,
}

fn default_cache_dir() -> String {
    "./data/models".to_string()
}

/// Response after downloading a model
#[derive(Debug, Serialize)]
pub struct DownloadModelResponse {
    pub success: bool,
    pub message: String,
    pub model_name: String,
    pub model_id: String,
}

/// Request to generate embeddings
#[derive(Debug, Deserialize)]
pub struct EmbedRequest {
    /// Model name to use
    pub model: String,
    /// Text or texts to embed
    #[serde(flatten)]
    pub input: EmbedInput,
}

/// Input can be single text or batch
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum EmbedInput {
    Single { text: String },
    Batch { texts: Vec<String> },
}

/// Response with embeddings
#[derive(Debug, Serialize)]
pub struct EmbedResponse {
    pub model: String,
    pub embeddings: Vec<Vec<f32>>,
    pub dimensions: usize,
    pub count: usize,
}

/// Model information
#[derive(Debug, Serialize)]
pub struct ModelInfo {
    pub name: String,
    pub loaded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<usize>,
}

/// List of models
#[derive(Debug, Serialize)]
pub struct ModelListResponse {
    pub models: Vec<String>,
    pub count: usize,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// Download a model from HuggingFace
#[cfg(feature = "nlp")]
pub async fn download_model(
    State(engine): State<Arc<crate::engine::MiracleEngine>>,
    Json(request): Json<DownloadModelRequest>,
) -> impl IntoResponse {
    info!(
        "Downloading model '{}' from HuggingFace: {}",
        request.name, request.model_id
    );

    match engine
        .candle_engine
        .download_model_from_hf(&request.model_id, &request.name, &request.cache_dir)
        .await
    {
        Ok(_) => {
            info!("Model '{}' downloaded successfully", request.name);
            (
                StatusCode::OK,
                Json(DownloadModelResponse {
                    success: true,
                    message: format!("Model '{}' downloaded and loaded successfully", request.name),
                    model_name: request.name,
                    model_id: request.model_id,
                }),
            )
        }
        Err(e) => {
            warn!("Failed to download model '{}': {}", request.name, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DownloadModelResponse {
                    success: false,
                    message: format!("Failed to download model: {}", e),
                    model_name: request.name,
                    model_id: request.model_id,
                }),
            )
        }
    }
}

#[cfg(not(feature = "nlp"))]
pub async fn download_model(
    State(_engine): State<Arc<crate::engine::MiracleEngine>>,
    Json(request): Json<DownloadModelRequest>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            error: "ML features not enabled".to_string(),
            details: Some("Compile with --features nlp to enable ML model management".to_string()),
        }),
    )
}

/// List all loaded models
#[cfg(feature = "nlp")]
pub async fn list_models(
    State(engine): State<Arc<crate::engine::MiracleEngine>>,
) -> impl IntoResponse {
    let models = engine.candle_engine.list_models();
    let count = models.len();

    info!("Listing {} loaded models", count);

    (
        StatusCode::OK,
        Json(ModelListResponse { models, count }),
    )
}

#[cfg(not(feature = "nlp"))]
pub async fn list_models(
    State(_engine): State<Arc<crate::engine::MiracleEngine>>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            error: "ML features not enabled".to_string(),
            details: Some("Compile with --features nlp to enable ML model management".to_string()),
        }),
    )
}

/// Get information about a specific model
#[cfg(feature = "nlp")]
pub async fn get_model_info(
    State(engine): State<Arc<crate::engine::MiracleEngine>>,
    Path(model_name): Path<String>,
) -> impl IntoResponse {
    let models = engine.candle_engine.list_models();
    let loaded = models.contains(&model_name);

    if loaded {
        info!("Model '{}' is loaded", model_name);
        (
            StatusCode::OK,
            Json(ModelInfo {
                name: model_name,
                loaded: true,
                dimensions: Some(384), // TODO: Get actual dimensions from model
            }),
        )
    } else {
        warn!("Model '{}' not found", model_name);
        (
            StatusCode::NOT_FOUND,
            Json(ModelInfo {
                name: model_name,
                loaded: false,
                dimensions: None,
            }),
        )
    }
}

#[cfg(not(feature = "nlp"))]
pub async fn get_model_info(
    State(_engine): State<Arc<crate::engine::MiracleEngine>>,
    Path(model_name): Path<String>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            error: "ML features not enabled".to_string(),
            details: Some("Compile with --features nlp to enable ML model management".to_string()),
        }),
    )
}

/// Unload a model
#[cfg(feature = "nlp")]
pub async fn unload_model(
    State(engine): State<Arc<crate::engine::MiracleEngine>>,
    Path(model_name): Path<String>,
) -> impl IntoResponse {
    info!("Unloading model '{}'", model_name);

    if engine.candle_engine.unload_model(&model_name) {
        info!("Model '{}' unloaded successfully", model_name);
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Model '{}' unloaded successfully", model_name)
            })),
        ).into_response()
    } else {
        warn!("Model '{}' not found", model_name);
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!("Model '{}' not found", model_name)
            })),
        ).into_response()
    }
}

#[cfg(not(feature = "nlp"))]
pub async fn unload_model(
    State(_engine): State<Arc<crate::engine::MiracleEngine>>,
    Path(model_name): Path<String>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            error: "ML features not enabled".to_string(),
            details: Some("Compile with --features nlp to enable ML model management".to_string()),
        }),
    )
}

/// Generate embeddings for testing
#[cfg(feature = "nlp")]
pub async fn embed(
    State(engine): State<Arc<crate::engine::MiracleEngine>>,
    Json(request): Json<EmbedRequest>,
) -> impl IntoResponse {
    info!("Generating embeddings with model '{}'", request.model);

    let embeddings = match request.input {
        EmbedInput::Single { text } => {
            match engine
                .candle_engine
                .generate_embedding(&request.model, &text)
            {
                Ok(emb) => vec![emb],
                Err(e) => {
                    warn!("Failed to generate embedding: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!("Failed to generate embedding: {}", e),
                            details: Some("Ensure the model is loaded first".to_string()),
                        }),
                    )
                        .into_response();
                }
            }
        }
        EmbedInput::Batch { texts } => {
            match engine
                .candle_engine
                .generate_embeddings_batch(&request.model, &texts)
            {
                Ok(embs) => embs,
                Err(e) => {
                    warn!("Failed to generate embeddings: {}", e);
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: format!("Failed to generate embeddings: {}", e),
                            details: Some("Ensure the model is loaded first".to_string()),
                        }),
                    )
                        .into_response();
                }
            }
        }
    };

    let dimensions = embeddings.first().map(|e| e.len()).unwrap_or(0);
    let count = embeddings.len();

    info!(
        "Generated {} embeddings with {} dimensions",
        count, dimensions
    );

    (
        StatusCode::OK,
        Json(EmbedResponse {
            model: request.model,
            embeddings,
            dimensions,
            count,
        }),
    )
        .into_response()
}

#[cfg(not(feature = "nlp"))]
pub async fn embed(
    State(_engine): State<Arc<crate::engine::MiracleEngine>>,
    Json(request): Json<EmbedRequest>,
) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse {
            error: "ML features not enabled".to_string(),
            details: Some("Compile with --features nlp to enable ML embeddings".to_string()),
        }),
    )
}
