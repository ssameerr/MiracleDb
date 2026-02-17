//! REST API endpoints for ONNX model management

use axum::{
    extract::{State, Path},
    http::StatusCode,
    Json,
    routing::{get, post, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::engine::MiracleEngine;

/// Response for ONNX operations
#[derive(Serialize)]
pub struct OnnxResponse {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Request to load a model from file
#[derive(Deserialize)]
pub struct LoadModelRequest {
    pub file_path: String,
}

/// Request to load a model from URL
#[derive(Deserialize)]
pub struct LoadModelFromUrlRequest {
    pub url: String,
}

/// List all loaded models
#[cfg(feature = "ml")]
pub async fn list_models(
    State(engine): State<Arc<MiracleEngine>>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    let models = engine.model_registry.list_models();

    Ok(Json(OnnxResponse {
        success: true,
        message: format!("Found {} models", models.len()),
        data: Some(serde_json::json!({
            "models": models,
        })),
    }))
}

#[cfg(not(feature = "ml"))]
pub async fn list_models(
    State(_engine): State<Arc<MiracleEngine>>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(OnnxResponse {
            success: false,
            message: "ONNX support not available - compile with 'ml' feature".to_string(),
            data: None,
        }),
    ))
}

/// Load a model from a file path
#[cfg(feature = "ml")]
pub async fn load_model(
    State(engine): State<Arc<MiracleEngine>>,
    Path(model_name): Path<String>,
    Json(request): Json<LoadModelRequest>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    engine.model_registry
        .load_model(&model_name, &request.file_path)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(OnnxResponse {
                    success: false,
                    message: format!("Failed to load model: {}", e),
                    data: None,
                }),
            )
        })?;

    // Update metrics
    crate::observability::metrics::ML_MODELS_LOADED.inc();

    Ok(Json(OnnxResponse {
        success: true,
        message: format!("Model '{}' loaded successfully", model_name),
        data: Some(serde_json::json!({
            "model": model_name,
            "path": request.file_path,
        })),
    }))
}

#[cfg(not(feature = "ml"))]
pub async fn load_model(
    State(_engine): State<Arc<MiracleEngine>>,
    Path(_model_name): Path<String>,
    Json(_request): Json<LoadModelRequest>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(OnnxResponse {
            success: false,
            message: "ONNX support not available - compile with 'ml' feature".to_string(),
            data: None,
        }),
    ))
}

/// Load a model from a URL
#[cfg(feature = "ml")]
pub async fn load_model_from_url(
    State(engine): State<Arc<MiracleEngine>>,
    Path(model_name): Path<String>,
    Json(request): Json<LoadModelFromUrlRequest>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    engine.model_registry
        .load_model_from_url(&model_name, &request.url)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(OnnxResponse {
                    success: false,
                    message: format!("Failed to load model from URL: {}", e),
                    data: None,
                }),
            )
        })?;

    // Update metrics
    crate::observability::metrics::ML_MODELS_LOADED.inc();

    Ok(Json(OnnxResponse {
        success: true,
        message: format!("Model '{}' loaded from URL successfully", model_name),
        data: Some(serde_json::json!({
            "model": model_name,
            "url": request.url,
        })),
    }))
}

#[cfg(not(feature = "ml"))]
pub async fn load_model_from_url(
    State(_engine): State<Arc<MiracleEngine>>,
    Path(_model_name): Path<String>,
    Json(_request): Json<LoadModelFromUrlRequest>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(OnnxResponse {
            success: false,
            message: "ONNX support not available - compile with 'ml' feature".to_string(),
            data: None,
        }),
    ))
}

/// Unload a model
#[cfg(feature = "ml")]
pub async fn unload_model(
    State(engine): State<Arc<MiracleEngine>>,
    Path(model_name): Path<String>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    engine.model_registry
        .unload_model(&model_name)
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(OnnxResponse {
                    success: false,
                    message: format!("Failed to unload model: {}", e),
                    data: None,
                }),
            )
        })?;

    // Update metrics
    crate::observability::metrics::ML_MODELS_LOADED.dec();

    Ok(Json(OnnxResponse {
        success: true,
        message: format!("Model '{}' unloaded successfully", model_name),
        data: Some(serde_json::json!({
            "model": model_name,
        })),
    }))
}

#[cfg(not(feature = "ml"))]
pub async fn unload_model(
    State(_engine): State<Arc<MiracleEngine>>,
    Path(_model_name): Path<String>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(OnnxResponse {
            success: false,
            message: "ONNX support not available - compile with 'ml' feature".to_string(),
            data: None,
        }),
    ))
}

/// Get information about a loaded model
#[cfg(feature = "ml")]
pub async fn get_model_info(
    State(engine): State<Arc<MiracleEngine>>,
    Path(model_name): Path<String>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    let model = engine.model_registry.get_model(&model_name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(OnnxResponse {
                success: false,
                message: format!("Model '{}' not found", model_name),
                data: None,
            }),
        )
    })?;

    // Get model metadata (lock mutex for access)
    let model_guard = model.lock().unwrap();
    let inputs = model_guard.inputs().len();
    let outputs = model_guard.outputs().len();

    // Get input/output names - ort 2.0 API uses name() method
    let input_names: Vec<String> = model_guard.inputs().iter().map(|input| input.name().to_string()).collect();
    let output_names: Vec<String> = model_guard.outputs().iter().map(|output| output.name().to_string()).collect();

    Ok(Json(OnnxResponse {
        success: true,
        message: format!("Model '{}' information retrieved", model_name),
        data: Some(serde_json::json!({
            "model": model_name,
            "num_inputs": inputs,
            "num_outputs": outputs,
            "input_names": input_names,
            "output_names": output_names,
        })),
    }))
}

#[cfg(not(feature = "ml"))]
pub async fn get_model_info(
    State(_engine): State<Arc<MiracleEngine>>,
    Path(_model_name): Path<String>,
) -> Result<Json<OnnxResponse>, (StatusCode, Json<OnnxResponse>)> {
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(OnnxResponse {
            success: false,
            message: "ONNX support not available - compile with 'ml' feature".to_string(),
            data: None,
        }),
    ))
}

/// Create the ONNX management router
pub fn onnx_router(engine: Arc<MiracleEngine>) -> Router {
    Router::new()
        .route("/models", get(list_models))
        .route("/models/:model_name", post(load_model))
        .route("/models/:model_name", delete(unload_model))
        .route("/models/:model_name/info", get(get_model_info))
        .route("/models/:model_name/url", post(load_model_from_url))
        .with_state(engine)
}
