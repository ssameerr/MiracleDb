//! REST API endpoints for WASM UDF management

use axum::{
    extract::{State, Path},
    http::StatusCode,
    Json,
    routing::{get, post, delete},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::udf::wasm::{WasmUdfConfig, get_wasm_manager, load_and_register_wasm_udf};
use crate::engine::MiracleEngine;

/// Response for UDF operations
#[derive(Serialize)]
pub struct UdfResponse {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Request to register a WASM UDF
#[derive(Deserialize)]
pub struct RegisterUdfRequest {
    pub name: String,
    pub function_name: String,
    pub return_type: String,  // "int64", "float64", "string"
    pub arg_types: Vec<String>,
}

/// List all loaded WASM modules
pub async fn list_modules(
    State(engine): State<Arc<MiracleEngine>>,
) -> Result<Json<UdfResponse>, (StatusCode, Json<UdfResponse>)> {
    let manager_lock = get_wasm_manager();
    let manager_opt = manager_lock.read().unwrap();

    if manager_opt.is_none() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(UdfResponse {
                success: false,
                message: "WASM manager not initialized".to_string(),
                data: None,
            }),
        ));
    }

    // TODO: Implement module listing
    // For now, return empty list

    Ok(Json(UdfResponse {
        success: true,
        message: "Modules listed successfully".to_string(),
        data: Some(serde_json::json!({
            "modules": []
        })),
    }))
}

/// Upload and load a WASM module from a file path
#[derive(Deserialize)]
pub struct UploadModuleRequest {
    pub file_path: String,
}

pub async fn upload_module(
    State(_engine): State<Arc<MiracleEngine>>,
    Path(module_name): Path<String>,
    Json(request): Json<UploadModuleRequest>,
) -> Result<Json<UdfResponse>, (StatusCode, Json<UdfResponse>)> {
    let manager_lock = get_wasm_manager();
    let mut manager_opt = manager_lock.write().unwrap();

    let manager = manager_opt
        .as_mut()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(UdfResponse {
                    success: false,
                    message: "WASM manager not initialized".to_string(),
                    data: None,
                }),
            )
        })?;

    // Load the module from file
    manager.load_module_from_file(&module_name, &request.file_path).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(UdfResponse {
                success: false,
                message: format!("Failed to load WASM module: {}", e),
                data: None,
            }),
        )
    })?;

    // Get exports
    let exports = manager.get_exports(&module_name).unwrap_or_default();

    Ok(Json(UdfResponse {
        success: true,
        message: format!("Module '{}' loaded successfully", module_name),
        data: Some(serde_json::json!({
            "module": module_name,
            "exports": exports,
        })),
    }))
}

/// Register a WASM function as a UDF
pub async fn register_udf(
    State(engine): State<Arc<MiracleEngine>>,
    Path(module_name): Path<String>,
    Json(request): Json<RegisterUdfRequest>,
) -> Result<Json<UdfResponse>, (StatusCode, Json<UdfResponse>)> {
    // Parse return type
    let return_type = match request.return_type.to_lowercase().as_str() {
        "int64" | "integer" | "i64" => crate::udf::wasm::WasmDataType::Int64,
        "float64" | "double" | "f64" => crate::udf::wasm::WasmDataType::Float64,
        "string" | "str" => crate::udf::wasm::WasmDataType::String,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(UdfResponse {
                    success: false,
                    message: format!("Invalid return type: {}", request.return_type),
                    data: None,
                }),
            ));
        }
    };

    // Parse argument types
    let arg_types: Result<Vec<_>, _> = request
        .arg_types
        .iter()
        .map(|t| match t.to_lowercase().as_str() {
            "int64" | "integer" | "i64" => Ok(crate::udf::wasm::WasmDataType::Int64),
            "float64" | "double" | "f64" => Ok(crate::udf::wasm::WasmDataType::Float64),
            "string" | "str" => Ok(crate::udf::wasm::WasmDataType::String),
            _ => Err(format!("Invalid argument type: {}", t)),
        })
        .collect();

    let arg_types = arg_types.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(UdfResponse {
                success: false,
                message: e,
                data: None,
            }),
        )
    })?;

    // Create config
    let config = WasmUdfConfig {
        name: request.name.clone(),
        wasm_path: module_name.clone(),
        function_name: request.function_name.clone(),
        return_type,
        arg_types,
    };

    // Register the UDF
    load_and_register_wasm_udf(&engine.ctx, config).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(UdfResponse {
                success: false,
                message: format!("Failed to register UDF: {}", e),
                data: None,
            }),
        )
    })?;

    Ok(Json(UdfResponse {
        success: true,
        message: format!("UDF '{}' registered successfully", request.name),
        data: Some(serde_json::json!({
            "name": request.name,
            "module": module_name,
            "function": request.function_name,
        })),
    }))
}

/// Get exports from a loaded module
pub async fn get_module_exports(
    State(engine): State<Arc<MiracleEngine>>,
    Path(module_name): Path<String>,
) -> Result<Json<UdfResponse>, (StatusCode, Json<UdfResponse>)> {
    let manager_lock = get_wasm_manager();
    let manager_opt = manager_lock.read().unwrap();

    let manager = manager_opt
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(UdfResponse {
                    success: false,
                    message: "WASM manager not initialized".to_string(),
                    data: None,
                }),
            )
        })?;

    let exports = manager.get_exports(&module_name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(UdfResponse {
                success: false,
                message: format!("Module '{}' not found", module_name),
                data: None,
            }),
        )
    })?;

    Ok(Json(UdfResponse {
        success: true,
        message: "Exports retrieved successfully".to_string(),
        data: Some(serde_json::json!({
            "module": module_name,
            "exports": exports,
        })),
    }))
}

/// Get information about a WASM function's signature
pub async fn get_function_info(
    State(_engine): State<Arc<MiracleEngine>>,
    Path((module_name, func_name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let manager_lock = get_wasm_manager();
    let manager_opt = manager_lock.read().unwrap();

    let manager = manager_opt
        .as_ref()
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "WASM manager not initialized"
                }))
            )
        })?;

    let sig = manager.get_function_signature(&module_name, &func_name)
        .map_err(|e| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": format!("Function '{}' not found: {}", func_name, e)
                }))
            )
        })?;

    Ok(Json(serde_json::json!({
        "module": module_name,
        "function": func_name,
        "arity": sig.params.len(),
        "param_types": sig.params.iter()
            .map(|t| format!("{:?}", t))
            .collect::<Vec<_>>(),
        "return_type": format!("{:?}", sig.results[0]),
    })))
}

/// Create the UDF management router
pub fn udf_router(engine: Arc<MiracleEngine>) -> Router {
    Router::new()
        .route("/modules", get(list_modules))
        .route("/modules/:module_name", post(upload_module))
        .route("/modules/:module_name/exports", get(get_module_exports))
        .route("/modules/:module_name/register", post(register_udf))
        .route("/modules/:module_name/functions/:func_name", get(get_function_info))
        .with_state(engine)
}
