//! Nucleus REST API - Reactive Data System Endpoints
//!
//! Provides REST endpoints for atoms, spin, electrons, and triggers

use axum::{
    Router,
    routing::{get, post, put, delete},
    extract::{Extension, Path, Json, Query},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::nucleus::{NucleusSystem, Atom, TriggerCondition};

// =============================================================================
// Request/Response Types
// =============================================================================

#[derive(Deserialize)]
pub struct CreateAtomRequest {
    pub id: String,
    pub nucleus: serde_json::Value,
    pub spin: Option<f64>,
    pub electrons: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct UpdateNucleusRequest {
    pub nucleus: serde_json::Value,
}

#[derive(Deserialize)]
pub struct UpdateSpinRequest {
    pub spin: f64,
}

#[derive(Deserialize)]
pub struct AddElectronRequest {
    pub electron_id: String,
}

#[derive(Deserialize)]
pub struct CreateTriggerRequest {
    pub id: String,
    pub condition_type: String,
    pub threshold: Option<f64>,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Serialize)]
pub struct AtomResponse {
    pub id: String,
    pub nucleus: serde_json::Value,
    pub electrons: Vec<String>,
    pub spin: f64,
    pub version: u64,
    pub last_updated: i64,
}

impl From<Atom> for AtomResponse {
    fn from(atom: Atom) -> Self {
        Self {
            id: atom.id,
            nucleus: atom.nucleus,
            electrons: atom.electrons,
            spin: atom.spin,
            version: atom.version,
            last_updated: atom.last_updated,
        }
    }
}

#[derive(Serialize)]
pub struct TriggerResponse {
    pub id: String,
    pub condition: String,
    pub active: bool,
}

// =============================================================================
// Routes
// =============================================================================

pub fn routes(nucleus: Arc<NucleusSystem>) -> Router {
    Router::new()
        // Atoms
        .route("/atoms", get(list_atoms).post(create_atom))
        .route("/atoms/:id", get(get_atom).put(update_atom).delete(delete_atom))
        .route("/atoms/:id/nucleus", put(update_nucleus))
        .route("/atoms/:id/spin", put(update_spin).get(get_spin))
        .route("/atoms/:id/electrons", get(list_electrons).post(add_electron))
        .route("/atoms/:id/electrons/:electron_id", delete(remove_electron))
        
        // Triggers
        .route("/triggers", get(list_triggers).post(create_trigger))
        .route("/triggers/:id", delete(delete_trigger))
        
        // Computed
        .route("/computed", get(list_computed))
        
        // Topology
        .route("/topology", get(get_topology))
        
        // Bulk
        .route("/bulk/atoms", post(bulk_create_atoms))
        .route("/bulk/spin", post(bulk_update_spin))
        
        // Stats
        .route("/stats", get(get_stats))
        
        .layer(Extension(nucleus))
}

// =============================================================================
// Atom Handlers
// =============================================================================

async fn list_atoms(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let atoms = nucleus.list_atoms().await;
    let limit = params.limit.unwrap_or(100);
    let offset = params.offset.unwrap_or(0);
    
    let result: Vec<AtomResponse> = atoms
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|a| a.into())
        .collect();
    
    Json(result)
}

async fn create_atom(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Json(req): Json<CreateAtomRequest>,
) -> impl IntoResponse {
    let id = req.id.clone();
    nucleus.create_atom(&req.id, req.nucleus.clone()).await;
    
    if let Some(spin) = req.spin {
        nucleus.update_spin(&id, spin).await;
    }
    
    if let Some(electrons) = req.electrons {
        for electron_id in electrons {
            nucleus.add_electron(&id, &electron_id).await;
        }
    }
    
    (StatusCode::CREATED, Json(serde_json::json!({
        "id": id,
        "message": "Atom created"
    })))
}

async fn get_atom(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match nucleus.get_atom(&id).await {
        Some(atom) => (StatusCode::OK, Json(serde_json::json!(AtomResponse::from(atom)))),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "Atom not found"}))),
    }
}

async fn update_atom(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateNucleusRequest>,
) -> impl IntoResponse {
    if nucleus.update_nucleus(&id, req.nucleus).await {
        Json(serde_json::json!({"message": "Atom updated"}))
    } else {
        Json(serde_json::json!({"error": "Atom not found"}))
    }
}

async fn delete_atom(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if nucleus.delete_atom(&id).await {
        Json(serde_json::json!({"message": "Atom deleted"}))
    } else {
        Json(serde_json::json!({"error": "Atom not found"}))
    }
}

async fn update_nucleus(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateNucleusRequest>,
) -> impl IntoResponse {
    if nucleus.update_nucleus(&id, req.nucleus).await {
        Json(serde_json::json!({"message": "Nucleus updated"}))
    } else {
        Json(serde_json::json!({"error": "Atom not found"}))
    }
}

async fn update_spin(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateSpinRequest>,
) -> impl IntoResponse {
    if nucleus.update_spin(&id, req.spin).await {
        Json(serde_json::json!({"spin": req.spin, "message": "Spin updated"}))
    } else {
        Json(serde_json::json!({"error": "Atom not found"}))
    }
}

async fn get_spin(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match nucleus.get_atom(&id).await {
        Some(atom) => Json(serde_json::json!({"id": id, "spin": atom.spin})),
        None => Json(serde_json::json!({"error": "Atom not found"})),
    }
}

// =============================================================================
// Electron Handlers
// =============================================================================

async fn list_electrons(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match nucleus.get_atom(&id).await {
        Some(atom) => Json(serde_json::json!({"electrons": atom.electrons})),
        None => Json(serde_json::json!({"error": "Atom not found"})),
    }
}

async fn add_electron(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
    Json(req): Json<AddElectronRequest>,
) -> impl IntoResponse {
    if nucleus.add_electron(&id, &req.electron_id).await {
        Json(serde_json::json!({"message": "Electron added"}))
    } else {
        Json(serde_json::json!({"error": "Atom not found"}))
    }
}

async fn remove_electron(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path((id, electron_id)): Path<(String, String)>,
) -> impl IntoResponse {
    if nucleus.remove_electron(&id, &electron_id).await {
        Json(serde_json::json!({"message": "Electron removed"}))
    } else {
        Json(serde_json::json!({"error": "Atom not found"}))
    }
}

// =============================================================================
// Trigger Handlers
// =============================================================================

async fn list_triggers(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
) -> impl IntoResponse {
    let triggers = nucleus.list_triggers().await;
    Json(triggers.into_iter().map(|(id, cond)| {
        TriggerResponse {
            id,
            condition: format!("{:?}", cond),
            active: true,
        }
    }).collect::<Vec<_>>())
}

async fn create_trigger(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Json(req): Json<CreateTriggerRequest>,
) -> impl IntoResponse {
    let trigger_id = req.id.clone();
    let condition = match req.condition_type.as_str() {
        "spin_above" => TriggerCondition::SpinAbove(req.threshold.unwrap_or(0.0)),
        "spin_below" => TriggerCondition::SpinBelow(req.threshold.unwrap_or(0.0)),
        "any_change" => TriggerCondition::AnyChange,
        field => TriggerCondition::FieldChanged(field.to_string()),
    };
    
    let tid = trigger_id.clone();
    nucleus.add_trigger(
        &trigger_id,
        condition,
        move |atom, change| {
            tracing::info!("Trigger {} fired for atom {}: {:?}", 
                tid, atom.id, change.change_type);
        }
    ).await;
    
    (StatusCode::CREATED, Json(serde_json::json!({
        "id": trigger_id,
        "message": "Trigger created"
    })))
}

async fn delete_trigger(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    nucleus.remove_trigger(&id).await;
    Json(serde_json::json!({"message": "Trigger deleted"}))
}

// =============================================================================
// Other Handlers
// =============================================================================

async fn list_computed(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
) -> impl IntoResponse {
    let computed = nucleus.list_computed().await;
    Json(computed)
}

async fn get_topology(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
) -> impl IntoResponse {
    let topology = nucleus.get_topology().await;
    Json(topology)
}

async fn get_stats(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
) -> impl IntoResponse {
    let count = nucleus.count().await;
    let triggers = nucleus.list_triggers().await;
    Json(serde_json::json!({
        "atom_count": count,
        "trigger_count": triggers.len(),
    }))
}

// =============================================================================
// Bulk Operations
// =============================================================================

#[derive(Deserialize)]
pub struct BulkCreateRequest {
    pub atoms: Vec<CreateAtomRequest>,
}

async fn bulk_create_atoms(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Json(req): Json<BulkCreateRequest>,
) -> impl IntoResponse {
    let mut created = 0;
    for atom_req in req.atoms {
        let id = atom_req.id.clone();
        nucleus.create_atom(&id, atom_req.nucleus).await;
        if let Some(spin) = atom_req.spin {
            nucleus.update_spin(&id, spin).await;
        }
        created += 1;
    }
    
    Json(serde_json::json!({
        "created": created,
        "message": "Bulk create complete"
    }))
}

#[derive(Deserialize)]
pub struct BulkSpinUpdate {
    pub id: String,
    pub spin: f64,
}

#[derive(Deserialize)]
pub struct BulkSpinRequest {
    pub updates: Vec<BulkSpinUpdate>,
}

async fn bulk_update_spin(
    Extension(nucleus): Extension<Arc<NucleusSystem>>,
    Json(req): Json<BulkSpinRequest>,
) -> impl IntoResponse {
    let mut updated = 0;
    for update in req.updates {
        if nucleus.update_spin(&update.id, update.spin).await {
            updated += 1;
        }
    }
    
    Json(serde_json::json!({
        "updated": updated,
        "message": "Bulk spin update complete"
    }))
}
