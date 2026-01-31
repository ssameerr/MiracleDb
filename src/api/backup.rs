//! Backup & Recovery API Endpoints
//!
//! Provides REST API for backup and recovery operations:
//! - Create and manage backups
//! - Restore from backups
//! - Monitor recovery progress
//! - PITR (Point-in-Time Recovery)

use axum::{
    extract::{Path, State},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::backup::{BackupManager, BackupMetadata, BackupType, BackupSchedule};
use crate::backup::scheduler::BackupScheduler;
use crate::recovery::{RecoveryManager, RecoveryConfig, RecoveryTarget, RecoveryProgress};

/// Backup API state
#[derive(Clone)]
pub struct BackupApiState {
    pub backup_manager: Arc<BackupManager>,
    pub recovery_manager: Arc<RecoveryManager>,
    pub backup_scheduler: Arc<BackupScheduler>,
    pub data_dir: String,
}

/// Create backup request
#[derive(Debug, Deserialize)]
pub struct CreateBackupRequest {
    pub backup_type: BackupType,
    pub tables: Option<Vec<String>>,
    pub base_backup_id: Option<String>,
}

/// Create backup response
#[derive(Debug, Serialize)]
pub struct CreateBackupResponse {
    pub success: bool,
    pub backup: Option<BackupMetadata>,
    pub error: Option<String>,
}

/// List backups response
#[derive(Debug, Serialize)]
pub struct ListBackupsResponse {
    pub backups: Vec<BackupMetadata>,
    pub total_count: usize,
}

/// Restore backup request
#[derive(Debug, Deserialize)]
pub struct RestoreBackupRequest {
    pub target_dir: Option<String>,
}

/// Restore response
#[derive(Debug, Serialize)]
pub struct RestoreBackupResponse {
    pub success: bool,
    pub error: Option<String>,
    pub message: Option<String>,
}

/// Verify backup response
#[derive(Debug, Serialize)]
pub struct VerifyBackupResponse {
    pub valid: bool,
    pub error: Option<String>,
}

/// Start recovery request
#[derive(Debug, Deserialize)]
pub struct StartRecoveryRequest {
    pub recovery_target: Option<RecoveryTargetInput>,
    pub standby_mode: bool,
    pub primary_conninfo: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum RecoveryTargetInput {
    Immediate,
    Time { timestamp: i64 },
    Lsn { lsn: u64 },
}

impl From<RecoveryTargetInput> for RecoveryTarget {
    fn from(input: RecoveryTargetInput) -> Self {
        match input {
            RecoveryTargetInput::Immediate => RecoveryTarget::Immediate,
            RecoveryTargetInput::Time { timestamp } => RecoveryTarget::Time(timestamp),
            RecoveryTargetInput::Lsn { lsn } => RecoveryTarget::Lsn(lsn),
        }
    }
}

/// Recovery status response
#[derive(Debug, Serialize)]
pub struct RecoveryStatusResponse {
    pub in_recovery: bool,
    pub progress: Option<RecoveryProgress>,
}

/// Add schedule request
#[derive(Debug, Deserialize)]
pub struct AddScheduleRequest {
    pub name: String,
    pub backup_type: BackupType,
    pub cron_expression: String,
    pub retention_days: u32,
    pub enabled: bool,
}

// ========== Backup Endpoints ==========

/// Create a new backup
async fn create_backup(
    State(state): State<BackupApiState>,
    Json(req): Json<CreateBackupRequest>,
) -> Json<CreateBackupResponse> {
    let start_time = std::time::Instant::now();

    crate::observability::metrics::BACKUPS_ACTIVE.inc();

    let result = match req.backup_type {
        BackupType::Full => {
            let tables = req.tables.unwrap_or_default();
            state.backup_manager
                .create_full_backup(&state.data_dir, tables)
                .await
        }
        BackupType::Incremental => {
            if let Some(base_id) = req.base_backup_id {
                state.backup_manager
                    .create_incremental_backup(&base_id, &state.data_dir)
                    .await
            } else {
                Err("base_backup_id required for incremental backups".to_string())
            }
        }
        BackupType::Snapshot => {
            Err("Snapshot backups not yet implemented".to_string())
        }
    };

    crate::observability::metrics::BACKUPS_ACTIVE.dec();

    let duration = start_time.elapsed().as_secs_f64();
    let backup_type_str = format!("{:?}", req.backup_type);

    match result {
        Ok(backup) => {
            crate::observability::metrics::BACKUP_TOTAL
                .with_label_values(&[&backup_type_str, "success"])
                .inc();

            crate::observability::metrics::BACKUP_DURATION
                .with_label_values(&[&backup_type_str])
                .observe(duration);

            crate::observability::metrics::BACKUP_SIZE_BYTES
                .with_label_values(&[&backup.id])
                .set(backup.size_bytes as f64);

            crate::observability::metrics::BACKUPS_STORED.inc();

            tracing::info!(
                "Backup created: {} ({} bytes, {:.2}s)",
                backup.id,
                backup.size_bytes,
                duration
            );

            Json(CreateBackupResponse {
                success: true,
                backup: Some(backup),
                error: None,
            })
        }
        Err(e) => {
            crate::observability::metrics::BACKUP_TOTAL
                .with_label_values(&[&backup_type_str, "error"])
                .inc();

            tracing::error!("Backup creation failed: {}", e);

            Json(CreateBackupResponse {
                success: false,
                backup: None,
                error: Some(e),
            })
        }
    }
}

/// List all backups
async fn list_backups(State(state): State<BackupApiState>) -> Json<ListBackupsResponse> {
    let backups = state.backup_manager.list_backups().await;
    let total_count = backups.len();

    // Update stored backups metric
    crate::observability::metrics::BACKUPS_STORED.set(total_count as f64);

    Json(ListBackupsResponse {
        backups,
        total_count,
    })
}

/// Get backup by ID
async fn get_backup(
    State(state): State<BackupApiState>,
    Path(backup_id): Path<String>,
) -> Json<Option<BackupMetadata>> {
    let backups = state.backup_manager.list_backups().await;
    let backup = backups.into_iter().find(|b| b.id == backup_id);
    Json(backup)
}

/// Delete a backup
async fn delete_backup(
    State(state): State<BackupApiState>,
    Path(backup_id): Path<String>,
) -> Json<RestoreBackupResponse> {
    match state.backup_manager.delete_backup(&backup_id).await {
        Ok(_) => {
            crate::observability::metrics::BACKUPS_STORED.dec();
            tracing::info!("Backup deleted: {}", backup_id);

            Json(RestoreBackupResponse {
                success: true,
                error: None,
                message: Some(format!("Backup {} deleted successfully", backup_id)),
            })
        }
        Err(e) => {
            tracing::error!("Failed to delete backup {}: {}", backup_id, e);

            Json(RestoreBackupResponse {
                success: false,
                error: Some(e),
                message: None,
            })
        }
    }
}

/// Restore from a backup
async fn restore_backup(
    State(state): State<BackupApiState>,
    Path(backup_id): Path<String>,
    Json(req): Json<RestoreBackupRequest>,
) -> Json<RestoreBackupResponse> {
    let start_time = std::time::Instant::now();

    let target_dir = req.target_dir.unwrap_or_else(|| state.data_dir.clone());

    match state.backup_manager.restore(&backup_id, &target_dir).await {
        Ok(_) => {
            let duration = start_time.elapsed().as_secs_f64();

            crate::observability::metrics::RESTORE_TOTAL
                .with_label_values(&["success"])
                .inc();

            crate::observability::metrics::RESTORE_DURATION.observe(duration);

            tracing::info!(
                "Backup {} restored to {} ({:.2}s)",
                backup_id,
                target_dir,
                duration
            );

            Json(RestoreBackupResponse {
                success: true,
                error: None,
                message: Some(format!("Backup {} restored successfully", backup_id)),
            })
        }
        Err(e) => {
            crate::observability::metrics::RESTORE_TOTAL
                .with_label_values(&["error"])
                .inc();

            tracing::error!("Failed to restore backup {}: {}", backup_id, e);

            Json(RestoreBackupResponse {
                success: false,
                error: Some(e),
                message: None,
            })
        }
    }
}

/// Verify backup integrity
async fn verify_backup(
    State(state): State<BackupApiState>,
    Path(backup_id): Path<String>,
) -> Json<VerifyBackupResponse> {
    match state.backup_manager.verify_backup(&backup_id).await {
        Ok(valid) => {
            tracing::info!("Backup {} verification: {}", backup_id, if valid { "valid" } else { "invalid" });

            Json(VerifyBackupResponse {
                valid,
                error: None,
            })
        }
        Err(e) => {
            tracing::error!("Failed to verify backup {}: {}", backup_id, e);

            Json(VerifyBackupResponse {
                valid: false,
                error: Some(e),
            })
        }
    }
}

// ========== Recovery Endpoints ==========

/// Start recovery process
async fn start_recovery(
    State(state): State<BackupApiState>,
    Json(req): Json<StartRecoveryRequest>,
) -> Json<RestoreBackupResponse> {
    let config = RecoveryConfig {
        restore_command: None,
        archive_cleanup_command: None,
        recovery_target: req.recovery_target.map(|t| t.into()),
        recovery_target_timeline: crate::recovery::TimelineTarget::Latest,
        recovery_target_inclusive: true,
        recovery_target_action: crate::recovery::RecoveryTargetAction::Promote,
        standby_mode: req.standby_mode,
        primary_conninfo: req.primary_conninfo,
        primary_slot_name: None,
        trigger_file: None,
    };

    match state.recovery_manager.start(config).await {
        Ok(_) => {
            tracing::info!("Recovery started (standby_mode: {})", req.standby_mode);

            Json(RestoreBackupResponse {
                success: true,
                error: None,
                message: Some("Recovery started successfully".to_string()),
            })
        }
        Err(e) => {
            tracing::error!("Failed to start recovery: {}", e);

            Json(RestoreBackupResponse {
                success: false,
                error: Some(e),
                message: None,
            })
        }
    }
}

/// Get recovery status
async fn get_recovery_status(
    State(state): State<BackupApiState>,
) -> Json<RecoveryStatusResponse> {
    let in_recovery = state.recovery_manager.is_in_recovery().await;
    let progress = if in_recovery {
        Some(state.recovery_manager.get_progress().await)
    } else {
        None
    };

    Json(RecoveryStatusResponse {
        in_recovery,
        progress,
    })
}

/// Promote standby to primary
async fn promote_to_primary(
    State(state): State<BackupApiState>,
) -> Json<RestoreBackupResponse> {
    match state.recovery_manager.promote().await {
        Ok(_) => {
            tracing::info!("Standby promoted to primary");

            Json(RestoreBackupResponse {
                success: true,
                error: None,
                message: Some("Promoted to primary successfully".to_string()),
            })
        }
        Err(e) => {
            tracing::error!("Failed to promote to primary: {}", e);

            Json(RestoreBackupResponse {
                success: false,
                error: Some(e),
                message: None,
            })
        }
    }
}

/// End recovery
async fn end_recovery(
    State(state): State<BackupApiState>,
) -> Json<RestoreBackupResponse> {
    match state.recovery_manager.end().await {
        Ok(_) => {
            tracing::info!("Recovery ended");

            Json(RestoreBackupResponse {
                success: true,
                error: None,
                message: Some("Recovery ended successfully".to_string()),
            })
        }
        Err(e) => {
            tracing::error!("Failed to end recovery: {}", e);

            Json(RestoreBackupResponse {
                success: false,
                error: Some(e),
                message: None,
            })
        }
    }
}

// ========== Backup Schedule Endpoints ==========

/// Add a backup schedule
async fn add_backup_schedule(
    State(state): State<BackupApiState>,
    Json(req): Json<AddScheduleRequest>,
) -> Json<RestoreBackupResponse> {
    let schedule = BackupSchedule {
        name: req.name.clone(),
        backup_type: req.backup_type,
        cron_expression: req.cron_expression,
        retention_days: req.retention_days,
        enabled: req.enabled,
    };

    state.backup_scheduler.add_schedule(schedule).await;

    tracing::info!("Backup schedule added: {}", req.name);

    Json(RestoreBackupResponse {
        success: true,
        error: None,
        message: Some(format!("Schedule '{}' added successfully", req.name)),
    })
}

/// List all backup schedules
async fn list_backup_schedules(
    State(state): State<BackupApiState>,
) -> Json<Vec<BackupSchedule>> {
    let schedules = state.backup_scheduler.list_schedules().await;
    Json(schedules)
}

/// Remove a backup schedule
async fn remove_backup_schedule(
    State(state): State<BackupApiState>,
    Path(schedule_name): Path<String>,
) -> Json<RestoreBackupResponse> {
    let removed = state.backup_scheduler.remove_schedule(&schedule_name).await;

    if removed {
        tracing::info!("Backup schedule removed: {}", schedule_name);

        Json(RestoreBackupResponse {
            success: true,
            error: None,
            message: Some(format!("Schedule '{}' removed successfully", schedule_name)),
        })
    } else {
        Json(RestoreBackupResponse {
            success: false,
            error: Some(format!("Schedule '{}' not found", schedule_name)),
            message: None,
        })
    }
}

/// Create backup API router
pub fn routes(state: BackupApiState) -> Router {
    Router::new()
        // Backup operations
        .route("/backup/create", post(create_backup))
        .route("/backup/list", get(list_backups))
        .route("/backup/:id", get(get_backup))
        .route("/backup/:id", delete(delete_backup))
        .route("/backup/:id/restore", post(restore_backup))
        .route("/backup/:id/verify", post(verify_backup))
        // Recovery operations
        .route("/recovery/start", post(start_recovery))
        .route("/recovery/status", get(get_recovery_status))
        .route("/recovery/promote", post(promote_to_primary))
        .route("/recovery/end", post(end_recovery))
        // Backup schedules
        .route("/backup/schedule/add", post(add_backup_schedule))
        .route("/backup/schedule/list", get(list_backup_schedules))
        .route("/backup/schedule/:name", delete(remove_backup_schedule))
        .with_state(state)
}
