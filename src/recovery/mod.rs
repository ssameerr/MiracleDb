//! Recovery Module - Database recovery and point-in-time recovery
//!
//! Includes:
//! - Database recovery (archive, crash, standby)
//! - Shamir secret sharing for key recovery

pub mod shamir;
pub mod backup;
pub use backup::{BackupManager, Snapshot};

pub use shamir::{
    ShamirRecovery,
    RecoveryShare,
    EncryptedShare,
    Guardian,
    SocialRecoveryConfig,
    KeyRecoverySession,
    ShamirError,
};

use std::path::PathBuf;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Recovery state
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum RecoveryState {
    #[default]
    NotInRecovery,
    InArchiveRecovery,
    InCrashRecovery,
    InStandbyMode,
}

/// Recovery target type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RecoveryTarget {
    Immediate,
    Time(i64),
    Xid(u64),
    Lsn(u64),
    Name(String),
}

/// Recovery configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryConfig {
    pub restore_command: Option<String>,
    pub archive_cleanup_command: Option<String>,
    pub recovery_target: Option<RecoveryTarget>,
    pub recovery_target_timeline: TimelineTarget,
    pub recovery_target_inclusive: bool,
    pub recovery_target_action: RecoveryTargetAction,
    pub standby_mode: bool,
    pub primary_conninfo: Option<String>,
    pub primary_slot_name: Option<String>,
    pub trigger_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum TimelineTarget {
    #[default]
    Latest,
    Current,
    Specific(u32),
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum RecoveryTargetAction {
    #[default]
    Pause,
    Promote,
    Shutdown,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            restore_command: None,
            archive_cleanup_command: None,
            recovery_target: None,
            recovery_target_timeline: TimelineTarget::default(),
            recovery_target_inclusive: true,
            recovery_target_action: RecoveryTargetAction::default(),
            standby_mode: false,
            primary_conninfo: None,
            primary_slot_name: None,
            trigger_file: None,
        }
    }
}

/// Recovery progress
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RecoveryProgress {
    pub state: RecoveryState,
    pub started_at: Option<i64>,
    pub current_lsn: u64,
    pub target_lsn: Option<u64>,
    pub replayed_lsn: u64,
    pub received_lsn: u64,
    pub latest_checkpoint_lsn: u64,
    pub recovery_percentage: Option<f64>,
}

/// Recovery manager
pub struct RecoveryManager {
    state: RwLock<RecoveryState>,
    config: RwLock<RecoveryConfig>,
    progress: RwLock<RecoveryProgress>,
}

impl RecoveryManager {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(RecoveryState::NotInRecovery),
            config: RwLock::new(RecoveryConfig::default()),
            progress: RwLock::new(RecoveryProgress::default()),
        }
    }

    /// Start recovery
    pub async fn start(&self, config: RecoveryConfig) -> Result<(), String> {
        let mut state = self.state.write().await;

        if *state != RecoveryState::NotInRecovery {
            return Err("Already in recovery".to_string());
        }

        *state = if config.standby_mode {
            RecoveryState::InStandbyMode
        } else {
            RecoveryState::InArchiveRecovery
        };

        let mut cfg = self.config.write().await;
        *cfg = config;

        let mut progress = self.progress.write().await;
        progress.state = *state;
        progress.started_at = Some(chrono::Utc::now().timestamp());

        Ok(())
    }

    /// Update progress
    pub async fn update_progress(&self, replayed_lsn: u64, received_lsn: u64) {
        let mut progress = self.progress.write().await;
        progress.replayed_lsn = replayed_lsn;
        progress.received_lsn = received_lsn;
        progress.current_lsn = replayed_lsn;

        if let Some(target) = progress.target_lsn {
            if target > 0 {
                progress.recovery_percentage = Some(
                    (replayed_lsn as f64 / target as f64) * 100.0
                );
            }
        }
    }

    /// Check if recovery target reached
    pub async fn is_target_reached(&self) -> bool {
        let config = self.config.read().await;
        let progress = self.progress.read().await;

        match &config.recovery_target {
            Some(RecoveryTarget::Lsn(target)) => progress.replayed_lsn >= *target,
            Some(RecoveryTarget::Time(target)) => {
                chrono::Utc::now().timestamp() >= *target
            }
            Some(RecoveryTarget::Immediate) => true,
            _ => false,
        }
    }

    /// End recovery
    pub async fn end(&self) -> Result<(), String> {
        let mut state = self.state.write().await;

        if *state == RecoveryState::NotInRecovery {
            return Err("Not in recovery".to_string());
        }

        *state = RecoveryState::NotInRecovery;

        let mut progress = self.progress.write().await;
        progress.state = RecoveryState::NotInRecovery;

        Ok(())
    }

    /// Promote to primary
    pub async fn promote(&self) -> Result<(), String> {
        let state = self.state.read().await;

        if *state != RecoveryState::InStandbyMode {
            return Err("Can only promote from standby mode".to_string());
        }

        self.end().await
    }

    /// Get state
    pub async fn get_state(&self) -> RecoveryState {
        *self.state.read().await
    }

    /// Get progress
    pub async fn get_progress(&self) -> RecoveryProgress {
        self.progress.read().await.clone()
    }

    /// Is in recovery
    pub async fn is_in_recovery(&self) -> bool {
        *self.state.read().await != RecoveryState::NotInRecovery
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod backup_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_snapshot() {
        let dir = TempDir::new().unwrap();
        let mgr = BackupManager::new(dir.path().to_str().unwrap());
        let result = mgr.create_snapshot("test_db").await;
        assert!(result.is_ok(), "snapshot failed: {:?}", result);
        let snap = result.unwrap();
        assert!(!snap.id.is_empty());
        assert!(snap.path.exists());
    }

    #[tokio::test]
    async fn test_list_snapshots() {
        let dir = TempDir::new().unwrap();
        let mgr = BackupManager::new(dir.path().to_str().unwrap());
        mgr.create_snapshot("test_db").await.unwrap();
        mgr.create_snapshot("test_db").await.unwrap();
        let snaps = mgr.list_snapshots("test_db").await.unwrap();
        assert_eq!(snaps.len(), 2);
    }

    #[tokio::test]
    async fn test_snapshot_compressed() {
        let dir = TempDir::new().unwrap();
        let mgr = BackupManager::new(dir.path().to_str().unwrap());
        let snap = mgr.create_snapshot("test_db").await.unwrap();
        let metadata = std::fs::metadata(&snap.path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[tokio::test]
    async fn test_restore_from_snapshot() {
        let dir = TempDir::new().unwrap();
        let mgr = BackupManager::new(dir.path().to_str().unwrap());
        let snap = mgr.create_snapshot("test_db").await.unwrap();
        let restore_dir = TempDir::new().unwrap();
        let result = mgr.restore_snapshot(&snap.id, restore_dir.path().to_str().unwrap()).await;
        assert!(result.is_ok(), "restore failed: {:?}", result);
    }

    #[tokio::test]
    async fn test_backup_metadata_persisted() {
        let dir = TempDir::new().unwrap();
        let mgr = BackupManager::new(dir.path().to_str().unwrap());
        let snap = mgr.create_snapshot("test_db").await.unwrap();
        let mgr2 = BackupManager::new(dir.path().to_str().unwrap());
        let snaps = mgr2.list_snapshots("test_db").await.unwrap();
        assert!(snaps.iter().any(|s| s.id == snap.id));
    }
}
