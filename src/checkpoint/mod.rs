//! Checkpoint Module - Database checkpointing

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Checkpoint info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: u64,
    pub redo_lsn: u64,
    pub timeline: u32,
    pub prev_timeline: u32,
    pub full_page_writes: bool,
    pub next_xid: u64,
    pub next_oid: u64,
    pub next_multi: u64,
    pub oldest_xid: u64,
    pub oldest_active_xid: u64,
    pub checkpoint_time: i64,
    pub oldest_commit_ts_xid: u64,
    pub newest_commit_ts_xid: u64,
}

impl Checkpoint {
    pub fn new(id: u64, redo_lsn: u64) -> Self {
        Self {
            id,
            redo_lsn,
            timeline: 1,
            prev_timeline: 0,
            full_page_writes: true,
            next_xid: 1,
            next_oid: 10000,
            next_multi: 1,
            oldest_xid: 1,
            oldest_active_xid: 1,
            checkpoint_time: chrono::Utc::now().timestamp(),
            oldest_commit_ts_xid: 0,
            newest_commit_ts_xid: 0,
        }
    }
}

/// Checkpoint statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointStats {
    pub checkpoints_timed: u64,
    pub checkpoints_requested: u64,
    pub checkpoint_write_time: f64,
    pub checkpoint_sync_time: f64,
    pub buffers_checkpoint: u64,
    pub buffers_clean: u64,
    pub maxwritten_clean: u64,
    pub buffers_backend: u64,
    pub buffers_backend_fsync: u64,
    pub buffers_alloc: u64,
}

/// Checkpoint progress
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckpointProgress {
    pub phase: CheckpointPhase,
    pub blocks_total: u64,
    pub blocks_done: u64,
    pub percent_complete: f64,
    pub start_time: i64,
    pub estimated_completion: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum CheckpointPhase {
    #[default]
    NotRunning,
    ScanningBuffers,
    WritingDirtyBuffers,
    SyncingFiles,
    UpdatingControlFile,
    Complete,
}

/// Checkpoint request type
#[derive(Clone, Copy, Debug)]
pub enum CheckpointRequest {
    Immediate,
    RestartPoint,
    Force,
    FlushAll,
    EndOfRecovery,
}

/// Checkpoint manager
pub struct CheckpointManager {
    next_id: AtomicU64,
    last_checkpoint: RwLock<Option<Checkpoint>>,
    stats: RwLock<CheckpointStats>,
    progress: RwLock<CheckpointProgress>,
    checkpoint_timeout: u64,
    checkpoint_completion_target: f64,
}

impl CheckpointManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            last_checkpoint: RwLock::new(None),
            stats: RwLock::new(CheckpointStats::default()),
            progress: RwLock::new(CheckpointProgress::default()),
            checkpoint_timeout: 300, // 5 minutes
            checkpoint_completion_target: 0.9,
        }
    }

    /// Request a checkpoint
    pub async fn request(&self, request: CheckpointRequest) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let mut progress = self.progress.write().await;
        progress.phase = CheckpointPhase::ScanningBuffers;
        progress.start_time = chrono::Utc::now().timestamp();
        progress.blocks_done = 0;
        progress.blocks_total = 1000; // Simulated

        match request {
            CheckpointRequest::Force => {
                let mut stats = self.stats.write().await;
                stats.checkpoints_requested += 1;
            }
            CheckpointRequest::Immediate => {
                let mut stats = self.stats.write().await;
                stats.checkpoints_requested += 1;
            }
            _ => {
                let mut stats = self.stats.write().await;
                stats.checkpoints_timed += 1;
            }
        }

        id
    }

    /// Update progress
    pub async fn update_progress(&self, blocks_done: u64) {
        let mut progress = self.progress.write().await;
        progress.blocks_done = blocks_done;
        progress.percent_complete = if progress.blocks_total > 0 {
            (blocks_done as f64 / progress.blocks_total as f64) * 100.0
        } else {
            0.0
        };
    }

    /// Complete checkpoint
    pub async fn complete(&self, redo_lsn: u64) -> Checkpoint {
        let id = self.next_id.load(Ordering::SeqCst) - 1;
        let checkpoint = Checkpoint::new(id, redo_lsn);

        let mut last = self.last_checkpoint.write().await;
        *last = Some(checkpoint.clone());

        let mut progress = self.progress.write().await;
        progress.phase = CheckpointPhase::Complete;
        progress.percent_complete = 100.0;

        checkpoint
    }

    /// Get last checkpoint
    pub async fn get_last(&self) -> Option<Checkpoint> {
        self.last_checkpoint.read().await.clone()
    }

    /// Get progress
    pub async fn get_progress(&self) -> CheckpointProgress {
        self.progress.read().await.clone()
    }

    /// Get statistics
    pub async fn get_stats(&self) -> CheckpointStats {
        self.stats.read().await.clone()
    }

    /// Check if checkpoint is needed
    pub async fn needs_checkpoint(&self, current_lsn: u64, max_wal_size: u64) -> bool {
        if let Some(last) = self.get_last().await {
            let wal_accumulated = current_lsn.saturating_sub(last.redo_lsn);
            let time_elapsed = chrono::Utc::now().timestamp() - last.checkpoint_time;

            wal_accumulated > max_wal_size || time_elapsed > self.checkpoint_timeout as i64
        } else {
            true // No checkpoint yet
        }
    }

    /// Update buffer stats
    pub async fn add_buffer_stats(&self, checkpoint: u64, clean: u64, backend: u64) {
        let mut stats = self.stats.write().await;
        stats.buffers_checkpoint += checkpoint;
        stats.buffers_clean += clean;
        stats.buffers_backend += backend;
    }
}

impl Default for CheckpointManager {
    fn default() -> Self {
        Self::new()
    }
}
