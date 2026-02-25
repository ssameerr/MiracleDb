//! Vacuum Module - Dead tuple cleanup and space reclamation

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Vacuum options
#[derive(Clone, Debug, Default)]
pub struct VacuumOptions {
    pub full: bool,
    pub freeze: bool,
    pub analyze: bool,
    pub verbose: bool,
    pub skip_locked: bool,
    pub index_cleanup: IndexCleanup,
    pub truncate: bool,
    pub parallel: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub enum IndexCleanup {
    #[default]
    Auto,
    On,
    Off,
}

/// Vacuum statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VacuumStats {
    pub pages_scanned: u64,
    pub pages_removed: u64,
    pub tuples_deleted: u64,
    pub tuples_frozen: u64,
    pub index_scans: u32,
    pub dead_tuples_removed: u64,
    pub oldest_xmin: u64,
    pub duration_ms: u64,
}

/// Table vacuum info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableVacuumInfo {
    pub table: String,
    pub last_vacuum: Option<i64>,
    pub last_autovacuum: Option<i64>,
    pub vacuum_count: u64,
    pub autovacuum_count: u64,
    pub dead_tuples: u64,
    pub live_tuples: u64,
    pub dead_tuple_ratio: f64,
}

/// Autovacuum settings
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AutovacuumSettings {
    pub enabled: bool,
    pub naptime_seconds: u64,
    pub vacuum_threshold: u64,
    pub vacuum_scale_factor: f64,
    pub analyze_threshold: u64,
    pub analyze_scale_factor: f64,
    pub freeze_max_age: u64,
    pub max_workers: u32,
}

impl Default for AutovacuumSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            naptime_seconds: 60,
            vacuum_threshold: 50,
            vacuum_scale_factor: 0.2,
            analyze_threshold: 50,
            analyze_scale_factor: 0.1,
            freeze_max_age: 200_000_000,
            max_workers: 3,
        }
    }
}

/// Vacuum manager
pub struct VacuumManager {
    settings: RwLock<AutovacuumSettings>,
    table_info: RwLock<HashMap<String, TableVacuumInfo>>,
    running: RwLock<HashMap<String, VacuumJob>>,
}

/// Vacuum job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VacuumJob {
    pub id: String,
    pub table: String,
    pub options: String,
    pub started_at: i64,
    pub progress: VacuumProgress,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VacuumProgress {
    pub phase: String,
    pub heap_blks_total: u64,
    pub heap_blks_scanned: u64,
    pub heap_blks_vacuumed: u64,
    pub index_vacuum_count: u32,
    pub percent_complete: f64,
}

impl VacuumManager {
    pub fn new() -> Self {
        Self {
            settings: RwLock::new(AutovacuumSettings::default()),
            table_info: RwLock::new(HashMap::new()),
            running: RwLock::new(HashMap::new()),
        }
    }

    /// Check if table needs vacuum
    pub async fn needs_vacuum(&self, table: &str) -> bool {
        let settings = self.settings.read().await;
        let info = self.table_info.read().await;

        if let Some(table_info) = info.get(table) {
            let threshold = settings.vacuum_threshold as f64 +
                settings.vacuum_scale_factor * table_info.live_tuples as f64;
            table_info.dead_tuples as f64 >= threshold
        } else {
            false
        }
    }

    /// Check if table needs analyze
    pub async fn needs_analyze(&self, table: &str) -> bool {
        let settings = self.settings.read().await;
        let info = self.table_info.read().await;

        if let Some(table_info) = info.get(table) {
            let threshold = settings.analyze_threshold as f64 +
                settings.analyze_scale_factor * table_info.live_tuples as f64;
            table_info.dead_tuples as f64 >= threshold
        } else {
            false
        }
    }

    /// Start vacuum job
    pub async fn start_vacuum(&self, table: &str, options: VacuumOptions) -> String {
        let job_id = uuid::Uuid::new_v4().to_string();

        let job = VacuumJob {
            id: job_id.clone(),
            table: table.to_string(),
            options: format!("{:?}", options),
            started_at: chrono::Utc::now().timestamp(),
            progress: VacuumProgress {
                phase: "initializing".to_string(),
                ..Default::default()
            },
        };

        let mut running = self.running.write().await;
        running.insert(job_id.clone(), job);

        job_id
    }

    /// Update vacuum progress
    pub async fn update_progress(&self, job_id: &str, progress: VacuumProgress) {
        let mut running = self.running.write().await;
        if let Some(job) = running.get_mut(job_id) {
            job.progress = progress;
        }
    }

    /// Complete vacuum job
    pub async fn complete_vacuum(&self, job_id: &str, stats: VacuumStats) {
        let mut running = self.running.write().await;
        if let Some(job) = running.remove(job_id) {
            let mut info = self.table_info.write().await;
            let table_info = info.entry(job.table.clone())
                .or_insert_with(|| TableVacuumInfo {
                    table: job.table.clone(),
                    last_vacuum: None,
                    last_autovacuum: None,
                    vacuum_count: 0,
                    autovacuum_count: 0,
                    dead_tuples: 0,
                    live_tuples: 0,
                    dead_tuple_ratio: 0.0,
                });

            table_info.last_vacuum = Some(chrono::Utc::now().timestamp());
            table_info.vacuum_count += 1;
            table_info.dead_tuples = table_info.dead_tuples.saturating_sub(stats.dead_tuples_removed);
        }
    }

    /// Get running vacuum jobs
    pub async fn get_running(&self) -> Vec<VacuumJob> {
        let running = self.running.read().await;
        running.values().cloned().collect()
    }

    /// Update table info
    pub async fn update_table_info(&self, table: &str, live: u64, dead: u64) {
        let mut info = self.table_info.write().await;
        let table_info = info.entry(table.to_string())
            .or_insert_with(|| TableVacuumInfo {
                table: table.to_string(),
                last_vacuum: None,
                last_autovacuum: None,
                vacuum_count: 0,
                autovacuum_count: 0,
                dead_tuples: 0,
                live_tuples: 0,
                dead_tuple_ratio: 0.0,
            });

        table_info.live_tuples = live;
        table_info.dead_tuples = dead;
        table_info.dead_tuple_ratio = if live > 0 {
            dead as f64 / live as f64
        } else {
            0.0
        };
    }

    /// Get settings
    pub async fn get_settings(&self) -> AutovacuumSettings {
        self.settings.read().await.clone()
    }

    /// Update settings
    pub async fn update_settings(&self, settings: AutovacuumSettings) {
        let mut s = self.settings.write().await;
        *s = settings;
    }
}

impl Default for VacuumManager {
    fn default() -> Self {
        Self::new()
    }
}
