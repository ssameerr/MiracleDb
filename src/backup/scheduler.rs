//! Automated Backup Scheduler
//!
//! Provides automated backup scheduling with:
//! - Cron-style scheduling
//! - Configurable retention policies
//! - Backup rotation and cleanup
//! - Health monitoring

use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc, Datelike, Timelike};
use serde::{Serialize, Deserialize};
use tokio::time::interval;
use tokio::sync::RwLock;

use super::{BackupManager, BackupType, BackupMetadata};

/// Backup schedule configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSchedule {
    /// Schedule name
    pub name: String,
    /// Backup type
    pub backup_type: BackupType,
    /// Cron expression (e.g., "0 2 * * *" for daily at 2 AM)
    pub cron_expression: String,
    /// Retention days
    pub retention_days: u32,
    /// Whether schedule is enabled
    pub enabled: bool,
}

/// Backup scheduler
pub struct BackupScheduler {
    schedules: Arc<RwLock<Vec<BackupSchedule>>>,
    backup_manager: Arc<BackupManager>,
    data_dir: String,
}

impl BackupScheduler {
    pub fn new(backup_manager: Arc<BackupManager>, data_dir: String) -> Self {
        Self {
            schedules: Arc::new(RwLock::new(Vec::new())),
            backup_manager,
            data_dir,
        }
    }

    /// Add a backup schedule
    pub async fn add_schedule(&self, schedule: BackupSchedule) {
        let mut schedules = self.schedules.write().await;
        schedules.push(schedule);
    }

    /// Remove a backup schedule by name
    pub async fn remove_schedule(&self, name: &str) -> bool {
        let mut schedules = self.schedules.write().await;
        let initial_len = schedules.len();
        schedules.retain(|s| s.name != name);
        schedules.len() < initial_len
    }

    /// List all schedules
    pub async fn list_schedules(&self) -> Vec<BackupSchedule> {
        let schedules = self.schedules.read().await;
        schedules.clone()
    }

    /// Start the scheduler
    pub async fn start(self: Arc<Self>) {
        tracing::info!("Backup scheduler started");

        // Run scheduler loop
        let mut interval = interval(Duration::from_secs(60)); // Check every minute

        loop {
            interval.tick().await;

            if let Err(e) = self.check_schedules().await {
                tracing::error!("Scheduler error: {}", e);
            }
        }
    }

    /// Check if any backups should run
    async fn check_schedules(&self) -> Result<(), String> {
        let schedules = self.schedules.read().await;
        let now = Utc::now();

        for schedule in schedules.iter() {
            if !schedule.enabled {
                continue;
            }

            // Parse cron expression and check if it should run
            // For simplicity, we'll just implement a basic daily backup check
            if self.should_run_backup(&schedule, now) {
                tracing::info!("Running scheduled backup: {}", schedule.name);

                // Run backup in background
                let backup_manager = self.backup_manager.clone();
                let data_dir = self.data_dir.clone();
                let schedule_clone = schedule.clone();

                tokio::spawn(async move {
                    match Self::execute_backup(&backup_manager, &data_dir, &schedule_clone).await {
                        Ok(metadata) => {
                            tracing::info!(
                                "Scheduled backup '{}' completed: {} ({}bytes)",
                                schedule_clone.name,
                                metadata.id,
                                metadata.size_bytes
                            );

                            // Record backup metrics
                            crate::observability::metrics::BACKUP_TOTAL
                                .with_label_values(&[&format!("{:?}", schedule_clone.backup_type), "success"])
                                .inc();

                            crate::observability::metrics::BACKUP_SIZE_BYTES
                                .with_label_values(&[&metadata.id])
                                .set(metadata.size_bytes as f64);
                        }
                        Err(e) => {
                            tracing::error!("Scheduled backup '{}' failed: {}", schedule_clone.name, e);

                            crate::observability::metrics::BACKUP_TOTAL
                                .with_label_values(&[&format!("{:?}", schedule_clone.backup_type), "error"])
                                .inc();
                        }
                    }
                });
            }

            // Clean up old backups
            if let Err(e) = self.cleanup_old_backups(&schedule).await {
                tracing::error!("Failed to cleanup old backups for {}: {}", schedule.name, e);
            }
        }

        Ok(())
    }

    /// Determine if a backup should run now
    fn should_run_backup(&self, schedule: &BackupSchedule, now: DateTime<Utc>) -> bool {
        // Simple implementation: parse basic cron expressions
        // Format: "minute hour day month dayofweek"
        // Example: "0 2 * * *" = daily at 2 AM
        // Example: "0 */6 * * *" = every 6 hours

        let parts: Vec<&str> = schedule.cron_expression.split_whitespace().collect();
        if parts.len() != 5 {
            tracing::warn!("Invalid cron expression: {}", schedule.cron_expression);
            return false;
        }

        let minute = now.minute();
        let hour = now.hour();
        let day = now.day();
        let month = now.month();
        let weekday = now.weekday().num_days_from_monday();

        // Check minute
        if !Self::matches_cron_field(parts[0], minute) {
            return false;
        }

        // Check hour
        if !Self::matches_cron_field(parts[1], hour) {
            return false;
        }

        // Check day
        if !Self::matches_cron_field(parts[2], day) {
            return false;
        }

        // Check month
        if !Self::matches_cron_field(parts[3], month) {
            return false;
        }

        // Check weekday
        if !Self::matches_cron_field(parts[4], weekday) {
            return false;
        }

        true
    }

    /// Match a cron field (simplified)
    fn matches_cron_field(field: &str, value: u32) -> bool {
        if field == "*" {
            return true;
        }

        // Handle ranges (e.g., "1-5")
        if field.contains('-') {
            let parts: Vec<&str> = field.split('-').collect();
            if parts.len() == 2 {
                if let (Ok(start), Ok(end)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                    return value >= start && value <= end;
                }
            }
        }

        // Handle intervals (e.g., "*/6")
        if field.starts_with("*/") {
            if let Ok(interval) = field[2..].parse::<u32>() {
                return value % interval == 0;
            }
        }

        // Handle comma-separated values (e.g., "1,3,5")
        if field.contains(',') {
            let values: Vec<u32> = field
                .split(',')
                .filter_map(|s| s.parse::<u32>().ok())
                .collect();
            return values.contains(&value);
        }

        // Handle exact match
        if let Ok(expected) = field.parse::<u32>() {
            return value == expected;
        }

        false
    }

    /// Execute a backup
    async fn execute_backup(
        backup_manager: &BackupManager,
        data_dir: &str,
        schedule: &BackupSchedule,
    ) -> Result<BackupMetadata, String> {
        match schedule.backup_type {
            BackupType::Full => {
                // Get list of all tables
                let tables = vec![]; // Would get from catalog
                backup_manager.create_full_backup(data_dir, tables).await
            }
            BackupType::Incremental => {
                // Get latest full backup
                let backups = backup_manager.list_backups().await;
                let latest_full = backups
                    .iter()
                    .filter(|b| matches!(b.backup_type, BackupType::Full))
                    .max_by_key(|b| b.created_at);

                if let Some(base) = latest_full {
                    backup_manager.create_incremental_backup(&base.id, data_dir).await
                } else {
                    Err("No full backup found for incremental backup".to_string())
                }
            }
            BackupType::Snapshot => {
                Err("Snapshot backups not yet implemented".to_string())
            }
        }
    }

    /// Clean up old backups beyond retention period
    async fn cleanup_old_backups(&self, schedule: &BackupSchedule) -> Result<(), String> {
        let now = Utc::now();
        let retention_duration = chrono::Duration::days(schedule.retention_days as i64);
        let cutoff_time = now - retention_duration;

        let backups = self.backup_manager.list_backups().await;

        for backup in backups {
            if backup.created_at < cutoff_time {
                tracing::info!("Deleting old backup: {} (created: {})", backup.id, backup.created_at);

                if let Err(e) = self.backup_manager.delete_backup(&backup.id).await {
                    tracing::error!("Failed to delete backup {}: {}", backup.id, e);
                } else {
                    // Record cleanup metric
                    crate::observability::metrics::BACKUP_CLEANUP_TOTAL.inc();
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cron_wildcard() {
        assert!(BackupScheduler::matches_cron_field("*", 5));
        assert!(BackupScheduler::matches_cron_field("*", 42));
    }

    #[test]
    fn test_cron_exact() {
        assert!(BackupScheduler::matches_cron_field("5", 5));
        assert!(!BackupScheduler::matches_cron_field("5", 6));
    }

    #[test]
    fn test_cron_interval() {
        assert!(BackupScheduler::matches_cron_field("*/6", 0));
        assert!(BackupScheduler::matches_cron_field("*/6", 6));
        assert!(BackupScheduler::matches_cron_field("*/6", 12));
        assert!(!BackupScheduler::matches_cron_field("*/6", 5));
    }

    #[test]
    fn test_cron_range() {
        assert!(BackupScheduler::matches_cron_field("1-5", 3));
        assert!(!BackupScheduler::matches_cron_field("1-5", 6));
    }

    #[test]
    fn test_cron_list() {
        assert!(BackupScheduler::matches_cron_field("1,3,5", 3));
        assert!(!BackupScheduler::matches_cron_field("1,3,5", 4));
    }
}
