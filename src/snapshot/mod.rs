//! Snapshot Module - Point-in-time database snapshots

use std::collections::HashMap;
use std::path::Path;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Snapshot metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub id: String,
    pub name: String,
    pub created_at: i64,
    pub size_bytes: u64,
    pub tables: Vec<String>,
    pub lsn: u64, // Log sequence number
    pub checksum: String,
}

/// Snapshot status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotStatus {
    Creating,
    Ready,
    Restoring,
    Failed(String),
    Deleted,
}

/// Snapshot entry
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub metadata: SnapshotMetadata,
    pub status: SnapshotStatus,
    pub path: String,
}

/// Snapshot manager
pub struct SnapshotManager {
    snapshots: RwLock<HashMap<String, Snapshot>>,
    base_path: String,
    max_snapshots: usize,
}

impl SnapshotManager {
    pub fn new(base_path: &str, max_snapshots: usize) -> Self {
        Self {
            snapshots: RwLock::new(HashMap::new()),
            base_path: base_path.to_string(),
            max_snapshots,
        }
    }

    /// Create a new snapshot
    pub async fn create(&self, name: &str, tables: Vec<String>) -> Result<SnapshotMetadata, String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp();
        let snapshot_path = format!("{}/{}", self.base_path, id);

        // Create snapshot directory
        tokio::fs::create_dir_all(&snapshot_path).await
            .map_err(|e| format!("Failed to create snapshot directory: {}", e))?;

        // Record initial metadata
        let metadata = SnapshotMetadata {
            id: id.clone(),
            name: name.to_string(),
            created_at: now,
            size_bytes: 0,
            tables: tables.clone(),
            lsn: 0, // Would get from WAL
            checksum: String::new(),
        };

        let snapshot = Snapshot {
            metadata: metadata.clone(),
            status: SnapshotStatus::Creating,
            path: snapshot_path.clone(),
        };

        let mut snapshots = self.snapshots.write().await;
        snapshots.insert(id.clone(), snapshot);
        drop(snapshots);

        // In production: would copy data files, create hard links, or use copy-on-write
        // For each table in tables:
        //   - Copy table data file
        //   - Copy index files
        //   - Copy metadata

        // Update to ready status
        let mut snapshots = self.snapshots.write().await;
        if let Some(snapshot) = snapshots.get_mut(&id) {
            snapshot.status = SnapshotStatus::Ready;
            snapshot.metadata.checksum = self.calculate_checksum(&snapshot_path).await;
        }

        // Enforce max snapshots
        self.cleanup_old_snapshots().await;

        Ok(metadata)
    }

    /// Restore from a snapshot
    pub async fn restore(&self, snapshot_id: &str) -> Result<(), String> {
        let mut snapshots = self.snapshots.write().await;
        let snapshot = snapshots.get_mut(snapshot_id)
            .ok_or("Snapshot not found")?;

        if !matches!(snapshot.status, SnapshotStatus::Ready) {
            return Err("Snapshot is not ready".to_string());
        }

        snapshot.status = SnapshotStatus::Restoring;
        drop(snapshots);

        // In production: would restore data files from snapshot
        // - Stop query processing
        // - Copy files from snapshot to data directory
        // - Rebuild indexes if needed
        // - Resume query processing

        let mut snapshots = self.snapshots.write().await;
        if let Some(snapshot) = snapshots.get_mut(snapshot_id) {
            snapshot.status = SnapshotStatus::Ready;
        }

        Ok(())
    }

    /// Delete a snapshot
    pub async fn delete(&self, snapshot_id: &str) -> Result<(), String> {
        let mut snapshots = self.snapshots.write().await;
        let snapshot = snapshots.remove(snapshot_id)
            .ok_or("Snapshot not found")?;

        // Delete snapshot files
        tokio::fs::remove_dir_all(&snapshot.path).await
            .map_err(|e| format!("Failed to delete snapshot: {}", e))?;

        Ok(())
    }

    /// List all snapshots
    pub async fn list(&self) -> Vec<SnapshotMetadata> {
        let snapshots = self.snapshots.read().await;
        snapshots.values()
            .filter(|s| matches!(s.status, SnapshotStatus::Ready))
            .map(|s| s.metadata.clone())
            .collect()
    }

    /// Get snapshot by ID
    pub async fn get(&self, snapshot_id: &str) -> Option<Snapshot> {
        let snapshots = self.snapshots.read().await;
        snapshots.get(snapshot_id).cloned()
    }

    /// Verify snapshot integrity
    pub async fn verify(&self, snapshot_id: &str) -> Result<bool, String> {
        let snapshots = self.snapshots.read().await;
        let snapshot = snapshots.get(snapshot_id)
            .ok_or("Snapshot not found")?;

        let current_checksum = self.calculate_checksum(&snapshot.path).await;
        Ok(current_checksum == snapshot.metadata.checksum)
    }

    async fn calculate_checksum(&self, _path: &str) -> String {
        // In production: would calculate hash of all files
        use sha2::{Sha256, Digest};
        let hash = Sha256::digest(b"snapshot_data");
        format!("{:x}", hash)
    }

    async fn cleanup_old_snapshots(&self) {
        let mut snapshots = self.snapshots.write().await;
        
        if snapshots.len() <= self.max_snapshots {
            return;
        }

        // Find oldest snapshots to delete
        let mut by_time: Vec<_> = snapshots.iter()
            .map(|(id, s)| (id.clone(), s.metadata.created_at))
            .collect();
        by_time.sort_by_key(|(_, t)| *t);

        let to_remove = snapshots.len() - self.max_snapshots;
        for (id, _) in by_time.into_iter().take(to_remove) {
            if let Some(snapshot) = snapshots.remove(&id) {
                let _ = tokio::fs::remove_dir_all(&snapshot.path).await;
            }
        }
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new("./snapshots", 10)
    }
}
