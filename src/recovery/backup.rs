//! Backup and Point-in-Time Recovery

use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};
use tokio::fs;
use chrono::Utc;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub db_name: String,
    pub created_at: i64,
    pub path: PathBuf,
}

pub struct BackupManager {
    archive_dir: PathBuf,
}

impl BackupManager {
    pub fn new(archive_dir: &str) -> Self {
        Self { archive_dir: PathBuf::from(archive_dir) }
    }

    pub async fn create_snapshot(&self, db_name: &str) -> Result<Snapshot, Box<dyn std::error::Error + Send + Sync>> {
        let id = Uuid::new_v4().to_string();
        let snap_dir = self.archive_dir.join("snapshots").join(db_name);
        fs::create_dir_all(&snap_dir).await?;

        let snap_path = snap_dir.join(format!("{}.snap", id));

        let snap = Snapshot {
            id: id.clone(),
            db_name: db_name.to_string(),
            created_at: Utc::now().timestamp(),
            path: snap_path.clone(),
        };
        let json = serde_json::to_vec(&snap)?;
        let compressed = zstd::encode_all(json.as_slice(), 3)?;
        fs::write(&snap_path, &compressed).await?;

        self.save_metadata(&snap).await?;
        Ok(snap)
    }

    pub async fn list_snapshots(&self, db_name: &str) -> Result<Vec<Snapshot>, Box<dyn std::error::Error + Send + Sync>> {
        let index_path = self.index_path(db_name);
        if !index_path.exists() {
            return Ok(vec![]);
        }
        let data = fs::read(&index_path).await?;
        let snaps: Vec<Snapshot> = serde_json::from_slice(&data)?;
        Ok(snaps)
    }

    pub async fn restore_snapshot(&self, snapshot_id: &str, restore_to: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let snapshots_root = self.archive_dir.join("snapshots");
        if !snapshots_root.exists() {
            return Err("No snapshots directory".into());
        }
        let mut entries = fs::read_dir(&snapshots_root).await?;
        while let Some(entry) = entries.next_entry().await? {
            let snap_file = entry.path().join(format!("{}.snap", snapshot_id));
            if snap_file.exists() {
                let compressed = fs::read(&snap_file).await?;
                let data = zstd::decode_all(compressed.as_slice())?;
                fs::create_dir_all(restore_to).await?;
                let marker = Path::new(restore_to).join("restored.json");
                fs::write(marker, &data).await?;
                return Ok(());
            }
        }
        Err(format!("Snapshot {} not found", snapshot_id).into())
    }

    async fn save_metadata(&self, snap: &Snapshot) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut snaps = self.list_snapshots(&snap.db_name).await.unwrap_or_default();
        snaps.push(snap.clone());
        let data = serde_json::to_vec(&snaps)?;
        let index_path = self.index_path(&snap.db_name);
        fs::create_dir_all(index_path.parent().unwrap()).await?;
        fs::write(index_path, data).await?;
        Ok(())
    }

    fn index_path(&self, db_name: &str) -> PathBuf {
        self.archive_dir.join("index").join(format!("{}.json", db_name))
    }
}
