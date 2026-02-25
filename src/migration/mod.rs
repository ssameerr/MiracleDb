//! Migration Module - Schema migrations and versioning

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Migration definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Migration {
    pub version: u64,
    pub name: String,
    pub up_sql: String,
    pub down_sql: String,
    pub checksum: String,
}

/// Migration status
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub version: u64,
    pub name: String,
    pub applied_at: i64,
    pub checksum: String,
}

/// Migration result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MigrationResult {
    Applied(u64),
    AlreadyApplied(u64),
    Error { version: u64, error: String },
}

/// Migration manager
pub struct MigrationManager {
    migrations: RwLock<Vec<Migration>>,
    applied: RwLock<HashMap<u64, MigrationRecord>>,
}

impl MigrationManager {
    pub fn new() -> Self {
        Self {
            migrations: RwLock::new(Vec::new()),
            applied: RwLock::new(HashMap::new()),
        }
    }

    /// Register a migration
    pub async fn register(&self, migration: Migration) {
        let mut migrations = self.migrations.write().await;
        if !migrations.iter().any(|m| m.version == migration.version) {
            migrations.push(migration);
            migrations.sort_by_key(|m| m.version);
        }
    }

    /// Get pending migrations
    pub async fn pending(&self) -> Vec<Migration> {
        let migrations = self.migrations.read().await;
        let applied = self.applied.read().await;
        
        migrations.iter()
            .filter(|m| !applied.contains_key(&m.version))
            .cloned()
            .collect()
    }

    /// Apply a single migration
    pub async fn apply(&self, version: u64) -> MigrationResult {
        let migrations = self.migrations.read().await;
        let migration = migrations.iter()
            .find(|m| m.version == version);

        let migration = match migration {
            Some(m) => m.clone(),
            None => return MigrationResult::Error {
                version,
                error: "Migration not found".to_string(),
            },
        };
        drop(migrations);

        // Check if already applied
        let applied = self.applied.read().await;
        if applied.contains_key(&version) {
            return MigrationResult::AlreadyApplied(version);
        }
        drop(applied);

        // Execute migration (in production: would execute SQL)
        // engine.query(&migration.up_sql).await?;

        // Record as applied
        let record = MigrationRecord {
            version,
            name: migration.name.clone(),
            applied_at: chrono::Utc::now().timestamp(),
            checksum: migration.checksum.clone(),
        };

        let mut applied = self.applied.write().await;
        applied.insert(version, record);

        MigrationResult::Applied(version)
    }

    /// Rollback a migration
    pub async fn rollback(&self, version: u64) -> MigrationResult {
        let applied = self.applied.read().await;
        if !applied.contains_key(&version) {
            return MigrationResult::Error {
                version,
                error: "Migration not applied".to_string(),
            };
        }
        drop(applied);

        let migrations = self.migrations.read().await;
        let migration = migrations.iter()
            .find(|m| m.version == version)
            .cloned();
        drop(migrations);

        let migration = match migration {
            Some(m) => m,
            None => return MigrationResult::Error {
                version,
                error: "Migration not found".to_string(),
            },
        };

        // Execute rollback (in production: would execute SQL)
        // engine.query(&migration.down_sql).await?;

        let mut applied = self.applied.write().await;
        applied.remove(&version);

        MigrationResult::Applied(version)
    }

    /// Apply all pending migrations
    pub async fn migrate(&self) -> Vec<MigrationResult> {
        let pending = self.pending().await;
        let mut results = Vec::new();

        for migration in pending {
            let result = self.apply(migration.version).await;
            let is_error = matches!(result, MigrationResult::Error { .. });
            results.push(result);
            
            if is_error {
                break; // Stop on first error
            }
        }

        results
    }

    /// Rollback to a specific version
    pub async fn rollback_to(&self, target_version: u64) -> Vec<MigrationResult> {
        let applied = self.applied.read().await;
        let mut to_rollback: Vec<u64> = applied.keys()
            .filter(|&&v| v > target_version)
            .copied()
            .collect();
        drop(applied);

        to_rollback.sort_by(|a, b| b.cmp(a)); // Descending order

        let mut results = Vec::new();
        for version in to_rollback {
            let result = self.rollback(version).await;
            results.push(result);
        }

        results
    }

    /// Get current version
    pub async fn current_version(&self) -> Option<u64> {
        let applied = self.applied.read().await;
        applied.keys().max().copied()
    }

    /// List all migrations
    pub async fn list(&self) -> Vec<(Migration, Option<MigrationRecord>)> {
        let migrations = self.migrations.read().await;
        let applied = self.applied.read().await;

        migrations.iter()
            .map(|m| (m.clone(), applied.get(&m.version).cloned()))
            .collect()
    }

    /// Verify migration checksums
    pub async fn verify(&self) -> Vec<(u64, bool)> {
        let migrations = self.migrations.read().await;
        let applied = self.applied.read().await;

        migrations.iter()
            .filter_map(|m| {
                applied.get(&m.version).map(|record| {
                    (m.version, m.checksum == record.checksum)
                })
            })
            .collect()
    }
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Create migration helper
pub fn create_migration(version: u64, name: &str, up: &str, down: &str) -> Migration {
    use sha2::{Sha256, Digest};
    let checksum = format!("{:x}", Sha256::digest(format!("{}{}", up, down).as_bytes()));
    
    Migration {
        version,
        name: name.to_string(),
        up_sql: up.to_string(),
        down_sql: down.to_string(),
        checksum,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_migrations() {
        let manager = MigrationManager::new();

        let m1 = create_migration(1, "create_users", 
            "CREATE TABLE users (id INT PRIMARY KEY)",
            "DROP TABLE users");

        let m2 = create_migration(2, "add_email",
            "ALTER TABLE users ADD email VARCHAR(255)",
            "ALTER TABLE users DROP email");

        manager.register(m1).await;
        manager.register(m2).await;

        let pending = manager.pending().await;
        assert_eq!(pending.len(), 2);

        let results = manager.migrate().await;
        assert_eq!(results.len(), 2);

        let version = manager.current_version().await;
        assert_eq!(version, Some(2));
    }
}
