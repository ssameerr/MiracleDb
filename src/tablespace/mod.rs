//! Tablespace Module - Storage location management

use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Tablespace
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tablespace {
    pub name: String,
    pub location: PathBuf,
    pub owner: String,
    pub options: TablespaceOptions,
    pub created_at: i64,
    pub size_bytes: u64,
}

/// Tablespace options
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TablespaceOptions {
    pub random_page_cost: Option<f64>,
    pub seq_page_cost: Option<f64>,
    pub effective_io_concurrency: Option<u32>,
    pub maintenance_io_concurrency: Option<u32>,
}

/// Tablespace usage info
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TablespaceUsage {
    pub tablespace: String,
    pub tables: Vec<String>,
    pub indexes: Vec<String>,
    pub total_size_bytes: u64,
    pub available_bytes: u64,
}

/// Tablespace manager
pub struct TablespaceManager {
    tablespaces: RwLock<HashMap<String, Tablespace>>,
    object_mapping: RwLock<HashMap<String, String>>, // object -> tablespace
    default_tablespace: String,
}

impl TablespaceManager {
    pub fn new(data_dir: PathBuf) -> Self {
        let mut manager = Self {
            tablespaces: RwLock::new(HashMap::new()),
            object_mapping: RwLock::new(HashMap::new()),
            default_tablespace: "pg_default".to_string(),
        };

        // Create default tablespaces
        let now = chrono::Utc::now().timestamp();
        
        let pg_default = Tablespace {
            name: "pg_default".to_string(),
            location: data_dir.join("base"),
            owner: "postgres".to_string(),
            options: TablespaceOptions::default(),
            created_at: now,
            size_bytes: 0,
        };

        let pg_global = Tablespace {
            name: "pg_global".to_string(),
            location: data_dir.join("global"),
            owner: "postgres".to_string(),
            options: TablespaceOptions::default(),
            created_at: now,
            size_bytes: 0,
        };

        // Note: These would be inserted in actual initialization
        
        manager
    }

    /// Create tablespace
    pub async fn create(&self, name: &str, location: PathBuf, owner: &str) -> Result<(), String> {
        let mut tablespaces = self.tablespaces.write().await;

        if tablespaces.contains_key(name) {
            return Err(format!("Tablespace {} already exists", name));
        }

        let tablespace = Tablespace {
            name: name.to_string(),
            location,
            owner: owner.to_string(),
            options: TablespaceOptions::default(),
            created_at: chrono::Utc::now().timestamp(),
            size_bytes: 0,
        };

        tablespaces.insert(name.to_string(), tablespace);
        Ok(())
    }

    /// Drop tablespace
    pub async fn drop(&self, name: &str) -> Result<(), String> {
        if name == "pg_default" || name == "pg_global" {
            return Err("Cannot drop system tablespace".to_string());
        }

        // Check if any objects use this tablespace
        let mapping = self.object_mapping.read().await;
        let in_use: Vec<_> = mapping.iter()
            .filter(|(_, ts)| *ts == name)
            .map(|(obj, _)| obj.clone())
            .collect();

        if !in_use.is_empty() {
            return Err(format!("Tablespace {} is used by: {:?}", name, in_use));
        }

        let mut tablespaces = self.tablespaces.write().await;
        tablespaces.remove(name)
            .ok_or_else(|| format!("Tablespace {} not found", name))?;

        Ok(())
    }

    /// Get tablespace
    pub async fn get(&self, name: &str) -> Option<Tablespace> {
        let tablespaces = self.tablespaces.read().await;
        tablespaces.get(name).cloned()
    }

    /// List all tablespaces
    pub async fn list(&self) -> Vec<Tablespace> {
        let tablespaces = self.tablespaces.read().await;
        tablespaces.values().cloned().collect()
    }

    /// Assign object to tablespace
    pub async fn assign(&self, object: &str, tablespace: &str) -> Result<(), String> {
        let tablespaces = self.tablespaces.read().await;
        if !tablespaces.contains_key(tablespace) {
            return Err(format!("Tablespace {} not found", tablespace));
        }
        drop(tablespaces);

        let mut mapping = self.object_mapping.write().await;
        mapping.insert(object.to_string(), tablespace.to_string());
        Ok(())
    }

    /// Get tablespace for object
    pub async fn get_tablespace(&self, object: &str) -> String {
        let mapping = self.object_mapping.read().await;
        mapping.get(object)
            .cloned()
            .unwrap_or_else(|| self.default_tablespace.clone())
    }

    /// Get tablespace usage
    pub async fn get_usage(&self, name: &str) -> Option<TablespaceUsage> {
        let tablespaces = self.tablespaces.read().await;
        let tablespace = tablespaces.get(name)?;

        let mapping = self.object_mapping.read().await;
        let mut tables = Vec::new();
        let mut indexes = Vec::new();

        for (obj, ts) in mapping.iter() {
            if ts == name {
                if obj.starts_with("idx_") {
                    indexes.push(obj.clone());
                } else {
                    tables.push(obj.clone());
                }
            }
        }

        Some(TablespaceUsage {
            tablespace: name.to_string(),
            tables,
            indexes,
            total_size_bytes: tablespace.size_bytes,
            available_bytes: 0, // Would check filesystem
        })
    }

    /// Set tablespace options
    pub async fn set_options(&self, name: &str, options: TablespaceOptions) -> Result<(), String> {
        let mut tablespaces = self.tablespaces.write().await;
        let tablespace = tablespaces.get_mut(name)
            .ok_or_else(|| format!("Tablespace {} not found", name))?;

        tablespace.options = options;
        Ok(())
    }

    /// Update tablespace size
    pub async fn update_size(&self, name: &str, size_bytes: u64) {
        let mut tablespaces = self.tablespaces.write().await;
        if let Some(tablespace) = tablespaces.get_mut(name) {
            tablespace.size_bytes = size_bytes;
        }
    }

    /// Get default tablespace
    pub fn default_tablespace(&self) -> &str {
        &self.default_tablespace
    }
}

impl Default for TablespaceManager {
    fn default() -> Self {
        Self::new(PathBuf::from("/var/lib/miracledb/data"))
    }
}
