//! Publication Module - Logical replication publications

use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Publication definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publication {
    pub name: String,
    pub owner: String,
    pub all_tables: bool,
    pub tables: HashSet<String>,
    pub operations: PublishOperations,
    pub via_root: bool,
    pub created_at: i64,
}

/// Operations to publish
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PublishOperations {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
}

impl PublishOperations {
    pub fn all() -> Self {
        Self {
            insert: true,
            update: true,
            delete: true,
            truncate: true,
        }
    }

    pub fn none() -> Self {
        Self::default()
    }

    pub fn to_string(&self) -> String {
        let mut ops = Vec::new();
        if self.insert { ops.push("insert"); }
        if self.update { ops.push("update"); }
        if self.delete { ops.push("delete"); }
        if self.truncate { ops.push("truncate"); }
        ops.join(", ")
    }
}

impl Publication {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            owner: "postgres".to_string(),
            all_tables: false,
            tables: HashSet::new(),
            operations: PublishOperations::all(),
            via_root: false,
            created_at: chrono::Utc::now().timestamp(),
        }
    }

    pub fn for_all_tables(mut self) -> Self {
        self.all_tables = true;
        self
    }

    pub fn add_table(mut self, table: &str) -> Self {
        self.tables.insert(table.to_string());
        self
    }

    pub fn with_operations(mut self, ops: PublishOperations) -> Self {
        self.operations = ops;
        self
    }

    /// Check if table is published
    pub fn publishes(&self, table: &str) -> bool {
        self.all_tables || self.tables.contains(table)
    }

    /// Check if operation is published
    pub fn publishes_operation(&self, op: &str) -> bool {
        match op {
            "INSERT" | "insert" => self.operations.insert,
            "UPDATE" | "update" => self.operations.update,
            "DELETE" | "delete" => self.operations.delete,
            "TRUNCATE" | "truncate" => self.operations.truncate,
            _ => false,
        }
    }
}

/// Publication manager
pub struct PublicationManager {
    publications: RwLock<HashMap<String, Publication>>,
}

impl PublicationManager {
    pub fn new() -> Self {
        Self {
            publications: RwLock::new(HashMap::new()),
        }
    }

    /// Create a publication
    pub async fn create(&self, publication: Publication) -> Result<(), String> {
        let mut publications = self.publications.write().await;

        if publications.contains_key(&publication.name) {
            return Err(format!("Publication {} already exists", publication.name));
        }

        publications.insert(publication.name.clone(), publication);
        Ok(())
    }

    /// Drop a publication
    pub async fn drop(&self, name: &str) -> Result<(), String> {
        let mut publications = self.publications.write().await;

        publications.remove(name)
            .ok_or_else(|| format!("Publication {} not found", name))?;
        Ok(())
    }

    /// Get a publication
    pub async fn get(&self, name: &str) -> Option<Publication> {
        let publications = self.publications.read().await;
        publications.get(name).cloned()
    }

    /// List publications
    pub async fn list(&self) -> Vec<Publication> {
        let publications = self.publications.read().await;
        publications.values().cloned().collect()
    }

    /// Add table to publication
    pub async fn add_table(&self, name: &str, table: &str) -> Result<(), String> {
        let mut publications = self.publications.write().await;

        let pub_ = publications.get_mut(name)
            .ok_or_else(|| format!("Publication {} not found", name))?;

        if pub_.all_tables {
            return Err("Cannot add tables to FOR ALL TABLES publication".to_string());
        }

        pub_.tables.insert(table.to_string());
        Ok(())
    }

    /// Remove table from publication
    pub async fn drop_table(&self, name: &str, table: &str) -> Result<(), String> {
        let mut publications = self.publications.write().await;

        let pub_ = publications.get_mut(name)
            .ok_or_else(|| format!("Publication {} not found", name))?;

        pub_.tables.remove(table);
        Ok(())
    }

    /// Get publications for table
    pub async fn get_for_table(&self, table: &str) -> Vec<Publication> {
        let publications = self.publications.read().await;
        publications.values()
            .filter(|p| p.publishes(table))
            .cloned()
            .collect()
    }
}

impl Default for PublicationManager {
    fn default() -> Self {
        Self::new()
    }
}
