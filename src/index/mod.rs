//! Index Module - Index management and operations

pub mod partial;
pub mod advisor;
pub mod bitmap;

pub use partial::{PartialIndex, PartialIndexManager};
pub use advisor::{IndexAdvisor, IndexRecommendation};
pub use bitmap::BitmapIndex as BitmapIndexV2;

use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Index entry
#[derive(Clone, Debug)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub row_ids: Vec<u64>,
}

/// B-Tree index
pub struct BTreeIndex {
    name: String,
    tree: RwLock<BTreeMap<Vec<u8>, Vec<u64>>>,
    unique: bool,
}

impl BTreeIndex {
    pub fn new(name: &str, unique: bool) -> Self {
        Self {
            name: name.to_string(),
            tree: RwLock::new(BTreeMap::new()),
            unique,
        }
    }

    /// Insert a key-value pair
    pub async fn insert(&self, key: Vec<u8>, row_id: u64) -> Result<(), String> {
        let mut tree = self.tree.write().await;
        
        if self.unique {
            if tree.contains_key(&key) {
                return Err(format!("Duplicate key in unique index {}", self.name));
            }
            tree.insert(key, vec![row_id]);
        } else {
            tree.entry(key).or_insert_with(Vec::new).push(row_id);
        }
        
        Ok(())
    }

    /// Delete a key-value pair
    pub async fn delete(&self, key: &[u8], row_id: u64) {
        let mut tree = self.tree.write().await;
        if let Some(row_ids) = tree.get_mut(key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                tree.remove(key);
            }
        }
    }

    /// Lookup exact key
    pub async fn get(&self, key: &[u8]) -> Vec<u64> {
        let tree = self.tree.read().await;
        tree.get(key).cloned().unwrap_or_default()
    }

    /// Range scan
    pub async fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u64>)> {
        let tree = self.tree.read().await;
        tree.range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Prefix scan
    pub async fn prefix_scan(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u64>)> {
        let tree = self.tree.read().await;
        tree.iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Count entries
    pub async fn count(&self) -> usize {
        let tree = self.tree.read().await;
        tree.len()
    }
}

/// Hash index for equality lookups
pub struct HashIndex {
    name: String,
    map: RwLock<HashMap<Vec<u8>, Vec<u64>>>,
    unique: bool,
}

impl HashIndex {
    pub fn new(name: &str, unique: bool) -> Self {
        Self {
            name: name.to_string(),
            map: RwLock::new(HashMap::new()),
            unique,
        }
    }

    pub async fn insert(&self, key: Vec<u8>, row_id: u64) -> Result<(), String> {
        let mut map = self.map.write().await;
        
        if self.unique && map.contains_key(&key) {
            return Err(format!("Duplicate key in unique index {}", self.name));
        }
        
        map.entry(key).or_insert_with(Vec::new).push(row_id);
        Ok(())
    }

    pub async fn delete(&self, key: &[u8], row_id: u64) {
        let mut map = self.map.write().await;
        if let Some(row_ids) = map.get_mut(key) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                map.remove(key);
            }
        }
    }

    pub async fn get(&self, key: &[u8]) -> Vec<u64> {
        let map = self.map.read().await;
        map.get(key).cloned().unwrap_or_default()
    }

    pub async fn count(&self) -> usize {
        let map = self.map.read().await;
        map.len()
    }
}

/// Index metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: String,
    pub created_at: i64,
    pub size_bytes: u64,
    pub entries: u64,
}

/// Index manager
pub struct IndexManager {
    btree_indexes: RwLock<HashMap<String, Arc<BTreeIndex>>>,
    hash_indexes: RwLock<HashMap<String, Arc<HashIndex>>>,
    metadata: RwLock<HashMap<String, IndexMetadata>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            btree_indexes: RwLock::new(HashMap::new()),
            hash_indexes: RwLock::new(HashMap::new()),
            metadata: RwLock::new(HashMap::new()),
        }
    }

    /// Create a B-Tree index
    pub async fn create_btree(&self, name: &str, table: &str, columns: Vec<String>, unique: bool) -> Result<(), String> {
        let index = Arc::new(BTreeIndex::new(name, unique));
        
        let meta = IndexMetadata {
            name: name.to_string(),
            table: table.to_string(),
            columns,
            unique,
            index_type: "btree".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            size_bytes: 0,
            entries: 0,
        };

        let mut indexes = self.btree_indexes.write().await;
        let mut metadata = self.metadata.write().await;

        indexes.insert(name.to_string(), index);
        metadata.insert(name.to_string(), meta);

        Ok(())
    }

    /// Create a Hash index
    pub async fn create_hash(&self, name: &str, table: &str, columns: Vec<String>, unique: bool) -> Result<(), String> {
        let index = Arc::new(HashIndex::new(name, unique));
        
        let meta = IndexMetadata {
            name: name.to_string(),
            table: table.to_string(),
            columns,
            unique,
            index_type: "hash".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            size_bytes: 0,
            entries: 0,
        };

        let mut indexes = self.hash_indexes.write().await;
        let mut metadata = self.metadata.write().await;

        indexes.insert(name.to_string(), index);
        metadata.insert(name.to_string(), meta);

        Ok(())
    }

    /// Get B-Tree index
    pub async fn get_btree(&self, name: &str) -> Option<Arc<BTreeIndex>> {
        let indexes = self.btree_indexes.read().await;
        indexes.get(name).cloned()
    }

    /// Get Hash index
    pub async fn get_hash(&self, name: &str) -> Option<Arc<HashIndex>> {
        let indexes = self.hash_indexes.read().await;
        indexes.get(name).cloned()
    }

    /// Drop an index
    pub async fn drop(&self, name: &str) -> Result<(), String> {
        let mut btree = self.btree_indexes.write().await;
        let mut hash = self.hash_indexes.write().await;
        let mut metadata = self.metadata.write().await;

        btree.remove(name);
        hash.remove(name);
        metadata.remove(name);

        Ok(())
    }

    /// List indexes for a table
    pub async fn list_for_table(&self, table: &str) -> Vec<IndexMetadata> {
        let metadata = self.metadata.read().await;
        metadata.values()
            .filter(|m| m.table == table)
            .cloned()
            .collect()
    }

    /// Get all indexes
    pub async fn list_all(&self) -> Vec<IndexMetadata> {
        let metadata = self.metadata.read().await;
        metadata.values().cloned().collect()
    }
}


/// Bitmap Index
pub struct BitmapIndex {
    name: String,
    bitmaps: RwLock<HashMap<Vec<u8>, Vec<bool>>>, // Value -> Bitmap
    row_count: usize,
}

impl BitmapIndex {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            bitmaps: RwLock::new(HashMap::new()),
            row_count: 0,
        }
    }

    pub async fn insert(&self, key: Vec<u8>, row_id: usize) {
        let mut maps = self.bitmaps.write().await;
        // Expand maps if needed
        if row_id >= self.row_count {
            // naive expansion
        }
        
        let bitmap = maps.entry(key).or_insert_with(|| vec![false; row_id + 1]);
        if row_id >= bitmap.len() {
            bitmap.resize(row_id + 1, false);
        }
        bitmap[row_id] = true;
    }
}

/// Nucleus-based Reactive Index
pub struct NucleusIndex {
    name: String,
    system: Arc<crate::nucleus::NucleusSystem>,
}

impl NucleusIndex {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            system: Arc::new(crate::nucleus::NucleusSystem::new()),
        }
    }

    pub async fn insert(&self, key: Vec<u8>, row_id: u64) {
        let key_str = hex::encode(&key);
        // Create an atom representing this index entry.
        // Spin could represent access frequency (adaptive indexing compatibility).
        self.system.create_atom(&key_str, serde_json::json!({
            "row_ids": [row_id],
            "index_name": self.name
        })).await;
    }

    pub async fn get(&self, key: &[u8]) -> Vec<u64> {
        let key_str = hex::encode(key);
        if let Some(atom) = self.system.get_atom(&key_str).await {
            // "Spin" up on access (Adaptive)
            self.system.update_spin(&key_str, atom.spin + 0.1).await;
            
            if let Some(rows) = atom.nucleus.get("row_ids").and_then(|v| v.as_array()) {
                return rows.iter().filter_map(|v| v.as_u64()).collect();
            }
        }
        vec![]
    }
    
    pub async fn get_hot_keys(&self, threshold: f64) -> Vec<String> {
        let atoms = self.system.get_high_spin(threshold).await;
        atoms.into_iter().map(|a| a.id).collect()
    }
}
