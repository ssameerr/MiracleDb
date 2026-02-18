use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// A partial index only covers rows matching a filter predicate.
/// e.g. CREATE INDEX active_users ON users(email) WHERE status = 'active'
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub filter: String,         // SQL WHERE clause
    pub indexed_row_ids: Vec<u64>,
}

impl PartialIndex {
    pub fn new(name: &str, table: &str, columns: Vec<String>, filter: &str) -> Self {
        Self {
            name: name.to_string(),
            table: table.to_string(),
            columns,
            filter: filter.to_string(),
            indexed_row_ids: vec![],
        }
    }

    /// Add a row ID that satisfies the partial index filter.
    pub fn add_row(&mut self, row_id: u64) {
        if !self.indexed_row_ids.contains(&row_id) {
            self.indexed_row_ids.push(row_id);
        }
    }

    /// Remove a row ID from the partial index (e.g. row no longer matches filter).
    pub fn remove_row(&mut self, row_id: u64) {
        self.indexed_row_ids.retain(|&id| id != row_id);
    }

    /// Returns the number of rows covered by this partial index.
    pub fn len(&self) -> usize {
        self.indexed_row_ids.len()
    }

    /// Returns true if no rows are indexed.
    pub fn is_empty(&self) -> bool {
        self.indexed_row_ids.is_empty()
    }
}

/// Manager for partial indexes, keyed by index name.
#[derive(Debug, Default)]
pub struct PartialIndexManager {
    indexes: HashMap<String, PartialIndex>,
}

impl PartialIndexManager {
    pub fn new() -> Self {
        Self { indexes: HashMap::new() }
    }

    /// Register a new partial index definition.
    pub fn create(&mut self, index: PartialIndex) {
        self.indexes.insert(index.name.clone(), index);
    }

    /// Drop a partial index by name.
    pub fn drop(&mut self, name: &str) -> bool {
        self.indexes.remove(name).is_some()
    }

    /// Look up a partial index by name.
    pub fn get(&self, name: &str) -> Option<&PartialIndex> {
        self.indexes.get(name)
    }

    /// Look up a partial index mutably by name.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut PartialIndex> {
        self.indexes.get_mut(name)
    }

    /// List all partial indexes for a given table.
    pub fn list_for_table(&self, table: &str) -> Vec<&PartialIndex> {
        self.indexes.values().filter(|i| i.table == table).collect()
    }

    /// List all registered partial indexes.
    pub fn list_all(&self) -> Vec<&PartialIndex> {
        self.indexes.values().collect()
    }
}
