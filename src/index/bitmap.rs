/// Bitmap index for low-cardinality columns (e.g. status, category).
/// Each distinct value maps to a set of row IDs.
/// Uses HashSet<usize> instead of an external bit_set crate.
pub struct BitmapIndex {
    pub column: String,
    pub bitmaps: std::collections::HashMap<String, std::collections::HashSet<usize>>,
}

impl BitmapIndex {
    pub fn new(column: &str) -> Self {
        Self {
            column: column.to_string(),
            bitmaps: std::collections::HashMap::new(),
        }
    }

    /// Insert a row ID for the given value.
    pub fn insert(&mut self, value: &str, row_id: usize) {
        self.bitmaps.entry(value.to_string()).or_default().insert(row_id);
    }

    /// Remove a row ID for the given value.
    pub fn remove(&mut self, value: &str, row_id: usize) {
        if let Some(set) = self.bitmaps.get_mut(value) {
            set.remove(&row_id);
            if set.is_empty() {
                self.bitmaps.remove(value);
            }
        }
    }

    /// Look up all row IDs matching a specific value.
    pub fn lookup(&self, value: &str) -> Option<&std::collections::HashSet<usize>> {
        self.bitmaps.get(value)
    }

    /// Intersection (AND) of two value sets — rows matching both v1 and v2.
    /// In a single-column bitmap index this is only meaningful if one row
    /// can appear under multiple values (e.g. multi-value columns); provided
    /// here for completeness and combined-index use cases.
    pub fn and(&self, v1: &str, v2: &str) -> std::collections::HashSet<usize> {
        match (self.bitmaps.get(v1), self.bitmaps.get(v2)) {
            (Some(a), Some(b)) => a.intersection(b).copied().collect(),
            _ => std::collections::HashSet::new(),
        }
    }

    /// Union (OR) of two value sets — rows matching either v1 or v2.
    pub fn or(&self, v1: &str, v2: &str) -> std::collections::HashSet<usize> {
        match (self.bitmaps.get(v1), self.bitmaps.get(v2)) {
            (Some(a), Some(b)) => a.union(b).copied().collect(),
            (Some(a), None) => a.clone(),
            (None, Some(b)) => b.clone(),
            _ => std::collections::HashSet::new(),
        }
    }

    /// Return the distinct values tracked by this index.
    pub fn distinct_values(&self) -> Vec<&String> {
        self.bitmaps.keys().collect()
    }

    /// Return the cardinality (number of distinct values).
    pub fn cardinality(&self) -> usize {
        self.bitmaps.len()
    }
}
