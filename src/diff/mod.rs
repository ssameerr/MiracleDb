//! Diff Module - Data comparison and change detection

use std::collections::{HashMap, HashSet};
use serde::{Serialize, Deserialize};

/// Diff type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DiffType {
    Added,
    Removed,
    Modified,
    Unchanged,
}

/// Field diff
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDiff {
    pub field: String,
    pub old_value: Option<serde_json::Value>,
    pub new_value: Option<serde_json::Value>,
    pub diff_type: DiffType,
}

/// Row diff
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RowDiff {
    pub key: String,
    pub diff_type: DiffType,
    pub fields: Vec<FieldDiff>,
}

/// Table diff result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableDiff {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
    pub unchanged: usize,
    pub rows: Vec<RowDiff>,
}

/// Data differ
pub struct Differ;

impl Differ {
    /// Compare two rows
    pub fn diff_rows(old: &serde_json::Value, new: &serde_json::Value) -> Vec<FieldDiff> {
        let mut diffs = Vec::new();

        let old_obj = old.as_object();
        let new_obj = new.as_object();

        if let (Some(old_map), Some(new_map)) = (old_obj, new_obj) {
            let all_keys: HashSet<_> = old_map.keys().chain(new_map.keys()).collect();

            for key in all_keys {
                let old_val = old_map.get(key);
                let new_val = new_map.get(key);

                match (old_val, new_val) {
                    (None, Some(v)) => {
                        diffs.push(FieldDiff {
                            field: key.clone(),
                            old_value: None,
                            new_value: Some(v.clone()),
                            diff_type: DiffType::Added,
                        });
                    }
                    (Some(v), None) => {
                        diffs.push(FieldDiff {
                            field: key.clone(),
                            old_value: Some(v.clone()),
                            new_value: None,
                            diff_type: DiffType::Removed,
                        });
                    }
                    (Some(old_v), Some(new_v)) => {
                        if old_v != new_v {
                            diffs.push(FieldDiff {
                                field: key.clone(),
                                old_value: Some(old_v.clone()),
                                new_value: Some(new_v.clone()),
                                diff_type: DiffType::Modified,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        diffs
    }

    /// Compare two tables
    pub fn diff_tables(
        old_rows: &[serde_json::Value],
        new_rows: &[serde_json::Value],
        key_field: &str,
    ) -> TableDiff {
        let old_map: HashMap<String, &serde_json::Value> = old_rows.iter()
            .filter_map(|r| {
                r.get(key_field)
                    .and_then(|v| v.as_str())
                    .map(|k| (k.to_string(), r))
            })
            .collect();

        let new_map: HashMap<String, &serde_json::Value> = new_rows.iter()
            .filter_map(|r| {
                r.get(key_field)
                    .and_then(|v| v.as_str())
                    .map(|k| (k.to_string(), r))
            })
            .collect();

        let all_keys: HashSet<_> = old_map.keys().chain(new_map.keys()).collect();
        let mut row_diffs = Vec::new();
        let mut added = 0;
        let mut removed = 0;
        let mut modified = 0;
        let mut unchanged = 0;

        for key in all_keys {
            match (old_map.get(key), new_map.get(key)) {
                (None, Some(new_row)) => {
                    added += 1;
                    row_diffs.push(RowDiff {
                        key: key.clone(),
                        diff_type: DiffType::Added,
                        fields: Self::diff_rows(&serde_json::Value::Null, new_row),
                    });
                }
                (Some(old_row), None) => {
                    removed += 1;
                    row_diffs.push(RowDiff {
                        key: key.clone(),
                        diff_type: DiffType::Removed,
                        fields: Self::diff_rows(old_row, &serde_json::Value::Null),
                    });
                }
                (Some(old_row), Some(new_row)) => {
                    let field_diffs = Self::diff_rows(old_row, new_row);
                    if field_diffs.is_empty() {
                        unchanged += 1;
                    } else {
                        modified += 1;
                        row_diffs.push(RowDiff {
                            key: key.clone(),
                            diff_type: DiffType::Modified,
                            fields: field_diffs,
                        });
                    }
                }
                _ => {}
            }
        }

        TableDiff {
            added,
            removed,
            modified,
            unchanged,
            rows: row_diffs,
        }
    }

    /// Generate patch from diff
    pub fn generate_patch(diff: &TableDiff, key_field: &str) -> Vec<PatchOperation> {
        let mut ops = Vec::new();

        for row in &diff.rows {
            match row.diff_type {
                DiffType::Added => {
                    let mut values = HashMap::new();
                    for field in &row.fields {
                        if let Some(v) = &field.new_value {
                            values.insert(field.field.clone(), v.clone());
                        }
                    }
                    ops.push(PatchOperation::Insert { values });
                }
                DiffType::Removed => {
                    ops.push(PatchOperation::Delete { 
                        key_field: key_field.to_string(),
                        key_value: serde_json::json!(row.key),
                    });
                }
                DiffType::Modified => {
                    let mut updates = HashMap::new();
                    for field in &row.fields {
                        if let Some(v) = &field.new_value {
                            updates.insert(field.field.clone(), v.clone());
                        }
                    }
                    ops.push(PatchOperation::Update { 
                        key_field: key_field.to_string(),
                        key_value: serde_json::json!(row.key),
                        updates,
                    });
                }
                _ => {}
            }
        }

        ops
    }
}

/// Patch operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PatchOperation {
    Insert { values: HashMap<String, serde_json::Value> },
    Update { key_field: String, key_value: serde_json::Value, updates: HashMap<String, serde_json::Value> },
    Delete { key_field: String, key_value: serde_json::Value },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_rows() {
        let old = serde_json::json!({"name": "John", "age": 30});
        let new = serde_json::json!({"name": "John", "age": 31, "city": "NYC"});
        
        let diffs = Differ::diff_rows(&old, &new);
        assert_eq!(diffs.len(), 2); // age modified, city added
    }

    #[test]
    fn test_diff_tables() {
        let old = vec![
            serde_json::json!({"id": "1", "name": "John"}),
            serde_json::json!({"id": "2", "name": "Jane"}),
        ];
        let new = vec![
            serde_json::json!({"id": "1", "name": "Johnny"}),
            serde_json::json!({"id": "3", "name": "Bob"}),
        ];

        let diff = Differ::diff_tables(&old, &new, "id");
        assert_eq!(diff.added, 1);
        assert_eq!(diff.removed, 1);
        assert_eq!(diff.modified, 1);
    }
}
