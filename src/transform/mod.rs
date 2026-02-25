//! Transform Module - Data transformation utilities

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Transform operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Transform {
    // Field operations
    Rename { from: String, to: String },
    Drop { field: String },
    Add { field: String, value: serde_json::Value },
    Set { field: String, expression: String },
    
    // Type conversions
    ToString { field: String },
    ToInt { field: String },
    ToFloat { field: String },
    ToBool { field: String },
    ToDate { field: String, format: String },
    
    // String operations
    Uppercase { field: String },
    Lowercase { field: String },
    Trim { field: String },
    Replace { field: String, from: String, to: String },
    Substring { field: String, start: usize, length: Option<usize> },
    Split { field: String, delimiter: String, into: Vec<String> },
    Concat { fields: Vec<String>, into: String, separator: String },
    
    // Numeric operations
    Round { field: String, decimals: u32 },
    Abs { field: String },
    Multiply { field: String, factor: f64 },
    AddNum { field: String, amount: f64 },
    
    // JSON operations
    Flatten { field: String, prefix: Option<String> },
    Nest { fields: Vec<String>, into: String },
    Extract { field: String, path: String, into: String },
    
    // Conditional
    IfElse { condition: String, then_transform: Box<Transform>, else_transform: Option<Box<Transform>> },
    Coalesce { fields: Vec<String>, into: String },
    
    // Masking
    Mask { field: String, mask_char: char, visible_start: usize, visible_end: usize },
    Hash { field: String },
    Redact { field: String, replacement: String },
}

/// Transformer
pub struct Transformer;

impl Transformer {
    /// Apply transforms to a row
    pub fn apply(row: serde_json::Value, transforms: &[Transform]) -> serde_json::Value {
        let mut result = row;
        
        for transform in transforms {
            result = Self::apply_one(result, transform);
        }
        
        result
    }

    fn apply_one(mut row: serde_json::Value, transform: &Transform) -> serde_json::Value {
        match transform {
            Transform::Rename { from, to } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(value) = obj.remove(from) {
                        obj.insert(to.clone(), value);
                    }
                }
            }
            Transform::Drop { field } => {
                if let Some(obj) = row.as_object_mut() {
                    obj.remove(field);
                }
            }
            Transform::Add { field, value } => {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert(field.clone(), value.clone());
                }
            }
            Transform::ToString { field } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(value) = obj.get(field) {
                        let string_val = match value {
                            serde_json::Value::String(s) => s.clone(),
                            _ => value.to_string(),
                        };
                        obj.insert(field.clone(), serde_json::json!(string_val));
                    }
                }
            }
            Transform::ToInt { field } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(value) = obj.get(field).cloned() {
                        let int_val = match &value {
                            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
                            serde_json::Value::String(s) => s.parse().unwrap_or(0),
                            _ => 0,
                        };
                        obj.insert(field.clone(), serde_json::json!(int_val));
                    }
                }
            }
            Transform::Uppercase { field } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(serde_json::Value::String(s)) = obj.get(field).cloned() {
                        obj.insert(field.clone(), serde_json::json!(s.to_uppercase()));
                    }
                }
            }
            Transform::Lowercase { field } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(serde_json::Value::String(s)) = obj.get(field).cloned() {
                        obj.insert(field.clone(), serde_json::json!(s.to_lowercase()));
                    }
                }
            }
            Transform::Trim { field } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(serde_json::Value::String(s)) = obj.get(field).cloned() {
                        obj.insert(field.clone(), serde_json::json!(s.trim()));
                    }
                }
            }
            Transform::Replace { field, from, to } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(serde_json::Value::String(s)) = obj.get(field).cloned() {
                        obj.insert(field.clone(), serde_json::json!(s.replace(from, to)));
                    }
                }
            }
            Transform::Concat { fields, into, separator } => {
                if let Some(obj) = row.as_object_mut() {
                    let parts: Vec<String> = fields.iter()
                        .filter_map(|f| obj.get(f))
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                        .collect();
                    obj.insert(into.clone(), serde_json::json!(parts.join(separator)));
                }
            }
            Transform::Round { field, decimals } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(serde_json::Value::Number(n)) = obj.get(field).cloned() {
                        if let Some(f) = n.as_f64() {
                            let factor = 10_f64.powi(*decimals as i32);
                            let rounded = (f * factor).round() / factor;
                            obj.insert(field.clone(), serde_json::json!(rounded));
                        }
                    }
                }
            }
            Transform::Mask { field, mask_char, visible_start, visible_end } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(serde_json::Value::String(s)) = obj.get(field).cloned() {
                        let chars: Vec<char> = s.chars().collect();
                        let masked: String = chars.iter().enumerate()
                            .map(|(i, c)| {
                                if i < *visible_start || i >= chars.len() - *visible_end {
                                    *c
                                } else {
                                    *mask_char
                                }
                            })
                            .collect();
                        obj.insert(field.clone(), serde_json::json!(masked));
                    }
                }
            }
            Transform::Hash { field } => {
                if let Some(obj) = row.as_object_mut() {
                    if let Some(value) = obj.get(field).cloned() {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        value.to_string().hash(&mut hasher);
                        let hash = format!("{:x}", hasher.finish());
                        obj.insert(field.clone(), serde_json::json!(hash));
                    }
                }
            }
            Transform::Redact { field, replacement } => {
                if let Some(obj) = row.as_object_mut() {
                    if obj.contains_key(field) {
                        obj.insert(field.clone(), serde_json::json!(replacement));
                    }
                }
            }
            Transform::Coalesce { fields, into } => {
                if let Some(obj) = row.as_object_mut() {
                    for field in fields {
                        if let Some(value) = obj.get(field) {
                            if !value.is_null() {
                                obj.insert(into.clone(), value.clone());
                                break;
                            }
                        }
                    }
                }
            }
            Transform::Nest { fields, into } => {
                if let Some(obj) = row.as_object_mut() {
                    let mut nested = serde_json::Map::new();
                    for field in fields {
                        if let Some(value) = obj.remove(field) {
                            nested.insert(field.clone(), value);
                        }
                    }
                    obj.insert(into.clone(), serde_json::Value::Object(nested));
                }
            }
            _ => {}
        }
        
        row
    }

    /// Apply transforms to multiple rows
    pub fn apply_batch(rows: Vec<serde_json::Value>, transforms: &[Transform]) -> Vec<serde_json::Value> {
        rows.into_iter()
            .map(|row| Self::apply(row, transforms))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rename() {
        let row = serde_json::json!({"old_name": "value"});
        let transforms = vec![Transform::Rename { from: "old_name".to_string(), to: "new_name".to_string() }];
        let result = Transformer::apply(row, &transforms);
        assert!(result.get("new_name").is_some());
        assert!(result.get("old_name").is_none());
    }

    #[test]
    fn test_mask() {
        let row = serde_json::json!({"ssn": "123-45-6789"});
        let transforms = vec![Transform::Mask { field: "ssn".to_string(), mask_char: '*', visible_start: 0, visible_end: 4 }];
        let result = Transformer::apply(row, &transforms);
        assert!(result.get("ssn").unwrap().as_str().unwrap().contains("*"));
    }
}
