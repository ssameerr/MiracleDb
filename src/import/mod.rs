//! Import Module - Data import utilities

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Import format
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ImportFormat {
    Csv,
    Json,
    JsonLines,
    Parquet,
    Avro,
    Sql,
}

/// Import options
#[derive(Clone, Debug)]
pub struct ImportOptions {
    pub format: ImportFormat,
    pub has_headers: bool,
    pub delimiter: char,
    pub quote: char,
    pub escape: char,
    pub null_string: String,
    pub skip_rows: usize,
    pub max_rows: Option<usize>,
    pub columns: Option<Vec<String>>,
    pub column_types: HashMap<String, ColumnType>,
    pub on_error: ErrorHandling,
    pub batch_size: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ColumnType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    Timestamp,
    Json,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ErrorHandling {
    Abort,
    Skip,
    Replace(char),
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            format: ImportFormat::Csv,
            has_headers: true,
            delimiter: ',',
            quote: '"',
            escape: '\\',
            null_string: "".to_string(),
            skip_rows: 0,
            max_rows: None,
            columns: None,
            column_types: HashMap::new(),
            on_error: ErrorHandling::Abort,
            batch_size: 1000,
        }
    }
}

/// Data importer
pub struct Importer;

impl Importer {
    /// Parse CSV data
    pub fn from_csv(data: &str, options: &ImportOptions) -> Result<Vec<serde_json::Value>, String> {
        let lines: Vec<&str> = data.lines().skip(options.skip_rows).collect();
        
        if lines.is_empty() {
            return Ok(vec![]);
        }

        let headers: Vec<String> = if options.has_headers {
            Self::parse_csv_line(lines[0], options)
        } else {
            options.columns.clone().unwrap_or_else(|| {
                (0..Self::parse_csv_line(lines[0], options).len())
                    .map(|i| format!("column_{}", i))
                    .collect()
            })
        };

        let start_row = if options.has_headers { 1 } else { 0 };
        let mut rows = Vec::new();

        for (i, line) in lines.iter().enumerate().skip(start_row) {
            if let Some(max) = options.max_rows {
                if rows.len() >= max {
                    break;
                }
            }

            let values = Self::parse_csv_line(line, options);
            let mut row = serde_json::Map::new();

            for (j, header) in headers.iter().enumerate() {
                let value = values.get(j).cloned().unwrap_or_default();
                let json_value = Self::convert_value(&value, options.column_types.get(header), options);
                row.insert(header.clone(), json_value);
            }

            rows.push(serde_json::Value::Object(row));
        }

        Ok(rows)
    }

    fn parse_csv_line(line: &str, options: &ImportOptions) -> Vec<String> {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if c == options.quote {
                if in_quotes {
                    if chars.peek() == Some(&options.quote) {
                        current.push(options.quote);
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                } else {
                    in_quotes = true;
                }
            } else if c == options.delimiter && !in_quotes {
                values.push(current.trim().to_string());
                current = String::new();
            } else {
                current.push(c);
            }
        }

        values.push(current.trim().to_string());
        values
    }

    fn convert_value(value: &str, column_type: Option<&ColumnType>, options: &ImportOptions) -> serde_json::Value {
        if value == options.null_string || value.is_empty() {
            return serde_json::Value::Null;
        }

        match column_type {
            Some(ColumnType::Integer) => {
                value.parse::<i64>()
                    .map(|n| serde_json::json!(n))
                    .unwrap_or(serde_json::Value::Null)
            }
            Some(ColumnType::Float) => {
                value.parse::<f64>()
                    .map(|n| serde_json::json!(n))
                    .unwrap_or(serde_json::Value::Null)
            }
            Some(ColumnType::Boolean) => {
                let lower = value.to_lowercase();
                serde_json::json!(lower == "true" || lower == "1" || lower == "yes")
            }
            Some(ColumnType::Json) => {
                serde_json::from_str(value).unwrap_or(serde_json::Value::Null)
            }
            _ => serde_json::json!(value),
        }
    }

    /// Parse JSON data
    pub fn from_json(data: &str) -> Result<Vec<serde_json::Value>, String> {
        serde_json::from_str(data)
            .map_err(|e| format!("JSON parse error: {}", e))
    }

    /// Parse JSON Lines data
    pub fn from_jsonlines(data: &str) -> Result<Vec<serde_json::Value>, String> {
        data.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|e| format!("JSON parse error: {}", e))
            })
            .collect()
    }

    /// Import from format
    pub fn import(data: &str, options: &ImportOptions) -> Result<Vec<serde_json::Value>, String> {
        match options.format {
            ImportFormat::Csv => Self::from_csv(data, options),
            ImportFormat::Json => Self::from_json(data),
            ImportFormat::JsonLines => Self::from_jsonlines(data),
            _ => Err("Unsupported format".to_string()),
        }
    }

    /// Validate imported data
    pub fn validate(rows: &[serde_json::Value], schema: &ImportSchema) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        for (i, row) in rows.iter().enumerate() {
            if let Some(obj) = row.as_object() {
                // Check required fields
                for field in &schema.required_fields {
                    if !obj.contains_key(field) || obj.get(field) == Some(&serde_json::Value::Null) {
                        errors.push(ValidationError {
                            row: i,
                            field: field.clone(),
                            error: "Required field missing".to_string(),
                        });
                    }
                }

                // Check field types
                for (field, expected_type) in &schema.field_types {
                    if let Some(value) = obj.get(field) {
                        if !Self::matches_type(value, expected_type) {
                            errors.push(ValidationError {
                                row: i,
                                field: field.clone(),
                                error: format!("Expected type {:?}", expected_type),
                            });
                        }
                    }
                }
            }
        }

        errors
    }

    fn matches_type(value: &serde_json::Value, expected: &ColumnType) -> bool {
        match expected {
            ColumnType::String => value.is_string(),
            ColumnType::Integer => value.is_i64(),
            ColumnType::Float => value.is_f64(),
            ColumnType::Boolean => value.is_boolean(),
            _ => true,
        }
    }
}

/// Import schema for validation
#[derive(Clone, Debug, Default)]
pub struct ImportSchema {
    pub required_fields: Vec<String>,
    pub field_types: HashMap<String, ColumnType>,
}

/// Validation error
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationError {
    pub row: usize,
    pub field: String,
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_import() {
        let csv = "name,age\nAlice,30\nBob,25";
        let result = Importer::from_csv(csv, &ImportOptions::default()).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["name"], "Alice");
    }

    #[test]
    fn test_json_import() {
        let json = r#"[{"name": "Alice"}, {"name": "Bob"}]"#;
        let result = Importer::from_json(json).unwrap();
        assert_eq!(result.len(), 2);
    }
}
