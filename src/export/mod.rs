//! Export Module - Data export utilities

use std::collections::HashMap;
use std::io::Write;
use serde::{Serialize, Deserialize};

/// Export format
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ExportFormat {
    Csv,
    Json,
    JsonLines,
    Parquet,
    Avro,
    Sql,
    Xml,
}

/// Export options
#[derive(Clone, Debug)]
pub struct ExportOptions {
    pub format: ExportFormat,
    pub include_headers: bool,
    pub null_string: String,
    pub delimiter: char,
    pub quote: char,
    pub escape: char,
    pub date_format: String,
    pub timestamp_format: String,
    pub compression: Option<Compression>,
    pub chunk_size: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Compression {
    Gzip,
    Zstd,
    Lz4,
    Snappy,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            format: ExportFormat::Csv,
            include_headers: true,
            null_string: "".to_string(),
            delimiter: ',',
            quote: '"',
            escape: '\\',
            date_format: "%Y-%m-%d".to_string(),
            timestamp_format: "%Y-%m-%d %H:%M:%S".to_string(),
            compression: None,
            chunk_size: 10000,
        }
    }
}

/// Data exporter
pub struct Exporter;

impl Exporter {
    /// Export rows to CSV
    pub fn to_csv(rows: &[serde_json::Value], options: &ExportOptions) -> String {
        let mut output = String::new();

        if rows.is_empty() {
            return output;
        }

        // Get headers
        let headers: Vec<String> = rows.first()
            .and_then(|r| r.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        // Write headers
        if options.include_headers {
            output.push_str(&headers.join(&options.delimiter.to_string()));
            output.push('\n');
        }

        // Write rows
        for row in rows {
            if let Some(obj) = row.as_object() {
                let values: Vec<String> = headers.iter()
                    .map(|h| {
                        obj.get(h)
                            .map(|v| Self::format_value(v, options))
                            .unwrap_or_else(|| options.null_string.clone())
                    })
                    .collect();
                output.push_str(&values.join(&options.delimiter.to_string()));
                output.push('\n');
            }
        }

        output
    }

    /// Export rows to JSON
    pub fn to_json(rows: &[serde_json::Value], pretty: bool) -> String {
        if pretty {
            serde_json::to_string_pretty(rows).unwrap_or_default()
        } else {
            serde_json::to_string(rows).unwrap_or_default()
        }
    }

    /// Export rows to JSON Lines
    pub fn to_jsonlines(rows: &[serde_json::Value]) -> String {
        rows.iter()
            .map(|r| serde_json::to_string(r).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Export rows to SQL INSERT statements
    pub fn to_sql(rows: &[serde_json::Value], table: &str) -> String {
        let mut output = String::new();

        if rows.is_empty() {
            return output;
        }

        let headers: Vec<String> = rows.first()
            .and_then(|r| r.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        for row in rows {
            if let Some(obj) = row.as_object() {
                let values: Vec<String> = headers.iter()
                    .map(|h| {
                        obj.get(h)
                            .map(|v| Self::format_sql_value(v))
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();

                output.push_str(&format!(
                    "INSERT INTO {} ({}) VALUES ({});\n",
                    table,
                    headers.join(", "),
                    values.join(", ")
                ));
            }
        }

        output
    }

    /// Export rows to XML
    pub fn to_xml(rows: &[serde_json::Value], root: &str, row_element: &str) -> String {
        let mut output = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{}>\n", root);

        for row in rows {
            output.push_str(&format!("  <{}>\n", row_element));
            if let Some(obj) = row.as_object() {
                for (key, value) in obj {
                    let text = match value {
                        serde_json::Value::Null => String::new(),
                        serde_json::Value::String(s) => Self::xml_escape(s),
                        _ => value.to_string(),
                    };
                    output.push_str(&format!("    <{}>{}</{}>\n", key, text, key));
                }
            }
            output.push_str(&format!("  </{}>\n", row_element));
        }

        output.push_str(&format!("</{}>\n", root));
        output
    }

    fn format_value(value: &serde_json::Value, options: &ExportOptions) -> String {
        match value {
            serde_json::Value::Null => options.null_string.clone(),
            serde_json::Value::String(s) => {
                if s.contains(options.delimiter) || s.contains(options.quote) || s.contains('\n') {
                    format!("{}{}{}", options.quote, s.replace(options.quote, &format!("{}{}", options.escape, options.quote)), options.quote)
                } else {
                    s.clone()
                }
            }
            serde_json::Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            _ => value.to_string(),
        }
    }

    fn format_sql_value(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            _ => value.to_string(),
        }
    }

    fn xml_escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    }

    /// Export to format
    pub fn export(rows: &[serde_json::Value], options: &ExportOptions, table: &str) -> String {
        match options.format {
            ExportFormat::Csv => Self::to_csv(rows, options),
            ExportFormat::Json => Self::to_json(rows, true),
            ExportFormat::JsonLines => Self::to_jsonlines(rows),
            ExportFormat::Sql => Self::to_sql(rows, table),
            ExportFormat::Xml => Self::to_xml(rows, "data", "row"),
            _ => Self::to_json(rows, false),
        }
    }
}

/// Export job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportJob {
    pub id: String,
    pub query: String,
    pub format: ExportFormat,
    pub destination: String,
    pub status: JobStatus,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub rows_exported: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_export() {
        let rows = vec![
            serde_json::json!({"name": "Alice", "age": 30}),
            serde_json::json!({"name": "Bob", "age": 25}),
        ];
        let csv = Exporter::to_csv(&rows, &ExportOptions::default());
        assert!(csv.contains("Alice"));
        assert!(csv.contains("Bob"));
    }

    #[test]
    fn test_sql_export() {
        let rows = vec![
            serde_json::json!({"id": 1, "name": "Test"}),
        ];
        let sql = Exporter::to_sql(&rows, "users");
        assert!(sql.contains("INSERT INTO users"));
    }
}
