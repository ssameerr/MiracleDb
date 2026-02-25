//! Format Module - Data formatting utilities

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Output format
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum OutputFormat {
    Json,
    JsonPretty,
    Csv,
    Table,
    Markdown,
    Yaml,
}

/// Result set formatter
pub struct ResultFormatter;

impl ResultFormatter {
    /// Format results as JSON
    pub fn to_json(rows: &[serde_json::Value]) -> String {
        serde_json::to_string(rows).unwrap_or_default()
    }

    /// Format results as pretty JSON
    pub fn to_json_pretty(rows: &[serde_json::Value]) -> String {
        serde_json::to_string_pretty(rows).unwrap_or_default()
    }

    /// Format results as CSV
    pub fn to_csv(rows: &[serde_json::Value]) -> String {
        if rows.is_empty() {
            return String::new();
        }

        let mut output = String::new();

        // Get headers from first row
        if let Some(first) = rows.first() {
            if let Some(obj) = first.as_object() {
                let headers: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
                output.push_str(&headers.join(","));
                output.push('\n');

                // Write rows
                for row in rows {
                    if let Some(obj) = row.as_object() {
                        let values: Vec<_> = headers
                            .iter()
                            .map(|h| {
                                obj.get(*h)
                                    .map(|v| Self::csv_escape(v))
                                    .unwrap_or_default()
                            })
                            .collect();
                        output.push_str(&values.join(","));
                        output.push('\n');
                    }
                }
            }
        }

        output
    }

    fn csv_escape(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => {
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            }
            serde_json::Value::Null => String::new(),
            _ => value.to_string(),
        }
    }

    /// Format results as ASCII table
    pub fn to_table(rows: &[serde_json::Value]) -> String {
        if rows.is_empty() {
            return "Empty result set".to_string();
        }

        let mut output = String::new();

        // Get headers and column widths
        let headers: Vec<String> = rows.first()
            .and_then(|r| r.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();

        // Calculate max widths
        for row in rows {
            if let Some(obj) = row.as_object() {
                for (i, header) in headers.iter().enumerate() {
                    let val = obj.get(header)
                        .map(|v| v.to_string())
                        .unwrap_or_default();
                    widths[i] = widths[i].max(val.len());
                }
            }
        }

        // Draw top border
        output.push_str(&Self::draw_border(&widths, '┌', '┬', '┐'));

        // Draw headers
        output.push('│');
        for (i, header) in headers.iter().enumerate() {
            output.push_str(&format!(" {:width$} │", header, width = widths[i]));
        }
        output.push('\n');

        // Draw header separator
        output.push_str(&Self::draw_border(&widths, '├', '┼', '┤'));

        // Draw rows
        for row in rows {
            output.push('│');
            if let Some(obj) = row.as_object() {
                for (i, header) in headers.iter().enumerate() {
                    let val = obj.get(header)
                        .map(|v| v.to_string())
                        .unwrap_or_default();
                    output.push_str(&format!(" {:width$} │", val, width = widths[i]));
                }
            }
            output.push('\n');
        }

        // Draw bottom border
        output.push_str(&Self::draw_border(&widths, '└', '┴', '┘'));

        output
    }

    fn draw_border(widths: &[usize], left: char, mid: char, right: char) -> String {
        let mut border = String::new();
        border.push(left);
        for (i, width) in widths.iter().enumerate() {
            border.push_str(&"─".repeat(*width + 2));
            if i < widths.len() - 1 {
                border.push(mid);
            }
        }
        border.push(right);
        border.push('\n');
        border
    }

    /// Format results as Markdown table
    pub fn to_markdown(rows: &[serde_json::Value]) -> String {
        if rows.is_empty() {
            return "No results".to_string();
        }

        let mut output = String::new();

        // Get headers
        let headers: Vec<String> = rows.first()
            .and_then(|r| r.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        // Header row
        output.push('|');
        for header in &headers {
            output.push_str(&format!(" {} |", header));
        }
        output.push('\n');

        // Separator row
        output.push('|');
        for _ in &headers {
            output.push_str(" --- |");
        }
        output.push('\n');

        // Data rows
        for row in rows {
            output.push('|');
            if let Some(obj) = row.as_object() {
                for header in &headers {
                    let val = obj.get(header)
                        .map(|v| v.to_string())
                        .unwrap_or_default();
                    output.push_str(&format!(" {} |", val));
                }
            }
            output.push('\n');
        }

        output
    }

    /// Format results in specified format
    pub fn format(rows: &[serde_json::Value], format: OutputFormat) -> String {
        match format {
            OutputFormat::Json => Self::to_json(rows),
            OutputFormat::JsonPretty => Self::to_json_pretty(rows),
            OutputFormat::Csv => Self::to_csv(rows),
            OutputFormat::Table => Self::to_table(rows),
            OutputFormat::Markdown => Self::to_markdown(rows),
            OutputFormat::Yaml => {
                serde_yaml::to_string(rows).unwrap_or_default()
            }
        }
    }
}

/// Number formatter
pub struct NumberFormatter;

impl NumberFormatter {
    /// Format with thousand separators
    pub fn with_separators(n: i64) -> String {
        let s = n.abs().to_string();
        let mut result = String::new();
        
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.insert(0, ',');
            }
            result.insert(0, c);
        }

        if n < 0 {
            result.insert(0, '-');
        }

        result
    }

    /// Format bytes as human readable
    pub fn bytes_to_human(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
        
        if bytes == 0 {
            return "0 B".to_string();
        }

        let exp = ((bytes as f64).ln() / 1024_f64.ln()).floor() as usize;
        let exp = exp.min(UNITS.len() - 1);
        let value = bytes as f64 / 1024_f64.powi(exp as i32);

        format!("{:.2} {}", value, UNITS[exp])
    }

    /// Format duration as human readable
    pub fn duration_to_human(ms: u64) -> String {
        if ms < 1000 {
            format!("{}ms", ms)
        } else if ms < 60_000 {
            format!("{:.2}s", ms as f64 / 1000.0)
        } else if ms < 3_600_000 {
            format!("{:.1}m", ms as f64 / 60_000.0)
        } else {
            format!("{:.1}h", ms as f64 / 3_600_000.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_formatting() {
        assert_eq!(NumberFormatter::bytes_to_human(0), "0 B");
        assert_eq!(NumberFormatter::bytes_to_human(1024), "1.00 KB");
        assert_eq!(NumberFormatter::bytes_to_human(1048576), "1.00 MB");
    }

    #[test]
    fn test_number_separators() {
        assert_eq!(NumberFormatter::with_separators(1000), "1,000");
        assert_eq!(NumberFormatter::with_separators(1000000), "1,000,000");
    }
}
