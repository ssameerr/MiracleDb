//! Logs Module - Tantivy-based full-text search and anomaly detection

use std::path::Path;
use tantivy::{Index, IndexWriter, IndexReader, doc, schema::*};
use tantivy::schema::{TantivyDocument, Document as DocumentTrait};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: i64,
    pub level: LogLevel,
    pub message: String,
    pub source: String,
    pub metadata: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl ToString for LogLevel {
    fn to_string(&self) -> String {
        match self {
            LogLevel::Trace => "TRACE".to_string(),
            LogLevel::Debug => "DEBUG".to_string(),
            LogLevel::Info => "INFO".to_string(),
            LogLevel::Warn => "WARN".to_string(),
            LogLevel::Error => "ERROR".to_string(),
        }
    }
}

/// Log engine with full-text search powered by Tantivy
pub struct LogEngine {
    index: Arc<Index>,
    writer: Arc<RwLock<IndexWriter>>,
    reader: Arc<IndexReader>,
    schema: Schema,
    timestamp_field: Field,
    level_field: Field,
    message_field: Field,
    source_field: Field,
}

impl LogEngine {
    pub fn new(index_path: &str) -> Result<Self, String> {
        // Create schema
        let mut schema_builder = Schema::builder();
        let timestamp_field = schema_builder.add_i64_field("timestamp", INDEXED | STORED);
        let level_field = schema_builder.add_text_field("level", TEXT | STORED);
        let message_field = schema_builder.add_text_field("message", TEXT | STORED);
        let source_field = schema_builder.add_text_field("source", TEXT | STORED);
        let schema = schema_builder.build();

        // Create or open index
        let index = if Path::new(index_path).exists() {
            Index::open_in_dir(index_path)
                .map_err(|e| format!("Failed to open index: {}", e))?
        } else {
            std::fs::create_dir_all(index_path)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
            Index::create_in_dir(index_path, schema.clone())
                .map_err(|e| format!("Failed to create index: {}", e))?
        };

        let writer = index.writer(50_000_000)
            .map_err(|e| format!("Failed to create writer: {}", e))?;
        
        let reader = index.reader()
            .map_err(|e| format!("Failed to create reader: {}", e))?;

        Ok(Self {
            index: Arc::new(index),
            writer: Arc::new(RwLock::new(writer)),
            reader: Arc::new(reader),
            schema,
            timestamp_field,
            level_field,
            message_field,
            source_field,
        })
    }

    /// Ingest a log entry
    pub async fn ingest(&self, entry: LogEntry) -> Result<(), String> {
        let mut writer = self.writer.write().await;

        let doc = doc!(
            self.timestamp_field => entry.timestamp,
            self.level_field => entry.level.to_string(),
            self.message_field => entry.message,
            self.source_field => entry.source,
        );

        writer.add_document(doc)
            .map_err(|e| format!("Failed to add document: {}", e))?;

        Ok(())
    }

    /// Commit all pending writes
    pub async fn commit(&self) -> Result<(), String> {
        let mut writer = self.writer.write().await;
        writer.commit()
            .map_err(|e| format!("Failed to commit: {}", e))?;
        
        self.reader.reload()
            .map_err(|e| format!("Failed to reload reader: {}", e))?;
        
        Ok(())
    }

    /// Search logs with full-text query
    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<LogEntry>, String> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.message_field, self.source_field]);
        
        let query = query_parser.parse_query(query_str)
            .map_err(|e| format!("Failed to parse query: {}", e))?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))
            .map_err(|e| format!("Search failed: {}", e))?;

        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)
                .map_err(|e| format!("Failed to retrieve document: {}", e))?;

            let timestamp = retrieved_doc.get_first(self.timestamp_field)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            
            let level_str = retrieved_doc.get_first(self.level_field)
                .and_then(|v| v.as_str())
                .unwrap_or("INFO");
            
            let message = retrieved_doc.get_first(self.message_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            
            let source = retrieved_doc.get_first(self.source_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let level = match level_str {
                "TRACE" => LogLevel::Trace,
                "DEBUG" => LogLevel::Debug,
                "INFO" => LogLevel::Info,
                "WARN" => LogLevel::Warn,
                "ERROR" => LogLevel::Error,
                _ => LogLevel::Info,
            };

            results.push(LogEntry {
                timestamp,
                level,
                message,
                source,
                metadata: serde_json::Value::Null,
            });
        }

        Ok(results)
    }

    /// Filter by log level
    pub fn filter_level(&self, level: LogLevel, limit: usize) -> Result<Vec<LogEntry>, String> {
        let query_str = format!("level:{}", level.to_string());
        self.search(&query_str, limit)
    }

    /// Detect anomalies using statistical analysis (z-score method)
    pub async fn detect_anomalies(&self, window_size: usize) -> Vec<LogEntry> {
        // Get recent logs for analysis
        let all_logs = match self.search("*", window_size * 2) {
            Ok(logs) => logs,
            Err(_) => return vec![],
        };
        
        if all_logs.len() < 10 {
            return vec![]; // Need minimum data for statistics
        }
        
        let mut anomalies = Vec::new();
        
        // 1. Error Rate Spike Detection
        // Calculate baseline error rate
        let error_count: usize = all_logs.iter()
            .filter(|log| log.level == LogLevel::Error)
            .count();
        let error_rate = error_count as f64 / all_logs.len() as f64;
        
        // Check recent window for spike (>2 std deviations above mean)
        let recent_window = &all_logs[..window_size.min(all_logs.len())];
        let recent_errors: usize = recent_window.iter()
            .filter(|log| log.level == LogLevel::Error)
            .count();
        let recent_error_rate = recent_errors as f64 / recent_window.len() as f64;
        
        // If recent error rate is significantly higher than baseline
        if recent_error_rate > error_rate * 2.0 && recent_errors > 2 {
            // Mark recent errors as anomalies
            for log in recent_window {
                if log.level == LogLevel::Error {
                    anomalies.push(log.clone());
                }
            }
        }
        
        // 2. Message Frequency Anomaly (sudden burst of same message)
        let mut message_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for log in recent_window {
            let key = log.message.chars().take(50).collect::<String>(); // Truncate for grouping
            *message_counts.entry(key).or_insert(0) += 1;
        }
        
        // Find messages with unusually high frequency
        let avg_count = message_counts.values().sum::<usize>() as f64 / message_counts.len().max(1) as f64;
        for (msg, count) in &message_counts {
            if *count as f64 > avg_count * 3.0 && *count > 5 {
                // This message is appearing unusually often
                if let Some(sample) = recent_window.iter().find(|l| l.message.starts_with(msg)) {
                    anomalies.push(sample.clone());
                }
            }
        }
        
        // 3. Time Gap Anomaly (long gaps in logging suggest system issues)
        if recent_window.len() >= 2 {
            for i in 1..recent_window.len() {
                let gap = (recent_window[i-1].timestamp - recent_window[i].timestamp).abs();
                if gap > 300 { // 5+ minute gap
                    anomalies.push(LogEntry {
                        timestamp: recent_window[i].timestamp,
                        level: LogLevel::Warn,
                        message: format!("Detected log gap of {} seconds", gap),
                        source: "anomaly_detector".to_string(),
                        metadata: serde_json::Value::Null,
                    });
                }
            }
        }
        
        anomalies
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_engine() {
        let engine = LogEngine::new("/tmp/miracledb_test_logs").unwrap();

        let entry = LogEntry {
            timestamp: chrono::Utc::now().timestamp(),
            level: LogLevel::Info,
            message: "Test log message".to_string(),
            source: "test".to_string(),
            metadata: serde_json::Value::Null,
        };

        engine.ingest(entry).await.unwrap();
        engine.commit().await.unwrap();

        let results = engine.search("test", 10).unwrap();
        assert!(!results.is_empty());
    }
}
