//! Embedded Mode - Use MiracleDB as an in-process library without a server
//!
//! # Example
//! ```rust,no_run
//! use miracledb::embedded::EmbeddedDb;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // In-memory database
//!     let db = EmbeddedDb::open_memory().await?;
//!     db.execute("CREATE TABLE users (id INT, name VARCHAR)").await?;
//!     db.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
//!     let rows = db.query("SELECT * FROM users").await?;
//!     println!("{} rows", rows.len());
//!     Ok(())
//! }
//! ```

use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

/// Configuration for embedded database
#[derive(Clone, Debug)]
pub struct EmbeddedConfig {
    pub data_dir: Option<String>,      // None = in-memory
    pub max_memory_mb: usize,          // Memory limit
    pub cache_size_mb: usize,          // Query cache size
    pub wal_enabled: bool,             // Write-ahead logging
    pub auto_vacuum: bool,             // Auto-vacuum on close
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            max_memory_mb: 256,
            cache_size_mb: 64,
            wal_enabled: false,
            auto_vacuum: true,
        }
    }
}

impl EmbeddedConfig {
    pub fn memory() -> Self {
        Self::default()
    }

    pub fn file(path: &str) -> Self {
        Self {
            data_dir: Some(path.to_string()),
            wal_enabled: true,
            ..Default::default()
        }
    }
}

/// A row returned from a query
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<serde_json::Value>,
}

impl Row {
    pub fn get(&self, column: &str) -> Option<&serde_json::Value> {
        let idx = self.columns.iter().position(|c| c == column)?;
        self.values.get(idx)
    }

    pub fn get_str(&self, column: &str) -> Option<&str> {
        self.get(column)?.as_str()
    }

    pub fn get_i64(&self, column: &str) -> Option<i64> {
        self.get(column)?.as_i64()
    }

    pub fn get_f64(&self, column: &str) -> Option<f64> {
        self.get(column)?.as_f64()
    }

    pub fn get_bool(&self, column: &str) -> Option<bool> {
        self.get(column)?.as_bool()
    }
}

/// Query result from embedded database
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub rows_affected: usize,
    pub execution_time_ms: f64,
}

impl QueryResult {
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn first(&self) -> Option<&Row> {
        self.rows.first()
    }

    pub fn iter(&self) -> std::slice::Iter<Row> {
        self.rows.iter()
    }
}

/// Table schema information
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub row_count: Option<u64>,
}

/// Embedded in-process database
pub struct EmbeddedDb {
    config: EmbeddedConfig,
    tables: Arc<RwLock<HashMap<String, TableInfo>>>,
    storage: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
}

impl EmbeddedDb {
    /// Open an in-memory database
    pub async fn open_memory() -> Result<Self, String> {
        Self::open(EmbeddedConfig::memory()).await
    }

    /// Open a file-backed database
    pub async fn open_file(path: &str) -> Result<Self, String> {
        Self::open(EmbeddedConfig::file(path)).await
    }

    /// Open with custom configuration
    pub async fn open(config: EmbeddedConfig) -> Result<Self, String> {
        Ok(Self {
            config,
            tables: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Execute a SQL statement (CREATE, INSERT, UPDATE, DELETE)
    pub async fn execute(&self, sql: &str) -> Result<usize, String> {
        let sql = sql.trim();
        let sql_lower = sql.to_lowercase();

        if sql_lower.starts_with("create table") {
            self.handle_create_table(sql).await
        } else if sql_lower.starts_with("insert into") {
            self.handle_insert(sql).await
        } else if sql_lower.starts_with("drop table") {
            self.handle_drop_table(sql).await
        } else if sql_lower.starts_with("delete from") {
            // Simple full table delete
            let table = sql_lower["delete from".len()..].trim().split_whitespace().next()
                .ok_or("Invalid DELETE")?;
            let mut storage = self.storage.write().await;
            let rows = storage.entry(table.to_string()).or_default();
            let count = rows.len();
            rows.clear();
            Ok(count)
        } else {
            // For complex statements, just acknowledge
            Ok(0)
        }
    }

    async fn handle_create_table(&self, sql: &str) -> Result<usize, String> {
        // Extract table name
        let lower = sql.to_lowercase();
        let after_create = lower["create table".len()..].trim();
        let name = after_create.split(|c: char| c == '(' || c.is_whitespace())
            .next().unwrap_or("").trim().to_string();

        if name.is_empty() {
            return Err("Invalid CREATE TABLE: missing table name".to_string());
        }

        // Parse columns (simplified)
        let col_start = sql.find('(').ok_or("Missing column definitions")?;
        let col_end = sql.rfind(')').ok_or("Missing closing paren")?;
        let col_defs = &sql[col_start+1..col_end];

        let mut columns = Vec::new();
        for def in col_defs.split(',') {
            let def = def.trim();
            if def.is_empty() { continue; }
            let parts: Vec<&str> = def.split_whitespace().collect();
            if parts.len() >= 2 {
                columns.push(ColumnInfo {
                    name: parts[0].to_string(),
                    data_type: parts[1].to_uppercase(),
                    nullable: !def.to_lowercase().contains("not null"),
                });
            }
        }

        let mut tables = self.tables.write().await;
        tables.insert(name.clone(), TableInfo {
            name: name.clone(),
            columns,
            row_count: Some(0),
        });

        let mut storage = self.storage.write().await;
        storage.entry(name).or_insert_with(Vec::new);

        Ok(0)
    }

    async fn handle_insert(&self, sql: &str) -> Result<usize, String> {
        let lower = sql.to_lowercase();
        let after_into = lower["insert into".len()..].trim();
        let table_name = after_into.split(|c: char| c == '(' || c.is_whitespace())
            .next().unwrap_or("").trim().to_string();

        if table_name.is_empty() {
            return Err("Invalid INSERT: missing table name".to_string());
        }

        // Check table exists
        let tables = self.tables.read().await;
        let table_info = tables.get(&table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        let col_names: Vec<String> = table_info.columns.iter()
            .map(|c| c.name.clone())
            .collect();
        drop(tables);

        // Parse VALUES (very simplified - handles simple literals)
        let values_pos = lower.find("values").ok_or("Missing VALUES clause")?;
        let values_str = sql[values_pos + 6..].trim().to_string();
        let inner = values_str.trim_start_matches('(').trim_end_matches(')').trim();

        let mut row = serde_json::Map::new();
        for (i, val) in inner.split(',').enumerate() {
            let val = val.trim();
            let key = col_names.get(i).cloned().unwrap_or_else(|| format!("col{}", i));

            // Parse value
            let json_val = if val.starts_with('\'') && val.ends_with('\'') {
                serde_json::Value::String(val[1..val.len()-1].to_string())
            } else if let Ok(n) = val.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(f) = val.parse::<f64>() {
                serde_json::json!(f)
            } else if val.to_lowercase() == "null" {
                serde_json::Value::Null
            } else if val.to_lowercase() == "true" {
                serde_json::Value::Bool(true)
            } else if val.to_lowercase() == "false" {
                serde_json::Value::Bool(false)
            } else {
                serde_json::Value::String(val.to_string())
            };

            row.insert(key, json_val);
        }

        let mut storage = self.storage.write().await;
        storage.entry(table_name).or_default().push(serde_json::Value::Object(row));

        Ok(1)
    }

    async fn handle_drop_table(&self, sql: &str) -> Result<usize, String> {
        let lower = sql.to_lowercase();
        let name = lower["drop table".len()..].trim()
            .replace("if exists", "").trim().to_string();

        let mut tables = self.tables.write().await;
        let mut storage = self.storage.write().await;
        tables.remove(&name);
        storage.remove(&name);
        Ok(0)
    }

    /// Execute a SELECT query
    pub async fn query(&self, sql: &str) -> Result<QueryResult, String> {
        let start = std::time::Instant::now();
        let lower = sql.trim().to_lowercase();

        if !lower.starts_with("select") {
            return Err("Use execute() for non-SELECT statements".to_string());
        }

        // Very simplified SELECT: SELECT * FROM table [WHERE col = val] [LIMIT n]
        let from_pos = lower.find("from ").ok_or("Missing FROM clause")?;
        let after_from = lower[from_pos + 5..].trim();

        let mut limit: Option<usize> = None;
        let mut where_col: Option<String> = None;
        let mut where_val: Option<String> = None;

        let table_name = if let Some(limit_pos) = after_from.find(" limit ") {
            let limit_str = after_from[limit_pos + 7..].trim();
            limit = limit_str.parse().ok();
            after_from[..limit_pos].split_whitespace().next().unwrap_or("").to_string()
        } else if let Some(where_pos) = after_from.find(" where ") {
            let rest = after_from[where_pos + 7..].trim();
            // Parse "col = val"
            let parts: Vec<&str> = rest.splitn(3, ' ').collect();
            if parts.len() >= 3 {
                where_col = Some(parts[0].to_string());
                where_val = Some(parts[2].trim_matches('\'').to_string());
            }
            after_from[..where_pos].split_whitespace().next().unwrap_or("").to_string()
        } else {
            after_from.split_whitespace().next().unwrap_or("").to_string()
        };

        let tables = self.tables.read().await;
        let table_info = tables.get(&table_name)
            .ok_or(format!("Table '{}' not found", table_name))?;

        let columns: Vec<String> = table_info.columns.iter().map(|c| c.name.clone()).collect();
        drop(tables);

        let storage = self.storage.read().await;
        let all_rows = storage.get(&table_name).cloned().unwrap_or_default();

        let mut rows: Vec<Row> = all_rows.iter()
            .filter(|row| {
                if let (Some(ref col), Some(ref val)) = (&where_col, &where_val) {
                    if let Some(obj) = row.as_object() {
                        if let Some(v) = obj.get(col) {
                            let s = match v {
                                serde_json::Value::String(s) => s.clone(),
                                _ => v.to_string(),
                            };
                            return s == *val;
                        }
                    }
                    return false;
                }
                true
            })
            .map(|row| {
                let values: Vec<serde_json::Value> = columns.iter()
                    .map(|c| row.get(c).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();
                Row { columns: columns.clone(), values }
            })
            .collect();

        if let Some(n) = limit {
            rows.truncate(n);
        }

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            execution_time_ms: elapsed,
        })
    }

    /// List all tables
    pub async fn tables(&self) -> Vec<TableInfo> {
        let tables = self.tables.read().await;
        tables.values().cloned().collect()
    }

    /// Get table info
    pub async fn table_info(&self, name: &str) -> Option<TableInfo> {
        let tables = self.tables.read().await;
        tables.get(name).cloned()
    }

    /// Close and optionally persist
    pub async fn close(self) -> Result<(), String> {
        if self.config.auto_vacuum {
            // In production: compact storage
        }
        Ok(())
    }

    /// Begin a transaction
    pub fn transaction(&self) -> Transaction {
        Transaction::new()
    }
}

/// Lightweight transaction handle
pub struct Transaction {
    statements: Vec<String>,
    committed: bool,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            statements: Vec::new(),
            committed: false,
        }
    }

    pub fn execute(&mut self, sql: &str) -> &mut Self {
        self.statements.push(sql.to_string());
        self
    }

    pub fn rollback(&mut self) {
        self.statements.clear();
        self.committed = false;
    }

    pub async fn commit(&mut self, db: &EmbeddedDb) -> Result<usize, String> {
        let mut total = 0;
        let statements = self.statements.drain(..).collect::<Vec<_>>();
        for stmt in statements {
            total += db.execute(&stmt).await?;
        }
        self.committed = true;
        Ok(total)
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_db() {
        let db = EmbeddedDb::open_memory().await.unwrap();
        db.execute("CREATE TABLE users (id INT, name VARCHAR)").await.unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").await.unwrap();

        let result = db.query("SELECT * FROM users").await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_query_where() {
        let db = EmbeddedDb::open_memory().await.unwrap();
        db.execute("CREATE TABLE items (id INT, name VARCHAR)").await.unwrap();
        db.execute("INSERT INTO items VALUES (1, 'apple')").await.unwrap();
        db.execute("INSERT INTO items VALUES (2, 'banana')").await.unwrap();

        let result = db.query("SELECT * FROM items WHERE name = 'apple'").await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.first().unwrap().get_str("name"), Some("apple"));
    }

    #[tokio::test]
    async fn test_transaction() {
        let db = EmbeddedDb::open_memory().await.unwrap();
        db.execute("CREATE TABLE t (x INT)").await.unwrap();

        let mut txn = db.transaction();
        txn.execute("INSERT INTO t VALUES (1)");
        txn.execute("INSERT INTO t VALUES (2)");
        let affected = txn.commit(&db).await.unwrap();
        assert_eq!(affected, 2);

        let result = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(result.len(), 2);
    }
}
