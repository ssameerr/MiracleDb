//! Schema Module - Database schema management

use std::collections::HashMap;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Column definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub primary_key: bool,
    pub unique: bool,
    pub foreign_key: Option<ForeignKey>,
    pub check_constraint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bytes,
    Timestamp,
    Date,
    Time,
    Uuid,
    Json,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

/// Index definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Gin,
    Gist,
    Brin,
}

/// Table definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<Index>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Schema change event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaChange {
    pub id: String,
    pub change_type: SchemaChangeType,
    pub table: String,
    pub applied_at: i64,
    pub sql: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SchemaChangeType {
    CreateTable,
    DropTable,
    AddColumn,
    DropColumn,
    AlterColumn,
    CreateIndex,
    DropIndex,
    AddConstraint,
    DropConstraint,
}

/// Schema registry
pub struct SchemaRegistry {
    tables: RwLock<HashMap<String, Table>>,
    changes: RwLock<Vec<SchemaChange>>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            changes: RwLock::new(Vec::new()),
        }
    }

    /// Create a table
    pub async fn create_table(&self, table: Table) -> Result<(), String> {
        let mut tables = self.tables.write().await;
        if tables.contains_key(&table.name) {
            return Err(format!("Table {} already exists", table.name));
        }

        let change = SchemaChange {
            id: uuid::Uuid::new_v4().to_string(),
            change_type: SchemaChangeType::CreateTable,
            table: table.name.clone(),
            applied_at: chrono::Utc::now().timestamp(),
            sql: format!("CREATE TABLE {}", table.name),
        };

        tables.insert(table.name.clone(), table);
        drop(tables);

        let mut changes = self.changes.write().await;
        changes.push(change);

        Ok(())
    }

    /// Drop a table
    pub async fn drop_table(&self, name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().await;
        tables.remove(name)
            .ok_or(format!("Table {} not found", name))?;

        let change = SchemaChange {
            id: uuid::Uuid::new_v4().to_string(),
            change_type: SchemaChangeType::DropTable,
            table: name.to_string(),
            applied_at: chrono::Utc::now().timestamp(),
            sql: format!("DROP TABLE {}", name),
        };

        drop(tables);
        let mut changes = self.changes.write().await;
        changes.push(change);

        Ok(())
    }

    /// Add a column to a table
    pub async fn add_column(&self, table_name: &str, column: Column) -> Result<(), String> {
        let mut tables = self.tables.write().await;
        let table = tables.get_mut(table_name)
            .ok_or(format!("Table {} not found", table_name))?;

        if table.columns.iter().any(|c| c.name == column.name) {
            return Err(format!("Column {} already exists", column.name));
        }

        table.columns.push(column.clone());
        table.updated_at = chrono::Utc::now().timestamp();

        let change = SchemaChange {
            id: uuid::Uuid::new_v4().to_string(),
            change_type: SchemaChangeType::AddColumn,
            table: table_name.to_string(),
            applied_at: chrono::Utc::now().timestamp(),
            sql: format!("ALTER TABLE {} ADD COLUMN {}", table_name, column.name),
        };

        drop(tables);
        let mut changes = self.changes.write().await;
        changes.push(change);

        Ok(())
    }

    /// Drop a column from a table
    pub async fn drop_column(&self, table_name: &str, column_name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().await;
        let table = tables.get_mut(table_name)
            .ok_or(format!("Table {} not found", table_name))?;

        let column_idx = table.columns.iter()
            .position(|c| c.name == column_name)
            .ok_or(format!("Column {} not found", column_name))?;

        table.columns.remove(column_idx);
        table.updated_at = chrono::Utc::now().timestamp();

        Ok(())
    }

    /// Create an index
    pub async fn create_index(&self, index: Index) -> Result<(), String> {
        let mut tables = self.tables.write().await;
        let table = tables.get_mut(&index.table)
            .ok_or(format!("Table {} not found", index.table))?;

        if table.indexes.iter().any(|i| i.name == index.name) {
            return Err(format!("Index {} already exists", index.name));
        }

        table.indexes.push(index);

        Ok(())
    }

    /// Get table schema
    pub async fn get_table(&self, name: &str) -> Option<Table> {
        let tables = self.tables.read().await;
        tables.get(name).cloned()
    }

    /// List all tables
    pub async fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().await;
        tables.keys().cloned().collect()
    }

    /// Get schema changes history
    pub async fn get_changes(&self) -> Vec<SchemaChange> {
        let changes = self.changes.read().await;
        changes.clone()
    }

    /// Validate schema
    pub async fn validate(&self) -> Vec<String> {
        let tables = self.tables.read().await;
        let mut errors = Vec::new();

        for (name, table) in tables.iter() {
            // Check for primary key
            if table.primary_key.is_empty() {
                errors.push(format!("Table {} has no primary key", name));
            }

            // Check foreign key references
            for column in &table.columns {
                if let Some(fk) = &column.foreign_key {
                    if !tables.contains_key(&fk.table) {
                        errors.push(format!(
                            "Foreign key in {}.{} references non-existent table {}",
                            name, column.name, fk.table
                        ));
                    }
                }
            }
        }

        errors
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}
