//! Schema Discovery and Analysis
//!
//! Automatically discovers and analyzes table schemas to generate appropriate APIs

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Represents a complete table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub relationships: Vec<Relationship>,
}

/// Information about a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
    pub auto_increment: bool,
    pub max_length: Option<i64>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
    /// Special column types for enhanced search
    pub special_type: Option<SpecialColumnType>,
}

/// Special column types that enable specific search capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpecialColumnType {
    /// Vector embedding for similarity search
    Vector { dimensions: usize },
    /// Full-text searchable text
    FullText,
    /// Geospatial coordinate
    Geography,
    /// JSON/JSONB data
    Json,
    /// Array data
    Array { element_type: String },
    /// Time series timestamp
    TimeSeries,
}

/// Index information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: IndexType,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Vector,
    FullText,
    Spatial,
}

/// Foreign key relationship
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    Restrict,
    NoAction,
}

/// Table relationships for automatic joins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    pub name: String,
    pub relationship_type: RelationshipType,
    pub related_table: String,
    pub local_column: String,
    pub foreign_column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RelationshipType {
    BelongsTo,
    HasOne,
    HasMany,
    ManyToMany { through_table: String },
}

impl TableSchema {
    /// Discover schema from database
    pub async fn discover(table_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // TODO: Implement actual schema discovery from database
        // This would query information_schema or pg_catalog

        Ok(TableSchema {
            table_name: table_name.to_string(),
            columns: vec![],
            indexes: vec![],
            primary_key: vec![],
            foreign_keys: vec![],
            relationships: vec![],
        })
    }

    /// Get vector columns for similarity search
    pub fn vector_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.iter()
            .filter(|col| matches!(col.special_type, Some(SpecialColumnType::Vector { .. })))
            .collect()
    }

    /// Get full-text searchable columns
    pub fn fulltext_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.iter()
            .filter(|col| {
                matches!(col.special_type, Some(SpecialColumnType::FullText))
                    || matches!(col.data_type.as_str(), "text" | "varchar" | "char")
            })
            .collect()
    }

    /// Get geospatial columns
    pub fn geo_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.iter()
            .filter(|col| matches!(col.special_type, Some(SpecialColumnType::Geography)))
            .collect()
    }

    /// Get time series columns
    pub fn timeseries_columns(&self) -> Vec<&ColumnInfo> {
        self.columns.iter()
            .filter(|col| {
                matches!(col.special_type, Some(SpecialColumnType::TimeSeries))
                    || matches!(col.data_type.as_str(), "timestamp" | "timestamptz" | "date" | "time")
            })
            .collect()
    }

    /// Get filterable columns
    pub fn filterable_columns(&self) -> Vec<&ColumnInfo> {
        // All columns are filterable except large binary objects
        self.columns.iter()
            .filter(|col| !matches!(col.data_type.as_str(), "bytea" | "blob"))
            .collect()
    }

    /// Get sortable columns
    pub fn sortable_columns(&self) -> Vec<&ColumnInfo> {
        // Most columns are sortable except complex types
        self.columns.iter()
            .filter(|col| !matches!(col.data_type.as_str(), "json" | "jsonb" | "array" | "bytea" | "blob"))
            .collect()
    }

    /// Generate OpenAPI schema for this table
    pub fn to_openapi_schema(&self) -> serde_json::Value {
        let mut properties = serde_json::Map::new();
        let mut required = vec![];

        for col in &self.columns {
            let prop_type = match col.data_type.as_str() {
                "integer" | "bigint" | "smallint" => "integer",
                "decimal" | "numeric" | "float" | "double" | "real" => "number",
                "boolean" | "bool" => "boolean",
                "json" | "jsonb" => "object",
                _ => "string",
            };

            let mut prop = serde_json::json!({
                "type": prop_type,
                "description": format!("Column: {}", col.name),
            });

            if let Some(max_len) = col.max_length {
                if prop_type == "string" {
                    prop["maxLength"] = serde_json::Value::from(max_len);
                }
            }

            if !col.nullable {
                required.push(col.name.clone());
            }

            properties.insert(col.name.clone(), prop);
        }

        serde_json::json!({
            "type": "object",
            "properties": properties,
            "required": required,
        })
    }
}

/// Discover table schema from the database
pub async fn discover_table_schema(
    table_name: &str,
    engine: &crate::engine::MiracleEngine,
) -> Result<TableSchema, Box<dyn std::error::Error>> {
    tracing::info!("Discovering schema for table: {}", table_name);

    // Get Arrow schema from the engine
    let arrow_schema = engine.get_table_schema(table_name).await
        .ok_or_else(|| format!("Table '{}' not found", table_name))?;

    // Convert Arrow schema to our TableSchema format
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();

    for field in arrow_schema.fields() {
        let data_type = format!("{:?}", field.data_type());

        // Detect special column types based on Arrow data type
        let special_type = match field.data_type() {
            arrow::datatypes::DataType::FixedSizeList(_, size) => {
                Some(SpecialColumnType::Vector { dimensions: *size as usize })
            }
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                // Could be full-text if there's an index hint (TODO: detect from metadata)
                None
            }
            arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
                // Could be geography data (TODO: detect from metadata)
                None
            }
            _ => None,
        };

        // Simple heuristic: if column name is "id", treat as primary key
        let is_primary_key = field.name().to_lowercase() == "id";
        if is_primary_key {
            primary_key.push(field.name().clone());
        }

        columns.push(ColumnInfo {
            name: field.name().clone(),
            data_type,
            nullable: field.is_nullable(),
            default_value: None,
            is_primary_key,
            is_foreign_key: false,
            auto_increment: is_primary_key,
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
            special_type,
        });
    }

    Ok(TableSchema {
        table_name: table_name.to_string(),
        columns,
        indexes: vec![], // TODO: Query actual indexes
        primary_key,
        foreign_keys: vec![], // TODO: Query actual foreign keys
        relationships: vec![], // TODO: Detect relationships
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_columns() {
        let schema = TableSchema {
            table_name: "test".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                    is_foreign_key: false,
                    auto_increment: true,
                    max_length: None,
                    numeric_precision: None,
                    numeric_scale: None,
                    special_type: None,
                },
                ColumnInfo {
                    name: "embedding".to_string(),
                    data_type: "vector".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                    is_foreign_key: false,
                    auto_increment: false,
                    max_length: None,
                    numeric_precision: None,
                    numeric_scale: None,
                    special_type: Some(SpecialColumnType::Vector { dimensions: 768 }),
                },
            ],
            indexes: vec![],
            primary_key: vec!["id".to_string()],
            foreign_keys: vec![],
            relationships: vec![],
        };

        let vector_cols = schema.vector_columns();
        assert_eq!(vector_cols.len(), 1);
        assert_eq!(vector_cols[0].name, "embedding");
    }
}
