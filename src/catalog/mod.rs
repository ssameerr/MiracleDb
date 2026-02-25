//! Catalog Module - Schema Registry
//! Manages metadata for tables, columns, and other database objects.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::smgr::RelFileNode;
use crate::heap::HeapRelation; // For now, Catalog holds references to relations?
// Actually Catalog just holds metadata (Schema), executor holds Relations.
// But keeping it simple for MVP: Catalog can hand out Table definitions.

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Int,
    Text,
    Boolean,
    // ...
}

pub struct Catalog {
    tables: RwLock<HashMap<String, TableInfo>>,
    next_oid: std::sync::atomic::AtomicU64,
}

struct TableInfo {
    oid: u64,
    schema: TableSchema,
    rel_node: RelFileNode, 
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            next_oid: std::sync::atomic::AtomicU64::new(1000), // Reserve system OIDs
        }
    }

    pub async fn create_table(&self, name: &str, schema: TableSchema) -> Result<u64, String> {
        let mut tables = self.tables.write().await;
        
        if tables.contains_key(name) {
            return Err(format!("Table '{}' already exists", name));
        }
        
        let oid = self.next_oid.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        // Assign storage location (RelFileNode)
        // For MVP: spc=0, db=0, rel=oid
        let rel_node = RelFileNode {
            spc_oid: 0,
            db_oid: 0,
            rel_oid: oid,
        };
        
        tables.insert(name.to_string(), TableInfo {
            oid,
            schema,
            rel_node,
        });
        
        Ok(oid)
    }

    pub async fn get_table_oid(&self, name: &str) -> Option<u64> {
        let tables = self.tables.read().await;
        tables.get(name).map(|t| t.oid)
    }
    
    pub async fn get_table_schema(&self, name: &str) -> Option<TableSchema> {
        let tables = self.tables.read().await;
        tables.get(name).map(|t| t.schema.clone())
    }
    
    pub async fn get_rel_node(&self, oid: u64) -> Option<RelFileNode> {
        let tables = self.tables.read().await;
        for t in tables.values() {
            if t.oid == oid {
                return Some(t.rel_node.clone());
            }
        }
        None
    }

    pub async fn get_rel_node_by_name(&self, name: &str) -> Option<RelFileNode> {
        let tables = self.tables.read().await;
        tables.get(name).map(|t| t.rel_node.clone())
    }
}
