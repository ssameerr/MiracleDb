//! Executor Module - Query Executor using Volcano Model
//! 
//! Implements a "pull-based" iterator model where each operator implements the `Executor` trait.

use async_trait::async_trait;
use std::sync::Arc;
use crate::planner::LogicalPlan;
use crate::catalog::Catalog;
use crate::heap::HeapRelation;
use crate::buffer::BufferPool;
use crate::smgr::{StorageManagerInterface, RelFileNode};
use crate::engine::{TransactionState, Transaction}; // Assuming Transaction context needed eventually

use crate::common::SimpleValue;

/// A Tuple execution result
#[derive(Debug, Clone)]
pub struct Tuple {
    pub values: Vec<SimpleValue>,
}

/// The Executor Trait (Async)
#[async_trait]
pub trait Executor: Send + Sync {
    /// Initialize the executor
    async fn init(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Produce the next tuple
    async fn next(&mut self) -> Result<Option<Tuple>, String>;
}

/// Sequential Scan Executor
pub struct SeqScanExecutor {
    pub plan: LogicalPlan,
    pub catalog: Arc<Catalog>,
    pub heap: Option<Arc<HeapRelation>>,
    pub buffer_pool: Arc<BufferPool>,
    pub smgr: Arc<StorageManagerInterface>,
    pub current_block: u64,
    pub current_offset: u16,
    pub finished: bool,
}

impl SeqScanExecutor {
    pub fn new(
        plan: LogicalPlan,
        catalog: Arc<Catalog>,
        buffer_pool: Arc<BufferPool>,
        smgr: Arc<StorageManagerInterface>,
    ) -> Self {
        Self {
            plan,
            catalog,
            heap: None,
            buffer_pool,
            smgr,
            current_block: 0,
            current_offset: 0,
            finished: false,
        }
    }
}

#[async_trait]
impl Executor for SeqScanExecutor {
    async fn init(&mut self) -> Result<(), String> {
        if let LogicalPlan::Scan { table_oid, .. } = &self.plan {
            let rel_node = self.catalog.get_rel_node(*table_oid).await
                .ok_or("Table not found in catalog")?;
            
            self.heap = Some(Arc::new(HeapRelation::with_smgr(
                rel_node,
                self.buffer_pool.clone(),
                self.smgr.clone(),
            )));
            Ok(())
        } else {
            Err("Invalid plan for SeqScan".to_string())
        }
    }

    async fn next(&mut self) -> Result<Option<Tuple>, String> {
        // Placeholder implementation to satisfy trait
        Ok(None)
    }
}

/// Buffer-based SeqScan (HACK for MVP until HeapScanner exists)
pub struct BufferedSeqScanExecutor {
    tuples: std::collections::VecDeque<Tuple>,
}

impl BufferedSeqScanExecutor {
    pub async fn new(
        plan: LogicalPlan, 
        catalog: Arc<Catalog>, 
        buffer_pool: Arc<BufferPool>,
        smgr: Arc<StorageManagerInterface>
    ) -> Result<Self, String> {
        if let LogicalPlan::Scan { table_oid, .. } = plan {
             let rel_node = catalog.get_rel_node(table_oid).await
                .ok_or("Table not found")?;
             
             let heap = HeapRelation::with_smgr(rel_node, buffer_pool, smgr);
             
             // Create a dummy snapshot that sees everything (for simple Sql test)
             // In real engine, we pass this in.
             use crate::engine::TransactionSnapshot;
             let snapshot = TransactionSnapshot { xmin: 0, xmax: u64::MAX, active_xids: vec![] };
             
             let heap_tuples = heap.scan(snapshot.xmin, snapshot.xmax, &snapshot.active_xids).await;
             
             let mut tuples = std::collections::VecDeque::new();
             for ht in heap_tuples {
                 if let Ok(row) = bincode::deserialize::<Vec<SimpleValue>>(&ht.data) {
                     tuples.push_back(Tuple { values: row });
                 }
             }
             
             Ok(Self { tuples })
        } else {
            Err("Invalid plan".to_string())
        }
    }
}

pub struct InsertExecutor {
    plan: LogicalPlan,
    catalog: Arc<Catalog>,
    buffer_pool: Arc<BufferPool>,
    smgr: Arc<StorageManagerInterface>,
}

impl InsertExecutor {
    pub fn new(
        plan: LogicalPlan,
        catalog: Arc<Catalog>,
        buffer_pool: Arc<BufferPool>,
        smgr: Arc<StorageManagerInterface>,
    ) -> Self {
        Self { plan, catalog, buffer_pool, smgr }
    }
}

#[async_trait]
impl Executor for BufferedSeqScanExecutor {
    async fn next(&mut self) -> Result<Option<Tuple>, String> {
        Ok(self.tuples.pop_front())
    }
}

// ...

#[async_trait]
impl Executor for InsertExecutor {
    async fn next(&mut self) -> Result<Option<Tuple>, String> {
        if let LogicalPlan::Insert { table_oid, values, .. } = &self.plan {
             let rel_node = self.catalog.get_rel_node(*table_oid).await
                .ok_or("Table not found")?;
             
             let heap = HeapRelation::with_smgr(rel_node, self.buffer_pool.clone(), self.smgr.clone());
             
             let mut count = 0;
             for row in values {
                 // Convert row (Vec<String>) to Vec<SimpleValue>
                 // For MVP: try to parse number, else string
                 let simple_row: Vec<SimpleValue> = row.iter().map(|s| {
                     if let Ok(i) = s.parse::<i64>() {
                         SimpleValue::Int(i)
                     } else if let Ok(f) = s.parse::<f64>() {
                         SimpleValue::Float(f)
                     } else {
                         SimpleValue::String(s.clone())
                     }
                 }).collect();
                 
                 let data = bincode::serialize(&simple_row).map_err(|e| e.to_string())?;
                 
                 // Need Transaction ID. Using 0 for auto-commit/test.
                 heap.insert(0, data).await
                     .map_err(|e| e.to_string())?;
                 count += 1;
             }
             
             Ok(Some(Tuple { values: vec![SimpleValue::String(format!("Inserted {} rows", count))] }))
        } else {
            Err("Invalid plan".to_string())
        }
    }
}

pub struct ExecutionEngine {
    pub catalog: Arc<Catalog>,
    pub buffer_pool: Arc<BufferPool>,
    pub smgr: Arc<StorageManagerInterface>,
}

impl ExecutionEngine {
    pub fn new(catalog: Arc<Catalog>, buffer_pool: Arc<BufferPool>, smgr: Arc<StorageManagerInterface>) -> Self {
        Self { catalog, buffer_pool, smgr }
    }

    pub async fn execute(&self, plan: LogicalPlan) -> Result<Vec<Tuple>, String> {
        let mut executor: Box<dyn Executor> = match plan {
            LogicalPlan::Scan { .. } => {
                Box::new(BufferedSeqScanExecutor::new(
                    plan, 
                    self.catalog.clone(),
                    self.buffer_pool.clone(),
                    self.smgr.clone()
                ).await?)
            },
            LogicalPlan::Insert { .. } => {
                Box::new(InsertExecutor::new(
                    plan,
                    self.catalog.clone(),
                    self.buffer_pool.clone(),
                    self.smgr.clone()
                ))
            },
            LogicalPlan::CreateTable { name, .. } => {
                // Execute immediately (DDL)
                // Need Schema conversion
                use crate::catalog::TableSchema;
                let schema = TableSchema { name: name.clone(), columns: vec![] }; // Dummy schema
                self.catalog.create_table(&name, schema).await
                    .map_err(|e| e.to_string())?;
                return Ok(vec![Tuple { values: vec![SimpleValue::String("Table Created".to_string())] }]);
            },
            // _ => return Err("Unsupported plan".to_string()),
        };

        executor.init().await?;
        
        let mut results = Vec::new();
        while let Some(tuple) = executor.next().await? {
            results.push(tuple);
        }
        Ok(results)
    }
}

// Helper to access heap::scan without importing private module logic if needed
// Actually we need to import `heap` module content.
use crate::heap;
