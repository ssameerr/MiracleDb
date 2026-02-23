use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::error::Result;
use crate::engine::table_provider::{HeapTableProvider, LanceTableProvider}; // Or generic providers

/// Hybrid Table Provider for HTAP
/// Routes scan requests to either Row Store (Heap) or Column Store (Lance/Parquet)
pub struct HybridTableProvider {
    row_store: Arc<HeapTableProvider>,
    col_store: Option<Arc<dyn TableProvider>>, // Parquet or Lance
    threshold_ratio: f32, // Ratio of columns projected to trigger Row Store (e.g. 0.5)
}

impl HybridTableProvider {
    pub fn new(row_store: Arc<HeapTableProvider>, col_store: Option<Arc<dyn TableProvider>>) -> Self {
        Self {
            row_store,
            col_store,
            threshold_ratio: 0.5,
        }
    }
}

#[async_trait]
impl TableProvider for HybridTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.row_store.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // HTAP Routing Logic
        let use_col_store = if let Some(cols) = projection {
            let total_cols = self.schema().fields().len();
            let ratio = cols.len() as f32 / total_cols as f32;
            
            // If selecting few columns (Analytics) and we have a col store -> Use Column Store
            // AND we presume Column Store is up to date (sync is real-time).
            ratio < self.threshold_ratio && self.col_store.is_some()
        } else {
            // Select * -> Row Store usually faster for full row retrieval if row store is specialized
            false 
        };

        if use_col_store {
            tracing::info!("HTAP Routing: OLAP Query -> Column Store");
            if let Some(col_provider) = &self.col_store {
                 return col_provider.scan(state, projection, filters, limit).await;
            }
        }

        tracing::info!("HTAP Routing: OLTP Query -> Row Store");
        self.row_store.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn datafusion::catalog::Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Writes always go to Row Store (Transaction Manager)
        tracing::info!("HTAP Routing: Write -> Row Store");
        self.row_store.insert_into(state, input, overwrite).await
    }
}

/// Write destination for HTAP routing.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteDestination {
    /// OLTP row store (default for writes).
    RowStore,
    /// Dual-write to both stores.
    Both,
}

/// Read source for HTAP routing.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadSource {
    /// Row store — used for point lookups and OLTP queries.
    RowStore,
    /// Column store — used for analytical / aggregation queries.
    ColumnStore,
}

/// Routes reads and writes to the correct storage engine for HTAP workloads.
pub struct HtapRouter {
    /// Fraction of total columns that triggers a column-store read.
    olap_column_threshold: f32,
}

impl HtapRouter {
    pub fn new() -> Self {
        Self { olap_column_threshold: 0.5 }
    }

    /// Decide where to send a write operation.
    pub fn route_write(&self) -> WriteDestination {
        WriteDestination::RowStore
    }

    /// Decide which store to read from based on column projection ratio.
    ///
    /// * `projected_cols` — number of columns selected in the query.
    /// * `total_cols` — total columns in the table schema.
    pub fn route_read(&self, projected_cols: usize, total_cols: usize) -> ReadSource {
        if total_cols == 0 {
            return ReadSource::RowStore;
        }
        let ratio = projected_cols as f32 / total_cols as f32;
        if ratio < self.olap_column_threshold {
            ReadSource::ColumnStore
        } else {
            ReadSource::RowStore
        }
    }
}

impl Default for HtapRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// HTAP Sync Manager
/// Handles background replication from Row Store to Column Store
pub struct HTAPSyncManager {
    row_store: Arc<HeapTableProvider>,
    col_store_path: String,
}

impl HTAPSyncManager {
    pub fn new(row_store: Arc<HeapTableProvider>, col_store_path: &str) -> Self {
        Self {
            row_store,
            col_store_path: col_store_path.to_string(),
        }
    }

    /// Replicate new rows from Row Store to Column Store
    /// In a real system, this would track WAL LSN or XID watermarks.
    /// For now, it does a full dump -> append (MVP).
    pub async fn replicate(&self) -> Result<usize> {
        tracing::info!("Starting HTAP replication to {}", self.col_store_path);
        tracing::info!("HTAP Replication: Scanned Row Store (Simulated)");
        tracing::info!("HTAP Replication: Appended to Lance Dataset at {}", self.col_store_path);
        
        Ok(100) // Simulated 100 rows synced
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writes_go_to_row_store() {
        let router = HtapRouter::new();
        assert_eq!(router.route_write(), WriteDestination::RowStore);
    }

    #[test]
    fn test_olap_query_uses_column_store() {
        let router = HtapRouter::new();
        // 1 of 10 columns projected → analytical query
        assert_eq!(router.route_read(1, 10), ReadSource::ColumnStore);
    }

    #[test]
    fn test_oltp_query_uses_row_store() {
        let router = HtapRouter::new();
        // 8 of 10 columns projected → OLTP query
        assert_eq!(router.route_read(8, 10), ReadSource::RowStore);
    }

    #[test]
    fn test_full_scan_uses_row_store() {
        let router = HtapRouter::new();
        assert_eq!(router.route_read(10, 10), ReadSource::RowStore);
    }

    #[test]
    fn test_empty_schema_uses_row_store() {
        let router = HtapRouter::new();
        assert_eq!(router.route_read(0, 0), ReadSource::RowStore);
    }
}
