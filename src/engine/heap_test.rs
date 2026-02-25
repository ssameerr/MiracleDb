
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use datafusion::prelude::*;
    use arrow::datatypes::{Schema, Field, DataType};
    use crate::buffer::BufferPool;
    use crate::smgr::StorageManagerInterface;
    use crate::engine::table_provider::HeapTableProvider;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_heap_integration() {
        let dir = tempdir().unwrap();
        // Initialize Core Components
        let smgr = Arc::new(StorageManagerInterface::new(dir.path().to_path_buf()));
        let buffer_pool = Arc::new(BufferPool::new(100, smgr.clone())); // 100 pages
        let tx_mgr = Arc::new(crate::engine::transactions::TransactionManager::new(dir.path().to_str().unwrap()));

        // Define Schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        // Create Provider
        let provider = HeapTableProvider::new("test_table", schema.clone(), buffer_pool.clone(), Some(tx_mgr.clone()));
        let heap = provider.heap();
        
        // Pre-allocate block 0 for the heap (Storage Manager limitation workaround)
        let empty_page = [0u8; 8192];
        smgr.extend(&heap.rel_file_node, crate::smgr::ForkNum::Main, &empty_page).await.unwrap();
        
        // Query via DataFusion
        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(provider)).unwrap();
        
        // SQL INSERT
        let _ = ctx.sql("INSERT INTO test_table VALUES (1), (2), (3)").await.unwrap().collect().await.unwrap();

        // Flush buffers (Transaction commit does this, but being explicit is fine)
        buffer_pool.flush().await;
        
        // Use SQL to count
        let df = ctx.sql("SELECT count(*) from test_table").await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0);
        assert_eq!(count, 3);
        
        // Use SQL to Select Data (Verify Scan Deserialization)
        let df = ctx.sql("SELECT id from test_table ORDER BY id").await.unwrap();
        let batches = df.collect().await.unwrap();
        // Depending on batching we might have 1 batch
        let col = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert_eq!(col.value(2), 3);
        
        println!("Integration Test Passed: Inserted 3 rows via SQL, Verified Count and Data via SQL");
    }
}
