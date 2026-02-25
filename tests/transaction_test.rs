use miracledb::engine::{TransactionManager, TransactionState};
use miracledb::heap::HeapRelation;
use miracledb::buffer::BufferPool;
use miracledb::smgr::{StorageManagerInterface, RelFileNode};
use std::sync::Arc;

#[tokio::test]
async fn test_mvcc_isolation() {
    let test_dir = tempfile::tempdir().unwrap();
    let data_path = test_dir.path().to_str().unwrap();
    
    // 1. Setup Components
    let tx_manager = Arc::new(TransactionManager::new(data_path));
    let smgr = Arc::new(StorageManagerInterface::new(std::path::PathBuf::from(data_path)));
    let buffer_pool = Arc::new(BufferPool::new(1024 * 1024, smgr.clone())); // 1MB pool
    
    let rel_node = RelFileNode { spc_oid: 0, db_oid: 0, rel_oid: 100 };
    let heap = Arc::new(HeapRelation::with_smgr(rel_node, buffer_pool, smgr));
    
    // 2. Start TX 1
    let tx1 = tx_manager.begin().await.unwrap();
    
    // 3. Insert Tuple in TX 1
    let data1 = b"Transaction 1 Data".to_vec();
    heap.insert(tx1, data1).await.expect("Insert failed");
    
    // 4. Verify Visibility BEFORE Commit
    // Snapshot should NOT see active tx1 (if we exclude it) or should see it if we are tx1.
    // In our implementation, `create_snapshot` includes active XIDs.
    // `check_visibility` Returns FALSE if xmin is in active list.
    
    let snapshot_during_tx1 = tx_manager.create_snapshot().await;
    // By default, scan uses the snapshot which includes tx1 as active.
    // So scan should NOT see the tuple (Read Committed behavior from OTHER's perspective).
    let visible_rows = heap.scan(snapshot_during_tx1.xmin, snapshot_during_tx1.xmax, &snapshot_during_tx1.active_xids).await;
    assert_eq!(visible_rows.len(), 0, "Uncommitted data should be invisible to generic snapshot");
    
    // 5. Commit TX 1
    tx_manager.commit(tx1).await.unwrap();
    
    // 6. Verify Visibility AFTER Commit
    let snapshot_after_tx1 = tx_manager.create_snapshot().await;
    let visible_rows = heap.scan(snapshot_after_tx1.xmin, snapshot_after_tx1.xmax, &snapshot_after_tx1.active_xids).await;
    assert_eq!(visible_rows.len(), 1, "Committed data should be visible");
    
    // 7. Start TX 2 and Rollback
    let tx2 = tx_manager.begin().await.unwrap();
    heap.insert(tx2, b"Aborted Data".to_vec()).await.unwrap();
    tx_manager.rollback(tx2).await.unwrap();
    
    // 8. Verify Rollback
    let snapshot_after_rollback = tx_manager.create_snapshot().await;
    let visible_rows = heap.scan(snapshot_after_rollback.xmin, snapshot_after_rollback.xmax, &snapshot_after_rollback.active_xids).await;
    assert_eq!(visible_rows.len(), 1, "Aborted data should not be visible (only TX1 data remains)");
    
    println!("Transaction MVCC Test Passed!");
}
