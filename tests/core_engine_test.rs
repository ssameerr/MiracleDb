use miracledb::engine::{MiracleEngine, EngineConfig};
use arrow::array::{Float32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use lance::dataset::{Dataset, WriteParams};

#[tokio::test]
async fn test_core_engine_full_flow() {
    // 1. Setup Test Environment
    let test_dir = tempfile::tempdir().unwrap();
    let data_path = test_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();
    
    let config = EngineConfig {
        data_dir: data_path.to_str().unwrap().to_string(),
        enable_transactions: true,
        enable_compliance: false,
        enable_triggers: false,
        max_concurrent_queries: 10,
    };
    
    let engine = MiracleEngine::with_config(config).await.expect("Failed to create engine");
    
    // 2. Create a Lance Dataset
    let lance_path = test_dir.path().join("test_vectors.lance");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            4 // Dimension 4
        ), true),
    ]));
    
    let ids = Int64Array::from(vec![1, 2, 3]);
    let vectors = Float32Array::from_iter_values(vec![
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 0.0, 0.0,
        0.0, 0.0, 1.0, 0.0,
    ]);
    
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(ids),
        Arc::new(arrow::array::FixedSizeListArray::try_new_from_values(Arc::new(vectors), 4).unwrap()),
    ]).unwrap();
    
    let mut write_params = WriteParams::default();
    Dataset::write(&mut vec![batch], lance_path.to_str().unwrap(), Some(write_params)).await.expect("Failed to write lance dataset");
    
    // 3. Register Table
    engine.register_lance("vectors", lance_path.to_str().unwrap()).await.expect("Failed to register table");
    
    // 4. SQL Query Test
    let df = engine.query("SELECT id FROM vectors ORDER BY id").await.expect("Query failed");
    let batches = df.collect().await.expect("Collect failed");
    assert!(!batches.is_empty());
    
    // 5. Verification
    // Verify we got 3 rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    
    println!("Core Engine Test Passed!");
}
