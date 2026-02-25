
use miracledb::engine::MiracleEngine;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};

#[tokio::test]
async fn test_dynamic_table_api() {
    let _ = tracing_subscriber::fmt::try_init();
    
    // 1. Init Engine
    let engine = MiracleEngine::new().await.unwrap();
    
    // 2. Create Table via API method (HeapTable)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    
    engine.create_table("users_api", schema).await.expect("Create table failed");
    
    // 3. Verify Table is visible in DataFusion
    let df = engine.query("SELECT * FROM users_api").await.expect("Query failed");
    assert_eq!(df.schema().fields().len(), 2);
    
    // 4. Insert Data via DataFusion (using TableProvider's insert_into if supported, or via SQL)
    // Our HeapTableProvider implements insert_into!
    // Note: DataFusion's `INSERT INTO` SQL support depends on the dialect and provider.
    // Let's try executing SQL INSERT.
    
    // "INSERT INTO users_api VALUES (1, 'Alice')"
    // DataFusion might not support VALUES directly on external table without `MemTable` or `Values` scan.
    // Actually, DF supports `INSERT INTO table VALUES ...`
    
    let sql_insert = "INSERT INTO users_api VALUES (1, 'Alice'), (2, 'Bob')";
    let df = engine.query(sql_insert).await.expect("Insert plan failed");
    let res = df.collect().await; // Execute!
        println!("Insert successful");
        
        // 5. Query Back
        let df = engine.query("SELECT * FROM users_api").await.expect("Select failed");
        let batches = df.collect().await.expect("Collect failed");
        
        // Verify rows
        let mut row_count = 0;
        for batch in batches {
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 2);
    // } (removed)
}
