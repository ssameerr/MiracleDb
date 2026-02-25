
use miracledb::catalog::Catalog;
use miracledb::buffer::BufferPool;
use miracledb::smgr::StorageManagerInterface;
use miracledb::executor::ExecutionEngine;
use miracledb::planner::Planner;
use miracledb::parser::Parser;
use std::sync::Arc;
use std::path::PathBuf;

#[tokio::test]
async fn test_sql_execution_flow() {
    let test_dir = tempfile::tempdir().unwrap();
    let data_path = test_dir.path().to_path_buf();
    
    // 1. Setup Components
    let smgr = Arc::new(StorageManagerInterface::new(data_path.clone()));
    let buffer_pool = Arc::new(BufferPool::new(1024 * 1024, smgr.clone())); // 1MB
    let catalog = Arc::new(Catalog::new());
    
    let engine = ExecutionEngine::new(catalog.clone(), buffer_pool.clone(), smgr.clone());
    let planner = Planner::new(catalog.clone());
    let parser = Parser::new();
    
    // 2. CREATE TABLE
    let sql_create = "CREATE TABLE users (id INT, name TEXT)";
    let stmts = parser.parse(sql_create).expect("Parse failed");
    let plan = planner.plan(stmts[0].clone()).await.expect("Plan failed");
    let result = engine.execute(plan).await.expect("Execution failed");
    assert_eq!(result[0].values[0].to_string(), "Table Created");
    
    // 3. INSERT
    let sql_insert = "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')";
    let stmts = parser.parse(sql_insert).expect("Parse failed");
    let plan = planner.plan(stmts[0].clone()).await.expect("Plan failed");
    let result = engine.execute(plan).await.expect("Execution failed");
    assert_eq!(result[0].values[0].to_string(), "Inserted 2 rows");
    
    // 4. SELECT
    let sql_select = "SELECT * FROM users";
    let stmts = parser.parse(sql_select).expect("Parse failed");
    let plan = planner.plan(stmts[0].clone()).await.expect("Plan failed");
    let result = engine.execute(plan).await.expect("Execution failed");
    
    assert_eq!(result.len(), 2);
    
    println!("Row 1: {:?}", result[0].values);
    println!("Row 2: {:?}", result[1].values);
    
    // Row might be (1, "Alice") - verify content
    let row_str = format!("{:?}", result[0].values);
    assert!(row_str.contains("Alice") || row_str.contains("Bob"));
}
