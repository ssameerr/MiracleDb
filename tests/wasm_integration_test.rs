//! End-to-end integration tests for multi-argument WASM UDFs
//!
//! These tests verify the complete workflow:
//! 1. Loading WASM modules
//! 2. Inspecting function signatures
//! 3. Registering multi-argument UDFs
//! 4. Executing SQL queries
//! 5. Handling NULLs and errors

use std::sync::Arc;
use datafusion::prelude::*;
use miracledb::udf::wasm::WasmUdfManager;

#[tokio::test]
async fn test_end_to_end_two_arg_add() {
    // Load manager
    let mut manager = WasmUdfManager::new();

    // Load WASM module
    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager
        .load_module("math", wasm_bytes)
        .expect("Failed to load module");

    // Verify function signature
    let sig = manager
        .get_function_signature("math", "add")
        .expect("Failed to get signature");
    assert_eq!(sig.params.len(), 2);

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register UDF
    manager
        .register_function(&ctx, "math", "add", "add_two")
        .expect("Failed to register function");

    // Execute SQL query
    let df = ctx
        .sql("SELECT add_two(10, 20) AS result")
        .await
        .expect("Query failed");

    let batches = df.collect().await.expect("Failed to collect results");

    // Verify result
    assert_eq!(batches.len(), 1);
    let result_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Wrong type");

    assert_eq!(result_col.value(0), 30);
}

#[tokio::test]
async fn test_end_to_end_three_arg_sum() {
    let mut manager = WasmUdfManager::new();

    // Load module with sum3 function
    let wasm_bytes = include_bytes!("../test_data/sum_three_f64.wasm");
    manager
        .load_module("math", wasm_bytes)
        .expect("Failed to load module");

    // Verify arity
    let sig = manager
        .get_function_signature("math", "sum3")
        .expect("Failed to get signature");
    assert_eq!(sig.params.len(), 3);

    let ctx = SessionContext::new();

    manager
        .register_function(&ctx, "math", "sum3", "sum_three")
        .expect("Failed to register function");

    // Test with various inputs
    let df = ctx
        .sql("SELECT sum_three(1.5, 2.5, 3.0) AS result")
        .await
        .expect("Query failed");

    let batches = df.collect().await.expect("Failed to collect");

    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Wrong type");

    assert!((result.value(0) - 7.0).abs() < 0.001);
}

#[tokio::test]
async fn test_null_handling() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "math", "add", "add_two").unwrap();

    // Create table with NULL values
    ctx.sql("CREATE TABLE test_nulls (a BIGINT, b BIGINT)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    ctx.sql("INSERT INTO test_nulls VALUES (10, 20), (NULL, 5), (5, NULL), (NULL, NULL)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Query with UDF
    let df = ctx
        .sql("SELECT a, b, add_two(a, b) AS result FROM test_nulls")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    let result_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    // First row: 10 + 20 = 30
    assert!(result_col.is_valid(0));
    assert_eq!(result_col.value(0), 30);

    // Second row: NULL + 5 = NULL
    assert!(!result_col.is_valid(1));

    // Third row: 5 + NULL = NULL
    assert!(!result_col.is_valid(2));

    // Fourth row: NULL + NULL = NULL
    assert!(!result_col.is_valid(3));
}

#[tokio::test]
async fn test_wrong_arity_error() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "math", "add", "add_two").unwrap();

    // Try to call with wrong number of arguments
    let result = ctx.sql("SELECT add_two(10) AS result").await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected") || err.contains("arity") || err.contains("argument") || err.contains("Expected"));
}

#[tokio::test]
async fn test_signature_introspection() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    // Test get_function_signature
    let sig = manager
        .get_function_signature("math", "add")
        .expect("Failed to get signature");

    assert_eq!(sig.params.len(), 2);
    assert_eq!(sig.results.len(), 1);

    // Params should be i64
    for param in &sig.params {
        assert_eq!(format!("{:?}", param), "I64");
    }

    // Return should be i64
    assert_eq!(format!("{:?}", sig.results[0]), "I64");

    // Test with sum3
    let wasm_bytes = include_bytes!("../test_data/sum_three_f64.wasm");
    manager.load_module("math2", wasm_bytes).unwrap();

    let sig = manager
        .get_function_signature("math2", "sum3")
        .expect("Failed to get signature");

    assert_eq!(sig.params.len(), 3);

    // Params should be f64
    for param in &sig.params {
        assert_eq!(format!("{:?}", param), "F64");
    }
}

#[tokio::test]
async fn test_multiple_functions_same_module() {
    let mut manager = WasmUdfManager::new();

    // Load the add_two_i64 module which should have the 'add' function
    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();

    // Register the add function
    manager.register_function(&ctx, "math", "add", "add_two").unwrap();

    // Execute query using the registered function
    let df = ctx.sql("SELECT add_two(5, 10) AS result").await.unwrap();
    let batches = df.collect().await.unwrap();

    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(result.value(0), 15);

    // Now load the f64 module and register sum3
    let wasm_bytes = include_bytes!("../test_data/sum_three_f64.wasm");
    manager.load_module("math_f64", wasm_bytes).unwrap();

    manager.register_function(&ctx, "math_f64", "sum3", "sum_three").unwrap();

    // Test both functions together in one query
    let df = ctx
        .sql("SELECT add_two(5, 10) AS sum_int, sum_three(1.0, 2.0, 3.0) AS sum_float")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let int_result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(int_result.value(0), 15);

    let float_result = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((float_result.value(0) - 6.0).abs() < 0.001);
}

#[tokio::test]
async fn test_udf_in_where_clause() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "math", "add", "add_two").unwrap();

    // Create test table
    ctx.sql("CREATE TABLE numbers (a BIGINT, b BIGINT)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    ctx.sql("INSERT INTO numbers VALUES (1, 2), (10, 20), (100, 200)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Use UDF in WHERE clause
    let df = ctx
        .sql("SELECT a, b, add_two(a, b) AS sum FROM numbers WHERE add_two(a, b) > 10")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    // Should return 2 rows: (10, 20) and (100, 200)
    assert_eq!(batches[0].num_rows(), 2);

    let result_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    assert_eq!(result_col.value(0), 30);
    assert_eq!(result_col.value(1), 300);
}

#[tokio::test]
async fn test_udf_in_aggregation() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "math", "add", "add_two").unwrap();

    // Create test table
    ctx.sql("CREATE TABLE data (category VARCHAR, x BIGINT, y BIGINT)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    ctx.sql("INSERT INTO data VALUES ('A', 1, 2), ('A', 3, 4), ('B', 5, 6), ('B', 7, 8)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Use UDF with aggregation
    let df = ctx
        .sql("SELECT category, SUM(add_two(x, y)) AS total FROM data GROUP BY category ORDER BY category")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), 2);

    let category_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();

    let total_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    // Category A: (1+2) + (3+4) = 3 + 7 = 10
    assert_eq!(category_col.value(0), "A");
    assert_eq!(total_col.value(0), 10);

    // Category B: (5+6) + (7+8) = 11 + 15 = 26
    assert_eq!(category_col.value(1), "B");
    assert_eq!(total_col.value(1), 26);
}

#[tokio::test]
async fn test_udf_with_constants_and_expressions() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "math", "add", "add_two").unwrap();

    // Test UDF with constants
    let df = ctx
        .sql("SELECT add_two(5, 10) AS const_result")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(result.value(0), 15);

    // Test UDF with expressions
    let df = ctx
        .sql("SELECT add_two(5 * 2, 10 + 5) AS expr_result")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(result.value(0), 25); // add_two(10, 15) = 25

    // Test nested UDF calls
    let df = ctx
        .sql("SELECT add_two(add_two(1, 2), add_two(3, 4)) AS nested_result")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(result.value(0), 10); // add_two(3, 7) = 10
}

#[tokio::test]
async fn test_float_precision() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/sum_three_f64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "math", "sum3", "sum_three").unwrap();

    // Test with various float values including small decimals
    let df = ctx
        .sql("SELECT sum_three(0.1, 0.2, 0.3) AS result")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();

    // Check result with appropriate epsilon for float comparison
    assert!((result.value(0) - 0.6).abs() < 1e-10);

    // Test with negative numbers
    let df = ctx
        .sql("SELECT sum_three(-1.5, 2.5, -3.0) AS result")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((result.value(0) - (-2.0)).abs() < 1e-10);
}

#[tokio::test]
async fn test_module_isolation() {
    let mut manager = WasmUdfManager::new();

    // Load same WASM module with different names
    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math1", wasm_bytes).unwrap();
    manager.load_module("math2", wasm_bytes).unwrap();

    let ctx = SessionContext::new();

    // Register same function from different modules with different names
    manager.register_function(&ctx, "math1", "add", "add1").unwrap();
    manager.register_function(&ctx, "math2", "add", "add2").unwrap();

    // Both should work independently
    let df = ctx
        .sql("SELECT add1(5, 10) AS result1, add2(3, 7) AS result2")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let result1 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(result1.value(0), 15);

    let result2 = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(result2.value(0), 10);
}

#[tokio::test]
async fn test_has_module() {
    let mut manager = WasmUdfManager::new();

    assert!(!manager.has_module("test"));

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("test", wasm_bytes).unwrap();

    assert!(manager.has_module("test"));
    assert!(!manager.has_module("nonexistent"));
}

#[tokio::test]
async fn test_get_exports() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let exports = manager.get_exports("math");
    assert!(exports.is_some());

    let exports = exports.unwrap();
    assert!(exports.contains(&"add".to_string()));

    // Test non-existent module
    let exports = manager.get_exports("nonexistent");
    assert!(exports.is_none());
}

#[tokio::test]
async fn test_error_nonexistent_module() {
    let mut manager = WasmUdfManager::new();

    let ctx = SessionContext::new();

    // Try to register function from non-existent module
    let result = manager.register_function(&ctx, "nonexistent", "add", "add_fn");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("not found") || err.contains("Module"));
}

#[tokio::test]
async fn test_error_nonexistent_function() {
    let mut manager = WasmUdfManager::new();

    let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
    manager.load_module("math", wasm_bytes).unwrap();

    let ctx = SessionContext::new();

    // Try to register non-existent function
    let result = manager.register_function(&ctx, "math", "nonexistent", "bad_fn");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("not found") || err.contains("Function"));
}
