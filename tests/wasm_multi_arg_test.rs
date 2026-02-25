//! Integration tests for multi-argument WASM UDFs

use miracledb::udf::wasm::WasmUdfManager;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::test]
async fn test_two_argument_add_i64() {
    // Create WASM manager
    let mut manager = WasmUdfManager::new();

    // Note: We'll need a test WASM module with: fn add(a: i64, b: i64) -> i64
    // For now, this test will fail because multi-arg support doesn't exist yet
    let test_wasm = include_bytes!("../test_data/add_two_i64.wasm");

    // Load module
    manager.load_module("math", test_wasm)
        .expect("Failed to load module");

    // Register function - this should auto-detect 2 arguments
    let ctx = SessionContext::new();

    // Note: The current API doesn't have a register_function method that takes module_name,
    // function_name, and alias. We need to use the register_udf method with WasmUdfConfig.
    // This test will fail because the current implementation only supports 1 argument.
    use miracledb::udf::wasm::{WasmUdfConfig, WasmDataType};

    let config = WasmUdfConfig {
        name: "add_two".to_string(),
        wasm_path: "math".to_string(),
        function_name: "add".to_string(),
        return_type: WasmDataType::Int64,
        arg_types: vec![WasmDataType::Int64, WasmDataType::Int64],
    };

    manager.register_udf(&ctx, config)
        .expect("Failed to register function");

    // Test the function
    let df = ctx.sql("SELECT add_two(10, 20) AS result").await
        .expect("Query failed");

    let batches = df.collect().await.expect("Failed to collect");
    assert_eq!(batches.len(), 1);

    let result_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Wrong type");

    assert_eq!(result_col.value(0), 30);
}

#[tokio::test]
async fn test_three_argument_sum_f64() {
    let mut manager = WasmUdfManager::new();

    // fn sum3(a: f64, b: f64, c: f64) -> f64
    let test_wasm = include_bytes!("../test_data/sum_three_f64.wasm");

    manager.load_module("math", test_wasm)
        .expect("Failed to load module");

    let ctx = SessionContext::new();

    use miracledb::udf::wasm::{WasmUdfConfig, WasmDataType};

    let config = WasmUdfConfig {
        name: "sum_three".to_string(),
        wasm_path: "math".to_string(),
        function_name: "sum3".to_string(),
        return_type: WasmDataType::Float64,
        arg_types: vec![WasmDataType::Float64, WasmDataType::Float64, WasmDataType::Float64],
    };

    manager.register_udf(&ctx, config)
        .expect("Failed to register function");

    let df = ctx.sql("SELECT sum_three(1.5, 2.5, 3.0) AS result").await
        .expect("Query failed");

    let batches = df.collect().await.expect("Failed to collect");
    let result_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Wrong type");

    assert!((result_col.value(0) - 7.0).abs() < 1e-10);
}

#[test]
fn test_backward_compatibility_single_arg() {
    // Ensure existing single-argument functions still work
    // This validates we didn't break anything

    // TODO: Implement once we have existing square.wasm from previous tests
    // This should verify that single-argument WASM functions continue to work
    // after multi-argument support is added.

    let manager = WasmUdfManager::new();

    // Verify manager can be created
    assert!(!manager.has_module("test"));

    // Once square.wasm exists, we'll add:
    // 1. Load the single-arg square.wasm module
    // 2. Register it with a single arg_type
    // 3. Execute a query like "SELECT square(5.0)"
    // 4. Verify result is 25.0
}
