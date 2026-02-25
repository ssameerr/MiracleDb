//! Integration tests for WASM String UDFs
//!
//! Tests string parameter and return value support in WASM UDFs

use std::sync::Arc;
use datafusion::prelude::*;
use miracledb::udf::wasm::WasmUdfManager;

/// Helper to load the strings WASM module
fn load_strings_module(manager: &Arc<WasmUdfManager>) {
    let wasm_bytes = include_bytes!("../test_data/strings.wasm");
    manager.load_module("strings", wasm_bytes.to_vec())
        .expect("Failed to load strings module");
}

#[tokio::test]
async fn test_uppercase_string() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "uppercase", "upper")
        .expect("Failed to register uppercase function");

    // Test basic uppercase
    let df = ctx.sql("SELECT upper('hello') AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1);

    let column = batch.column(0);
    let string_array = column.as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "HELLO");
}

#[tokio::test]
async fn test_lowercase_string() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "lowercase", "lower")
        .expect("Failed to register lowercase function");

    let df = ctx.sql("SELECT lower('WORLD') AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "world");
}

#[tokio::test]
async fn test_concat_two_strings() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "concat", "str_concat")
        .expect("Failed to register concat function");

    let df = ctx.sql("SELECT str_concat('Hello', ' World') AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "Hello World");
}

#[tokio::test]
async fn test_substring_mixed_types() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "substring", "substr")
        .expect("Failed to register substring function");

    // Test substring with numeric parameters (mixed types)
    let df = ctx.sql("SELECT substr('Hello World', 0, 5) AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "Hello");
}

#[tokio::test]
async fn test_str_length_mixed_types() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "str_length", "strlen")
        .expect("Failed to register str_length function");

    // Test string -> i64 conversion
    let df = ctx.sql("SELECT strlen('Hello') AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let int_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Expected Int64Array");

    assert_eq!(int_array.value(0), 5);
}

#[tokio::test]
async fn test_repeat_string_mixed_types() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "repeat_string", "str_repeat")
        .expect("Failed to register repeat_string function");

    let df = ctx.sql("SELECT str_repeat('ab', 3) AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "ababab");
}

#[tokio::test]
async fn test_empty_string() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "uppercase", "upper")
        .expect("Failed to register uppercase function");

    let df = ctx.sql("SELECT upper('') AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "");
}

#[tokio::test]
async fn test_unicode_strings() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "uppercase", "upper")
        .expect("Failed to register uppercase function");

    // Test Unicode support (emoji, CJK)
    let df = ctx.sql("SELECT upper('hello 世界 🌍') AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    // Rust's to_uppercase handles Unicode correctly
    assert_eq!(string_array.value(0), "HELLO 世界 🌍");
}

#[tokio::test]
async fn test_null_handling_single_arg() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "uppercase", "upper")
        .expect("Failed to register uppercase function");

    // Create a table with NULL values
    ctx.sql("CREATE TABLE test_nulls (id INT, text VARCHAR) AS VALUES (1, 'hello'), (2, NULL), (3, 'world')")
        .await
        .expect("Create table failed");

    let df = ctx.sql("SELECT id, upper(text) AS result FROM test_nulls ORDER BY id").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let string_array = batch.column(1).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "HELLO");
    assert!(string_array.is_null(1)); // NULL input -> NULL output
    assert_eq!(string_array.value(2), "WORLD");
}

#[tokio::test]
async fn test_null_handling_multi_arg() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "concat", "str_concat")
        .expect("Failed to register concat function");

    // Create a table with NULL values
    ctx.sql("CREATE TABLE test_nulls2 (id INT, text1 VARCHAR, text2 VARCHAR) AS VALUES (1, 'hello', ' world'), (2, NULL, ' world'), (3, 'hello', NULL)")
        .await
        .expect("Create table failed");

    let df = ctx.sql("SELECT id, str_concat(text1, text2) AS result FROM test_nulls2 ORDER BY id").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(1).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "hello world");
    assert!(string_array.is_null(1)); // NULL in first arg
    assert!(string_array.is_null(2)); // NULL in second arg
}

#[tokio::test]
async fn test_multiple_rows() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "uppercase", "upper")
        .expect("Failed to register uppercase function");

    // Create table with multiple rows
    ctx.sql("CREATE TABLE test_multi (id INT, text VARCHAR) AS VALUES (1, 'hello'), (2, 'world'), (3, 'foo'), (4, 'bar')")
        .await
        .expect("Create table failed");

    let df = ctx.sql("SELECT id, upper(text) AS result FROM test_multi ORDER BY id").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 4);

    let string_array = batch.column(1).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "HELLO");
    assert_eq!(string_array.value(1), "WORLD");
    assert_eq!(string_array.value(2), "FOO");
    assert_eq!(string_array.value(3), "BAR");
}

#[tokio::test]
async fn test_large_string() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "str_length", "strlen")
        .expect("Failed to register str_length function");

    // Test with a large string (10KB)
    let large_string = "a".repeat(10000);
    let query = format!("SELECT strlen('{}') AS result", large_string);

    let df = ctx.sql(&query).await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let int_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Expected Int64Array");

    assert_eq!(int_array.value(0), 10000);
}

#[tokio::test]
async fn test_substring_edge_cases() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "substring", "substr")
        .expect("Failed to register substring function");

    // Test various substring edge cases
    let test_cases = vec![
        ("substr('Hello', 0, 100)", "Hello"), // count exceeds length
        ("substr('Hello', 5, 0)", ""),        // start at end
        ("substr('Hello', 100, 5)", ""),      // start beyond end
        ("substr('Hello', 1, 3)", "ell"),     // middle substring
    ];

    for (query, expected) in test_cases {
        let df = ctx.sql(&format!("SELECT {} AS result", query)).await
            .expect(&format!("Query failed: {}", query));
        let results = df.collect().await.expect("Collect failed");

        let batch = &results[0];
        let string_array = batch.column(0).as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("Expected StringArray");

        assert_eq!(string_array.value(0), expected, "Failed for query: {}", query);
    }
}

#[tokio::test]
async fn test_repeat_edge_cases() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "repeat_string", "str_repeat")
        .expect("Failed to register repeat_string function");

    // Test repeat with 0 and negative counts
    let df = ctx.sql("SELECT str_repeat('ab', 0) AS result").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(0).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "");
}

#[tokio::test]
async fn test_concat_with_table_data() {
    let manager = Arc::new(WasmUdfManager::new());
    load_strings_module(&manager);

    let ctx = SessionContext::new();
    manager.register_function(&ctx, "strings", "concat", "str_concat")
        .expect("Failed to register concat function");

    // Create table
    ctx.sql("CREATE TABLE users (id INT, first_name VARCHAR, last_name VARCHAR) AS VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')")
        .await
        .expect("Create table failed");

    // Use UDF with table columns
    let df = ctx.sql("SELECT id, str_concat(first_name, last_name) AS full_name FROM users ORDER BY id").await
        .expect("Query failed");
    let results = df.collect().await.expect("Collect failed");

    let batch = &results[0];
    let string_array = batch.column(1).as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("Expected StringArray");

    assert_eq!(string_array.value(0), "JohnDoe");
    assert_eq!(string_array.value(1), "JaneSmith");
}
