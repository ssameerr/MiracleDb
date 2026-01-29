//! Performance benchmarks for WASM UDFs
//!
//! Run with: cargo bench --bench wasm_udf_bench
//!
//! Measures:
//! - Single-arg vs multi-arg overhead
//! - WASM vs native performance
//! - Throughput at different batch sizes

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::arrow::array::{Int64Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use miracledb::udf::wasm::WasmUdfManager;

/// Benchmark single-argument WASM UDF (using add with same arg twice)
fn bench_single_arg_wasm(c: &mut Criterion) {
    let mut group = c.benchmark_group("wasm_single_arg");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Setup
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let mut manager = WasmUdfManager::new();
            let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
            manager.load_module("bench", wasm_bytes).unwrap();

            let ctx = SessionContext::new();

            // Create test data - single column
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
            let values: Int64Array = (0..size).collect();
            let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();

            runtime.block_on(async {
                ctx.register_batch("test_data", batch).unwrap();
                // Register the add function (takes 2 args, we'll use same value twice)
                manager.register_function(&ctx, "bench", "add", "bench_add").unwrap();
            });

            // Benchmark
            b.iter(|| {
                runtime.block_on(async {
                    let df = ctx
                        .sql("SELECT bench_add(value, value) AS result FROM test_data")
                        .await
                        .unwrap();
                    let _batches = df.collect().await.unwrap();
                })
            });
        });
    }

    group.finish();
}

/// Benchmark two-argument WASM UDF
fn bench_two_arg_wasm(c: &mut Criterion) {
    let mut group = c.benchmark_group("wasm_two_arg");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let mut manager = WasmUdfManager::new();
            let wasm_bytes = include_bytes!("../test_data/add_two_i64.wasm");
            manager.load_module("bench", wasm_bytes).unwrap();

            let ctx = SessionContext::new();

            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]));
            let a: Int64Array = (0..size).collect();
            let b: Int64Array = (0..size).collect();
            let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap();

            runtime.block_on(async {
                ctx.register_batch("test_data", batch).unwrap();
                manager.register_function(&ctx, "bench", "add", "bench_add").unwrap();
            });

            b.iter(|| {
                runtime.block_on(async {
                    let df = ctx
                        .sql("SELECT bench_add(a, b) AS result FROM test_data")
                        .await
                        .unwrap();
                    let _batches = df.collect().await.unwrap();
                })
            });
        });
    }

    group.finish();
}

/// Benchmark three-argument WASM UDF (f64)
fn bench_three_arg_wasm(c: &mut Criterion) {
    let mut group = c.benchmark_group("wasm_three_arg");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let mut manager = WasmUdfManager::new();
            let wasm_bytes = include_bytes!("../test_data/sum_three_f64.wasm");
            manager.load_module("bench", wasm_bytes).unwrap();

            let ctx = SessionContext::new();

            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Float64, false),
                Field::new("b", DataType::Float64, false),
                Field::new("c", DataType::Float64, false),
            ]));
            let a: Float64Array = (0..size).map(|i| i as f64).collect();
            let b: Float64Array = (0..size).map(|i| i as f64).collect();
            let c: Float64Array = (0..size).map(|i| i as f64).collect();
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(a), Arc::new(b), Arc::new(c)],
            )
            .unwrap();

            runtime.block_on(async {
                ctx.register_batch("test_data", batch).unwrap();
                manager.register_function(&ctx, "bench", "sum3", "bench_sum3").unwrap();
            });

            b.iter(|| {
                runtime.block_on(async {
                    let df = ctx
                        .sql("SELECT bench_sum3(a, b, c) AS result FROM test_data")
                        .await
                        .unwrap();
                    let _batches = df.collect().await.unwrap();
                })
            });
        });
    }

    group.finish();
}

/// Benchmark native SQL addition for comparison
fn bench_native_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_add");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let ctx = SessionContext::new();

            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ]));
            let a: Int64Array = (0..size).collect();
            let b: Int64Array = (0..size).collect();
            let batch = RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap();

            runtime.block_on(async {
                ctx.register_batch("test_data", batch).unwrap();
            });

            b.iter(|| {
                runtime.block_on(async {
                    let df = ctx
                        .sql("SELECT a + b AS result FROM test_data")
                        .await
                        .unwrap();
                    let _batches = df.collect().await.unwrap();
                })
            });
        });
    }

    group.finish();
}

/// Benchmark native three-column addition for comparison with three-arg WASM
fn bench_native_three_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_three_add");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let ctx = SessionContext::new();

            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Float64, false),
                Field::new("b", DataType::Float64, false),
                Field::new("c", DataType::Float64, false),
            ]));
            let a: Float64Array = (0..size).map(|i| i as f64).collect();
            let b: Float64Array = (0..size).map(|i| i as f64).collect();
            let c: Float64Array = (0..size).map(|i| i as f64).collect();
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(a), Arc::new(b), Arc::new(c)],
            )
            .unwrap();

            runtime.block_on(async {
                ctx.register_batch("test_data", batch).unwrap();
            });

            b.iter(|| {
                runtime.block_on(async {
                    let df = ctx
                        .sql("SELECT a + b + c AS result FROM test_data")
                        .await
                        .unwrap();
                    let _batches = df.collect().await.unwrap();
                })
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_arg_wasm,
    bench_two_arg_wasm,
    bench_three_arg_wasm,
    bench_native_add,
    bench_native_three_add
);
criterion_main!(benches);
