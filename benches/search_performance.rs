//! Performance benchmarks for search operations
//!
//! Run with: cargo bench --bench search_performance

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use serde_json::json;
use std::time::Duration;

const API_URL: &str = "http://localhost:8080";

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

fn setup_benchmark_data(client: &reqwest::blocking::Client, num_docs: usize) {
    // Drop existing table
    let _ = client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({"sql": "DROP TABLE IF EXISTS bench_docs"}))
        .send();

    // Create table
    client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({
            "sql": "CREATE TABLE bench_docs (
                id INTEGER PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                category VARCHAR(50),
                embedding BLOB
            )"
        }))
        .send()
        .expect("Failed to create table");

    // Insert documents
    for i in 0..num_docs {
        let category = match i % 5 {
            0 => "Technology",
            1 => "Science",
            2 => "Business",
            3 => "Health",
            _ => "Education",
        };

        client
            .post(&format!("{}/api/v1/query", API_URL))
            .json(&json!({
                "sql": format!(
                    "INSERT INTO bench_docs (id, title, content, category) VALUES ({}, 'Document {}', 'This is a sample document about {} with content for testing search performance. Machine learning and artificial intelligence are transforming how we process and analyze data.', '{}')",
                    i + 1, i + 1, category.to_lowercase(), category
                )
            }))
            .send()
            .expect("Failed to insert document");
    }

    // Generate embeddings
    client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({
            "sql": "UPDATE bench_docs SET embedding = candle_embed('minilm', content) WHERE embedding IS NULL"
        }))
        .send()
        .expect("Failed to generate embeddings");
}

fn cleanup_benchmark_data(client: &reqwest::blocking::Client) {
    let _ = client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({"sql": "DROP TABLE IF EXISTS bench_docs"}))
        .send();
}

fn bench_vector_search(c: &mut Criterion) {
    let client = client();

    // Check if server is running
    if client.get(&format!("{}/health", API_URL)).send().is_err() {
        eprintln!("Server not running at {}. Skipping benchmarks.", API_URL);
        return;
    }

    let mut group = c.benchmark_group("vector_search");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(20));

    for size in [100, 500, 1000].iter() {
        setup_benchmark_data(&client, *size);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let response = client
                    .post(&format!("{}/api/v1/auto/bench_docs/search", API_URL))
                    .json(&json!({
                        "search_types": [{
                            "type": "vector",
                            "query": {"text": "machine learning artificial intelligence"},
                            "field": "embedding",
                            "k": 10
                        }]
                    }))
                    .send()
                    .expect("Search failed");

                black_box(response.json::<serde_json::Value>().unwrap());
            });
        });

        cleanup_benchmark_data(&client);
    }

    group.finish();
}

fn bench_text_search(c: &mut Criterion) {
    let client = client();

    if client.get(&format!("{}/health", API_URL)).send().is_err() {
        return;
    }

    let mut group = c.benchmark_group("text_search");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(20));

    for size in [100, 500, 1000].iter() {
        setup_benchmark_data(&client, *size);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let response = client
                    .post(&format!("{}/api/v1/auto/bench_docs/search", API_URL))
                    .json(&json!({
                        "search_types": [{
                            "type": "text",
                            "fields": ["title", "content"],
                            "query": "machine learning"
                        }]
                    }))
                    .send()
                    .expect("Search failed");

                black_box(response.json::<serde_json::Value>().unwrap());
            });
        });

        cleanup_benchmark_data(&client);
    }

    group.finish();
}

fn bench_hybrid_search(c: &mut Criterion) {
    let client = client();

    if client.get(&format!("{}/health", API_URL)).send().is_err() {
        return;
    }

    let mut group = c.benchmark_group("hybrid_search");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(20));

    for size in [100, 500, 1000].iter() {
        setup_benchmark_data(&client, *size);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let response = client
                    .post(&format!("{}/api/v1/auto/bench_docs/search", API_URL))
                    .json(&json!({
                        "search_types": [
                            {
                                "type": "vector",
                                "query": {"text": "machine learning"},
                                "field": "embedding",
                                "k": 20
                            },
                            {
                                "type": "text",
                                "fields": ["title", "content"],
                                "query": "artificial intelligence"
                            },
                            {
                                "type": "filter",
                                "field": "category",
                                "value": "Technology"
                            }
                        ],
                        "combine": "rrf",
                        "limit": 10
                    }))
                    .send()
                    .expect("Search failed");

                black_box(response.json::<serde_json::Value>().unwrap());
            });
        });

        cleanup_benchmark_data(&client);
    }

    group.finish();
}

fn bench_embedding_generation(c: &mut Criterion) {
    let client = client();

    if client.get(&format!("{}/health", API_URL)).send().is_err() {
        return;
    }

    let mut group = c.benchmark_group("embedding_generation");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(30));

    for batch_size in [1, 10, 50].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), batch_size, |b, size| {
            b.iter(|| {
                let texts: Vec<String> = (0..*size)
                    .map(|i| format!("This is test document {} for embedding generation benchmarking", i))
                    .collect();

                let response = client
                    .post(&format!("{}/api/v1/ml/embed", API_URL))
                    .json(&json!({
                        "model": "minilm",
                        "texts": texts
                    }))
                    .send()
                    .expect("Embedding generation failed");

                black_box(response.json::<serde_json::Value>().unwrap());
            });
        });
    }

    group.finish();
}

fn bench_filter_performance(c: &mut Criterion) {
    let client = client();

    if client.get(&format!("{}/health", API_URL)).send().is_err() {
        return;
    }

    let mut group = c.benchmark_group("filter_performance");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(20));

    setup_benchmark_data(&client, 1000);

    group.bench_function("exact_match", |b| {
        b.iter(|| {
            let response = client
                .post(&format!("{}/api/v1/auto/bench_docs/search", API_URL))
                .json(&json!({
                    "search_types": [{
                        "type": "filter",
                        "field": "category",
                        "value": "Technology"
                    }]
                }))
                .send()
                .expect("Filter failed");

            black_box(response.json::<serde_json::Value>().unwrap());
        });
    });

    cleanup_benchmark_data(&client);
    group.finish();
}

criterion_group!(
    benches,
    bench_vector_search,
    bench_text_search,
    bench_hybrid_search,
    bench_embedding_generation,
    bench_filter_performance
);
criterion_main!(benches);
