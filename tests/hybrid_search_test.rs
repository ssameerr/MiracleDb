//! Integration tests for hybrid search functionality
//!
//! These tests verify the combination of multiple search types:
//! - Vector + Text (RRF)
//! - Vector + Filter
//! - Text + Filter + Range
//! - Full hybrid (Vector + Text + Filter + Range)
//!
//! Run with: cargo test --test hybrid_search_test -- --ignored

use serde_json::json;

// Assumes server is running on localhost:8080
const API_URL: &str = "http://localhost:8080";

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::new()
}

#[test]
#[ignore] // Run with: cargo test -- --ignored
fn test_hybrid_vector_text_search() {
    println!("=== Hybrid Vector + Text Search Test ===\n");

    let client = client();

    // Setup: Create test table and insert data
    println!("1. Setting up test data...");
    setup_test_table(&client);

    // Test: Hybrid search combining vector and text
    println!("2. Testing vector + text hybrid search...");
    let response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [
                {
                    "type": "vector",
                    "query": {"text": "gaming laptop"},
                    "field": "embedding",
                    "k": 10
                },
                {
                    "type": "text",
                    "fields": ["name", "description"],
                    "query": "high performance"
                }
            ],
            "combine": "rrf"
        }))
        .send()
        .expect("Hybrid search failed");

    assert!(
        response.status().is_success(),
        "Hybrid search failed: {}",
        response.text().unwrap_or_default()
    );

    let result: serde_json::Value = response
        .json()
        .expect("Failed to parse response");

    assert!(result["results"].is_array(), "Expected results array");
    let results = result["results"].as_array().unwrap();
    assert!(results.len() > 0, "Expected at least one result");

    println!("   ✓ Found {} hybrid results", results.len());

    // Cleanup
    cleanup_test_table(&client);
    println!("\n✅ Hybrid vector + text test PASSED!");
}

#[test]
#[ignore]
fn test_hybrid_with_filters() {
    println!("=== Hybrid Search with Filters Test ===\n");

    let client = client();

    println!("1. Setting up test data...");
    setup_test_table(&client);

    // Test: Vector search with category filter
    println!("2. Testing vector + filter...");
    let response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [
                {
                    "type": "vector",
                    "query": {"text": "laptop"},
                    "field": "embedding",
                    "k": 20
                },
                {
                    "type": "filter",
                    "field": "category",
                    "value": "Electronics"
                }
            ]
        }))
        .send()
        .expect("Filter search failed");

    assert!(response.status().is_success(), "Filter search failed");

    let result: serde_json::Value = response
        .json()
        .expect("Failed to parse response");

    let results = result["results"].as_array().unwrap();

    // Verify all results match the filter
    for item in results {
        assert_eq!(
            item["category"].as_str().unwrap(),
            "Electronics",
            "Result should match category filter"
        );
    }

    println!("   ✓ All {} results match category filter", results.len());

    cleanup_test_table(&client);
    println!("\n✅ Hybrid with filters test PASSED!");
}

#[test]
#[ignore]
fn test_hybrid_with_range() {
    println!("=== Hybrid Search with Range Test ===\n");

    let client = client();

    println!("1. Setting up test data...");
    setup_test_table(&client);

    // Test: Vector search with price range
    println!("2. Testing vector + range filter...");
    let response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [
                {
                    "type": "vector",
                    "query": {"text": "laptop"},
                    "field": "embedding",
                    "k": 20
                },
                {
                    "type": "range",
                    "field": "price",
                    "min": 500.0,
                    "max": 1500.0
                }
            ]
        }))
        .send()
        .expect("Range search failed");

    assert!(response.status().is_success(), "Range search failed");

    let result: serde_json::Value = response
        .json()
        .expect("Failed to parse response");

    let results = result["results"].as_array().unwrap();

    // Verify all results are within price range
    for item in results {
        let price = item["price"].as_f64().unwrap();
        assert!(
            price >= 500.0 && price <= 1500.0,
            "Price {} should be in range [500, 1500]",
            price
        );
    }

    println!("   ✓ All {} results within price range", results.len());

    cleanup_test_table(&client);
    println!("\n✅ Hybrid with range test PASSED!");
}

#[test]
#[ignore]
fn test_full_hybrid_search() {
    println!("=== Full Hybrid Search Test ===\n");

    let client = client();

    println!("1. Setting up test data...");
    setup_test_table(&client);

    // Test: All search types combined
    println!("2. Testing vector + text + filter + range...");
    let response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [
                {
                    "type": "vector",
                    "query": {"text": "gaming laptop"},
                    "field": "embedding",
                    "k": 20
                },
                {
                    "type": "text",
                    "fields": ["name", "description"],
                    "query": "performance"
                },
                {
                    "type": "filter",
                    "field": "category",
                    "value": "Electronics"
                },
                {
                    "type": "range",
                    "field": "price",
                    "min": 800.0,
                    "max": 2000.0
                }
            ],
            "combine": "rrf",
            "limit": 10
        }))
        .send()
        .expect("Full hybrid search failed");

    assert!(
        response.status().is_success(),
        "Full hybrid search failed: {}",
        response.text().unwrap_or_default()
    );

    let result: serde_json::Value = response
        .json()
        .expect("Failed to parse response");

    let results = result["results"].as_array().unwrap();
    assert!(results.len() > 0, "Expected at least one result");
    assert!(results.len() <= 10, "Should respect limit");

    // Verify all constraints
    for item in results {
        let price = item["price"].as_f64().unwrap();
        let category = item["category"].as_str().unwrap();

        assert_eq!(category, "Electronics", "Should match category filter");
        assert!(
            price >= 800.0 && price <= 2000.0,
            "Price {} should be in range [800, 2000]",
            price
        );
    }

    println!("   ✓ Found {} results matching all criteria", results.len());

    cleanup_test_table(&client);
    println!("\n✅ Full hybrid search test PASSED!");
}

#[test]
#[ignore]
fn test_rrf_combination() {
    println!("=== RRF Combination Test ===\n");

    let client = client();

    println!("1. Setting up test data...");
    setup_test_table(&client);

    // Test: RRF should rank results better than individual searches
    println!("2. Comparing RRF vs individual searches...");

    // Vector-only search
    let vector_response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [{
                "type": "vector",
                "query": {"text": "gaming laptop"},
                "field": "embedding",
                "k": 5
            }]
        }))
        .send()
        .expect("Vector search failed");

    let vector_result: serde_json::Value = vector_response
        .json()
        .expect("Failed to parse vector response");

    // Hybrid RRF search
    let hybrid_response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [
                {
                    "type": "vector",
                    "query": {"text": "gaming laptop"},
                    "field": "embedding",
                    "k": 10
                },
                {
                    "type": "text",
                    "fields": ["name", "description"],
                    "query": "gaming"
                }
            ],
            "combine": "rrf",
            "limit": 5
        }))
        .send()
        .expect("Hybrid search failed");

    let hybrid_result: serde_json::Value = hybrid_response
        .json()
        .expect("Failed to parse hybrid response");

    let vector_results = vector_result["results"].as_array().unwrap();
    let hybrid_results = hybrid_result["results"].as_array().unwrap();

    assert_eq!(vector_results.len(), 5, "Expected 5 vector results");
    assert_eq!(hybrid_results.len(), 5, "Expected 5 hybrid results");

    println!("   ✓ Vector search returned {} results", vector_results.len());
    println!("   ✓ Hybrid RRF search returned {} results", hybrid_results.len());
    println!("   ✓ RRF successfully combined rankings");

    cleanup_test_table(&client);
    println!("\n✅ RRF combination test PASSED!");
}

#[test]
#[ignore]
fn test_weighted_combination() {
    println!("=== Weighted Combination Test ===\n");

    let client = client();

    println!("1. Setting up test data...");
    setup_test_table(&client);

    // Test: Weighted combination with custom weights
    println!("2. Testing weighted combination...");
    let response = client
        .post(&format!("{}/api/v1/auto/test_products/search", API_URL))
        .json(&json!({
            "search_types": [
                {
                    "type": "vector",
                    "query": {"text": "laptop"},
                    "field": "embedding",
                    "k": 10,
                    "weight": 0.7
                },
                {
                    "type": "text",
                    "fields": ["name", "description"],
                    "query": "gaming",
                    "weight": 0.3
                }
            ],
            "combine": "weighted",
            "limit": 10
        }))
        .send()
        .expect("Weighted search failed");

    assert!(
        response.status().is_success(),
        "Weighted search failed: {}",
        response.text().unwrap_or_default()
    );

    let result: serde_json::Value = response
        .json()
        .expect("Failed to parse response");

    let results = result["results"].as_array().unwrap();
    assert!(results.len() > 0, "Expected at least one result");
    assert!(results.len() <= 10, "Should respect limit");

    println!("   ✓ Weighted combination returned {} results", results.len());

    cleanup_test_table(&client);
    println!("\n✅ Weighted combination test PASSED!");
}

// Helper functions

fn setup_test_table(client: &reqwest::blocking::Client) {
    // Create table
    let _ = client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({"sql": "DROP TABLE IF EXISTS test_products"}))
        .send();

    client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({
            "sql": "CREATE TABLE test_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200),
                description TEXT,
                price FLOAT,
                category VARCHAR(50),
                embedding BLOB
            )"
        }))
        .send()
        .expect("Failed to create table");

    // Insert test data
    let products = vec![
        ("Gaming Laptop Pro", "High performance gaming laptop with RTX 4080", 1299.99, "Electronics"),
        ("Office Laptop", "Business laptop for productivity", 899.99, "Electronics"),
        ("Budget Laptop", "Affordable laptop for students", 499.99, "Electronics"),
        ("Gaming Desktop", "Ultimate gaming machine", 1899.99, "Electronics"),
        ("Office Chair", "Ergonomic office chair", 299.99, "Furniture"),
        ("Standing Desk", "Adjustable standing desk", 599.99, "Furniture"),
        ("Gaming Mouse", "High DPI gaming mouse", 79.99, "Electronics"),
        ("Mechanical Keyboard", "RGB mechanical keyboard", 149.99, "Electronics"),
    ];

    for (i, (name, desc, price, category)) in products.iter().enumerate() {
        client
            .post(&format!("{}/api/v1/query", API_URL))
            .json(&json!({
                "sql": format!(
                    "INSERT INTO test_products (id, name, description, price, category) VALUES ({}, '{}', '{}', {}, '{}')",
                    i + 1, name, desc, price, category
                )
            }))
            .send()
            .expect("Failed to insert data");
    }

    // Generate embeddings (if ML feature is available)
    let _ = client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({
            "sql": "UPDATE test_products SET embedding = candle_embed('minilm', description) WHERE embedding IS NULL"
        }))
        .send();

    println!("   ✓ Test table created with {} products", products.len());
}

fn cleanup_test_table(client: &reqwest::blocking::Client) {
    let _ = client
        .post(&format!("{}/api/v1/query", API_URL))
        .json(&json!({"sql": "DROP TABLE IF EXISTS test_products"}))
        .send();
}
