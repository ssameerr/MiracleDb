//! Integration tests for ML workflow
//!
//! These tests verify the complete ML workflow from model download
//! to embedding generation to vector search.
//!
//! Run with: cargo test --test ml_workflow_test --features nlp -- --ignored

#[cfg(feature = "nlp")]
mod ml_workflow {
    use serde_json::json;

    // Assumes server is running on localhost:8080
    const API_URL: &str = "http://localhost:8080";

    fn client() -> reqwest::blocking::Client {
        reqwest::blocking::Client::new()
    }

    #[test]
    #[ignore] // Run with: cargo test -- --ignored
    fn test_end_to_end_ml_workflow() {
        println!("=== ML Workflow Integration Test ===\n");

        let client = client();

        // Step 1: Check if model exists, download if needed
        println!("1. Checking for ML model...");
        let list_response = client
            .get(&format!("{}/api/v1/ml/models", API_URL))
            .send()
            .expect("Failed to list models");

        assert!(list_response.status().is_success(), "Model list failed");

        let models: serde_json::Value = list_response
            .json()
            .expect("Failed to parse models list");

        let model_exists = models["models"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .any(|m| m.as_str() == Some("minilm"));

        if !model_exists {
            println!("   Downloading model (this may take a minute)...");
            let download_response = client
                .post(&format!("{}/api/v1/ml/models/download", API_URL))
                .json(&json!({
                    "model_id": "sentence-transformers/all-MiniLM-L6-v2",
                    "name": "minilm",
                    "cache_dir": "./data/models"
                }))
                .send()
                .expect("Failed to download model");

            assert!(
                download_response.status().is_success(),
                "Model download failed: {}",
                download_response.text().unwrap_or_default()
            );
            println!("   ✓ Model downloaded");
        } else {
            println!("   ✓ Model already exists");
        }

        // Step 2: Create test table
        println!("2. Creating test table...");
        let create_response = client
            .post(&format!("{}/api/v1/query", API_URL))
            .json(&json!({
                "sql": "CREATE TABLE IF NOT EXISTS test_ml_docs (
                    id INTEGER PRIMARY KEY,
                    text VARCHAR(500),
                    embedding BLOB
                )"
            }))
            .send()
            .expect("Failed to create table");

        assert!(create_response.status().is_success(), "Table creation failed");
        println!("   ✓ Table created");

        // Step 3: Insert test data
        println!("3. Inserting test documents...");
        let test_docs = vec![
            "Machine learning is a subset of artificial intelligence that focuses on learning from data",
            "Deep learning uses neural networks with multiple layers to learn complex patterns",
            "Natural language processing enables computers to understand and generate human language",
            "Computer vision allows machines to interpret and understand visual information",
            "Reinforcement learning trains agents through trial and error using rewards",
        ];

        for (i, text) in test_docs.iter().enumerate() {
            let insert_response = client
                .post(&format!("{}/api/v1/query", API_URL))
                .json(&json!({
                    "sql": format!(
                        "INSERT INTO test_ml_docs (id, text) VALUES ({}, '{}') ON CONFLICT DO NOTHING",
                        i + 1, text
                    )
                }))
                .send()
                .expect("Failed to insert");

            assert!(insert_response.status().is_success(), "Insert failed for doc {}", i + 1);
        }
        println!("   ✓ {} documents inserted", test_docs.len());

        // Step 4: Generate embeddings
        println!("4. Generating embeddings...");
        let embed_response = client
            .post(&format!("{}/api/v1/query", API_URL))
            .json(&json!({
                "sql": "UPDATE test_ml_docs SET embedding = candle_embed('minilm', text) WHERE embedding IS NULL"
            }))
            .send()
            .expect("Failed to generate embeddings");

        assert!(
            embed_response.status().is_success(),
            "Embedding generation failed: {}",
            embed_response.text().unwrap_or_default()
        );
        println!("   ✓ Embeddings generated");

        // Step 5: Verify embeddings
        println!("5. Verifying embeddings...");
        let verify_response = client
            .post(&format!("{}/api/v1/query", API_URL))
            .json(&json!({
                "sql": "SELECT COUNT(*) as count FROM test_ml_docs WHERE embedding IS NOT NULL"
            }))
            .send()
            .expect("Failed to verify");

        let result: serde_json::Value = verify_response
            .json()
            .expect("Failed to parse verify response");

        let count = result["results"][0]["count"].as_i64().unwrap_or(0);
        assert_eq!(count, test_docs.len() as i64, "Expected {} embeddings", test_docs.len());
        println!("   ✓ {} embeddings verified", count);

        // Step 6: Test vector search with auto-embedding
        println!("6. Testing vector search with auto-embedding...");
        let search_response = client
            .post(&format!("{}/api/v1/auto/test_ml_docs/search", API_URL))
            .json(&json!({
                "search_types": [{
                    "type": "vector",
                    "query": {"text": "neural networks and deep learning"},
                    "field": "embedding",
                    "k": 3
                }]
            }))
            .send()
            .expect("Failed to search");

        assert!(
            search_response.status().is_success(),
            "Search failed: {}",
            search_response.text().unwrap_or_default()
        );

        let search_result: serde_json::Value = search_response
            .json()
            .expect("Failed to parse search response");

        assert!(
            search_result["results"].is_array(),
            "Expected search results array"
        );

        let results = search_result["results"].as_array().unwrap();
        assert!(results.len() > 0, "Expected at least one search result");

        // Verify the most relevant document is about deep learning
        let top_result = &results[0];
        let top_text = top_result["text"].as_str().unwrap_or("");
        assert!(
            top_text.contains("Deep learning") || top_text.contains("neural network"),
            "Expected top result to be about deep learning, got: {}",
            top_text
        );

        println!("   ✓ Vector search returned {} results", results.len());
        println!("   ✓ Top result: {}", top_text);

        // Step 7: Test direct embedding generation via API
        println!("7. Testing direct embedding generation...");
        let embed_api_response = client
            .post(&format!("{}/api/v1/ml/embed", API_URL))
            .json(&json!({
                "model": "minilm",
                "text": "Test embedding generation"
            }))
            .send()
            .expect("Failed to generate embedding via API");

        assert!(
            embed_api_response.status().is_success(),
            "Direct embedding generation failed"
        );

        let embed_result: serde_json::Value = embed_api_response
            .json()
            .expect("Failed to parse embedding response");

        assert!(embed_result["embeddings"].is_array(), "Expected embeddings array");
        let embeddings = embed_result["embeddings"].as_array().unwrap();
        assert_eq!(embeddings.len(), 1, "Expected 1 embedding");
        assert_eq!(
            embeddings[0].as_array().unwrap().len(),
            384,
            "Expected 384-dimensional embedding"
        );
        println!("   ✓ Direct embedding generation works (384 dimensions)");

        // Cleanup
        println!("8. Cleaning up...");
        let _ = client
            .post(&format!("{}/api/v1/query", API_URL))
            .json(&json!({"sql": "DROP TABLE IF EXISTS test_ml_docs"}))
            .send();
        println!("   ✓ Cleanup complete");

        println!("\n✅ All ML workflow tests PASSED!");
    }

    #[test]
    #[ignore]
    fn test_ml_error_handling() {
        println!("=== ML Error Handling Test ===\n");

        let client = client();

        // Test 1: Model not found
        println!("1. Testing model not found error...");
        let response = client
            .post(&format!("{}/api/v1/ml/embed", API_URL))
            .json(&json!({
                "model": "nonexistent_model_xyz",
                "text": "test"
            }))
            .send()
            .expect("Request failed");

        assert_eq!(response.status(), 404, "Expected 404 for missing model");
        println!("   ✓ Model not found handled correctly (404)");

        // Test 2: Invalid request (missing required field)
        println!("2. Testing invalid request...");
        let response = client
            .post(&format!("{}/api/v1/ml/embed", API_URL))
            .json(&json!({"invalid": "data"}))
            .send()
            .expect("Request failed");

        assert!(!response.status().is_success(), "Expected error for invalid request");
        println!("   ✓ Invalid request rejected");

        // Test 3: Empty text
        println!("3. Testing empty text...");
        let response = client
            .post(&format!("{}/api/v1/ml/embed", API_URL))
            .json(&json!({
                "model": "minilm",
                "text": ""
            }))
            .send()
            .expect("Request failed");

        // Should either succeed with empty embedding or return error
        println!("   ✓ Empty text handled (status: {})", response.status());

        println!("\n✅ Error handling tests PASSED!");
    }

    #[test]
    #[ignore]
    fn test_batch_embedding_generation() {
        println!("=== Batch Embedding Test ===\n");

        let client = client();

        println!("1. Generating batch embeddings...");
        let texts = vec![
            "First document about machine learning",
            "Second document about data science",
            "Third document about artificial intelligence",
        ];

        let response = client
            .post(&format!("{}/api/v1/ml/embed", API_URL))
            .json(&json!({
                "model": "minilm",
                "texts": texts
            }))
            .send()
            .expect("Batch embedding request failed");

        assert!(
            response.status().is_success(),
            "Batch embedding failed: {}",
            response.text().unwrap_or_default()
        );

        let result: serde_json::Value = response
            .json()
            .expect("Failed to parse batch response");

        assert!(result["embeddings"].is_array(), "Expected embeddings array");
        let embeddings = result["embeddings"].as_array().unwrap();
        assert_eq!(embeddings.len(), texts.len(), "Expected {} embeddings", texts.len());

        for (i, embedding) in embeddings.iter().enumerate() {
            assert_eq!(
                embedding.as_array().unwrap().len(),
                384,
                "Embedding {} should be 384-dimensional",
                i
            );
        }

        println!("   ✓ Generated {} embeddings", embeddings.len());
        println!("   ✓ All embeddings are 384-dimensional");

        println!("\n✅ Batch embedding test PASSED!");
    }
}
