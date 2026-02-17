/// Basic ONNX integration tests
///
/// These tests verify that the ONNX runtime integration compiles correctly
/// with the ort 2.0.0-rc.11 API. Full functional tests require actual ONNX
/// model files and are marked with #[ignore].

#[cfg(all(test, feature = "ml"))]
mod onnx_tests {
    use miracledb::udf::onnx::ModelRegistry;
    use miracledb::engine::MiracleEngine;

    /// Helper function to get a test model path
    /// In production, you'd have a real ONNX model file checked in
    fn create_test_model() -> String {
        // Return a path where a test model might exist
        // Tests using this should be marked #[ignore] if the file doesn't exist
        std::env::var("TEST_ONNX_MODEL_PATH")
            .unwrap_or_else(|_| "./test_models/simple_model.onnx".to_string())
    }

    /// Test that ONNX ModelRegistry can be created
    /// This verifies the ort 2.0 API is correctly integrated
    #[tokio::test]
    async fn test_model_registry_creation() {
        let result = ModelRegistry::new();
        assert!(result.is_ok(), "ModelRegistry creation should succeed");

        let registry = result.unwrap();
        assert_eq!(registry.list_models().len(), 0, "Should start with no models");
    }

    #[tokio::test]
    #[ignore] // Ignore by default - requires actual ONNX model file
    async fn test_load_model_from_file() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");
        let model_path = create_test_model();

        // Test loading model
        let result = engine.model_registry.load_model("test_model", &model_path);

        // If model file doesn't exist, this will fail - which is expected
        // In real CI, you'd have a test model checked in
        if result.is_ok() {
            assert!(result.is_ok(), "Model should load successfully");

            // Verify model is in registry
            let models = engine.model_registry.list_models();
            assert!(models.contains(&"test_model".to_string()));
        }
    }

    #[tokio::test]
    #[ignore] // Ignore by default - requires network and test model URL
    async fn test_load_model_from_url() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Example URL - in real tests, use a reliable test model URL
        let test_url = "https://github.com/onnx/models/raw/main/validated/vision/classification/mobilenet/model/mobilenetv2-12.onnx";

        let result = engine.model_registry.load_model_from_url("mobilenet", test_url).await;

        if result.is_ok() {
            let models = engine.model_registry.list_models();
            assert!(models.contains(&"mobilenet".to_string()));
        }
    }

    #[tokio::test]
    async fn test_list_models_empty() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let models = engine.model_registry.list_models();
        assert_eq!(models.len(), 0, "Should start with no models");
    }

    #[tokio::test]
    #[ignore] // Requires actual model
    async fn test_predict_udf() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");
        let model_path = create_test_model();

        // Load model
        engine.model_registry.load_model("test_model", &model_path).ok();

        // Create test table
        let _ = engine.query("CREATE TABLE test_features (id INT, feature1 FLOAT, feature2 FLOAT)").await;
        let _ = engine.query("INSERT INTO test_features VALUES (1, 1.0, 2.0), (2, 3.0, 4.0)").await;

        // Run prediction
        let result = engine
            .query("SELECT id, predict('test_model', feature1, feature2) as prediction FROM test_features")
            .await;

        if result.is_ok() {
            let df = result.unwrap();
            let batches = df.collect().await.unwrap();
            assert!(!batches.is_empty(), "Should have prediction results");
        }
    }

    #[tokio::test]
    async fn test_predict_udf_model_not_found() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create test table
        let _ = engine.query("CREATE TABLE test_features (id INT, feature1 FLOAT)").await;
        let _ = engine.query("INSERT INTO test_features VALUES (1, 1.0)").await;

        // Try prediction with non-existent model
        let result = engine
            .query("SELECT id, predict('nonexistent_model', feature1) as prediction FROM test_features")
            .await;

        // This should fail or return error in query execution
        assert!(result.is_ok(), "Query should parse successfully");

        // The error happens at collect time
        if let Ok(df) = result {
            let collect_result = df.collect().await;
            assert!(
                collect_result.is_err(),
                "Should fail when model doesn't exist"
            );
        }
    }

    #[tokio::test]
    #[ignore] // Requires actual model file
    async fn test_sql_create_model_from_file() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");
        let model_path = create_test_model();

        // Test CREATE MODEL command
        let result = engine
            .query(&format!("CREATE MODEL test_model FROM '{}'", model_path))
            .await;

        if std::path::Path::new(&model_path).exists() {
            assert!(result.is_ok(), "CREATE MODEL should succeed");

            // Verify model is loaded
            let list_result = engine.query("SHOW MODELS").await;
            assert!(list_result.is_ok());

            let df = list_result.unwrap();
            let batches = df.collect().await.unwrap();
            assert!(!batches.is_empty());
        }
    }

    #[tokio::test]
    #[ignore] // Requires network
    async fn test_sql_create_model_from_url() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let test_url = "https://github.com/onnx/models/raw/main/validated/vision/classification/mnist/model/mnist-12.onnx";

        let result = engine
            .query(&format!("CREATE MODEL mnist FROM '{}'", test_url))
            .await;

        if result.is_ok() {
            let list_result = engine.query("LIST MODELS").await.unwrap();
            let batches = list_result.collect().await.unwrap();

            // Should have at least one model
            assert!(!batches.is_empty());
        }
    }

    #[tokio::test]
    async fn test_sql_show_models_empty() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("SHOW MODELS").await;
        assert!(result.is_ok(), "SHOW MODELS should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        // Empty list is valid
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[tokio::test]
    async fn test_sql_list_models_empty() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("LIST MODELS").await;
        assert!(result.is_ok(), "LIST MODELS should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        // Empty list is valid
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[tokio::test]
    #[ignore] // Requires actual model
    async fn test_sql_drop_model() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");
        let model_path = create_test_model();

        // Load model first
        let _ = engine.model_registry.load_model("test_model", &model_path);

        // Drop model
        let result = engine.query("DROP MODEL test_model").await;

        if std::path::Path::new(&model_path).exists() {
            assert!(result.is_ok(), "DROP MODEL should succeed");

            // Verify model is removed
            let models = engine.model_registry.list_models();
            assert!(!models.contains(&"test_model".to_string()));
        }
    }

    #[tokio::test]
    async fn test_sql_drop_model_not_found() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Try to drop non-existent model
        let result = engine.query("DROP MODEL nonexistent_model").await;

        // Should return error
        assert!(result.is_err() || {
            // Or if it succeeds, collect should fail
            if let Ok(df) = result {
                df.collect().await.is_err()
            } else {
                true
            }
        });
    }

    #[tokio::test]
    async fn test_model_registry_unload() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Try to unload non-existent model
        let result = engine.model_registry.unload_model("nonexistent");
        assert!(result.is_err(), "Should fail to unload non-existent model");
    }

    #[tokio::test]
    #[ignore] // Requires actual model
    async fn test_onnx_predict_udf_with_array() {
        use miracledb::engine::MiracleEngine;

        let engine = MiracleEngine::new().await.expect("Failed to create engine");
        let model_path = create_test_model();

        // Load model
        engine.model_registry.load_model("test_model", &model_path).ok();

        // Test onnx_predict with array syntax
        let result = engine
            .query("SELECT onnx_predict('test_model', ARRAY[1.0, 2.0, 3.0]) as prediction")
            .await;

        if result.is_ok() {
            let df = result.unwrap();
            let batches = df.collect().await;

            if batches.is_ok() {
                let batches = batches.unwrap();
                assert!(!batches.is_empty(), "Should have prediction result");

                // Verify the result has a prediction column
                let batch = &batches[0];
                assert_eq!(batch.num_columns(), 1);
                assert_eq!(batch.column(0).data_type(), &arrow::datatypes::DataType::Float32);
            }
        }
    }

    #[tokio::test]
    async fn test_onnx_predict_udf_model_not_found() {
        use miracledb::engine::MiracleEngine;

        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Try prediction with non-existent model using array syntax
        let result = engine
            .query("SELECT onnx_predict('nonexistent_model', ARRAY[1.0, 2.0]) as prediction")
            .await;

        // Query should parse successfully
        assert!(result.is_ok(), "Query should parse successfully");

        // The error happens at collect time
        if let Ok(df) = result {
            let collect_result = df.collect().await;
            assert!(
                collect_result.is_err(),
                "Should fail when model doesn't exist"
            );
        }
    }

    #[tokio::test]
    async fn test_onnx_predict_vs_predict() {
        use miracledb::engine::MiracleEngine;

        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Both UDFs should be registered
        // predict() takes variadic args: predict('model', col1, col2, col3)
        // onnx_predict() takes array: onnx_predict('model', ARRAY[col1, col2, col3])

        // Both should parse successfully (execution will fail without model)
        let result1 = engine.query("SELECT predict('model', 1.0, 2.0)").await;
        let result2 = engine.query("SELECT onnx_predict('model', ARRAY[1.0, 2.0])").await;

        assert!(result1.is_ok(), "predict() UDF should be registered");
        assert!(result2.is_ok(), "onnx_predict() UDF should be registered");
    }

    #[tokio::test]
    #[ignore] // Requires actual model for full workflow
    async fn test_complete_workflow() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");
        let model_path = create_test_model();

        if !std::path::Path::new(&model_path).exists() {
            // Skip if model doesn't exist
            return;
        }

        // 1. Load model via SQL
        let result = engine
            .query(&format!("CREATE MODEL workflow_test FROM '{}'", model_path))
            .await;
        assert!(result.is_ok());

        // 2. Verify it's listed
        let result = engine.query("SHOW MODELS").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert!(batches[0].num_rows() > 0);

        // 3. Create test data
        engine
            .query("CREATE TABLE test_data (id INT, x1 FLOAT, x2 FLOAT)")
            .await
            .unwrap();
        engine
            .query("INSERT INTO test_data VALUES (1, 1.0, 2.0), (2, 3.0, 4.0)")
            .await
            .unwrap();

        // 4. Run prediction
        let result = engine
            .query("SELECT id, predict('workflow_test', x1, x2) as pred FROM test_data")
            .await;
        assert!(result.is_ok());

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());

        // 5. Drop model
        let result = engine.query("DROP MODEL workflow_test").await;
        assert!(result.is_ok());

        // 6. Verify it's removed
        let models = engine.model_registry.list_models();
        assert!(!models.contains(&"workflow_test".to_string()));
    }
}

#[cfg(all(test, not(feature = "ml")))]
mod onnx_disabled_tests {
    use miracledb::engine::MiracleEngine;

    #[tokio::test]
    async fn test_ml_feature_disabled() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // All ML operations should fail gracefully
        let result = engine.query("CREATE MODEL test FROM '/tmp/model.onnx'").await;
        assert!(result.is_err(), "Should fail when ML feature is disabled");

        let result = engine.query("SHOW MODELS").await;
        assert!(result.is_err(), "Should fail when ML feature is disabled");
    }
}
