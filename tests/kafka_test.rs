#[cfg(all(test, feature = "kafka"))]
mod kafka_tests {
    use miracledb::engine::MiracleEngine;
    use miracledb::integration::kafka::{KafkaSourceConfig, KafkaSinkConfig, KafkaAdmin};
    use std::sync::Arc;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    /// Test Kafka source configuration
    #[test]
    fn test_kafka_source_config_default() {
        let config = KafkaSourceConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "miracledb-input");
        assert_eq!(config.group_id, "miracledb-consumer");
        assert_eq!(config.auto_offset_reset, "latest");
    }

    /// Test Kafka sink configuration
    #[test]
    fn test_kafka_sink_config_default() {
        let config = KafkaSinkConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "miracledb-output");
        assert_eq!(config.compression, "snappy");
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.linger_ms, 100);
    }

    /// Test CREATE SOURCE SQL command
    #[tokio::test]
    async fn test_sql_create_source() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // CREATE SOURCE should parse and execute
        let sql = "CREATE SOURCE test_source FROM KAFKA (brokers='localhost:9092', topic='test')";
        let result = engine.query(sql).await;

        // Should succeed
        assert!(result.is_ok(), "CREATE SOURCE should succeed");

        if let Ok(df) = result {
            let batches = df.collect().await.unwrap();
            assert!(!batches.is_empty());
            assert_eq!(batches[0].num_rows(), 1);

            // Should return status="created"
            let status_col = batches[0].column(0);
            let status_arr = status_col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
            assert_eq!(status_arr.value(0), "created");
        }
    }

    /// Test CREATE SINK SQL command
    #[tokio::test]
    async fn test_sql_create_sink() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let sql = "CREATE SINK test_sink TO KAFKA (brokers='localhost:9092', topic='output')";
        let result = engine.query(sql).await;

        assert!(result.is_ok(), "CREATE SINK should succeed");

        if let Ok(df) = result {
            let batches = df.collect().await.unwrap();
            assert!(!batches.is_empty());
            assert_eq!(batches[0].num_rows(), 1);
        }
    }

    /// Test SHOW SOURCES command
    #[tokio::test]
    async fn test_sql_show_sources_empty() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("SHOW SOURCES").await;
        assert!(result.is_ok(), "SHOW SOURCES should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        // Empty list is valid
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    /// Test SHOW SINKS command
    #[tokio::test]
    async fn test_sql_show_sinks_empty() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("SHOW SINKS").await;
        assert!(result.is_ok(), "SHOW SINKS should work");

        let df = result.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    /// Test LIST SOURCES command
    #[tokio::test]
    async fn test_sql_list_sources() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("LIST SOURCES").await;
        assert!(result.is_ok(), "LIST SOURCES should work");
    }

    /// Test LIST SINKS command
    #[tokio::test]
    async fn test_sql_list_sinks() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("LIST SINKS").await;
        assert!(result.is_ok(), "LIST SINKS should work");
    }

    /// Test DROP SOURCE command
    #[tokio::test]
    async fn test_sql_drop_source() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create source first
        let _ = engine.query("CREATE SOURCE drop_test FROM KAFKA (brokers='localhost:9092')").await;

        // Drop source
        let result = engine.query("DROP SOURCE drop_test").await;
        assert!(result.is_ok(), "DROP SOURCE should succeed");

        // Verify it's removed
        let sources = engine.kafka_sources.read().unwrap();
        assert!(!sources.contains_key("drop_test"));
    }

    /// Test DROP SINK command
    #[tokio::test]
    async fn test_sql_drop_sink() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create sink first
        let _ = engine.query("CREATE SINK drop_test TO KAFKA (brokers='localhost:9092')").await;

        // Drop sink
        let result = engine.query("DROP SINK drop_test").await;
        assert!(result.is_ok(), "DROP SINK should succeed");

        // Verify it's removed
        let sinks = engine.kafka_sinks.read().unwrap();
        assert!(!sinks.contains_key("drop_test"));
    }

    /// Test DROP SOURCE not found
    #[tokio::test]
    async fn test_sql_drop_source_not_found() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("DROP SOURCE nonexistent_source").await;
        assert!(result.is_err(), "Should fail when source doesn't exist");
    }

    /// Test DROP SINK not found
    #[tokio::test]
    async fn test_sql_drop_sink_not_found() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        let result = engine.query("DROP SINK nonexistent_sink").await;
        assert!(result.is_err(), "Should fail when sink doesn't exist");
    }

    /// Test complete workflow: create, list, drop
    #[tokio::test]
    async fn test_complete_workflow() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // 1. Create source
        let result = engine.query("CREATE SOURCE workflow_source FROM KAFKA (brokers='localhost:9092')").await;
        assert!(result.is_ok());

        // 2. Create sink
        let result = engine.query("CREATE SINK workflow_sink TO KAFKA (brokers='localhost:9092')").await;
        assert!(result.is_ok());

        // 3. List sources - should have 1
        let result = engine.query("SHOW SOURCES").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        // 4. List sinks - should have 1
        let result = engine.query("SHOW SINKS").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);

        // 5. Drop source
        let result = engine.query("DROP SOURCE workflow_source").await;
        assert!(result.is_ok());

        // 6. Drop sink
        let result = engine.query("DROP SINK workflow_sink").await;
        assert!(result.is_ok());

        // 7. Verify both removed
        let result = engine.query("SHOW SOURCES").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 0);

        let result = engine.query("SHOW SINKS").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    /// Test Kafka source creation with schema
    #[tokio::test]
    #[ignore] // Requires running Kafka broker
    async fn test_kafka_source_creation() {
        let config = KafkaSourceConfig {
            brokers: "localhost:9092".to_string(),
            topic: "test-topic".to_string(),
            group_id: "test-group".to_string(),
            auto_offset_reset: "earliest".to_string(),
            schema_registry_url: None,
            consumer_config: std::collections::HashMap::new(),
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ]));

        let source_result = miracledb::integration::kafka::KafkaSource::new(config, schema);

        // Will succeed even without Kafka (connection is lazy)
        assert!(source_result.is_ok());
    }

    /// Test Kafka sink creation
    #[tokio::test]
    #[ignore] // Requires running Kafka broker
    async fn test_kafka_sink_creation() {
        let config = KafkaSinkConfig {
            brokers: "localhost:9092".to_string(),
            topic: "test-output".to_string(),
            compression: "snappy".to_string(),
            batch_size: 1000,
            linger_ms: 10,
            schema_registry_url: None,
            producer_config: std::collections::HashMap::new(),
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let sink_result = miracledb::integration::kafka::KafkaSink::new(config, schema);

        assert!(sink_result.is_ok());
    }

    /// Test multiple sources can coexist
    #[tokio::test]
    async fn test_multiple_sources() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create multiple sources
        let _ = engine.query("CREATE SOURCE source1 FROM KAFKA (brokers='localhost:9092')").await;
        let _ = engine.query("CREATE SOURCE source2 FROM KAFKA (brokers='localhost:9092')").await;
        let _ = engine.query("CREATE SOURCE source3 FROM KAFKA (brokers='localhost:9092')").await;

        // List should show all 3
        let result = engine.query("SHOW SOURCES").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 3);

        // Verify all exist
        let sources = engine.kafka_sources.read().unwrap();
        assert!(sources.contains_key("source1"));
        assert!(sources.contains_key("source2"));
        assert!(sources.contains_key("source3"));
    }

    /// Test multiple sinks can coexist
    #[tokio::test]
    async fn test_multiple_sinks() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // Create multiple sinks
        let _ = engine.query("CREATE SINK sink1 TO KAFKA (brokers='localhost:9092')").await;
        let _ = engine.query("CREATE SINK sink2 TO KAFKA (brokers='localhost:9092')").await;

        // List should show both
        let result = engine.query("SHOW SINKS").await.unwrap();
        let batches = result.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }
}

#[cfg(all(test, not(feature = "kafka")))]
mod kafka_disabled_tests {
    use miracledb::engine::MiracleEngine;

    #[tokio::test]
    async fn test_kafka_feature_disabled() {
        let engine = MiracleEngine::new().await.expect("Failed to create engine");

        // All Kafka operations should fail gracefully
        let result = engine.query("CREATE SOURCE test FROM KAFKA (brokers='localhost:9092')").await;
        assert!(result.is_err(), "Should fail when Kafka feature is disabled");

        let result = engine.query("SHOW SOURCES").await;
        assert!(result.is_err(), "Should fail when Kafka feature is disabled");

        let result = engine.query("CREATE SINK test TO KAFKA (brokers='localhost:9092')").await;
        assert!(result.is_err(), "Should fail when Kafka feature is disabled");
    }
}
