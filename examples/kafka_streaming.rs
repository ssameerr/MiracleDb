//! Kafka Streaming Example - Real-time Data Ingestion & CDC
//!
//! This example demonstrates:
//! 1. Real-time data ingestion from Kafka topics
//! 2. Change Data Capture (CDC) output to Kafka
//! 3. Integration with ONNX ML for real-time predictions
//! 4. Stream processing patterns
//!
//! Prerequisites:
//! 1. Compile with kafka feature: `cargo run --example kafka_streaming --features kafka`
//! 2. Running Kafka broker on localhost:9092
//! 3. Create topics: `transactions-input` and `fraud-alerts-output`
//!
//! Setup Kafka (Docker):
//! ```bash
//! docker run -d --name kafka -p 9092:9092 \
//!   apache/kafka:latest
//!
//! # Create topics
//! docker exec kafka /opt/kafka/bin/kafka-topics.sh \
//!   --create --topic transactions-input \
//!   --bootstrap-server localhost:9092
//!
//! docker exec kafka /opt/kafka/bin/kafka-topics.sh \
//!   --create --topic fraud-alerts-output \
//!   --bootstrap-server localhost:9092
//! ```

use miracledb::engine::{MiracleEngine, EngineConfig};
use std::error::Error;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== MiracleDb Kafka Streaming Example ===\n");

    // 1. Create engine
    let engine = MiracleEngine::new().await?;
    println!("✓ Engine initialized with Kafka support\n");

    // 2. Check Kafka availability
    println!("Checking Kafka connection...");
    #[cfg(feature = "kafka")]
    {
        match miracledb::integration::kafka::KafkaAdmin::new("localhost:9092") {
            Ok(_) => {
                println!("✓ Kafka broker available at localhost:9092\n");
            }
            Err(e) => {
                println!("⚠ Warning: Cannot connect to Kafka broker");
                println!("  Error: {}", e);
                println!("  Please ensure Kafka is running on localhost:9092");
                println!("  Continuing with SQL demonstration...\n");
            }
        }
    }

    // 3. Create Kafka source for incoming transactions
    println!("Creating Kafka source for real-time transactions...");

    let create_source_sql = "
        CREATE SOURCE transactions_stream
        FROM KAFKA (
            brokers = 'localhost:9092',
            topic = 'transactions-input',
            group_id = 'miracledb-consumer',
            auto_offset_reset = 'latest'
        )
        SCHEMA (
            transaction_id BIGINT,
            user_id BIGINT,
            amount DOUBLE,
            merchant_id BIGINT,
            merchant_risk DOUBLE,
            location_score DOUBLE,
            timestamp BIGINT
        )
    ";

    match engine.query(create_source_sql).await {
        Ok(df) => {
            let batches = df.collect().await?;
            println!("✓ Kafka source 'transactions_stream' created");
            println!("  Topic: transactions-input");
            println!("  Broker: localhost:9092");
            println!("  Group: miracledb-consumer\n");
        }
        Err(e) => {
            println!("Note: CREATE SOURCE requires Kafka broker. Error: {}\n", e);
        }
    }

    // 4. Create Kafka sink for fraud alerts (CDC output)
    println!("Creating Kafka sink for fraud alerts...");

    let create_sink_sql = "
        CREATE SINK fraud_alerts
        TO KAFKA (
            brokers = 'localhost:9092',
            topic = 'fraud-alerts-output',
            compression = 'snappy'
        )
        FROM TABLE fraud_transactions
    ";

    match engine.query(create_sink_sql).await {
        Ok(df) => {
            let batches = df.collect().await?;
            println!("✓ Kafka sink 'fraud_alerts' created");
            println!("  Topic: fraud-alerts-output");
            println!("  Compression: snappy\n");
        }
        Err(e) => {
            println!("Note: CREATE SINK requires Kafka broker. Error: {}\n", e);
        }
    }

    // 5. List all Kafka sources and sinks
    println!("Listing Kafka connectors...");

    match engine.query("SHOW SOURCES").await {
        Ok(df) => {
            let batches = df.collect().await?;
            println!("Active Kafka Sources:");
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let name_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(0), row)?;
                    println!("  - {}", name_scalar);
                }
            }
            if batches.is_empty() || batches[0].num_rows() == 0 {
                println!("  (no sources)");
            }
        }
        Err(e) => println!("Error listing sources: {}", e),
    }

    match engine.query("SHOW SINKS").await {
        Ok(df) => {
            let batches = df.collect().await?;
            println!("Active Kafka Sinks:");
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let name_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(0), row)?;
                    println!("  - {}", name_scalar);
                }
            }
            if batches.is_empty() || batches[0].num_rows() == 0 {
                println!("  (no sinks)");
            }
        }
        Err(e) => println!("Error listing sinks: {}", e),
    }
    println!();

    // 6. Demonstrate stream processing workflow
    println!("Stream Processing Workflow:");
    println!("==========================\n");

    println!("Step 1: Kafka Producer sends transaction to 'transactions-input' topic");
    println!("        {{");
    println!("          \"transaction_id\": 1001,");
    println!("          \"user_id\": 100,");
    println!("          \"amount\": 9500.00,");
    println!("          \"merchant_id\": 5002,");
    println!("          \"merchant_risk\": 0.85,");
    println!("          \"location_score\": 0.15,");
    println!("          \"timestamp\": 1706200000");
    println!("        }}\n");

    println!("Step 2: MiracleDb Kafka Source ingests message into transactions table");
    println!("        (Automatic conversion from JSON to Arrow RecordBatch)\n");

    println!("Step 3: Real-time fraud detection with ONNX ML");
    println!("        SELECT");
    println!("          transaction_id,");
    println!("          predict('fraud_detector', amount, merchant_risk, location_score) as fraud_score");
    println!("        FROM transactions_stream");
    println!("        WHERE fraud_score > 0.9;\n");

    println!("Step 4: Insert high-risk transactions into fraud_transactions table");
    println!("        INSERT INTO fraud_transactions");
    println!("        SELECT * FROM transactions_stream WHERE fraud_score > 0.9;\n");

    println!("Step 5: CDC trigger automatically sends to Kafka sink");
    println!("        Kafka Sink publishes to 'fraud-alerts-output' topic\n");

    println!("Step 6: Downstream consumers process fraud alerts");
    println!("        - Block transaction API");
    println!("        - Notify fraud team");
    println!("        - Update user risk score\n");

    // 7. Demonstrate SQL patterns for streaming
    println!("Common Stream Processing Patterns:");
    println!("===================================\n");

    println!("1. Real-Time Aggregation (Tumbling Window):");
    println!("   ```sql");
    println!("   SELECT");
    println!("     time_bucket('1 minute', timestamp) as window,");
    println!("     COUNT(*) as transaction_count,");
    println!("     AVG(amount) as avg_amount,");
    println!("     SUM(CASE WHEN fraud_score > 0.9 THEN 1 ELSE 0 END) as fraud_count");
    println!("   FROM transactions_stream");
    println!("   GROUP BY window;");
    println!("   ```\n");

    println!("2. Join Streaming Data with Static Tables:");
    println!("   ```sql");
    println!("   SELECT");
    println!("     t.transaction_id,");
    println!("     t.amount,");
    println!("     u.name,");
    println!("     u.credit_score");
    println!("   FROM transactions_stream t");
    println!("   JOIN users u ON t.user_id = u.id");
    println!("   WHERE t.amount > u.daily_limit;");
    println!("   ```\n");

    println!("3. Enrichment with Vector Search:");
    println!("   ```sql");
    println!("   SELECT");
    println!("     t.transaction_id,");
    println!("     similar_tx.transaction_id as similar_to,");
    println!("     vector_distance(t.pattern_embedding, similar_tx.pattern_embedding) as similarity");
    println!("   FROM transactions_stream t");
    println!("   CROSS JOIN historical_transactions similar_tx");
    println!("   WHERE vector_distance(t.pattern_embedding, similar_tx.pattern_embedding) < 0.3");
    println!("     AND similar_tx.is_fraud = true;");
    println!("   ```\n");

    println!("4. Stateful Processing (Session Window):");
    println!("   ```sql");
    println!("   SELECT");
    println!("     user_id,");
    println!("     COUNT(*) as tx_count,");
    println!("     SUM(amount) as total_amount,");
    println!("     MAX(timestamp) - MIN(timestamp) as session_duration");
    println!("   FROM transactions_stream");
    println!("   GROUP BY user_id, session(timestamp, interval '5 minutes');");
    println!("   ```\n");

    // 8. Performance considerations
    println!("Performance Tips:");
    println!("==================");
    println!("1. Batch Size: Configure consumer batch size for throughput");
    println!("   - Larger batches (10K+) for high throughput");
    println!("   - Smaller batches (100-1K) for low latency");
    println!();
    println!("2. Compression: Use 'snappy' or 'lz4' for balanced performance");
    println!("   - snappy: Fastest compression (default)");
    println!("   - lz4: Better compression ratio");
    println!("   - zstd: Best compression, slower");
    println!();
    println!("3. Partitioning: Use Kafka partitions for parallelism");
    println!("   - 1 partition per CPU core for optimal throughput");
    println!("   - Key-based partitioning for ordering guarantees");
    println!();
    println!("4. Consumer Groups: Multiple consumers for horizontal scaling");
    println!("   - Each consumer processes different partitions");
    println!("   - Automatic rebalancing on consumer add/remove");
    println!();

    // 9. Integration with other MiracleDb features
    println!("Integration Examples:");
    println!("=====================\n");

    println!("Streaming + Vector Search:");
    println!("  Real-time similarity detection on incoming data\n");

    println!("Streaming + ONNX ML:");
    println!("  In-flight inference without external services\n");

    println!("Streaming + Time-Series:");
    println!("  Time-bucketed aggregations for metrics\n");

    println!("Streaming + Graph:");
    println!("  Real-time fraud ring detection\n");

    println!("Streaming + Compliance:");
    println!("  Automatic PII masking on ingestion\n");

    // 10. Cleanup
    println!("Cleanup Commands:");
    println!("==================");
    println!("DROP SOURCE transactions_stream;");
    println!("DROP SINK fraud_alerts;\n");

    // Optional: Actually drop if they exist
    let _ = engine.query("DROP SOURCE transactions_stream").await;
    let _ = engine.query("DROP SINK fraud_alerts").await;

    println!("✓ Example complete!\n");

    println!("Next Steps:");
    println!("===========");
    println!("1. Start Kafka broker (see setup instructions above)");
    println!("2. Create Kafka topics");
    println!("3. Run this example with: cargo run --example kafka_streaming --features kafka");
    println!("4. Produce test messages to 'transactions-input' topic");
    println!("5. Consume fraud alerts from 'fraud-alerts-output' topic");
    println!();
    println!("Kafka Console Producer:");
    println!("  docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \\");
    println!("    --topic transactions-input \\");
    println!("    --bootstrap-server localhost:9092");
    println!();
    println!("Kafka Console Consumer:");
    println!("  docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \\");
    println!("    --topic fraud-alerts-output \\");
    println!("    --bootstrap-server localhost:9092 \\");
    println!("    --from-beginning");

    Ok(())
}
