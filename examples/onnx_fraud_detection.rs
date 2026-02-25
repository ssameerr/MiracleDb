//! ONNX ML Inference Example - Fraud Detection
//!
//! This example demonstrates how to use ONNX models in MiracleDb for real-time fraud detection.
//!
//! Prerequisites:
//! 1. Compile with 'ml' feature: `cargo run --example onnx_fraud_detection --features ml`
//! 2. Download a fraud detection ONNX model or use the provided script
//!
//! Model Training (Python):
//! ```python
//! import pandas as pd
//! import numpy as np
//! from sklearn.ensemble import RandomForestClassifier
//! from sklearn.model_selection import train_test_split
//! from skl2onnx import to_onnx
//!
//! # Generate synthetic fraud data
//! np.random.seed(42)
//! n_samples = 10000
//!
//! data = {
//!     'amount': np.random.uniform(1, 10000, n_samples),
//!     'merchant_risk': np.random.uniform(0, 1, n_samples),
//!     'location_score': np.random.uniform(0, 1, n_samples),
//!     'time_since_last': np.random.uniform(0, 24, n_samples),
//! }
//!
//! # Create fraud labels (10% fraud rate)
//! df = pd.DataFrame(data)
//! df['is_fraud'] = (
//!     (df['amount'] > 5000) &
//!     (df['merchant_risk'] > 0.7) &
//!     (df['location_score'] < 0.3)
//! ).astype(int)
//!
//! # Train model
//! X = df[['amount', 'merchant_risk', 'location_score', 'time_since_last']]
//! y = df['is_fraud']
//!
//! model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
//! model.fit(X, y)
//!
//! # Convert to ONNX
//! onnx_model = to_onnx(model, X[:1].values.astype(np.float32))
//! with open("fraud_detector.onnx", "wb") as f:
//!     f.write(onnx_model.SerializeToString())
//! ```

use miracledb::engine::{MiracleEngine, EngineConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== MiracleDb ONNX Fraud Detection Example ===\n");

    // 1. Create engine with ML features
    let engine = MiracleEngine::new().await?;
    println!("✓ Engine initialized with ONNX Runtime support\n");

    // 2. Load fraud detection model
    println!("Loading fraud detection model...");

    // Option A: Load from local file
    let model_path = "fraud_detector.onnx";

    #[cfg(feature = "ml")]
    {
        if std::path::Path::new(model_path).exists() {
            engine.model_registry.load_model("fraud_detector", model_path)?;
            println!("✓ Loaded model from local file: {}\n", model_path);
        } else {
            println!("⚠ Model file not found: {}", model_path);
            println!("  Please run the Python script above to generate the model");
            println!("  Or use SQL to load from URL:");
            println!("  CREATE MODEL fraud_detector FROM 'https://example.com/fraud_detector.onnx';\n");

            // For demo purposes, we'll continue with synthetic queries
            println!("Continuing with SQL demonstration...\n");
        }
    }

    // Option B: Load from S3/URL using SQL
    println!("Alternative loading methods:");
    println!("  SQL: CREATE MODEL fraud_detector FROM 'https://models.example.com/fraud.onnx';");
    println!("  SQL: CREATE MODEL fraud_detector FROM 's3://my-bucket/models/fraud.onnx';\n");

    // 3. Create transactions table
    println!("Creating transactions table...");
    engine.query(
        "CREATE TABLE transactions (
            transaction_id BIGINT,
            amount DOUBLE,
            merchant_id BIGINT,
            merchant_risk DOUBLE,
            location_score DOUBLE,
            time_since_last_hours DOUBLE,
            user_id BIGINT,
            timestamp BIGINT
        )"
    ).await?;
    println!("✓ Table created\n");

    // 4. Insert sample transactions
    println!("Inserting sample transactions...");
    engine.query(
        "INSERT INTO transactions VALUES
        (1001, 150.50, 5001, 0.1, 0.9, 12.5, 100, 1706200000),
        (1002, 8500.00, 5002, 0.8, 0.2, 0.5, 101, 1706200100),
        (1003, 45.20, 5003, 0.3, 0.8, 24.0, 102, 1706200200),
        (1004, 9200.00, 5004, 0.9, 0.1, 0.2, 103, 1706200300),
        (1005, 320.00, 5001, 0.2, 0.85, 6.0, 104, 1706200400),
        (1006, 12000.00, 5005, 0.95, 0.05, 0.1, 105, 1706200500)"
    ).await?;
    println!("✓ Inserted 6 sample transactions\n");

    // 5. Run fraud detection using predict() UDF
    println!("Running fraud detection with ONNX model...\n");

    let query = "
        SELECT
            transaction_id,
            amount,
            merchant_id,
            predict('fraud_detector',
                    amount,
                    merchant_risk,
                    location_score,
                    time_since_last_hours) as fraud_score
        FROM transactions
        WHERE predict('fraud_detector',
                     amount,
                     merchant_risk,
                     location_score,
                     time_since_last_hours) > 0.7
        ORDER BY fraud_score DESC
    ";

    println!("Query:");
    println!("{}\n", query);

    #[cfg(feature = "ml")]
    {
        if std::path::Path::new(model_path).exists() {
            match engine.query(query).await {
                Ok(df) => {
                    let batches = df.collect().await?;
                    println!("Fraud Detection Results:");
                    println!("========================");

                    for batch in &batches {
                        for row in 0..batch.num_rows() {
                            let tx_id = batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                                .unwrap().value(row);
                            let amount = batch.column(1).as_any().downcast_ref::<arrow::array::Float64Array>()
                                .unwrap().value(row);
                            let merchant = batch.column(2).as_any().downcast_ref::<arrow::array::Int64Array>()
                                .unwrap().value(row);
                            let fraud_score = batch.column(3).as_any().downcast_ref::<arrow::array::Float32Array>()
                                .unwrap().value(row);

                            println!(
                                "🚨 Transaction {} - Amount: ${:.2} - Merchant: {} - Fraud Score: {:.3}",
                                tx_id, amount, merchant, fraud_score
                            );
                        }
                    }
                    println!();
                }
                Err(e) => {
                    println!("Note: Query requires loaded model. Error: {}\n", e);
                }
            }
        }
    }

    // 6. Advanced use case: Batch scoring all transactions
    println!("Batch scoring all transactions...\n");

    let batch_query = "
        SELECT
            transaction_id,
            user_id,
            amount,
            merchant_risk,
            predict('fraud_detector',
                    amount,
                    merchant_risk,
                    location_score,
                    time_since_last_hours) as fraud_score,
            CASE
                WHEN predict('fraud_detector', amount, merchant_risk, location_score, time_since_last_hours) > 0.9
                    THEN 'BLOCK'
                WHEN predict('fraud_detector', amount, merchant_risk, location_score, time_since_last_hours) > 0.7
                    THEN 'REVIEW'
                ELSE 'APPROVE'
            END as recommendation
        FROM transactions
        ORDER BY fraud_score DESC
    ";

    println!("Query:");
    println!("{}\n", batch_query);

    #[cfg(feature = "ml")]
    {
        if std::path::Path::new(model_path).exists() {
            match engine.query(batch_query).await {
                Ok(df) => {
                    let batches = df.collect().await?;
                    println!("All Transactions with Recommendations:");
                    println!("========================================");

                    for batch in &batches {
                        for row in 0..batch.num_rows() {
                            let tx_id = batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                                .unwrap().value(row);
                            let user_id = batch.column(1).as_any().downcast_ref::<arrow::array::Int64Array>()
                                .unwrap().value(row);
                            let amount = batch.column(2).as_any().downcast_ref::<arrow::array::Float64Array>()
                                .unwrap().value(row);
                            let merchant_risk = batch.column(3).as_any().downcast_ref::<arrow::array::Float64Array>()
                                .unwrap().value(row);
                            let fraud_score = batch.column(4).as_any().downcast_ref::<arrow::array::Float32Array>()
                                .unwrap().value(row);

                            let recommendation_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(5), row)
                                .unwrap();
                            let recommendation = format!("{}", recommendation_scalar);

                            let emoji = match recommendation.trim_matches('"') {
                                "BLOCK" => "🚫",
                                "REVIEW" => "⚠️",
                                _ => "✅",
                            };

                            println!(
                                "{} TX:{} User:{} Amount:${:.2} Risk:{:.2} Score:{:.3} -> {}",
                                emoji, tx_id, user_id, amount, merchant_risk, fraud_score, recommendation.trim_matches('"')
                            );
                        }
                    }
                    println!();
                }
                Err(e) => {
                    println!("Note: Query requires loaded model. Error: {}\n", e);
                }
            }
        }
    }

    // 7. Model management with SQL
    println!("Model Management Commands:");
    println!("=========================");
    println!("List models:        SHOW MODELS;");
    println!("Load model:         CREATE MODEL name FROM 'path/to/model.onnx';");
    println!("Load from URL:      CREATE MODEL name FROM 'https://...';");
    println!("Load from S3:       CREATE MODEL name FROM 's3://bucket/model.onnx';");
    println!("Drop model:         DROP MODEL name;\n");

    // List loaded models
    match engine.query("SHOW MODELS").await {
        Ok(df) => {
            let batches = df.collect().await?;
            println!("Currently loaded models:");
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let name_scalar = datafusion::common::ScalarValue::try_from_array(batch.column(0), row)
                        .unwrap();
                    println!("  - {}", name_scalar);
                }
            }
            if batches.is_empty() || batches[0].num_rows() == 0 {
                println!("  (no models loaded)");
            }
            println!();
        }
        Err(e) => {
            println!("Error listing models: {}\n", e);
        }
    }

    // 8. Performance considerations
    println!("Performance Tips:");
    println!("=================");
    println!("1. Batch Predictions: Process multiple rows in a single query");
    println!("2. GPU Acceleration: ONNX Runtime uses CUDA if available");
    println!("3. Model Caching: Models stay loaded in memory for fast inference");
    println!("4. Optimize Models: Use ONNX optimization tools before deployment");
    println!("5. Feature Engineering: Pre-compute features for better performance\n");

    // 9. Integration with other features
    println!("Integration Examples:");
    println!("====================");
    println!("Vector Search + ML:");
    println!("  SELECT *, predict('fraud_detector', ...) as score");
    println!("  FROM transactions");
    println!("  WHERE vector_distance(embedding, query_vector) < 0.5;");
    println!();
    println!("Time-Series + ML:");
    println!("  SELECT time_bucket('1 hour', timestamp) as hour,");
    println!("         avg(predict('fraud_detector', ...)) as avg_fraud_score");
    println!("  FROM transactions");
    println!("  GROUP BY hour;");
    println!();
    println!("Streaming CDC + ML:");
    println!("  -- Real-time fraud detection on incoming transactions");
    println!("  CREATE TRIGGER fraud_check AFTER INSERT ON transactions");
    println!("  FOR EACH ROW EXECUTE FUNCTION check_fraud_score();");
    println!();

    println!("✓ Example complete!\n");

    Ok(())
}
