//! Kafka Integration for MiracleDb
//!
//! Provides streaming data ingestion and Change Data Capture (CDC) via Apache Kafka.
//!
//! Features:
//! - Kafka consumer for data ingestion (CREATE SOURCE)
//! - Kafka producer for CDC output (CREATE SINK)
//! - Schema Registry support
//! - Automatic serialization/deserialization
//! - Error handling and retry logic

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

#[cfg(feature = "kafka")]
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    message::{BorrowedMessage, Header, Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    Message,
};

/// Kafka source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceConfig {
    /// Kafka bootstrap servers
    pub brokers: String,
    /// Topic to consume from
    pub topic: String,
    /// Consumer group ID
    pub group_id: String,
    /// Starting offset: "earliest" or "latest"
    pub auto_offset_reset: String,
    /// Optional schema registry URL
    pub schema_registry_url: Option<String>,
    /// Consumer-specific configuration
    pub consumer_config: HashMap<String, String>,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "miracledb-input".to_string(),
            group_id: "miracledb-consumer".to_string(),
            auto_offset_reset: "latest".to_string(),
            schema_registry_url: None,
            consumer_config: HashMap::new(),
        }
    }
}

/// Kafka sink configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    /// Kafka bootstrap servers
    pub brokers: String,
    /// Topic to produce to
    pub topic: String,
    /// Message compression: none, gzip, snappy, lz4, zstd
    pub compression: String,
    /// Batch size
    pub batch_size: usize,
    /// Linger time (ms)
    pub linger_ms: u64,
    /// Optional schema registry URL
    pub schema_registry_url: Option<String>,
    /// Producer-specific configuration
    pub producer_config: HashMap<String, String>,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "miracledb-output".to_string(),
            compression: "snappy".to_string(),
            batch_size: 10000,
            linger_ms: 100,
            schema_registry_url: None,
            producer_config: HashMap::new(),
        }
    }
}

/// Kafka source for consuming data into MiracleDb
pub struct KafkaSource {
    config: KafkaSourceConfig,
    #[cfg(feature = "kafka")]
    consumer: Option<Arc<StreamConsumer>>,
    schema: SchemaRef,
    running: Arc<RwLock<bool>>,
}

impl KafkaSource {
    /// Create new Kafka source
    pub fn new(config: KafkaSourceConfig, schema: SchemaRef) -> Result<Self> {
        #[cfg(feature = "kafka")]
        {
            info!(
                "Creating Kafka source: brokers={}, topic={}, group={}",
                config.brokers, config.topic, config.group_id
            );

            let mut client_config = ClientConfig::new();
            client_config
                .set("bootstrap.servers", &config.brokers)
                .set("group.id", &config.group_id)
                .set("enable.auto.commit", "true")
                .set("auto.offset.reset", &config.auto_offset_reset)
                .set("session.timeout.ms", "6000")
                .set("enable.partition.eof", "false");

            // Apply custom config
            for (key, value) in &config.consumer_config {
                client_config.set(key, value);
            }

            let consumer: StreamConsumer = client_config
                .create()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            consumer
                .subscribe(&[&config.topic])
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            info!("Kafka consumer created and subscribed to topic '{}'", config.topic);

            Ok(Self {
                config,
                consumer: Some(Arc::new(consumer)),
                schema,
                running: Arc::new(RwLock::new(false)),
            })
        }

        #[cfg(not(feature = "kafka"))]
        {
            warn!("Kafka feature not enabled - consumer will not work");
            Ok(Self {
                config,
                schema,
                running: Arc::new(RwLock::new(false)),
            })
        }
    }

    /// Start consuming messages
    #[cfg(feature = "kafka")]
    pub fn start<F>(&self, callback: F) -> JoinHandle<()>
    where
        F: Fn(RecordBatch) + Send + 'static,
    {
        let consumer = self.consumer.as_ref().unwrap().clone();
        let running = self.running.clone();
        let schema = self.schema.clone();

        // Set running flag
        *running.write().unwrap() = true;

        tokio::spawn(async move {
            info!("Kafka consumer started");

            while *running.read().unwrap() {
                match consumer.recv().await {
                    Ok(message) => {
                        debug!("Received Kafka message: offset={}", message.offset());

                        // Parse message and convert to RecordBatch
                        if let Some(payload) = message.payload() {
                            match Self::parse_message(payload, &schema) {
                                Ok(batch) => {
                                    callback(batch);
                                }
                                Err(e) => {
                                    error!("Failed to parse message: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Kafka consumer error: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            info!("Kafka consumer stopped");
        })
    }

    #[cfg(not(feature = "kafka"))]
    pub fn start<F>(&self, _callback: F) -> JoinHandle<()>
    where
        F: Fn(RecordBatch) + Send + 'static,
    {
        tokio::spawn(async {
            warn!("Kafka feature not enabled - consumer will not run");
        })
    }

    /// Stop consuming
    pub fn stop(&self) {
        *self.running.write().unwrap() = false;
    }

    /// Parse message payload into RecordBatch
    #[cfg(feature = "kafka")]
    fn parse_message(payload: &[u8], schema: &SchemaRef) -> Result<RecordBatch> {
        // Try JSON parsing
        let json_value: serde_json::Value = serde_json::from_slice(payload)
            .map_err(|e| DataFusionError::Execution(format!("JSON parse error: {}", e)))?;

        // Convert JSON to Arrow arrays
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for field in schema.fields() {
            let value = &json_value[field.name()];

            let array: ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    let val = value.as_i64().unwrap_or(0);
                    Arc::new(arrow::array::Int64Array::from(vec![val]))
                }
                DataType::Float64 => {
                    let val = value.as_f64().unwrap_or(0.0);
                    Arc::new(arrow::array::Float64Array::from(vec![val]))
                }
                DataType::Utf8 => {
                    let val = value.as_str().unwrap_or("").to_string();
                    Arc::new(arrow::array::StringArray::from(vec![val]))
                }
                DataType::Boolean => {
                    let val = value.as_bool().unwrap_or(false);
                    Arc::new(arrow::array::BooleanArray::from(vec![val]))
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type: {:?}",
                        field.data_type()
                    )))
                }
            };

            arrays.push(array);
        }

        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| DataFusionError::Execution(format!("RecordBatch creation failed: {}", e)))
    }
}

/// Kafka sink for Change Data Capture (CDC)
pub struct KafkaSink {
    config: KafkaSinkConfig,
    #[cfg(feature = "kafka")]
    producer: Option<Arc<FutureProducer>>,
    schema: SchemaRef,
}

impl KafkaSink {
    /// Create new Kafka sink
    pub fn new(config: KafkaSinkConfig, schema: SchemaRef) -> Result<Self> {
        #[cfg(feature = "kafka")]
        {
            info!(
                "Creating Kafka sink: brokers={}, topic={}, compression={}",
                config.brokers, config.topic, config.compression
            );

            let mut client_config = ClientConfig::new();
            client_config
                .set("bootstrap.servers", &config.brokers)
                .set("compression.type", &config.compression)
                .set("batch.size", config.batch_size.to_string())
                .set("linger.ms", config.linger_ms.to_string())
                .set("acks", "all")
                .set("retries", "3");

            // Apply custom config
            for (key, value) in &config.producer_config {
                client_config.set(key, value);
            }

            let producer: FutureProducer = client_config
                .create()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            info!("Kafka producer created for topic '{}'", config.topic);

            Ok(Self {
                config,
                producer: Some(Arc::new(producer)),
                schema,
            })
        }

        #[cfg(not(feature = "kafka"))]
        {
            warn!("Kafka feature not enabled - producer will not work");
            Ok(Self {
                config,
                schema,
            })
        }
    }

    /// Send a record batch to Kafka
    #[cfg(feature = "kafka")]
    pub async fn send(&self, batch: &RecordBatch) -> Result<()> {
        let producer = self.producer.as_ref().unwrap();

        info!("Sending {} rows to Kafka", batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            // Convert row to JSON
            let mut row_json = serde_json::Map::new();

            for (col_idx, field) in self.schema.fields().iter().enumerate() {
                let array = batch.column(col_idx);
                let value = Self::extract_value(array, row_idx, field.data_type())?;
                row_json.insert(field.name().clone(), value);
            }

            let json_payload =
                serde_json::to_string(&row_json).map_err(|e| DataFusionError::Execution(e.to_string()))?;

            // Create Kafka record
            let record = FutureRecord::to(&self.config.topic)
                .payload(&json_payload)
                .key(&row_idx.to_string());

            // Send to Kafka
            match producer.send(record, Timeout::After(Duration::from_secs(5))).await {
                Ok((partition, offset)) => {
                    debug!("Sent message to partition {} at offset {}", partition, offset);
                }
                Err((e, _)) => {
                    error!("Failed to send message: {:?}", e);
                    return Err(DataFusionError::Execution(format!("Kafka send error: {:?}", e)));
                }
            }
        }

        info!("Successfully sent {} rows to Kafka", batch.num_rows());
        Ok(())
    }

    #[cfg(not(feature = "kafka"))]
    pub async fn send(&self, batch: &RecordBatch) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "Kafka feature not enabled".to_string(),
        ))
    }

    /// Extract value from Arrow array
    #[cfg(feature = "kafka")]
    fn extract_value(array: &ArrayRef, row_idx: usize, data_type: &DataType) -> Result<serde_json::Value> {
        let value = match data_type {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                serde_json::Value::Number(arr.value(row_idx).into())
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                serde_json::Number::from_f64(arr.value(row_idx))
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                serde_json::Value::String(arr.value(row_idx).to_string())
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
                serde_json::Value::Bool(arr.value(row_idx))
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type: {:?}",
                    data_type
                )))
            }
        };

        Ok(value)
    }
}

/// Kafka topic manager for admin operations
pub struct KafkaAdmin {
    #[cfg(feature = "kafka")]
    admin_client: Option<AdminClient<DefaultClientContext>>,
}

impl KafkaAdmin {
    /// Create new Kafka admin client
    pub fn new(brokers: &str) -> Result<Self> {
        #[cfg(feature = "kafka")]
        {
            let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .create()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(Self {
                admin_client: Some(admin_client),
            })
        }

        #[cfg(not(feature = "kafka"))]
        {
            Ok(Self {})
        }
    }

    /// Create topic
    #[cfg(feature = "kafka")]
    pub async fn create_topic(&self, name: &str, partitions: i32, replication: i32) -> Result<()> {
        let admin = self.admin_client.as_ref().unwrap();

        let new_topic = NewTopic::new(name, partitions, TopicReplication::Fixed(replication));

        admin
            .create_topics(&[new_topic], &AdminOptions::new())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        info!("Created Kafka topic: {}", name);
        Ok(())
    }

    #[cfg(not(feature = "kafka"))]
    pub async fn create_topic(&self, name: &str, partitions: i32, replication: i32) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "Kafka feature not enabled".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_source_config_default() {
        let config = KafkaSourceConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "miracledb-input");
        assert_eq!(config.group_id, "miracledb-consumer");
    }

    #[test]
    fn test_kafka_sink_config_default() {
        let config = KafkaSinkConfig::default();
        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "miracledb-output");
        assert_eq!(config.compression, "snappy");
    }
}
