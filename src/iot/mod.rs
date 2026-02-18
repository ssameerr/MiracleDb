use tokio::sync::mpsc;

pub struct MqttBridge {
    broker_url: String,
}

impl MqttBridge {
    pub fn new(url: &str) -> Self {
        Self { broker_url: url.to_string() }
    }

    pub async fn start(&self, _topic: &str, _tx: mpsc::Sender<serde_json::Value>) {
        // Connect to rumqttd/broker and forward messages to tx
        println!("Starting MQTT bridge to {}", self.broker_url);
    }
}

pub struct Downsampler;

impl Downsampler {
    pub fn downsample(data: &[f64], factor: usize) -> Vec<f64> {
        data.chunks(factor).map(|chunk| {
             let sum: f64 = chunk.iter().sum();
             sum / chunk.len() as f64
        }).collect()
    }
}

pub mod telemetry;
pub use telemetry::{SensorReading, SensorQuality, TelemetryWindow, bucket_aggregate};
