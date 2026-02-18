//! IoT telemetry processing: time-series data, anomaly detection, aggregation

use serde::{Serialize, Deserialize};
use std::collections::VecDeque;

/// IoT sensor reading
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SensorReading {
    pub device_id: String,
    pub sensor_id: String,
    pub timestamp: i64,  // Unix timestamp ms
    pub value: f64,
    pub unit: String,
    pub quality: SensorQuality,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SensorQuality {
    Good,
    Uncertain,
    Bad,
}

/// Telemetry window for rolling statistics
pub struct TelemetryWindow {
    window: VecDeque<f64>,
    capacity: usize,
}

impl TelemetryWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            window: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, value: f64) {
        if self.window.len() >= self.capacity {
            self.window.pop_front();
        }
        self.window.push_back(value);
    }

    pub fn mean(&self) -> f64 {
        if self.window.is_empty() { return 0.0; }
        self.window.iter().sum::<f64>() / self.window.len() as f64
    }

    pub fn std_dev(&self) -> f64 {
        if self.window.len() < 2 { return 0.0; }
        let mean = self.mean();
        let variance = self.window.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (self.window.len() - 1) as f64;
        variance.sqrt()
    }

    pub fn min(&self) -> f64 {
        self.window.iter().cloned().fold(f64::INFINITY, f64::min)
    }

    pub fn max(&self) -> f64 {
        self.window.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    }

    /// Z-score anomaly detection
    pub fn is_anomaly(&self, value: f64, threshold_sigma: f64) -> bool {
        let std = self.std_dev();
        if std == 0.0 { return false; }
        let z = (value - self.mean()).abs() / std;
        z > threshold_sigma
    }
}

/// Aggregate IoT readings by time bucket
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryAggregate {
    pub device_id: String,
    pub bucket_start: i64,
    pub bucket_end: i64,
    pub count: usize,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub std_dev: f64,
}

/// Aggregate a sorted list of readings into fixed-size time buckets
pub fn bucket_aggregate(readings: &[SensorReading], bucket_ms: i64) -> Vec<TelemetryAggregate> {
    if readings.is_empty() { return Vec::new(); }

    let mut buckets: Vec<TelemetryAggregate> = Vec::new();

    // Group by device first
    let mut by_device: std::collections::HashMap<String, Vec<&SensorReading>> = std::collections::HashMap::new();
    for r in readings {
        by_device.entry(r.device_id.clone()).or_default().push(r);
    }

    for (device_id, device_readings) in &by_device {
        if device_readings.is_empty() { continue; }

        let first_ts = device_readings[0].timestamp;
        let bucket_start = (first_ts / bucket_ms) * bucket_ms;

        let mut current_bucket_start = bucket_start;
        let mut current_values: Vec<f64> = Vec::new();

        for reading in device_readings {
            let b = (reading.timestamp / bucket_ms) * bucket_ms;
            if b != current_bucket_start && !current_values.is_empty() {
                buckets.push(compute_aggregate(device_id, current_bucket_start, current_bucket_start + bucket_ms, &current_values));
                current_values.clear();
                current_bucket_start = b;
            }
            if reading.quality != SensorQuality::Bad {
                current_values.push(reading.value);
            }
        }

        if !current_values.is_empty() {
            buckets.push(compute_aggregate(device_id, current_bucket_start, current_bucket_start + bucket_ms, &current_values));
        }
    }

    buckets
}

fn compute_aggregate(device_id: &str, start: i64, end: i64, values: &[f64]) -> TelemetryAggregate {
    let n = values.len();
    let mean = values.iter().sum::<f64>() / n as f64;
    let variance = if n > 1 {
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1) as f64
    } else { 0.0 };

    TelemetryAggregate {
        device_id: device_id.to_string(),
        bucket_start: start,
        bucket_end: end,
        count: n,
        mean,
        min: values.iter().cloned().fold(f64::INFINITY, f64::min),
        max: values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        std_dev: variance.sqrt(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_window() {
        let mut w = TelemetryWindow::new(10);
        for v in &[1.0, 2.0, 3.0, 4.0, 5.0] {
            w.push(*v);
        }
        assert!((w.mean() - 3.0).abs() < 0.001);
        assert_eq!(w.min(), 1.0);
        assert_eq!(w.max(), 5.0);
    }

    #[test]
    fn test_anomaly_detection() {
        let mut w = TelemetryWindow::new(100);
        for _ in 0..50 { w.push(10.0); }
        for _ in 0..50 { w.push(11.0); }
        // Normal value
        assert!(!w.is_anomaly(10.5, 3.0));
        // Extreme outlier
        assert!(w.is_anomaly(100.0, 3.0));
    }
}
