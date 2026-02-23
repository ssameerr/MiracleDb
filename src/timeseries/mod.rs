use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::arrow::array::{Array, ArrayRef, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Result;
use std::sync::Arc;
use chrono::{Duration, TimeZone, Utc};

// Function: time_bucket('1 hour', timestamp_column) -> timestamp
// MVP: time_bucket(interval_ns, timestamp_ns)

pub fn register_timeseries_functions(ctx: &SessionContext) {
    let time_bucket = create_udf(
        "time_bucket",
        vec![DataType::Int64, DataType::Timestamp(TimeUnit::Nanosecond, None)],
        Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Volatility::Immutable,
        make_scalar_function(time_bucket_impl),
    );
    
    ctx.register_udf(time_bucket);
}

fn time_bucket_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    // args: [interval_ns, timestamp]
    if args.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution("time_bucket requires 2 arguments".to_string()));
    }
    
    // Simplification: Assume constant interval for now or handle array
    // ... implementation ...
    // For now, return stub or simple truncation
    
    let intervals = datafusion::common::cast::as_int64_array(&args[0])?;
    let timestamps = datafusion::common::cast::as_timestamp_nanosecond_array(&args[1])?;
    
    let len = timestamps.len();
    let mut result = Vec::with_capacity(len);
    
    for i in 0..len {
        if timestamps.is_null(i) || intervals.is_null(i) {
            result.push(None);
            continue;
        }
        
        let ts = timestamps.value(i);
        let interval = intervals.value(i);
        
        if interval == 0 {
            result.push(Some(ts));
            continue;
        }
        
        let bucket = (ts / interval) * interval;
        result.push(Some(bucket));
    }
    
    Ok(Arc::new(TimestampNanosecondArray::from(result)))
}

// Partitioning Logic
pub struct TimePartitioner {
    pub interval: Duration,
}

impl TimePartitioner {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    pub fn get_partition(&self, check_time: chrono::DateTime<Utc>) -> String {
        // Example: partition_2023_01_01
        let ts = check_time.timestamp();
        // ... logic
        format!("partition_{}", ts)
    }
}

// ===== Time Series Analysis Functions =====

/// Aggregation function for downsampling
#[derive(Clone, Copy, Debug)]
pub enum AggFn {
    Mean,
    Sum,
    Min,
    Max,
    Last,
    First,
    Count,
}

/// Gap-fill interpolation method
#[derive(Clone, Copy, Debug)]
pub enum GapFillMethod {
    /// Last Observation Carried Forward
    Forward,
    /// Linear interpolation between neighbors
    Linear,
    /// Fill with zero
    Zero,
}

/// Downsample time series data into fixed-width buckets.
/// `data`: (timestamp_ms, value) pairs sorted ascending.
/// `bucket_ms`: bucket width in milliseconds.
pub fn downsample(data: &[(i64, f64)], bucket_ms: i64, agg: AggFn) -> Vec<(i64, f64)> {
    if data.is_empty() || bucket_ms <= 0 { return vec![]; }
    let start = (data[0].0 / bucket_ms) * bucket_ms;

    let mut buckets: std::collections::BTreeMap<i64, Vec<f64>> = std::collections::BTreeMap::new();
    for (t, v) in data {
        let bucket = ((*t - start) / bucket_ms) * bucket_ms + start;
        buckets.entry(bucket).or_default().push(*v);
    }

    buckets.into_iter().map(|(ts, vals)| {
        let agg_val = match agg {
            AggFn::Mean  => vals.iter().sum::<f64>() / vals.len() as f64,
            AggFn::Sum   => vals.iter().sum(),
            AggFn::Min   => vals.iter().cloned().fold(f64::INFINITY, f64::min),
            AggFn::Max   => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            AggFn::Last  => *vals.last().unwrap(),
            AggFn::First => *vals.first().unwrap(),
            AggFn::Count => vals.len() as f64,
        };
        (ts, agg_val)
    }).collect()
}

/// Fill gaps in time series data at regular `step_ms` intervals.
pub fn gap_fill(data: &[(i64, f64)], step_ms: i64, method: GapFillMethod) -> Vec<(i64, f64)> {
    if data.is_empty() || step_ms <= 0 { return vec![]; }
    let start = data[0].0;
    let end = data.last().unwrap().0;
    let mut result: Vec<(i64, f64)> = Vec::new();
    let mut data_idx = 0;

    let mut t = start;
    while t <= end {
        if data_idx < data.len() && data[data_idx].0 == t {
            result.push(data[data_idx]);
            data_idx += 1;
        } else {
            let fill_val = match method {
                GapFillMethod::Forward => result.last().map(|&(_, v)| v).unwrap_or(0.0),
                GapFillMethod::Zero => 0.0,
                GapFillMethod::Linear => {
                    let prev = result.last().cloned();
                    let next = data[data_idx..].iter().find(|(nt, _)| *nt > t);
                    match (prev, next) {
                        (Some((pt, pv)), Some((nt, nv))) => {
                            let ratio = (t - pt) as f64 / (*nt - pt) as f64;
                            pv + ratio * (nv - pv)
                        }
                        (Some((_, pv)), None) => pv,
                        _ => 0.0,
                    }
                }
            };
            result.push((t, fill_val));
        }
        t += step_ms;
    }
    result
}

/// Compute N-period simple moving average. Short windows return partial averages.
pub fn moving_average(data: &[f64], n: usize) -> Vec<f64> {
    if n == 0 { return data.to_vec(); }
    data.iter().enumerate().map(|(i, _)| {
        let start = if i + 1 >= n { i + 1 - n } else { 0 };
        let window = &data[start..=i];
        window.iter().sum::<f64>() / window.len() as f64
    }).collect()
}

/// Lag: shift values forward by `n` positions. First `n` values are `None`.
pub fn lag(data: &[f64], n: usize) -> Vec<Option<f64>> {
    data.iter().enumerate()
        .map(|(i, _)| if i >= n { Some(data[i - n]) } else { None })
        .collect()
}

/// Lead: shift values backward by `n` positions. Last `n` values are `None`.
pub fn lead(data: &[f64], n: usize) -> Vec<Option<f64>> {
    data.iter().enumerate()
        .map(|(i, _)| if i + n < data.len() { Some(data[i + n]) } else { None })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_downsample_mean() {
        let readings = vec![
            (0i64,   1.0f64),
            (10_000, 2.0),
            (20_000, 3.0),
            (30_000, 4.0),
            (40_000, 5.0),
            (50_000, 6.0),
        ];
        let result = downsample(&readings, 30_000, AggFn::Mean);
        assert_eq!(result.len(), 2);
        assert!((result[0].1 - 2.0).abs() < 0.001, "bucket 0 mean should be 2.0, got {}", result[0].1);
        assert!((result[1].1 - 5.0).abs() < 0.001, "bucket 1 mean should be 5.0, got {}", result[1].1);
    }

    #[test]
    fn test_downsample_last() {
        let readings = vec![
            (0i64,  1.0f64),
            (10_000, 2.0),
            (20_000, 3.0),
        ];
        let result = downsample(&readings, 30_000, AggFn::Last);
        assert_eq!(result.len(), 1);
        assert!((result[0].1 - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_gap_fill_forward() {
        let data = vec![(0i64, 1.0f64), (10_000, 2.0), (30_000, 3.0)];
        let filled = gap_fill(&data, 10_000, GapFillMethod::Forward);
        assert!(filled.len() >= 3, "should have at least 4 entries (0, 10000, 20000, 30000)");
        let t20 = filled.iter().find(|(t, _)| *t == 20_000);
        assert!(t20.is_some(), "gap at 20000 should be filled");
        assert!((t20.unwrap().1 - 2.0).abs() < 0.001, "LOCF at 20000 should be 2.0");
    }

    #[test]
    fn test_gap_fill_linear() {
        let data = vec![(0i64, 0.0f64), (20_000, 20.0f64)];
        let filled = gap_fill(&data, 10_000, GapFillMethod::Linear);
        let t10 = filled.iter().find(|(t, _)| *t == 10_000);
        assert!(t10.is_some(), "midpoint should be filled");
        assert!((t10.unwrap().1 - 10.0).abs() < 0.001, "linear interp at 10000 should be 10.0, got {}", t10.unwrap().1);
    }

    #[test]
    fn test_moving_average() {
        let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = moving_average(&data, 3);
        assert_eq!(result.len(), 5);
        assert!((result[2] - 2.0).abs() < 0.001, "avg(1,2,3)=2.0, got {}", result[2]);
        assert!((result[3] - 3.0).abs() < 0.001, "avg(2,3,4)=3.0, got {}", result[3]);
        assert!((result[4] - 4.0).abs() < 0.001, "avg(3,4,5)=4.0, got {}", result[4]);
    }

    #[test]
    fn test_lag() {
        let data = vec![1.0f64, 2.0, 3.0, 4.0];
        let lagged = lag(&data, 1);
        assert!(lagged[0].is_none());
        assert_eq!(lagged[1], Some(1.0));
        assert_eq!(lagged[3], Some(3.0));
    }

    #[test]
    fn test_lead() {
        let data = vec![1.0f64, 2.0, 3.0, 4.0];
        let led = lead(&data, 1);
        assert_eq!(led[0], Some(2.0));
        assert_eq!(led[2], Some(4.0));
        assert!(led[3].is_none());
    }
}
