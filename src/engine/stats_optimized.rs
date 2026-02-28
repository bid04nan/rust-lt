/// Optimized statistics collection with incremental aggregation
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

/// Incremental stats accumulator - avoids storing raw metrics
#[derive(Debug, Clone)]
pub struct IncrementalStats {
    /// Total requests counter
    pub total_requests: usize,
    /// Successful requests counter
    pub successful_requests: usize,
    /// Failed requests counter
    pub failed_requests: usize,
    /// Sum of response times (for mean calculation)
    pub response_time_sum: f64,
    /// Sum of squares (for stddev calculation)
    pub response_time_sq_sum: f64,
    /// Min response time
    pub response_time_min: f64,
    /// Max response time
    pub response_time_max: f64,
    /// Total bytes sent
    pub bytes_sent_total: f64,
    /// Total bytes received
    pub bytes_received_total: f64,
    /// Percentile bucket for response times (sorted values for p50/p90/p95/p99)
    pub response_time_samples: Vec<f64>,
    /// Time-series buckets: timestamp_sec -> (count, error_count, sum_response_time)