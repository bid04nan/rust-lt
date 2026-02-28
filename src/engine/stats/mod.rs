/// Statistics collection and aggregation for load testing
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

/// Optimized metrics collector using incremental aggregation
/// Avoids storing millions of individual metric objects by maintaining running aggregates
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    /// Incremental stats accumulator (avoids storing raw metrics)
    stats: Arc<RwLock<IncrementalStats>>,
    /// Start time of collection
    start_time: Instant,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            stats: Arc::new(RwLock::new(IncrementalStats::new())),
            start_time: Instant::now(),
        }
    }

    /// Record a metric (optimized: updates running aggregates only)
    pub async fn record(&self, metric: Metric) {
        let mut stats = self.stats.write().await;
        stats.record_metric(&metric);
    }

    /// Get current aggregated statistics (no raw metric storage needed)
    pub async fn get_stats(&self) -> IncrementalStats {
        self.stats.read().await.clone()
    }

    /// Get elapsed time since collection started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Clear all metrics
    pub async fn clear(&self) {
        let mut stats = self.stats.write().await;
        *stats = IncrementalStats::new();
    }

    /// Generate statistics from collected metrics
    pub async fn generate_stats(&self) -> MetricStats {
        let incremental_stats = self.stats.read().await.clone();
        MetricStats::from_incremental(incremental_stats, self.elapsed())
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// A single metric data point
#[derive(Debug, Clone)]
pub struct Metric {
    /// Timestamp when metric was recorded
    pub timestamp: Instant,
    /// Type of metric
    pub metric_type: MetricType,
    /// Name/label for the metric
    pub name: String,
    /// Value of the metric
    pub value: f64,
    /// Tags for filtering/grouping
    pub tags: HashMap<String, String>,
}

impl Metric {
    /// Create a new metric
    pub fn new(metric_type: MetricType, name: String, value: f64) -> Self {
        Self {
            timestamp: Instant::now(),
            metric_type,
            name,
            value,
            tags: HashMap::new(),
        }
    }

    /// Add a tag to the metric
    pub fn with_tag(mut self, key: String, value: String) -> Self {
        self.tags.insert(key, value);
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags.extend(tags);
        self
    }
}

/// Types of metrics
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Response time in milliseconds
    ResponseTime,
    /// Throughput (requests per second)
    Throughput,
    /// Error rate
    ErrorRate,
    /// Bytes sent
    BytesSent,
    /// Bytes received
    BytesReceived,
    /// Active VUs count
    ActiveVUs,
    /// Success count
    Success,
    /// Failure count
    Failure,
    
    // Low-level connection timings
    /// DNS lookup time in milliseconds
    DnsLookup,
    /// TCP connection time in milliseconds
    TcpConnect,
    /// TLS handshake time in milliseconds
    TlsHandshake,
    /// Time to first byte in milliseconds
    TimeToFirstByte,
    /// Content download time in milliseconds
    ContentDownload,
    /// Total connection time (DNS + TCP + TLS)
    ConnectionTime,
    
    // VU lifecycle events
    /// VU started
    VuStarted,
    /// VU completed successfully
    VuCompleted,
    /// VU failed
    VuFailed,
    /// VU is currently active
    VuActive,
    /// VU is idle/waiting
    VuIdle,
    
    /// Custom metric
    Custom(String),
}

/// Incremental statistics accumulator - avoids storing raw metrics
/// Maintains running aggregates for efficient memory usage and real-time stats
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
    /// Histogram samples for percentile calculations (T-Digest approximation)
    pub response_time_samples: Vec<f64>,
    
    // Connection timing aggregates
    pub dns_lookup_sum: f64,
    pub dns_lookup_count: usize,
    pub dns_lookup_min: f64,
    pub dns_lookup_max: f64,
    
    pub tcp_connect_sum: f64,
    pub tcp_connect_count: usize,
    pub tcp_connect_min: f64,
    pub tcp_connect_max: f64,
    
    pub tls_handshake_sum: f64,
    pub tls_handshake_count: usize,
    pub tls_handshake_min: f64,
    pub tls_handshake_max: f64,
    
    pub ttfb_sum: f64,
    pub ttfb_count: usize,
    pub ttfb_min: f64,
    pub ttfb_max: f64,
    
    pub content_download_sum: f64,
    pub content_download_count: usize,
    pub content_download_min: f64,
    pub content_download_max: f64,
    
    pub connection_time_sum: f64,
    pub connection_time_count: usize,
    pub connection_time_min: f64,
    pub connection_time_max: f64,
    
    // VU lifecycle counters
    pub vu_started: usize,
    pub vu_completed: usize,
    pub vu_failed: usize,
    
    // Time-series data for throughput graphs
    pub time_buckets: HashMap<u64, TimeBucketStats>,
}

impl IncrementalStats {
    pub fn new() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            response_time_sum: 0.0,
            response_time_sq_sum: 0.0,
            response_time_min: f64::INFINITY,
            response_time_max: 0.0,
            bytes_sent_total: 0.0,
            bytes_received_total: 0.0,
            response_time_samples: Vec::with_capacity(10000), // Pre-allocate for efficiency
            
            dns_lookup_sum: 0.0,
            dns_lookup_count: 0,
            dns_lookup_min: f64::INFINITY,
            dns_lookup_max: 0.0,
            
            tcp_connect_sum: 0.0,
            tcp_connect_count: 0,
            tcp_connect_min: f64::INFINITY,
            tcp_connect_max: 0.0,
            
            tls_handshake_sum: 0.0,
            tls_handshake_count: 0,
            tls_handshake_min: f64::INFINITY,
            tls_handshake_max: 0.0,
            
            ttfb_sum: 0.0,
            ttfb_count: 0,
            ttfb_min: f64::INFINITY,
            ttfb_max: 0.0,
            
            content_download_sum: 0.0,
            content_download_count: 0,
            content_download_min: f64::INFINITY,
            content_download_max: 0.0,
            
            connection_time_sum: 0.0,
            connection_time_count: 0,
            connection_time_min: f64::INFINITY,
            connection_time_max: 0.0,
            
            vu_started: 0,
            vu_completed: 0,
            vu_failed: 0,
            
            time_buckets: HashMap::new(),
        }
    }
    
    /// Record a single metric point (incremental update)
    pub fn record_metric(&mut self, metric: &Metric) {
        match &metric.metric_type {
            MetricType::ResponseTime => {
                self.total_requests += 1;
                self.response_time_sum += metric.value;
                self.response_time_sq_sum += metric.value * metric.value;
                self.response_time_min = self.response_time_min.min(metric.value);
                self.response_time_max = self.response_time_max.max(metric.value);
                
                // Store sample for percentile calculation (downsampled)
                if self.response_time_samples.len() < 50000 {
                    self.response_time_samples.push(metric.value);
                }
                
                // Add to time bucket
                let bucket_key = self.get_time_bucket(metric.timestamp);
                self.time_buckets.entry(bucket_key)
                    .or_insert_with(TimeBucketStats::new)
                    .add_response_time(metric.value);
            }
            MetricType::Success => {
                self.successful_requests += 1;
                self.total_requests += 1;
                
                let bucket_key = self.get_time_bucket(metric.timestamp);
                self.time_buckets.entry(bucket_key)
                    .or_insert_with(TimeBucketStats::new)
                    .success_count += 1;
            }
            MetricType::Failure => {
                self.failed_requests += 1;
                self.total_requests += 1;
                
                let bucket_key = self.get_time_bucket(metric.timestamp);
                self.time_buckets.entry(bucket_key)
                    .or_insert_with(TimeBucketStats::new)
                    .failure_count += 1;
            }
            MetricType::BytesSent => {
                self.bytes_sent_total += metric.value;
            }
            MetricType::BytesReceived => {
                self.bytes_received_total += metric.value;
            }
            MetricType::DnsLookup => {
                self.dns_lookup_sum += metric.value;
                self.dns_lookup_count += 1;
                self.dns_lookup_min = self.dns_lookup_min.min(metric.value);
                self.dns_lookup_max = self.dns_lookup_max.max(metric.value);
            }
            MetricType::TcpConnect => {
                self.tcp_connect_sum += metric.value;
                self.tcp_connect_count += 1;
                self.tcp_connect_min = self.tcp_connect_min.min(metric.value);
                self.tcp_connect_max = self.tcp_connect_max.max(metric.value);
            }
            MetricType::TlsHandshake => {
                self.tls_handshake_sum += metric.value;
                self.tls_handshake_count += 1;
                self.tls_handshake_min = self.tls_handshake_min.min(metric.value);
                self.tls_handshake_max = self.tls_handshake_max.max(metric.value);
            }
            MetricType::TimeToFirstByte => {
                self.ttfb_sum += metric.value;
                self.ttfb_count += 1;
                self.ttfb_min = self.ttfb_min.min(metric.value);
                self.ttfb_max = self.ttfb_max.max(metric.value);
            }
            MetricType::ContentDownload => {
                self.content_download_sum += metric.value;
                self.content_download_count += 1;
                self.content_download_min = self.content_download_min.min(metric.value);
                self.content_download_max = self.content_download_max.max(metric.value);
            }
            MetricType::ConnectionTime => {
                self.connection_time_sum += metric.value;
                self.connection_time_count += 1;
                self.connection_time_min = self.connection_time_min.min(metric.value);
                self.connection_time_max = self.connection_time_max.max(metric.value);
            }
            MetricType::VuStarted => {
                self.vu_started += 1;
            }
            MetricType::VuCompleted => {
                self.vu_completed += 1;
            }
            MetricType::VuFailed => {
                self.vu_failed += 1;
            }
            _ => {}
        }
    }
    
    fn get_time_bucket(&self, timestamp: Instant) -> u64 {
        // 1-second buckets
        timestamp.elapsed().as_secs()
    }
}

impl Default for IncrementalStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-second time bucket for tracking throughput over time
#[derive(Debug, Clone)]
pub struct TimeBucketStats {
    pub success_count: usize,
    pub failure_count: usize,
    pub response_time_sum: f64,
    pub response_time_count: usize,
}

impl TimeBucketStats {
    pub fn new() -> Self {
        Self {
            success_count: 0,
            failure_count: 0,
            response_time_sum: 0.0,
            response_time_count: 0,
        }
    }
    
    pub fn add_response_time(&mut self, value: f64) {
        self.response_time_sum += value;
        self.response_time_count += 1;
    }
    
    pub fn avg_response_time(&self) -> f64 {
        if self.response_time_count == 0 {
            0.0
        } else {
            self.response_time_sum / self.response_time_count as f64
        }
    }
    
    pub fn total_requests(&self) -> usize {
        self.success_count + self.failure_count
    }
}

/// Aggregated statistics from metrics
#[derive(Debug, Clone)]
pub struct MetricStats {
    /// Total duration of the test
    pub duration: Duration,
    /// Total number of requests
    pub total_requests: usize,
    /// Successful requests
    pub successful_requests: usize,
    /// Failed requests
    pub failed_requests: usize,
    /// Response time statistics
    pub response_times: ResponseTimeStats,
    /// Throughput statistics
    pub throughput: ThroughputStats,
    /// Data transfer statistics
    pub data_transfer: DataTransferStats,
    /// Connection timing statistics
    pub connection_timings: ConnectionTimingStats,
    /// VU lifecycle statistics
    pub vu_stats: VuLifecycleStats,
    /// Custom metrics by name
    pub custom_metrics: HashMap<String, Vec<f64>>,
}

impl MetricStats {
    /// Generate statistics from incremental stats (optimized path - no iteration needed)
    pub fn from_incremental(stats: IncrementalStats, duration: Duration) -> Self {
        let duration_secs = duration.as_secs_f64();
        
        // Calculate response time stats from incremental aggregates
        let response_times = ResponseTimeStats::from_incremental(
            stats.response_time_sum,
            stats.response_time_sq_sum,
            stats.response_time_min,
            stats.response_time_max,
            stats.total_requests,
            &stats.response_time_samples,
        );
        
        // Build connection timing stats
        let connection_timings = ConnectionTimingStats {
            dns_lookup: ResponseTimeStats::from_aggregates(
                stats.dns_lookup_sum,
                stats.dns_lookup_count,
                stats.dns_lookup_min,
                stats.dns_lookup_max,
            ),
            tcp_connect: ResponseTimeStats::from_aggregates(
                stats.tcp_connect_sum,
                stats.tcp_connect_count,
                stats.tcp_connect_min,
                stats.tcp_connect_max,
            ),
            tls_handshake: ResponseTimeStats::from_aggregates(
                stats.tls_handshake_sum,
                stats.tls_handshake_count,
                stats.tls_handshake_min,
                stats.tls_handshake_max,
            ),
            time_to_first_byte: ResponseTimeStats::from_aggregates(
                stats.ttfb_sum,
                stats.ttfb_count,
                stats.ttfb_min,
                stats.ttfb_max,
            ),
            content_download: ResponseTimeStats::from_aggregates(
                stats.content_download_sum,
                stats.content_download_count,
                stats.content_download_min,
                stats.content_download_max,
            ),
            total_connection: ResponseTimeStats::from_aggregates(
                stats.connection_time_sum,
                stats.connection_time_count,
                stats.connection_time_min,
                stats.connection_time_max,
            ),
        };
        
        // Build VU lifecycle stats
        let vu_stats = VuLifecycleStats {
            total_started: stats.vu_started,
            total_completed: stats.vu_completed,
            total_failed: stats.vu_failed,
            currently_active: stats.vu_started - stats.vu_completed - stats.vu_failed,
            events: Vec::new(), // Not needed for final stats
            state_snapshots: Vec::new(), // Not needed for final stats
        };
        
        Self {
            duration,
            total_requests: stats.total_requests,
            successful_requests: stats.successful_requests,
            failed_requests: stats.failed_requests,
            response_times,
            throughput: ThroughputStats::calculate(stats.total_requests, duration_secs),
            data_transfer: DataTransferStats::new(stats.bytes_sent_total, stats.bytes_received_total, duration_secs),
            connection_timings,
            vu_stats,
            custom_metrics: HashMap::new(), // Not tracked in incremental stats
        }
    }

    /// Generate statistics from a collection of metrics (kept for compatibility)
    pub fn from_metrics(metrics: &[Metric], duration: Duration) -> Self {
        let mut response_times = Vec::new();
        let mut successful_requests = 0;
        let mut failed_requests = 0;
        let mut bytes_sent = 0.0;
        let mut bytes_received = 0.0;
        let mut custom_metrics: HashMap<String, Vec<f64>> = HashMap::new();
        
        // Connection timing collections
        let mut dns_lookups = Vec::new();
        let mut tcp_connects = Vec::new();
        let mut tls_handshakes = Vec::new();
        let mut ttfb = Vec::new();
        let mut content_downloads = Vec::new();
        let mut connection_times = Vec::new();
        
        // VU lifecycle tracking
        let mut vu_events: Vec<VuEvent> = Vec::new();

        for metric in metrics {
            match &metric.metric_type {
                MetricType::ResponseTime => {
                    response_times.push(metric.value);
                }
                MetricType::Success => {
                    successful_requests += metric.value as usize;
                }
                MetricType::Failure => {
                    failed_requests += metric.value as usize;
                }
                MetricType::BytesSent => {
                    bytes_sent += metric.value;
                }
                MetricType::BytesReceived => {
                    bytes_received += metric.value;
                }
                MetricType::DnsLookup => {
                    dns_lookups.push(metric.value);
                }
                MetricType::TcpConnect => {
                    tcp_connects.push(metric.value);
                }
                MetricType::TlsHandshake => {
                    tls_handshakes.push(metric.value);
                }
                MetricType::TimeToFirstByte => {
                    ttfb.push(metric.value);
                }
                MetricType::ContentDownload => {
                    content_downloads.push(metric.value);
                }
                MetricType::ConnectionTime => {
                    connection_times.push(metric.value);
                }
                MetricType::VuStarted => {
                    vu_events.push(VuEvent {
                        timestamp: metric.timestamp,
                        vu_id: metric.tags.get("vu_id").and_then(|id| id.parse().ok()).unwrap_or(0),
                        event_type: VuEventType::Started,
                    });
                }
                MetricType::VuCompleted => {
                    vu_events.push(VuEvent {
                        timestamp: metric.timestamp,
                        vu_id: metric.tags.get("vu_id").and_then(|id| id.parse().ok()).unwrap_or(0),
                        event_type: VuEventType::Completed,
                    });
                }
                MetricType::VuFailed => {
                    vu_events.push(VuEvent {
                        timestamp: metric.timestamp,
                        vu_id: metric.tags.get("vu_id").and_then(|id| id.parse().ok()).unwrap_or(0),
                        event_type: VuEventType::Failed,
                    });
                }
                MetricType::Custom(name) => {
                    custom_metrics
                        .entry(name.clone())
                        .or_insert_with(Vec::new)
                        .push(metric.value);
                }
                _ => {}
            }
        }

        let total_requests = successful_requests + failed_requests;
        let duration_secs = duration.as_secs_f64();

        Self {
            duration,
            total_requests,
            successful_requests,
            failed_requests,
            response_times: ResponseTimeStats::from_values(&response_times),
            throughput: ThroughputStats::calculate(total_requests, duration_secs),
            data_transfer: DataTransferStats::new(bytes_sent, bytes_received, duration_secs),
            connection_timings: ConnectionTimingStats::from_timings(
                dns_lookups,
                tcp_connects,
                tls_handshakes,
                ttfb,
                content_downloads,
                connection_times,
            ),
            vu_stats: VuLifecycleStats::from_events(vu_events),
            custom_metrics,
        }
    }

    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        (self.successful_requests as f64 / self.total_requests as f64) * 100.0
    }

    /// Get error rate as percentage
    pub fn error_rate(&self) -> f64 {
        100.0 - self.success_rate()
    }
}

/// Response time statistics
#[derive(Debug, Clone)]
pub struct ResponseTimeStats {
    pub count: usize,
    pub mean: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub std_dev: f64,
}

impl ResponseTimeStats {
    /// Calculate statistics from response time values
    pub fn from_values(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self::default();
        }

        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let count = sorted.len();
        let sum: f64 = sorted.iter().sum();
        let mean = sum / count as f64;

        let variance = sorted
            .iter()
            .map(|v| {
                let diff = mean - v;
                diff * diff
            })
            .sum::<f64>()
            / count as f64;

        let std_dev = variance.sqrt();

        Self {
            count,
            mean,
            median: percentile(&sorted, 50.0),
            min: sorted[0],
            max: sorted[count - 1],
            p50: percentile(&sorted, 50.0),
            p75: percentile(&sorted, 75.0),
            p90: percentile(&sorted, 90.0),
            p95: percentile(&sorted, 95.0),
            p99: percentile(&sorted, 99.0),
            std_dev,
        }
    }
    
    /// Calculate statistics from incremental aggregates (optimized path)
    pub fn from_incremental(
        sum: f64,
        sq_sum: f64,
        min: f64,
        max: f64,
        count: usize,
        samples: &[f64],
    ) -> Self {
        if count == 0 {
            return Self::default();
        }
        
        let mean = sum / count as f64;
        let variance = (sq_sum / count as f64) - (mean * mean);
        let std_dev = variance.max(0.0).sqrt(); // Handle floating point errors
        
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let actual_min = if min.is_infinite() { 0.0 } else { min };
        let actual_max = if max.is_infinite() { 0.0 } else { max };
        
        Self {
            count,
            mean,
            median: if !sorted.is_empty() { percentile(&sorted, 50.0) } else { 0.0 },
            min: actual_min,
            max: actual_max,
            p50: if !sorted.is_empty() { percentile(&sorted, 50.0) } else { 0.0 },
            p75: if !sorted.is_empty() { percentile(&sorted, 75.0) } else { 0.0 },
            p90: if !sorted.is_empty() { percentile(&sorted, 90.0) } else { 0.0 },
            p95: if !sorted.is_empty() { percentile(&sorted, 95.0) } else { 0.0 },
            p99: if !sorted.is_empty() { percentile(&sorted, 99.0) } else { 0.0 },
            std_dev,
        }
    }
    
    /// Calculate statistics from aggregates only (no samples)
    pub fn from_aggregates(
        sum: f64,
        count: usize,
        min: f64,
        max: f64,
    ) -> Self {
        if count == 0 {
            return Self::default();
        }
        
        let mean = sum / count as f64;
        let actual_min = if min.is_infinite() { 0.0 } else { min };
        let actual_max = if max.is_infinite() { 0.0 } else { max };
        
        Self {
            count,
            mean,
            median: mean, // Approximate as mean
            min: actual_min,
            max: actual_max,
            p50: mean,
            p75: mean,
            p90: mean,
            p95: mean,
            p99: mean,
            std_dev: 0.0, // Cannot calculate without variance info
        }
    }
}


impl Default for ResponseTimeStats {
    fn default() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            median: 0.0,
            min: 0.0,
            max: 0.0,
            p50: 0.0,
            p75: 0.0,
            p90: 0.0,
            p95: 0.0,
            p99: 0.0,
            std_dev: 0.0,
        }
    }
}

/// Throughput statistics
#[derive(Debug, Clone)]
pub struct ThroughputStats {
    pub requests_per_second: f64,
    pub requests_per_minute: f64,
}

impl ThroughputStats {
    pub fn calculate(total_requests: usize, duration_secs: f64) -> Self {
        let rps = if duration_secs > 0.0 {
            total_requests as f64 / duration_secs
        } else {
            0.0
        };

        Self {
            requests_per_second: rps,
            requests_per_minute: rps * 60.0,
        }
    }
}

/// Data transfer statistics
#[derive(Debug, Clone)]
pub struct DataTransferStats {
    pub total_bytes_sent: f64,
    pub total_bytes_received: f64,
    pub bytes_sent_per_second: f64,
    pub bytes_received_per_second: f64,
    pub megabytes_sent: f64,
    pub megabytes_received: f64,
}

impl DataTransferStats {
    pub fn new(bytes_sent: f64, bytes_received: f64, duration_secs: f64) -> Self {
        let bytes_sent_per_second = if duration_secs > 0.0 {
            bytes_sent / duration_secs
        } else {
            0.0
        };

        let bytes_received_per_second = if duration_secs > 0.0 {
            bytes_received / duration_secs
        } else {
            0.0
        };

        Self {
            total_bytes_sent: bytes_sent,
            total_bytes_received: bytes_received,
            bytes_sent_per_second,
            bytes_received_per_second,
            megabytes_sent: bytes_sent / (1024.0 * 1024.0),
            megabytes_received: bytes_received / (1024.0 * 1024.0),
        }
    }
}

/// Time-series metrics for tracking changes over time
#[derive(Debug, Clone)]
pub struct TimeSeriesMetrics {
    /// Window size for aggregation (seconds)
    pub window_size: u64,
    /// Data points organized by time windows
    pub windows: Vec<TimeWindow>,
}

impl TimeSeriesMetrics {
    pub fn new(window_size: u64) -> Self {
        Self {
            window_size,
            windows: Vec::new(),
        }
    }

    /// Add metrics to time series
    pub fn add_metrics(&mut self, metrics: &[Metric], test_start: Instant) {
        let mut window_map: HashMap<u64, Vec<&Metric>> = HashMap::new();

        for metric in metrics {
            let elapsed = metric.timestamp.duration_since(test_start).as_secs();
            let window_id = elapsed / self.window_size;
            window_map.entry(window_id).or_insert_with(Vec::new).push(metric);
        }

        for (window_id, window_metrics) in window_map {
            let window_start = test_start + Duration::from_secs(window_id * self.window_size);
            let window = TimeWindow::from_metrics(window_start, self.window_size, &window_metrics);
            self.windows.push(window);
        }

        self.windows.sort_by_key(|w| w.start_time);
    }
}

/// Connection timing statistics
#[derive(Debug, Clone)]
pub struct ConnectionTimingStats {
    pub dns_lookup: ResponseTimeStats,
    pub tcp_connect: ResponseTimeStats,
    pub tls_handshake: ResponseTimeStats,
    pub time_to_first_byte: ResponseTimeStats,
    pub content_download: ResponseTimeStats,
    pub total_connection: ResponseTimeStats,
}

impl ConnectionTimingStats {
    pub fn from_timings(
        dns: Vec<f64>,
        tcp: Vec<f64>,
        tls: Vec<f64>,
        ttfb: Vec<f64>,
        download: Vec<f64>,
        total: Vec<f64>,
    ) -> Self {
        Self {
            dns_lookup: ResponseTimeStats::from_values(&dns),
            tcp_connect: ResponseTimeStats::from_values(&tcp),
            tls_handshake: ResponseTimeStats::from_values(&tls),
            time_to_first_byte: ResponseTimeStats::from_values(&ttfb),
            content_download: ResponseTimeStats::from_values(&download),
            total_connection: ResponseTimeStats::from_values(&total),
        }
    }
}

impl Default for ConnectionTimingStats {
    fn default() -> Self {
        Self {
            dns_lookup: ResponseTimeStats::default(),
            tcp_connect: ResponseTimeStats::default(),
            tls_handshake: ResponseTimeStats::default(),
            time_to_first_byte: ResponseTimeStats::default(),
            content_download: ResponseTimeStats::default(),
            total_connection: ResponseTimeStats::default(),
        }
    }
}

/// VU lifecycle statistics
#[derive(Debug, Clone)]
pub struct VuLifecycleStats {
    /// Total VUs started
    pub total_started: usize,
    /// Total VUs completed
    pub total_completed: usize,
    /// Total VUs failed
    pub total_failed: usize,
    /// Currently active VUs (started but not completed/failed)
    pub currently_active: usize,
    /// VU events timeline
    pub events: Vec<VuEvent>,
    /// VU states at different timestamps
    pub state_snapshots: Vec<VuStateSnapshot>,
}

impl VuLifecycleStats {
    pub fn from_events(mut events: Vec<VuEvent>) -> Self {
        events.sort_by_key(|e| e.timestamp);
        
        let mut total_started = 0;
        let mut total_completed = 0;
        let mut total_failed = 0;
        let mut active_vus: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut state_snapshots = Vec::new();
        
        for event in &events {
            match event.event_type {
                VuEventType::Started => {
                    total_started += 1;
                    active_vus.insert(event.vu_id);
                }
                VuEventType::Completed => {
                    total_completed += 1;
                    active_vus.remove(&event.vu_id);
                }
                VuEventType::Failed => {
                    total_failed += 1;
                    active_vus.remove(&event.vu_id);
                }
            }
            
            // Create snapshot after each event
            state_snapshots.push(VuStateSnapshot {
                timestamp: event.timestamp,
                active: active_vus.len(),
                completed: total_completed,
                failed: total_failed,
                total_started,
            });
        }
        
        Self {
            total_started,
            total_completed,
            total_failed,
            currently_active: active_vus.len(),
            events,
            state_snapshots,
        }
    }
    
    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        let total_finished = self.total_completed + self.total_failed;
        if total_finished == 0 {
            return 0.0;
        }
        (self.total_completed as f64 / total_finished as f64) * 100.0
    }
    
    /// Get VU state at a specific time
    pub fn state_at(&self, target_time: Instant) -> Option<&VuStateSnapshot> {
        self.state_snapshots
            .iter()
            .rev()
            .find(|snapshot| snapshot.timestamp <= target_time)
    }
}

impl Default for VuLifecycleStats {
    fn default() -> Self {
        Self {
            total_started: 0,
            total_completed: 0,
            total_failed: 0,
            currently_active: 0,
            events: Vec::new(),
            state_snapshots: Vec::new(),
        }
    }
}

/// VU event for lifecycle tracking
#[derive(Debug, Clone)]
pub struct VuEvent {
    pub timestamp: Instant,
    pub vu_id: usize,
    pub event_type: VuEventType,
}

/// Type of VU lifecycle event
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VuEventType {
    Started,
    Completed,
    Failed,
}

/// Snapshot of VU states at a point in time
#[derive(Debug, Clone)]
pub struct VuStateSnapshot {
    pub timestamp: Instant,
    pub active: usize,
    pub completed: usize,
    pub failed: usize,
    pub total_started: usize,
}

impl VuStateSnapshot {
    /// Get total finished VUs
    pub fn total_finished(&self) -> usize {
        self.completed + self.failed
    }
    
    /// Get pending VUs (started but not finished)
    pub fn pending(&self) -> usize {
        self.total_started - self.total_finished()
    }
}

/// Calculate percentile from sorted values
fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = (percentile / 100.0 * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

/// A time window containing aggregated metrics
#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub start_time: Instant,
    pub duration_secs: u64,
    pub request_count: usize,
    pub error_count: usize,
    pub avg_response_time: f64,
    pub throughput: f64,
}

impl TimeWindow {
    fn from_metrics(start_time: Instant, duration_secs: u64, metrics: &[&Metric]) -> Self {
        let mut request_count = 0;
        let mut error_count = 0;
        let mut response_times = Vec::new();

        for metric in metrics {
            match metric.metric_type {
                MetricType::ResponseTime => response_times.push(metric.value),
                MetricType::Success => request_count += 1,
                MetricType::Failure => {
                    error_count += 1;
                    request_count += 1;
                }
                _ => {}
            }
        }

        let avg_response_time = if !response_times.is_empty() {
            response_times.iter().sum::<f64>() / response_times.len() as f64
        } else {
            0.0
        };

        let throughput = request_count as f64 / duration_secs as f64;

        Self {
            start_time,
            duration_secs,
            request_count,
            error_count,
            avg_response_time,
            throughput,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metric_stats_generation() {
        let metrics = vec![
            Metric::new(MetricType::ResponseTime, "req".to_string(), 100.0),
            Metric::new(MetricType::ResponseTime, "req".to_string(), 200.0),
            Metric::new(MetricType::ResponseTime, "req".to_string(), 150.0),
            Metric::new(MetricType::Success, "req".to_string(), 1.0),
            Metric::new(MetricType::Success, "req".to_string(), 1.0),
            Metric::new(MetricType::Failure, "req".to_string(), 1.0),
            Metric::new(MetricType::BytesSent, "data".to_string(), 1024.0),
            Metric::new(MetricType::BytesReceived, "data".to_string(), 2048.0),
        ];

        let duration = Duration::from_secs(10);
        let stats = MetricStats::from_metrics(&metrics, duration);

        assert_eq!(stats.total_requests, 3);
        assert_eq!(stats.successful_requests, 2);
        assert_eq!(stats.failed_requests, 1);
        assert_eq!(stats.response_times.count, 3);
        assert_eq!(stats.response_times.mean, 150.0);
        assert!((stats.success_rate() - 66.66).abs() < 0.1);
    }
    
    #[test]
    fn test_connection_timing_stats() {
        let dns = vec![10.0, 15.0, 12.0];
        let tcp = vec![20.0, 25.0, 22.0];
        let tls = vec![30.0, 35.0, 32.0];
        let ttfb = vec![100.0, 110.0, 105.0];
        let download = vec![50.0, 60.0, 55.0];
        let total = vec![210.0, 245.0, 226.0];
        
        let stats = ConnectionTimingStats::from_timings(dns, tcp, tls, ttfb, download, total);
        
        assert_eq!(stats.dns_lookup.count, 3);
        assert_eq!(stats.dns_lookup.mean, 12.333333333333334);
        assert_eq!(stats.tcp_connect.mean, 22.333333333333332);
        assert_eq!(stats.tls_handshake.mean, 32.333333333333336);
    }
    
    #[test]
    fn test_vu_lifecycle_stats() {
        let start = Instant::now();
        let events = vec![
            VuEvent {
                timestamp: start,
                vu_id: 1,
                event_type: VuEventType::Started,
            },
            VuEvent {
                timestamp: start + Duration::from_millis(100),
                vu_id: 2,
                event_type: VuEventType::Started,
            },
            VuEvent {
                timestamp: start + Duration::from_millis(200),
                vu_id: 1,
                event_type: VuEventType::Completed,
            },
            VuEvent {
                timestamp: start + Duration::from_millis(300),
                vu_id: 3,
                event_type: VuEventType::Started,
            },
            VuEvent {
                timestamp: start + Duration::from_millis(400),
                vu_id: 2,
                event_type: VuEventType::Failed,
            },
        ];
        
        let stats = VuLifecycleStats::from_events(events);
        
        assert_eq!(stats.total_started, 3);
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.total_failed, 1);
        assert_eq!(stats.currently_active, 1); // VU 3 is still active
        assert_eq!(stats.events.len(), 5);
        assert_eq!(stats.state_snapshots.len(), 5);
        assert_eq!(stats.success_rate(), 50.0);
    }
    
    #[test]
    fn test_vu_state_snapshot() {
        let snapshot = VuStateSnapshot {
            timestamp: Instant::now(),
            active: 5,
            completed: 10,
            failed: 2,
            total_started: 17,
        };
        
        assert_eq!(snapshot.total_finished(), 12);
        assert_eq!(snapshot.pending(), 5);
    }
    
    #[test]
    fn test_metric_with_connection_types() {
        let metric = Metric::new(MetricType::DnsLookup, "dns".to_string(), 15.5);
        assert_eq!(metric.metric_type, MetricType::DnsLookup);
        
        let metric = Metric::new(MetricType::TlsHandshake, "tls".to_string(), 45.2);
        assert_eq!(metric.metric_type, MetricType::TlsHandshake);
    }
    
    #[test]
    fn test_vu_lifecycle_event() {
        let event = VuEvent {
            timestamp: Instant::now(),
            vu_id: 42,
            event_type: VuEventType::Started,
        };
        
        assert_eq!(event.vu_id, 42);
        assert_eq!(event.event_type, VuEventType::Started);
    }
}
