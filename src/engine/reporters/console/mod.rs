use super::super::stats::{MetricsCollector, MetricStats};
use super::super::runner::TestResult;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;

/// Console reporter for displaying test execution progress and results
pub struct ConsoleReporter {
    /// Whether to show live progress updates
    show_progress: bool,
    /// Update interval for live stats (in seconds)
    update_interval_secs: u64,
    /// Start time of the test
    start_time: Option<Instant>,
}

impl ConsoleReporter {
    /// Create a new console reporter
    pub fn new() -> Self {
        Self {
            show_progress: true,
            update_interval_secs: 5,
            start_time: None,
        }
    }

    /// Create with custom settings
    pub fn with_config(show_progress: bool, update_interval_secs: u64) -> Self {
        Self {
            show_progress,
            update_interval_secs,
            start_time: None,
        }
    }

    /// Print test start banner
    pub fn print_start_banner(&mut self, scenario_name: &str, total_vus: usize) {
        self.start_time = Some(Instant::now());
        
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║              RUST LOAD TESTING - TEST EXECUTION              ║");
        println!("╚═══════════════════════════════════════════════════════════════╝");
        println!();
        println!("  Scenario:  {}", scenario_name);
        println!("  Total VUs: {}", total_vus);
        println!("  Started:   {}", chrono::Local::now().format("%Y-%m-%d %H:%M:%S"));
        println!();
        println!("─────────────────────────────────────────────────────────────────");
        println!();
    }

    /// Print live progress update
    pub async fn print_progress(&self, metrics: &Arc<MetricsCollector>) {
        if !self.show_progress {
            return;
        }

        let stats = metrics.generate_stats().await;
        let elapsed = self.start_time
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0);

        // Clear previous lines and print updated stats
        print!("\r");
        
        println!("┌─ Progress [{:02}:{:02}] ────────────────────────────────────────────┐",
            elapsed / 60, elapsed % 60);
        
        // VU Status
        println!("│ VUs: Started: {:4}  Active: {:4}  Completed: {:4}  Failed: {:4}  │",
            stats.vu_stats.total_started,
            stats.vu_stats.currently_active,
            stats.vu_stats.total_completed,
            stats.vu_stats.total_failed
        );
        
        // Request Stats
        println!("│ Requests: Total: {:6}  Success: {:6}  Failed: {:6}         │",
            stats.total_requests,
            stats.successful_requests,
            stats.failed_requests
        );
        
        // Performance Metrics
        if stats.response_times.count > 0 {
            println!("│ Response Time: Avg: {:6.0}ms  P95: {:6.0}ms  Max: {:6.0}ms      │",
                stats.response_times.mean,
                stats.response_times.p95,
                stats.response_times.max
            );
        }
        
        // Throughput
        println!("│ Throughput: {:.2} req/s  │  {:.2} req/min                     │",
            stats.throughput.requests_per_second,
            stats.throughput.requests_per_minute
        );
        
        println!("└────────────────────────────────────────────────────────────────┘");
        println!();
    }

    /// Start live monitoring (spawns background task)
    pub fn start_monitoring(&self, metrics: Arc<MetricsCollector>) -> tokio::task::JoinHandle<()> {
        let interval = self.update_interval_secs;
        let show_progress = self.show_progress;
        let start_time = self.start_time;

        tokio::spawn(async move {
            if !show_progress {
                return;
            }

            let mut interval_timer = time::interval(Duration::from_secs(interval));
            interval_timer.tick().await; // Skip first immediate tick

            loop {
                interval_timer.tick().await;
                
                let stats = metrics.generate_stats().await;
                let elapsed = start_time
                    .map(|t| t.elapsed().as_secs())
                    .unwrap_or(0);

                // Check if test is done (no more active VUs and we have completed VUs)
                if stats.vu_stats.currently_active == 0 && stats.vu_stats.total_completed > 0 {
                    break;
                }

                print!("\x1B[6A\x1B[J"); // Move up 6 lines and clear to end
                
                println!("┌─ Progress [{:02}:{:02}] ────────────────────────────────────────────┐",
                    elapsed / 60, elapsed % 60);
                
                println!("│ VUs: Started: {:4}  Active: {:4}  Completed: {:4}  Failed: {:4}  │",
                    stats.vu_stats.total_started,
                    stats.vu_stats.currently_active,
                    stats.vu_stats.total_completed,
                    stats.vu_stats.total_failed
                );
                
                println!("│ Requests: Total: {:6}  Success: {:6}  Failed: {:6}         │",
                    stats.total_requests,
                    stats.successful_requests,
                    stats.failed_requests
                );
                
                if stats.response_times.count > 0 {
                    println!("│ Response Time: Avg: {:6.0}ms  P95: {:6.0}ms  Max: {:6.0}ms      │",
                        stats.response_times.mean,
                        stats.response_times.p95,
                        stats.response_times.max
                    );
                } else {
                    println!("│ Response Time: Avg:      0ms  P95:      0ms  Max:      0ms      │");
                }
                
                println!("│ Throughput: {:.2} req/s  │  {:.2} req/min                     │",
                    stats.throughput.requests_per_second,
                    stats.throughput.requests_per_minute
                );
                
                println!("└────────────────────────────────────────────────────────────────┘");
            }
        })
    }

    /// Print final summary report
    pub async fn print_summary(&self, result: &TestResult) {
        println!();
        println!("═════════════════════════════════════════════════════════════════");
        println!("                        TEST SUMMARY                             ");
        println!("═════════════════════════════════════════════════════════════════");
        println!();

        let duration_secs = result.duration_ms as f64 / 1000.0;
        
        // Basic Info
        println!("  Scenario:      {}", result.scenario_name);
        println!("  Duration:      {:.2}s", duration_secs);
        println!("  Status:        {}", if result.success { "✓ SUCCESS" } else { "✗ FAILED" });
        println!();

        // Get detailed stats if metrics available
        if let Some(ref metrics) = result.metrics {
            let stats = metrics.generate_stats().await;
            
            // VU Stats
            println!("┌─ Virtual Users ──────────────────────────────────────────────┐");
            println!("│  Total Started:    {:6}                                      │", stats.vu_stats.total_started);
            println!("│  Completed:        {:6}                                      │", stats.vu_stats.total_completed);
            println!("│  Failed:           {:6}                                      │", stats.vu_stats.total_failed);
            println!("│  Success Rate:     {:5.1}%                                     │", stats.vu_stats.success_rate());
            println!("└──────────────────────────────────────────────────────────────┘");
            println!();

            // Request Stats
            println!("┌─ Requests ───────────────────────────────────────────────────┐");
            println!("│  Total:            {:6}                                      │", stats.total_requests);
            println!("│  Successful:       {:6}                                      │", stats.successful_requests);
            println!("│  Failed:           {:6}                                      │", stats.failed_requests);
            if stats.total_requests > 0 {
                let success_rate = (stats.successful_requests as f64 / stats.total_requests as f64) * 100.0;
                println!("│  Success Rate:     {:5.1}%                                     │", success_rate);
            }
            println!("└──────────────────────────────────────────────────────────────┘");
            println!();

            // Response Time Stats
            if stats.response_times.count > 0 {
                println!("┌─ Response Times (ms) ────────────────────────────────────────┐");
                println!("│  Mean:             {:8.2}                                  │", stats.response_times.mean);
                println!("│  Median:           {:8.2}                                  │", stats.response_times.median);
                println!("│  Min:              {:8.2}                                  │", stats.response_times.min);
                println!("│  Max:              {:8.2}                                  │", stats.response_times.max);
                println!("│  Std Dev:          {:8.2}                                  │", stats.response_times.std_dev);
                println!("│                                                              │");
                println!("│  Percentiles:                                                │");
                println!("│    P50:            {:8.2}                                  │", stats.response_times.p50);
                println!("│    P75:            {:8.2}                                  │", stats.response_times.p75);
                println!("│    P90:            {:8.2}                                  │", stats.response_times.p90);
                println!("│    P95:            {:8.2}                                  │", stats.response_times.p95);
                println!("│    P99:            {:8.2}                                  │", stats.response_times.p99);
                println!("└──────────────────────────────────────────────────────────────┘");
                println!();
            }

            // Throughput
            println!("┌─ Throughput ─────────────────────────────────────────────────┐");
            println!("│  Requests/sec:     {:8.2}                                  │", stats.throughput.requests_per_second);
            println!("│  Requests/min:     {:8.2}                                  │", stats.throughput.requests_per_minute);
            println!("└──────────────────────────────────────────────────────────────┘");
            println!();

            // Data Transfer
            if stats.data_transfer.total_bytes_sent > 0.0 || stats.data_transfer.total_bytes_received > 0.0 {
                println!("┌─ Data Transfer ──────────────────────────────────────────────┐");
                println!("│  Sent:             {:8.2} MB  ({:8.2} KB/s)                │",
                    stats.data_transfer.megabytes_sent,
                    stats.data_transfer.bytes_sent_per_second / 1024.0
                );
                println!("│  Received:         {:8.2} MB  ({:8.2} KB/s)                │",
                    stats.data_transfer.megabytes_received,
                    stats.data_transfer.bytes_received_per_second / 1024.0
                );
                println!("└──────────────────────────────────────────────────────────────┘");
                println!();
            }

            // Connection Timings (if available)
            if stats.connection_timings.dns_lookup.count > 0 
                || stats.connection_timings.tcp_connect.count > 0 
                || stats.connection_timings.tls_handshake.count > 0 {
                println!("┌─ Connection Timings (ms) ────────────────────────────────────┐");
                
                if stats.connection_timings.dns_lookup.count > 0 {
                    println!("│  DNS Lookup:       {:8.2} (avg)  {:8.2} (p95)             │",
                        stats.connection_timings.dns_lookup.mean,
                        stats.connection_timings.dns_lookup.p95
                    );
                }
                
                if stats.connection_timings.tcp_connect.count > 0 {
                    println!("│  TCP Connect:      {:8.2} (avg)  {:8.2} (p95)             │",
                        stats.connection_timings.tcp_connect.mean,
                        stats.connection_timings.tcp_connect.p95
                    );
                }
                
                if stats.connection_timings.tls_handshake.count > 0 {
                    println!("│  TLS Handshake:    {:8.2} (avg)  {:8.2} (p95)             │",
                        stats.connection_timings.tls_handshake.mean,
                        stats.connection_timings.tls_handshake.p95
                    );
                }
                
                if stats.connection_timings.time_to_first_byte.count > 0 {
                    println!("│  Time to First Byte: {:6.2} (avg)  {:8.2} (p95)           │",
                        stats.connection_timings.time_to_first_byte.mean,
                        stats.connection_timings.time_to_first_byte.p95
                    );
                }
                
                println!("└──────────────────────────────────────────────────────────────┘");
                println!();
            }
        }

        println!("═════════════════════════════════════════════════════════════════");
        println!();
    }

    /// Print error details if test failed
    pub fn print_errors(&self, result: &TestResult) {
        if let Some(ref error) = result.error {
            println!("❌ Test Error:");
            println!("   {}", error);
            println!();
        }

        // Print failed VU details
        let failed_vus: Vec<_> = result.vu_results.iter()
            .filter(|r| !r.success)
            .collect();

        if !failed_vus.is_empty() {
            println!("❌ Failed VUs:");
            for vu in failed_vus.iter().take(10) {
                println!("   VU {}: {} failed actions", vu.vu_id, vu.failed_actions);
            }
            if failed_vus.len() > 10 {
                println!("   ... and {} more failed VUs", failed_vus.len() - 10);
            }
            println!();
        }
    }
}

impl Default for ConsoleReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_console_reporter_creation() {
        let reporter = ConsoleReporter::new();
        assert!(reporter.show_progress);
        assert_eq!(reporter.update_interval_secs, 5);
    }

    #[test]
    fn test_console_reporter_with_config() {
        let reporter = ConsoleReporter::with_config(false, 10);
        assert!(!reporter.show_progress);
        assert_eq!(reporter.update_interval_secs, 10);
    }

    #[tokio::test]
    async fn test_print_summary_with_metrics() {
        let metrics = Arc::new(MetricsCollector::new());
        
        let mut result = TestResult {
            scenario_name: "test".to_string(),
            success: true,
            duration_ms: 5000,
            total_vus: 10,
            successful_vus: 10,
            failed_vus: 0,
            total_actions: 100,
            successful_actions: 100,
            failed_actions: 0,
            total_bytes_sent: 1000,
            total_bytes_received: 2000,
            avg_response_time_ms: 50,
            min_response_time_ms: 10,
            max_response_time_ms: 100,
            requests_per_second: 20.0,
            error: None,
            vu_results: vec![],
            metrics: Some(metrics),
            db_logger: None,
        };

        let reporter = ConsoleReporter::new();
        reporter.print_summary(&result).await;
        
        // Just verify it doesn't panic
        assert!(result.success);
    }
}
