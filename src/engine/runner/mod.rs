use super::core::scenario::Scenario;
use super::core::injector::{build_schedule, UserInjector};
use super::core::vu::{VirtualUser, VuResult, load_feeders};
use super::stats::{MetricsCollector, Metric, MetricType};
use super::reporters::ParquetLogger;

use std::sync::Arc;
use tokio::task::JoinHandle;
use std::time::Instant;

/// Test execution configuration
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum number of concurrent VUs
    pub max_concurrent_vus: Option<usize>,
    
    /// Whether to stop on first error
    pub stop_on_error: bool,
    
    /// Whether to show real-time progress
    pub show_progress: bool,
    
    /// Directory to write CSV logs (None = no CSV logging)
    pub log_directory: Option<String>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_vus: None, // No limit by default
            stop_on_error: false,
            show_progress: true,
            log_directory: None, // No CSV logging by default
        }
    }
}

/// Test executor that orchestrates VU execution
pub struct TestExecutor {
    scenario: Scenario,
    config: ExecutorConfig,
    pub db_logger: Option<Arc<ParquetLogger>>,
}

impl TestExecutor {
    /// Create a new test executor
    pub fn new(scenario: Scenario) -> Self {
        Self {
            scenario,
            config: ExecutorConfig::default(),
            db_logger: None,
        }
    }
    
    /// Create with custom configuration
    pub fn with_config(scenario: Scenario, config: ExecutorConfig) -> Self {
        Self {
            scenario,
            config,
            db_logger: None,
        }
    }
    
    /// Execute the test scenario
    pub async fn execute(&self) -> TestResult {
        let start_time = Instant::now();
        
        // Create metrics collector
        let metrics_collector = Arc::new(MetricsCollector::new());
        
        // Create DuckDB logger if configured
        // Note: new() now returns Arc and sets the global logger automatically
        let db_logger = if let Some(ref log_dir) = self.config.log_directory {
            match ParquetLogger::new(log_dir, self.scenario.name.clone()) {
                Ok(logger_arc) => Some(logger_arc),
                Err(e) => {
                    eprintln!("Failed to create Parquet logger: {:?}. Continuing without logging.", e);
                    None
                }
            }
        } else {
            None
        };
        
        // Start VU state logging task if logger is available
        let vu_state_logger_handle = if let Some(ref logger) = db_logger {
            let logger_clone: Arc<ParquetLogger> = Arc::clone(logger);
            let metrics_clone = Arc::clone(&metrics_collector);
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
                interval.tick().await; // Skip first immediate tick

                loop {
                    interval.tick().await;
                    let stats = metrics_clone.generate_stats().await;

                    // Active VUs snapshot
                    let _ = logger_clone.log_vu_state(
                        chrono::Local::now(),
                        0,
                        "active",
                        None,
                    );

                    // Periodic throughput metric
                    let _ = logger_clone.log_metric(
                        chrono::Local::now(),
                        "throughput",
                        "requests_per_second",
                        stats.throughput.requests_per_second,
                        None,
                        None,
                    );

                    // Periodic active VUs metric
                    let _ = logger_clone.log_metric(
                        chrono::Local::now(),
                        "active_vus",
                        "active_vus",
                        stats.vu_stats.currently_active as f64,
                        None,
                        None,
                    );

                    if stats.vu_stats.currently_active == 0 && stats.vu_stats.total_completed > 0 {
                        break;
                    }
                }
            }))
        } else { None };
        
        if self.config.show_progress {
            println!("\n=== Starting Test Execution ===");
            println!("Scenario: {}", self.scenario.name);
            println!("Description: {}", self.scenario.description.as_deref().unwrap_or(""));
        }
        
        // Build injection schedule
        let schedule = build_schedule(&self.scenario.load_model);
        let total_users = schedule.total_users();
        
        if self.config.show_progress {
            println!("Total VUs to inject: {}", total_users);
            println!("Test duration: {} seconds", schedule.duration.unwrap_or(0));
            println!();
        }
        
        // Load feeders once and share across VUs
        let feeders = match load_feeders(&self.scenario).await {
            Ok(f) => Arc::new(f),
            Err(e) => {
                eprintln!("Failed to load feeders: {}", e);
                return TestResult::error(format!("Failed to load feeders: {}", e));
            }
        };
        
        // Create injector
        let mut injector = UserInjector::new(schedule);
        injector.start();
        
        // Spawn VUs according to schedule
        let mut vu_handles: Vec<JoinHandle<VuResult>> = Vec::new();
        let mut spawned_vus = 0;
        
        for vu_id in 0..total_users {
            // Wait for injection time
            if let Some((user_id, _delay)) = injector.next_user(vu_id).await {
                spawned_vus += 1;
                
                if self.config.show_progress && spawned_vus % 10 == 0 {
                    println!("Spawned {} / {} VUs...", spawned_vus, total_users);
                }
                
                // Clone necessary data for the VU (optimized - minimal cloning)
                let scenario = self.scenario.clone();
                let feeders_ref = Arc::clone(&feeders);
                let metrics_ref = Arc::clone(&metrics_collector);
                let db_logger_ref = db_logger.as_ref().map(Arc::clone);
                
                // Pre-format VU name once (avoid repeated allocations)
                let vu_name = format!("vu_{}", user_id);
                
                // Record VU started event
                let metric = Metric::new(MetricType::VuStarted, vu_name.clone(), 1.0);
                metrics_ref.record(metric).await;
                if let Some(ref logger) = db_logger_ref {
                    let _ = logger.log_metric(
                        chrono::Local::now(),
                        "vu_started",
                        &vu_name,
                        1.0,
                        Some(user_id),
                        None,
                    );
                }
                
                // Spawn VU in background
                let handle = tokio::spawn(async move {
                    // OPTIMIZED: Pass Arc directly, no HashMap clone needed
                    let mut vu = VirtualUser::with_feeders_arc(user_id, scenario, feeders_ref);
                    vu.set_metrics_collector(metrics_ref);
                    
                    // Set Parquet logger if available
                    if let Some(logger) = &db_logger_ref {
                        vu.set_parquet_logger(Arc::clone(logger));
                        // Log VU state: started
                        let _ = logger.log_vu_state(chrono::Local::now(), user_id, "started", None);
                    }
                    
                    let result = vu.run().await;
                    
                    // Record VU completion event (reuse vu_name to avoid re-allocation)
                    let event_type = if result.success { "vu_completed" } else { "vu_failed" };
                    let metric_type = if result.success { MetricType::VuCompleted } else { MetricType::VuFailed };
                    
                    let metric = Metric::new(metric_type, vu_name.clone(), 1.0);
                    vu.record_metric(metric).await;
                    
                    if let Some(logger) = &db_logger_ref {
                        let _ = logger.log_metric(
                            chrono::Local::now(),
                            event_type,
                            &vu_name,
                            1.0,
                            Some(user_id),
                            None,
                        );
                    }

                    // Log VU state: completed/failed with duration
                    if let Some(logger) = &db_logger_ref {
                        let state = if result.success { "completed" } else { "failed" };
                        let _ = logger.log_vu_state(chrono::Local::now(), user_id, state, Some(result.duration_ms as i64));
                    }
                    
                    result
                });
                
                vu_handles.push(handle);
                
                // Apply concurrency limit if configured
                if let Some(max_concurrent) = self.config.max_concurrent_vus {
                    while vu_handles.len() >= max_concurrent {
                        // Wait for at least one VU to complete
                        if let Some(handle) = vu_handles.first() {
                            if handle.is_finished() {
                                vu_handles.remove(0);
                            } else {
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                            }
                        }
                    }
                }
            }
        }
        
        if self.config.show_progress {
            println!("All {} VUs spawned. Waiting for completion...", total_users);
        }
        
        // Collect results
        let mut vu_results = Vec::new();
        let mut completed = 0;
        
        for handle in vu_handles {
            match handle.await {
                Ok(result) => {
                    completed += 1;
                    
                    if self.config.show_progress && completed % 10 == 0 {
                        println!("Completed {} / {} VUs...", completed, total_users);
                    }
                    
                    if self.config.stop_on_error && !result.success {
                        eprintln!("Stopping test due to VU {} failure", result.vu_id);
                        vu_results.push(result);
                        break;
                    }
                    
                    vu_results.push(result);
                }
                Err(e) => {
                    eprintln!("VU task failed: {}", e);
                    if self.config.stop_on_error {
                        break;
                    }
                }
            }
        }
        
        let duration = start_time.elapsed();
        
        if self.config.show_progress {
            println!("\n=== Test Execution Complete ===");
            println!("Duration: {:.2}s", duration.as_secs_f64());
            println!("VUs completed: {}", vu_results.len());
        }
        
        // Build test result with metrics
        let mut result = TestResult::from_vu_results(
            self.scenario.name.clone(),
            vu_results,
            duration.as_millis() as u64,
        );
        
        // Add collected metrics to result
        result.metrics = Some(Arc::clone(&metrics_collector));
        result.db_logger = db_logger.clone();

        // Await background VU state / metrics task then flush
        if let Some(handle) = vu_state_logger_handle {
            let _ = handle.await;
        }
        if let Some(ref logger) = db_logger { logger.flush(); }
        
        result
    }
}

/// Result of a complete test execution
#[derive(Clone)]
pub struct TestResult {
    pub scenario_name: String,
    pub success: bool,
    pub duration_ms: u64,
    pub total_vus: usize,
    pub successful_vus: usize,
    pub failed_vus: usize,
    pub total_actions: usize,
    pub successful_actions: usize,
    pub failed_actions: usize,
    pub total_bytes_sent: usize,
    pub total_bytes_received: usize,
    pub avg_response_time_ms: u64,
    pub min_response_time_ms: u64,
    pub max_response_time_ms: u64,
    pub requests_per_second: f64,
    pub error: Option<String>,
    pub vu_results: Vec<VuResult>,
    pub metrics: Option<Arc<MetricsCollector>>,
    pub db_logger: Option<Arc<ParquetLogger>>,
}

impl TestResult {
    /// Create from VU results
    pub fn from_vu_results(scenario_name: String, vu_results: Vec<VuResult>, duration_ms: u64) -> Self {
        let total_vus = vu_results.len();
        let successful_vus = vu_results.iter().filter(|r| r.success).count();
        let failed_vus = total_vus - successful_vus;
        
        let total_actions: usize = vu_results.iter().map(|r| r.total_actions).sum();
        let successful_actions: usize = vu_results.iter().map(|r| r.successful_actions).sum();
        let failed_actions: usize = vu_results.iter().map(|r| r.failed_actions).sum();
        
        let total_bytes_sent: usize = vu_results.iter().map(|r| r.total_bytes_sent).sum();
        let total_bytes_received: usize = vu_results.iter().map(|r| r.total_bytes_received).sum();
        
        // Calculate response time stats
        let all_response_times: Vec<u64> = vu_results.iter()
            .flat_map(|r| r.action_results.iter().map(|a| a.response_time_ms))
            .collect();
        
        let avg_response_time_ms = if !all_response_times.is_empty() {
            all_response_times.iter().sum::<u64>() / all_response_times.len() as u64
        } else {
            0
        };
        
        let min_response_time_ms = all_response_times.iter().min().copied().unwrap_or(0);
        let max_response_time_ms = all_response_times.iter().max().copied().unwrap_or(0);
        
        // Calculate throughput
        let duration_sec = duration_ms as f64 / 1000.0;
        let requests_per_second = if duration_sec > 0.0 {
            total_actions as f64 / duration_sec
        } else {
            0.0
        };
        
        Self {
            scenario_name,
            success: failed_vus == 0,
            duration_ms,
            total_vus,
            successful_vus,
            failed_vus,
            total_actions,
            successful_actions,
            failed_actions,
            total_bytes_sent,
            total_bytes_received,
            avg_response_time_ms,
            min_response_time_ms,
            max_response_time_ms,
            requests_per_second,
            error: None,
            vu_results,
            metrics: None,
            db_logger: None,
        }
    }
    
    /// Create error result
    pub fn error(error: String) -> Self {
        Self {
            scenario_name: "unknown".to_string(),
            success: false,
            duration_ms: 0,
            total_vus: 0,
            successful_vus: 0,
            failed_vus: 0,
            total_actions: 0,
            successful_actions: 0,
            failed_actions: 0,
            total_bytes_sent: 0,
            total_bytes_received: 0,
            avg_response_time_ms: 0,
            min_response_time_ms: 0,
            max_response_time_ms: 0,
            requests_per_second: 0.0,
            error: Some(error),
            vu_results: Vec::new(),
            metrics: None,
            db_logger: None,
        }
    }
    
    /// Print summary report
    pub fn print_summary(&self) {
        println!("\n╔══════════════════════════════════════════════════════════╗");
        println!("║           TEST EXECUTION SUMMARY                          ║");
        println!("╚══════════════════════════════════════════════════════════╝");
        
        println!("\nScenario: {}", self.scenario_name);
        println!("Status: {}", if self.success { "✓ SUCCESS" } else { "✗ FAILED" });
        println!("Duration: {:.2}s", self.duration_ms as f64 / 1000.0);
        
        println!("\n--- Virtual Users ---");
        println!("  Total: {}", self.total_vus);
        println!("  Successful: {} ({:.1}%)", 
            self.successful_vus,
            if self.total_vus > 0 { self.successful_vus as f64 / self.total_vus as f64 * 100.0 } else { 0.0 }
        );
        println!("  Failed: {}", self.failed_vus);
        
        println!("\n--- Actions ---");
        println!("  Total: {}", self.total_actions);
        println!("  Successful: {} ({:.1}%)", 
            self.successful_actions,
            if self.total_actions > 0 { self.successful_actions as f64 / self.total_actions as f64 * 100.0 } else { 0.0 }
        );
        println!("  Failed: {}", self.failed_actions);
        
        println!("\n--- Performance ---");
        println!("  Requests/sec: {:.2}", self.requests_per_second);
        println!("  Avg Response Time: {}ms", self.avg_response_time_ms);
        println!("  Min Response Time: {}ms", self.min_response_time_ms);
        println!("  Max Response Time: {}ms", self.max_response_time_ms);
        
        println!("\n--- Data Transfer ---");
        println!("  Sent: {:.2} KB", self.total_bytes_sent as f64 / 1024.0);
        println!("  Received: {:.2} KB", self.total_bytes_received as f64 / 1024.0);
        
        if let Some(err) = &self.error {
            println!("\n--- Error ---");
            println!("  {}", err);
        }
        
        println!("\n═══════════════════════════════════════════════════════════");
    }
    
    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_actions == 0 {
            return 0.0;
        }
        (self.successful_actions as f64 / self.total_actions as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::core::scenario::{LoadModel, FlowStep};
    use std::collections::HashMap;

    fn create_simple_scenario() -> Scenario {
        Scenario {
            name: "test_executor".to_string(),
            description: Some("Test scenario for executor".to_string()),
            load_model: LoadModel::AtOnceUsers { users: 3 },
            feeders: HashMap::new(),
            variables: HashMap::new(),
            steps: vec![
                FlowStep::Wait { duration: 10 },
            ],
            max_duration: None,
            think_time: None,
        }
    }

    #[tokio::test]
    async fn test_executor_creation() {
        let scenario = create_simple_scenario();
        let executor = TestExecutor::new(scenario);
        
        assert_eq!(executor.scenario.name, "test_executor");
        assert!(!executor.config.stop_on_error);
    }

    #[tokio::test]
    async fn test_executor_with_config() {
        let scenario = create_simple_scenario();
        let config = ExecutorConfig {
            max_concurrent_vus: Some(2),
            stop_on_error: true,
            show_progress: false,
            log_directory: None,
        };
        
        let executor = TestExecutor::with_config(scenario, config);
        assert_eq!(executor.config.max_concurrent_vus, Some(2));
        assert!(executor.config.stop_on_error);
    }

    #[tokio::test]
    async fn test_execute_simple_scenario() {
        let scenario = create_simple_scenario();
        let config = ExecutorConfig {
            max_concurrent_vus: None,
            stop_on_error: false,
            show_progress: false,
            log_directory: None,
        };
        
        let executor = TestExecutor::with_config(scenario, config);
        let result = executor.execute().await;
        
        assert!(result.success);
        assert_eq!(result.total_vus, 3);
        assert_eq!(result.successful_vus, 3);
        assert_eq!(result.failed_vus, 0);
        assert_eq!(result.total_actions, 3); // 3 VUs × 1 wait action
        assert!(result.duration_ms >= 10); // Should take at least 10ms
    }

    #[tokio::test]
    async fn test_execute_with_ramp() {
        let mut scenario = create_simple_scenario();
        scenario.load_model = LoadModel::RampUp { users: 5, duration: 1 };
        
        let config = ExecutorConfig {
            max_concurrent_vus: None,
            stop_on_error: false,
            show_progress: false,
            log_directory: None,
        };
        
        let executor = TestExecutor::with_config(scenario, config);
        let result = executor.execute().await;
        
        assert!(result.success);
        assert_eq!(result.total_vus, 5);
        assert!(result.duration_ms >= 1000); // Should take at least 1 second for ramp
    }

    #[tokio::test]
    async fn test_test_result_metrics() {
        let scenario = create_simple_scenario();
        let config = ExecutorConfig {
            max_concurrent_vus: None,
            stop_on_error: false,
            show_progress: false,
            log_directory: None,
        };
        
        let executor = TestExecutor::with_config(scenario, config);
        let result = executor.execute().await;
        
        assert_eq!(result.success_rate(), 100.0);
        assert!(result.requests_per_second > 0.0);
        assert!(result.avg_response_time_ms >= 0);
    }

    #[test]
    fn test_test_result_creation() {
        let vu_results = vec![];
        let result = TestResult::from_vu_results("test".to_string(), vu_results, 1000);
        
        assert_eq!(result.scenario_name, "test");
        assert_eq!(result.duration_ms, 1000);
        assert_eq!(result.total_vus, 0);
    }

    #[test]
    fn test_error_result() {
        let result = TestResult::error("Test error".to_string());
        
        assert!(!result.success);
        assert_eq!(result.error, Some("Test error".to_string()));
    }

    #[tokio::test]
    async fn test_vu_lifecycle_metrics() {
        let scenario = create_simple_scenario();
        let config = ExecutorConfig {
            max_concurrent_vus: None,
            stop_on_error: false,
            show_progress: false,
            log_directory: None,
        };
        
        let executor = TestExecutor::with_config(scenario, config);
        let result = executor.execute().await;
        
        // Verify metrics collector is attached
        assert!(result.metrics.is_some());
        
        let metrics = result.metrics.unwrap();
        let stats = metrics.generate_stats().await;
        
        // Verify VU lifecycle stats
        assert_eq!(stats.vu_stats.total_started, 3); // 3 VUs started
        assert_eq!(stats.vu_stats.total_completed, 3); // 3 VUs completed
        assert_eq!(stats.vu_stats.total_failed, 0); // 0 VUs failed
        assert_eq!(stats.vu_stats.success_rate(), 100.0);
    }

    #[tokio::test]
    async fn test_action_metrics_recording() {
        let scenario = create_simple_scenario();
        let config = ExecutorConfig {
            max_concurrent_vus: None,
            stop_on_error: false,
            show_progress: false,
            log_directory: None,
        };
        
        let executor = TestExecutor::with_config(scenario, config);
        let result = executor.execute().await;
        
        // Verify metrics collector is attached
        assert!(result.metrics.is_some());
        
        let metrics = result.metrics.unwrap();
        let stats = metrics.generate_stats().await;
        
        // Verify action metrics were recorded
        assert!(stats.total_requests > 0);
        assert!(stats.response_times.count > 0);
        
        // Each VU executes 1 wait action, so we should have 3 successful actions
        assert_eq!(stats.successful_requests, 3);
        assert_eq!(stats.failed_requests, 0);
    }
}

