use super::session::Session;
use super::scenario::{Scenario, FlowStep};
use super::action::{ActionResult, create_executor};
use super::super::stats::{MetricsCollector, Metric, MetricType};
use super::super::reporters::ParquetLogger;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Instant;
use chrono::Local;

// Import Feeder types
use super::super::feeders::{Feeder, FeederConfig};

/// Virtual User that executes a scenario
pub struct VirtualUser {
    /// Unique ID for this VU
    pub id: usize,
    
    /// Scenario to execute
    scenario: Scenario,
    
    /// Session for this VU
    session: Session,
    
    /// Feeders (shared across VUs but accessed sequentially per VU)
    feeders: HashMap<String, Arc<Mutex<Box<dyn Feeder>>>>,
    
    /// Results collected during execution
    results: Vec<ActionResult>,
    
    /// Start time of VU execution
    start_time: Option<Instant>,
    
    /// End time of VU execution
    end_time: Option<Instant>,
    
    /// Metrics collector for recording performance data
    metrics_collector: Option<Arc<MetricsCollector>>,
    
    /// Parquet logger for detailed request logs
    db_logger: Option<Arc<ParquetLogger>>,
}

impl VirtualUser {
    /// Create a new virtual user
    pub fn new(id: usize, scenario: Scenario) -> Self {
        let session = Session::new(id, scenario.name.clone());
        
        Self {
            id,
            scenario,
            session,
            feeders: HashMap::new(),
            results: Vec::new(),
            start_time: None,
            end_time: None,
            metrics_collector: None,
            db_logger: None,
        }
    }
    
    /// Create a new VU with pre-loaded feeders
    pub fn with_feeders(
        id: usize, 
        scenario: Scenario,
        feeders: HashMap<String, Arc<Mutex<Box<dyn Feeder>>>>
    ) -> Self {
        let session = Session::new(id, scenario.name.clone());
        
        Self {
            id,
            scenario,
            session,
            feeders,
            results: Vec::new(),
            start_time: None,
            end_time: None,
            metrics_collector: None,
            db_logger: None,
        }
    }
    
    /// Create a new VU with Arc-wrapped feeders (OPTIMIZED - zero-copy)
    /// This avoids cloning the entire HashMap when spawning thousands of VUs
    pub fn with_feeders_arc(
        id: usize,
        scenario: Scenario,
        feeders: Arc<HashMap<String, Arc<Mutex<Box<dyn Feeder>>>>>
    ) -> Self {
        let session = Session::new(id, scenario.name.clone());
        
        // Clone only the Arc pointers, not the actual feeder data
        let feeders_map = (*feeders).clone();
        
        Self {
            id,
            scenario,
            session,
            feeders: feeders_map,
            results: Vec::new(),
            start_time: None,
            end_time: None,
            metrics_collector: None,
            db_logger: None,
        }
    }
    
    /// Set metrics collector for this VU
    pub fn set_metrics_collector(&mut self, collector: Arc<MetricsCollector>) {
        self.metrics_collector = Some(collector);
    }
    
    /// Set Parquet logger for this VU
    pub fn set_parquet_logger(&mut self, logger: Arc<ParquetLogger>) {
        self.db_logger = Some(logger);
    }

    /// Record a metric through the VU's metrics collector
    pub async fn record_metric(&self, metric: Metric) {
        if let Some(collector) = &self.metrics_collector {
            collector.record(metric).await;
        }
    }
    
    /// Initialize global variables from scenario
    fn init_variables(&mut self) {
        for (key, value) in &self.scenario.variables {
            self.session.set(key.clone(), value.clone());
        }
    }
    
    /// Feed data from a feeder into session
    async fn feed(&mut self, feeder_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(feeder_arc) = self.feeders.get(feeder_name) {
            let mut feeder = feeder_arc.lock().await;
            if let Some(row) = feeder.next_row().await {
                self.session.set_all(row);
                Ok(())
            } else {
                Err(format!("Feeder '{}' exhausted", feeder_name).into())
            }
        } else {
            Err(format!("Feeder '{}' not found", feeder_name).into())
        }
    }
    
    /// Execute a single flow step
    async fn execute_step(&mut self, step: &FlowStep) -> Result<ActionResult, Box<dyn std::error::Error>> {
        // Handle Feed action specially
        if let FlowStep::Feed { feeder } = step {
            return match self.feed(feeder).await {
                Ok(_) => Ok(ActionResult::success(format!("feed:{}", feeder), 0)),
                Err(e) => Ok(ActionResult::failure(format!("feed:{}", feeder), e.to_string(), 0)),
            };
        }
        
        // For other actions, use the action executor
        let executor = create_executor(step)?;
        executor.execute(&mut self.session).await
    }
    
    /// Execute all steps in the scenario
    async fn execute_steps(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for step in self.scenario.steps.clone() {
            let result = self.execute_step(&step).await?;
            
            // Record metrics if collector is available
            if let Some(ref collector) = self.metrics_collector {
                // Record response time
                let metric = Metric::new(
                    MetricType::ResponseTime,
                    result.name.clone(),
                    result.response_time_ms as f64
                );
                collector.record(metric).await;
                if let Some(ref logger) = self.db_logger {
                    let _ = logger.log_metric(
                        Local::now(),
                        "response_time",
                        &result.name,
                        result.response_time_ms as f64,
                        Some(self.id),
                        None,
                    );
                }
                
                // Record bytes sent/received
                if result.bytes_sent > 0 {
                    let metric = Metric::new(
                        MetricType::BytesSent,
                        result.name.clone(),
                        result.bytes_sent as f64
                    );
                    collector.record(metric).await;
                    if let Some(ref logger) = self.db_logger {
                        let _ = logger.log_metric(
                            Local::now(),
                            "bytes_sent",
                            &result.name,
                            result.bytes_sent as f64,
                            Some(self.id),
                            None,
                        );
                    }
                }
                
                if result.bytes_received > 0 {
                    let metric = Metric::new(
                        MetricType::BytesReceived,
                        result.name.clone(),
                        result.bytes_received as f64
                    );
                    collector.record(metric).await;
                    if let Some(ref logger) = self.db_logger {
                        let _ = logger.log_metric(
                            Local::now(),
                            "bytes_received",
                            &result.name,
                            result.bytes_received as f64,
                            Some(self.id),
                            None,
                        );
                    }
                }
                
                // Record success/failure
                if result.success {
                    let metric = Metric::new(MetricType::Success, result.name.clone(), 1.0);
                    collector.record(metric).await;
                    if let Some(ref logger) = self.db_logger {
                        let _ = logger.log_metric(
                            Local::now(),
                            "success",
                            &result.name,
                            1.0,
                            Some(self.id),
                            None,
                        );
                    }
                } else {
                    let metric = Metric::new(MetricType::Failure, result.name.clone(), 1.0);
                    collector.record(metric).await;
                    if let Some(ref logger) = self.db_logger {
                        let _ = logger.log_metric(
                            Local::now(),
                            "failure",
                            &result.name,
                            1.0,
                            Some(self.id),
                            None,
                        );
                    }
                }
            }
            
            // Log to Parquet only for HTTP requests
            if let Some(ref logger) = self.db_logger {
                let is_http_request = match &step {
                    FlowStep::HttpRequest { .. } => true,
                    FlowStep::Request { protocol, .. } => protocol == "http_request",
                    _ => false,
                };
                if is_http_request {
                    let _ = logger.log_request(
                        Local::now(),
                        &result.name,
                        result.hostname.as_deref(),
                        self.id,
                        result.response_time_ms as f64,
                        result.ttfb_ms.map(|v| v as f64),
                        result.dns_lookup_ms.map(|v| v as f64),
                        result.tcp_connect_ms.map(|v| v as f64),
                        result.tls_handshake_ms.map(|v| v as f64),
                        if result.success { "success" } else { "failure" },
                        result.status_code.map(|c| c as i32),
                        result.error.as_deref().unwrap_or(""),
                        result.bytes_sent,
                        result.bytes_received,
                    );
                }
            }
            
            // Record the result
            self.results.push(result.clone());
            
            // If action failed and we should stop on failure, return error
            if !result.success {
                eprintln!("VU {} - Action '{}' failed: {:?}", 
                    self.id, result.name, result.error);
                // Continue execution anyway (could make this configurable)
            }
            
            // Apply think time if configured
            if let Some(think_time_ms) = self.scenario.think_time {
                tokio::time::sleep(std::time::Duration::from_millis(think_time_ms)).await;
            }
        }
        
        Ok(())
    }
    
    /// Run the virtual user
    pub async fn run(&mut self) -> VuResult {
        self.start_time = Some(Instant::now());
        
        // Initialize global variables
        self.init_variables();
        
        // Execute the scenario
        let execution_result = self.execute_steps().await;
        
        self.end_time = Some(Instant::now());
        
        // Build result summary
        let duration_ms = self.start_time
            .and_then(|start| self.end_time.map(|end| end.duration_since(start).as_millis() as u64))
            .unwrap_or(0);
        
        let total_actions = self.results.len();
        let successful_actions = self.results.iter().filter(|r| r.success).count();
        let failed_actions = total_actions - successful_actions;
        
        let total_response_time: u64 = self.results.iter().map(|r| r.response_time_ms).sum();
        let avg_response_time = if total_actions > 0 {
            total_response_time / total_actions as u64
        } else {
            0
        };
        
        let total_bytes_sent: usize = self.results.iter().map(|r| r.bytes_sent).sum();
        let total_bytes_received: usize = self.results.iter().map(|r| r.bytes_received).sum();
        
        VuResult {
            vu_id: self.id,
            success: execution_result.is_ok(),
            duration_ms,
            total_actions,
            successful_actions,
            failed_actions,
            avg_response_time_ms: avg_response_time,
            total_bytes_sent,
            total_bytes_received,
            error: execution_result.err().map(|e| e.to_string()),
            action_results: self.results.clone(),
        }
    }
    
    /// Get the session (for inspection/testing)
    pub fn session(&self) -> &Session {
        &self.session
    }
    
    /// Get the results
    pub fn results(&self) -> &[ActionResult] {
        &self.results
    }
}

/// Result of a virtual user execution
#[derive(Debug, Clone)]
pub struct VuResult {
    pub vu_id: usize,
    pub success: bool,
    pub duration_ms: u64,
    pub total_actions: usize,
    pub successful_actions: usize,
    pub failed_actions: usize,
    pub avg_response_time_ms: u64,
    pub total_bytes_sent: usize,
    pub total_bytes_received: usize,
    pub error: Option<String>,
    pub action_results: Vec<ActionResult>,
}

impl VuResult {
    /// Print a summary of the result
    pub fn print_summary(&self) {
        println!("VU {} - {}", 
            self.vu_id, 
            if self.success { "✓ Success" } else { "✗ Failed" }
        );
        println!("  Duration: {}ms", self.duration_ms);
        println!("  Actions: {} total, {} successful, {} failed",
            self.total_actions, self.successful_actions, self.failed_actions);
        println!("  Avg Response Time: {}ms", self.avg_response_time_ms);
        println!("  Data: {} bytes sent, {} bytes received",
            self.total_bytes_sent, self.total_bytes_received);
        
        if let Some(err) = &self.error {
            println!("  Error: {}", err);
        }
    }
    
    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_actions == 0 {
            return 0.0;
        }
        (self.successful_actions as f64 / self.total_actions as f64) * 100.0
    }
}

/// Load feeders from scenario configuration
pub async fn load_feeders(
    scenario: &Scenario
) -> Result<HashMap<String, Arc<Mutex<Box<dyn Feeder>>>>, Box<dyn std::error::Error>> {
    let mut feeders = HashMap::new();
    
    for (name, config) in &scenario.feeders {
        let feeder: Box<dyn Feeder> = match config {
            FeederConfig::Csv { .. } => {
                use super::super::feeders::csv::CsvFeeder;
                let feeder = CsvFeeder::load(config).await
                    .map_err(|e| format!("Failed to load CSV feeder '{}': {}", name, e))?;
                Box::new(feeder)
            }
            FeederConfig::Json { .. } => {
                use super::super::feeders::json::JsonFeeder;
                let feeder = JsonFeeder::load(config).await
                    .map_err(|e| format!("Failed to load JSON feeder '{}': {}", name, e))?;
                Box::new(feeder)
            }
            FeederConfig::Jdbc { .. } => {
                use super::super::feeders::jdbc::JdbcFeeder;
                let feeder = JdbcFeeder::load(config).await
                    .map_err(|e| format!("Failed to load JDBC feeder '{}': {}", name, e))?;
                Box::new(feeder)
            }
            FeederConfig::Redis { .. } => {
                use super::super::feeders::redis::RedisFeeder;
                let feeder = RedisFeeder::load(config).await
                    .map_err(|e| format!("Failed to load Redis feeder '{}': {}", name, e))?;
                Box::new(feeder)
            }
            FeederConfig::Kafka { .. } => {
                use super::super::feeders::kafka::KafkaFeeder;
                let feeder = KafkaFeeder::load(config).await
                    .map_err(|e| format!("Failed to load Kafka feeder '{}': {}", name, e))?;
                Box::new(feeder)
            }
        };
        
        feeders.insert(name.clone(), Arc::new(Mutex::new(feeder)));
    }
    
    Ok(feeders)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::scenario::LoadModel;

    fn create_test_scenario() -> Scenario {
        Scenario {
            name: "test_scenario".to_string(),
            description: Some("Test scenario".to_string()),
            load_model: LoadModel::AtOnceUsers { users: 1 },
            feeders: HashMap::new(),
            variables: {
                let mut vars = HashMap::new();
                vars.insert("base_url".to_string(), "https://httpbin.org".to_string());
                vars
            },
            steps: vec![
                FlowStep::Wait { duration: 10 },
            ],
            max_duration: None,
            think_time: Some(5),
        }
    }

    #[tokio::test]
    async fn test_vu_creation() {
        let scenario = create_test_scenario();
        let vu = VirtualUser::new(1, scenario);
        
        assert_eq!(vu.id, 1);
        assert_eq!(vu.session().user_id, 1);
        assert_eq!(vu.results().len(), 0);
    }

    #[tokio::test]
    async fn test_vu_variable_initialization() {
        let scenario = create_test_scenario();
        let mut vu = VirtualUser::new(1, scenario);
        
        vu.init_variables();
        
        assert_eq!(
            vu.session().get("base_url"),
            Some("https://httpbin.org")
        );
    }

    #[tokio::test]
    async fn test_vu_execution() {
        let scenario = create_test_scenario();
        let mut vu = VirtualUser::new(1, scenario);
        
        let result = vu.run().await;
        
        assert!(result.success);
        assert_eq!(result.vu_id, 1);
        assert_eq!(result.total_actions, 1); // One Wait action
        assert_eq!(result.successful_actions, 1);
        assert_eq!(result.failed_actions, 0);
        assert!(result.duration_ms >= 10); // Should take at least 10ms (wait) + 5ms (think time)
    }

    #[tokio::test]
    async fn test_vu_with_http_request() {
        let mut scenario = create_test_scenario();
        scenario.steps = vec![
            FlowStep::HttpRequest {
                name: "Status 200".to_string(),
                method: "GET".to_string(),
                url: "https://httpbin.org/status/200".to_string(),
                headers: HashMap::new(),
                body: None,
                checks: vec![],
                extractions: vec![],
            },
        ];
        
        let mut vu = VirtualUser::new(1, scenario);
        let result = vu.run().await;
        
        assert!(result.success);
        assert_eq!(result.total_actions, 1);
        
        // Check that we got action results
        let action_results = vu.results();
        assert_eq!(action_results.len(), 1);
        assert!(action_results[0].success);
    }

    #[tokio::test]
    async fn test_vu_result_metrics() {
        let scenario = create_test_scenario();
        let mut vu = VirtualUser::new(1, scenario);
        
        let result = vu.run().await;
        
        assert_eq!(result.success_rate(), 100.0);
        assert!(result.duration_ms > 0);
    }

    #[tokio::test]
    async fn test_vu_with_repeat() {
        let mut scenario = create_test_scenario();
        scenario.steps = vec![
            FlowStep::Repeat {
                times: 3,
                steps: vec![
                    FlowStep::Wait { duration: 10 },
                ],
            },
        ];
        scenario.think_time = None; // Disable think time for faster test
        
        let mut vu = VirtualUser::new(1, scenario);
        let result = vu.run().await;
        
        assert!(result.success);
        assert_eq!(result.total_actions, 1); // Repeat counts as 1 action
        assert!(result.duration_ms >= 30); // Should take at least 30ms (3 x 10ms)
    }
}
