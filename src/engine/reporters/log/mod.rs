use super::super::stats::{MetricsCollector, Metric, MetricType};
use super::super::runner::TestResult;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Local;

/// CSV logger for detailed request logs
#[derive(Clone)]
pub struct CsvRequestLogger {
    file: Arc<Mutex<File>>,
    scenario_name: String,
}

impl CsvRequestLogger {
    /// Create a new CSV request logger
    pub fn new(output_path: &str, scenario_name: String) -> std::io::Result<Self> {
        let mut file = File::create(output_path)?;
        
        // Write CSV header
        writeln!(file, "timestamp,scenario,request_name,user_id,response_time_ms,status,pass_count,fail_count,error_message,bytes_sent,bytes_received,dns_lookup_ms,tcp_connect_ms,tls_handshake_ms,time_to_first_byte_ms,content_download_ms")?;
        
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            scenario_name,
        })
    }

    /// Log a request entry
    pub async fn log_request(
        &self,
        timestamp: chrono::DateTime<Local>,
        request_name: &str,
        user_id: usize,
        response_time_ms: f64,
        status: &str,
        pass_count: usize,
        fail_count: usize,
        error_message: &str,
        bytes_sent: usize,
        bytes_received: usize,
        dns_lookup_ms: Option<f64>,
        tcp_connect_ms: Option<f64>,
        tls_handshake_ms: Option<f64>,
        time_to_first_byte_ms: Option<f64>,
        content_download_ms: Option<f64>,
    ) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
            self.scenario_name,
            request_name,
            user_id,
            response_time_ms,
            status,
            pass_count,
            fail_count,
            error_message.replace(",", ";"), // Escape commas
            bytes_sent,
            bytes_received,
            dns_lookup_ms.map(|v| format!("{:.3}", v)).unwrap_or_else(|| "".to_string()),
            tcp_connect_ms.map(|v| format!("{:.3}", v)).unwrap_or_else(|| "".to_string()),
            tls_handshake_ms.map(|v| format!("{:.3}", v)).unwrap_or_else(|| "".to_string()),
            time_to_first_byte_ms.map(|v| format!("{:.3}", v)).unwrap_or_else(|| "".to_string()),
            content_download_ms.map(|v| format!("{:.3}", v)).unwrap_or_else(|| "".to_string()),
        )?;
        
        file.flush()?;
        Ok(())
    }

    /// Flush and close the file
    pub async fn close(&self) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.flush()
    }
}

/// CSV logger for VU state tracking
pub struct CsvVuStateLogger {
    file: Arc<Mutex<File>>,
    scenario_name: String,
}

impl CsvVuStateLogger {
    /// Create a new CSV VU state logger
    pub fn new(output_path: &str, scenario_name: String) -> std::io::Result<Self> {
        let mut file = File::create(output_path)?;
        
        // Write CSV header
        writeln!(file, "timestamp,scenario,total_started,active,completed,failed,success_rate")?;
        
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            scenario_name,
        })
    }

    /// Log VU state snapshot
    pub async fn log_state(
        &self,
        timestamp: chrono::DateTime<Local>,
        total_started: usize,
        active: usize,
        completed: usize,
        failed: usize,
        success_rate: f64,
    ) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        
        writeln!(
            file,
            "{},{},{},{},{},{},{:.2}",
            timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
            self.scenario_name,
            total_started,
            active,
            completed,
            failed,
            success_rate,
        )?;
        
        file.flush()?;
        Ok(())
    }

    /// Flush and close the file
    pub async fn close(&self) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.flush()
    }
}

/// Combined CSV logger manager
pub struct CsvLogger {
    request_logger: Option<CsvRequestLogger>,
    vu_state_logger: Option<CsvVuStateLogger>,
    output_dir: String,
    scenario_name: String,
}

impl CsvLogger {
    /// Create a new CSV logger with default filenames
    pub fn new(output_dir: &str, scenario_name: String) -> std::io::Result<Self> {
        // Create timestamped subdirectory for this test run
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let run_dir = format!("{}/{}_{}", output_dir, scenario_name.replace(" ", "_"), timestamp);
        std::fs::create_dir_all(&run_dir)?;
        
        // Create request log file with simple name
        let request_log_path = format!("{}/requests.csv", run_dir);
        let request_logger = CsvRequestLogger::new(&request_log_path, scenario_name.clone())?;
        
        // Create VU state log file with simple name
        let vu_state_log_path = format!("{}/vu_states.csv", run_dir);
        let vu_state_logger = CsvVuStateLogger::new(&vu_state_log_path, scenario_name.clone())?;
        
        println!("📊 CSV logs will be written to: {}", run_dir);
        println!("   - Requests: requests.csv");
        println!("   - VU States: vu_states.csv");
        println!();
        
        Ok(Self {
            request_logger: Some(request_logger),
            vu_state_logger: Some(vu_state_logger),
            output_dir: run_dir,
            scenario_name,
        })
    }

    /// Get reference to request logger
    pub fn request_logger(&self) -> Option<&CsvRequestLogger> {
        self.request_logger.as_ref()
    }

    /// Get reference to VU state logger
    pub fn vu_state_logger(&self) -> Option<&CsvVuStateLogger> {
        self.vu_state_logger.as_ref()
    }

    /// Log a request from metrics
    pub async fn log_request_from_metric(
        &self,
        metric: &Metric,
        user_id: usize,
        status: &str,
        error_message: &str,
    ) -> std::io::Result<()> {
        if let Some(logger) = &self.request_logger {
            logger.log_request(
                Local::now(),
                &metric.name,
                user_id,
                metric.value,
                status,
                if status == "success" { 1 } else { 0 },
                if status == "failure" { 1 } else { 0 },
                error_message,
                metric.tags.get("bytes_sent").and_then(|v| v.parse().ok()).unwrap_or(0),
                metric.tags.get("bytes_received").and_then(|v| v.parse().ok()).unwrap_or(0),
                None, // DNS lookup - would need to be in tags
                None, // TCP connect - would need to be in tags
                None, // TLS handshake - would need to be in tags
                None, // TTFB - would need to be in tags
                None, // Content download - would need to be in tags
            ).await?;
        }
        Ok(())
    }

    /// Log VU state from metrics collector
    pub async fn log_vu_state_from_stats(&self, metrics: &Arc<MetricsCollector>) -> std::io::Result<()> {
        if let Some(logger) = &self.vu_state_logger {
            let stats = metrics.generate_stats().await;
            
            logger.log_state(
                Local::now(),
                stats.vu_stats.total_started,
                stats.vu_stats.currently_active,
                stats.vu_stats.total_completed,
                stats.vu_stats.total_failed,
                stats.vu_stats.success_rate(),
            ).await?;
        }
        Ok(())
    }

    /// Close all loggers
    pub async fn close(&mut self) -> std::io::Result<()> {
        if let Some(logger) = &self.request_logger {
            logger.close().await?;
        }
        if let Some(logger) = &self.vu_state_logger {
            logger.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_csv_request_logger_creation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_requests.csv");
        
        let logger = CsvRequestLogger::new(
            path.to_str().unwrap(),
            "test_scenario".to_string()
        ).unwrap();
        
        // Log a test entry
        logger.log_request(
            Local::now(),
            "test_request",
            1,
            100.5,
            "success",
            1,
            0,
            "",
            500,
            1000,
            Some(10.5),
            Some(20.3),
            Some(30.2),
            Some(50.0),
            Some(50.5),
        ).await.unwrap();
        
        logger.close().await.unwrap();
        
        // Verify file was created
        assert!(path.exists());
        
        // Read and verify content
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("timestamp,scenario,request_name"));
        assert!(content.contains("test_scenario"));
        assert!(content.contains("test_request"));
    }

    #[tokio::test]
    async fn test_csv_vu_state_logger_creation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_vu_states.csv");
        
        let logger = CsvVuStateLogger::new(
            path.to_str().unwrap(),
            "test_scenario".to_string()
        ).unwrap();
        
        // Log a test entry
        logger.log_state(
            Local::now(),
            10,
            5,
            3,
            2,
            60.0,
        ).await.unwrap();
        
        logger.close().await.unwrap();
        
        // Verify file was created
        assert!(path.exists());
        
        // Read and verify content
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("timestamp,scenario,total_started,active,completed,failed"));
        assert!(content.contains("test_scenario"));
    }

    #[tokio::test]
    async fn test_csv_logger_combined() {
        let dir = tempdir().unwrap();
        
        let logger = CsvLogger::new(
            dir.path().to_str().unwrap(),
            "test_scenario".to_string()
        ).unwrap();
        
        assert!(logger.request_logger().is_some());
        assert!(logger.vu_state_logger().is_some());
    }
}
