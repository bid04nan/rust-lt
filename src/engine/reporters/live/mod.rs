use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use csv::ReaderBuilder;

/// Parsed request data from CSV
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestRecord {
    pub timestamp: String,
    pub scenario: String,
    pub request_name: String,
    pub user_id: usize,
    pub response_time_ms: f64,
    pub status: String,
    pub pass_count: usize,
    pub fail_count: usize,
    pub error_message: String,
    pub bytes_sent: usize,
    pub bytes_received: usize,
    pub dns_lookup_ms: Option<f64>,
    pub tcp_connect_ms: Option<f64>,
    pub tls_handshake_ms: Option<f64>,
    pub time_to_first_byte_ms: Option<f64>,
    pub content_download_ms: Option<f64>,
}

/// Parsed VU state data from CSV
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VuStateRecord {
    pub timestamp: String,
    pub scenario: String,
    pub total_started: usize,
    pub active: usize,
    pub completed: usize,
    pub failed: usize,
    pub success_rate: f64,
}

/// Aggregated statistics for JSON output
#[derive(Debug, Serialize, Deserialize)]
pub struct LiveStats {
    pub scenario: String,
    pub current_time: String,
    pub vu_state: VuStateRecord,
    pub vu_state_history: Vec<VuStateRecord>,
    pub recent_requests: Vec<RequestRecord>,
    pub summary: StatsSummary,
}

impl LiveStats {
    /// Write stats to JSON file
    pub fn to_json_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

/// Summary statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct StatsSummary {
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub success_rate: f64,
    pub avg_response_time_ms: f64,
    pub min_response_time_ms: f64,
    pub max_response_time_ms: f64,
    pub p50_response_time_ms: f64,
    pub p95_response_time_ms: f64,
    pub p99_response_time_ms: f64,
    pub total_bytes_sent: usize,
    pub total_bytes_received: usize,
    pub throughput_rps: f64,
}

/// CSV to JSON parser for live reporting
pub struct CsvParser {
    requests_csv_path: String,
    vu_states_csv_path: String,
}

impl CsvParser {
    /// Create a new CSV parser
    pub fn new(requests_csv_path: String, vu_states_csv_path: String) -> Self {
        Self {
            requests_csv_path,
            vu_states_csv_path,
        }
    }

    /// Parse request CSV file
    pub fn parse_requests(&self) -> Result<Vec<RequestRecord>, Box<dyn std::error::Error>> {
        let file = File::open(&self.requests_csv_path)?;
        let reader = BufReader::new(file);
        let mut csv_reader = ReaderBuilder::new().from_reader(reader);
        
        let mut records = Vec::new();
        
        for result in csv_reader.records() {
            let record = result?;
            
            // Parse optional fields
            let parse_optional_f64 = |idx: usize| -> Option<f64> {
                record.get(idx)
                    .and_then(|s| if s.is_empty() { None } else { s.parse().ok() })
            };
            
            records.push(RequestRecord {
                timestamp: record.get(0).unwrap_or("").to_string(),
                scenario: record.get(1).unwrap_or("").to_string(),
                request_name: record.get(2).unwrap_or("").to_string(),
                user_id: record.get(3).and_then(|s| s.parse().ok()).unwrap_or(0),
                response_time_ms: record.get(4).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                status: record.get(5).unwrap_or("").to_string(),
                pass_count: record.get(6).and_then(|s| s.parse().ok()).unwrap_or(0),
                fail_count: record.get(7).and_then(|s| s.parse().ok()).unwrap_or(0),
                error_message: record.get(8).unwrap_or("").to_string(),
                bytes_sent: record.get(9).and_then(|s| s.parse().ok()).unwrap_or(0),
                bytes_received: record.get(10).and_then(|s| s.parse().ok()).unwrap_or(0),
                dns_lookup_ms: parse_optional_f64(11),
                tcp_connect_ms: parse_optional_f64(12),
                tls_handshake_ms: parse_optional_f64(13),
                time_to_first_byte_ms: parse_optional_f64(14),
                content_download_ms: parse_optional_f64(15),
            });
        }
        
        Ok(records)
    }

    /// Parse VU state CSV file
    pub fn parse_vu_states(&self) -> Result<Vec<VuStateRecord>, Box<dyn std::error::Error>> {
        let file = File::open(&self.vu_states_csv_path)?;
        let reader = BufReader::new(file);
        let mut csv_reader = ReaderBuilder::new().from_reader(reader);
        
        let mut records = Vec::new();
        
        for result in csv_reader.records() {
            let record = result?;
            
            records.push(VuStateRecord {
                timestamp: record.get(0).unwrap_or("").to_string(),
                scenario: record.get(1).unwrap_or("").to_string(),
                total_started: record.get(2).and_then(|s| s.parse().ok()).unwrap_or(0),
                active: record.get(3).and_then(|s| s.parse().ok()).unwrap_or(0),
                completed: record.get(4).and_then(|s| s.parse().ok()).unwrap_or(0),
                failed: record.get(5).and_then(|s| s.parse().ok()).unwrap_or(0),
                success_rate: record.get(6).and_then(|s| s.parse().ok()).unwrap_or(0.0),
            });
        }
        
        Ok(records)
    }

    /// Generate live stats JSON
    pub fn generate_live_stats(&self, recent_count: usize) -> Result<LiveStats, Box<dyn std::error::Error>> {
        let requests = self.parse_requests()?;
        let vu_states = self.parse_vu_states()?;
        
        // Get latest VU state
        let current_vu_state = vu_states.last()
            .cloned()
            .unwrap_or(VuStateRecord {
                timestamp: chrono::Local::now().to_rfc3339(),
                scenario: "unknown".to_string(),
                total_started: 0,
                active: 0,
                completed: 0,
                failed: 0,
                success_rate: 0.0,
            });
        
        // Get recent requests
        let recent_requests: Vec<RequestRecord> = requests.iter()
            .rev()
            .take(recent_count)
            .cloned()
            .collect();
        
        // Calculate summary statistics
        let summary = self.calculate_summary(&requests)?;
        
        Ok(LiveStats {
            scenario: current_vu_state.scenario.clone(),
            current_time: chrono::Local::now().to_rfc3339(),
            vu_state: current_vu_state,
            vu_state_history: vu_states,
            recent_requests,
            summary,
        })
    }

    /// Calculate summary statistics from requests
    fn calculate_summary(&self, requests: &[RequestRecord]) -> Result<StatsSummary, Box<dyn std::error::Error>> {
        if requests.is_empty() {
            return Ok(StatsSummary {
                total_requests: 0,
                successful_requests: 0,
                failed_requests: 0,
                success_rate: 0.0,
                avg_response_time_ms: 0.0,
                min_response_time_ms: 0.0,
                max_response_time_ms: 0.0,
                p50_response_time_ms: 0.0,
                p95_response_time_ms: 0.0,
                p99_response_time_ms: 0.0,
                total_bytes_sent: 0,
                total_bytes_received: 0,
                throughput_rps: 0.0,
            });
        }
        
        let total_requests = requests.len();
        let successful_requests = requests.iter().filter(|r| r.status == "success").count();
        let failed_requests = total_requests - successful_requests;
        let success_rate = (successful_requests as f64 / total_requests as f64) * 100.0;
        
        // Response times
        let mut response_times: Vec<f64> = requests.iter()
            .map(|r| r.response_time_ms)
            .collect();
        response_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let sum: f64 = response_times.iter().sum();
        let avg_response_time_ms = sum / response_times.len() as f64;
        let min_response_time_ms = *response_times.first().unwrap_or(&0.0);
        let max_response_time_ms = *response_times.last().unwrap_or(&0.0);
        
        // Percentiles
        let p50_index = (response_times.len() as f64 * 0.50) as usize;
        let p95_index = (response_times.len() as f64 * 0.95) as usize;
        let p99_index = (response_times.len() as f64 * 0.99) as usize;
        
        let p50_response_time_ms = response_times.get(p50_index).copied().unwrap_or(0.0);
        let p95_response_time_ms = response_times.get(p95_index).copied().unwrap_or(0.0);
        let p99_response_time_ms = response_times.get(p99_index).copied().unwrap_or(0.0);
        
        // Data transfer
        let total_bytes_sent: usize = requests.iter().map(|r| r.bytes_sent).sum();
        let total_bytes_received: usize = requests.iter().map(|r| r.bytes_received).sum();
        
        // Throughput calculation (requests per second)
        // Parse first and last timestamps to calculate duration
        let throughput_rps = if requests.len() >= 2 {
            if let (Some(first), Some(last)) = (requests.first(), requests.last()) {
                if let (Ok(first_time), Ok(last_time)) = (
                    chrono::DateTime::parse_from_str(&first.timestamp, "%Y-%m-%d %H:%M:%S%.3f"),
                    chrono::DateTime::parse_from_str(&last.timestamp, "%Y-%m-%d %H:%M:%S%.3f")
                ) {
                    let duration_secs = (last_time - first_time).num_milliseconds() as f64 / 1000.0;
                    if duration_secs > 0.0 {
                        requests.len() as f64 / duration_secs
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };
        
        Ok(StatsSummary {
            total_requests,
            successful_requests,
            failed_requests,
            success_rate,
            avg_response_time_ms,
            min_response_time_ms,
            max_response_time_ms,
            p50_response_time_ms,
            p95_response_time_ms,
            p99_response_time_ms,
            total_bytes_sent,
            total_bytes_received,
            throughput_rps,
        })
    }

    /// Export live stats to JSON file
    pub fn export_to_json(&self, output_path: &str, recent_count: usize) -> Result<(), Box<dyn std::error::Error>> {
        let stats = self.generate_live_stats(recent_count)?;
        let json = serde_json::to_string_pretty(&stats)?;
        std::fs::write(output_path, json)?;
        Ok(())
    }

    /// Get live stats as JSON string
    pub fn to_json_string(&self, recent_count: usize) -> Result<String, Box<dyn std::error::Error>> {
        let stats = self.generate_live_stats(recent_count)?;
        let json = serde_json::to_string_pretty(&stats)?;
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::io::Write;

    #[test]
    fn test_csv_parser_parse_requests() {
        let dir = tempdir().unwrap();
        let requests_path = dir.path().join("test_requests.csv");
        let vu_states_path = dir.path().join("test_vu_states.csv");
        
        // Create test CSV
        let mut file = File::create(&requests_path).unwrap();
        writeln!(file, "timestamp,scenario,request_name,user_id,response_time_ms,status,pass_count,fail_count,error_message,bytes_sent,bytes_received,dns_lookup_ms,tcp_connect_ms,tls_handshake_ms,time_to_first_byte_ms,content_download_ms").unwrap();
        writeln!(file, "2025-11-29 19:00:00.000,test_scenario,request1,1,100.5,success,1,0,,500,1000,10.0,20.0,30.0,40.0,50.0").unwrap();
        writeln!(file, "2025-11-29 19:00:01.000,test_scenario,request2,2,200.3,failure,0,1,timeout,300,500,,,,,").unwrap();
        drop(file);
        
        let parser = CsvParser::new(
            requests_path.to_str().unwrap().to_string(),
            vu_states_path.to_str().unwrap().to_string()
        );
        
        let requests = parser.parse_requests().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].request_name, "request1");
        assert_eq!(requests[0].status, "success");
        assert_eq!(requests[0].response_time_ms, 100.5);
        assert_eq!(requests[0].dns_lookup_ms, Some(10.0));
        assert_eq!(requests[1].status, "failure");
        assert_eq!(requests[1].dns_lookup_ms, None);
    }

    #[test]
    fn test_csv_parser_parse_vu_states() {
        let dir = tempdir().unwrap();
        let requests_path = dir.path().join("test_requests.csv");
        let vu_states_path = dir.path().join("test_vu_states.csv");
        
        // Create test CSV
        let mut file = File::create(&vu_states_path).unwrap();
        writeln!(file, "timestamp,scenario,total_started,active,completed,failed,success_rate").unwrap();
        writeln!(file, "2025-11-29 19:00:00.000,test_scenario,10,5,3,2,60.00").unwrap();
        writeln!(file, "2025-11-29 19:00:02.000,test_scenario,10,3,5,2,71.43").unwrap();
        drop(file);
        
        let parser = CsvParser::new(
            requests_path.to_str().unwrap().to_string(),
            vu_states_path.to_str().unwrap().to_string()
        );
        
        let vu_states = parser.parse_vu_states().unwrap();
        assert_eq!(vu_states.len(), 2);
        assert_eq!(vu_states[0].total_started, 10);
        assert_eq!(vu_states[0].active, 5);
        assert_eq!(vu_states[1].completed, 5);
    }

    #[test]
    fn test_generate_live_stats() {
        let dir = tempdir().unwrap();
        let requests_path = dir.path().join("test_requests.csv");
        let vu_states_path = dir.path().join("test_vu_states.csv");
        
        // Create test requests CSV
        let mut file = File::create(&requests_path).unwrap();
        writeln!(file, "timestamp,scenario,request_name,user_id,response_time_ms,status,pass_count,fail_count,error_message,bytes_sent,bytes_received,dns_lookup_ms,tcp_connect_ms,tls_handshake_ms,time_to_first_byte_ms,content_download_ms").unwrap();
        writeln!(file, "2025-11-29 19:00:00.000,test_scenario,request1,1,100.0,success,1,0,,500,1000,,,,,").unwrap();
        writeln!(file, "2025-11-29 19:00:01.000,test_scenario,request2,2,200.0,success,1,0,,300,800,,,,,").unwrap();
        drop(file);
        
        // Create test VU states CSV
        let mut file = File::create(&vu_states_path).unwrap();
        writeln!(file, "timestamp,scenario,total_started,active,completed,failed,success_rate").unwrap();
        writeln!(file, "2025-11-29 19:00:02.000,test_scenario,5,2,3,0,100.00").unwrap();
        drop(file);
        
        let parser = CsvParser::new(
            requests_path.to_str().unwrap().to_string(),
            vu_states_path.to_str().unwrap().to_string()
        );
        
        let stats = parser.generate_live_stats(10).unwrap();
        assert_eq!(stats.scenario, "test_scenario");
        assert_eq!(stats.summary.total_requests, 2);
        assert_eq!(stats.summary.successful_requests, 2);
        assert_eq!(stats.summary.success_rate, 100.0);
    }
}

/// Get the path to the dashboard HTML template
/// Your live server should serve this file at GET /
pub fn get_dashboard_html_path() -> &'static str {
    "src/engine/reporters/live/templates/live.hbs"
}

/// Get the path to the dashboard JavaScript
/// Your live server should serve this file at GET /live.js
pub fn get_dashboard_js_path() -> &'static str {
    "src/engine/reporters/live/templates/live.js"
}

/// Get the dashboard template HTML content
/// Use this if you want to serve the HTML directly without file I/O
pub fn get_dashboard_html() -> &'static str {
    include_str!("templates/live.hbs")
}

/// Get the dashboard JavaScript content
/// Use this if you want to serve the JS directly without file I/O
pub fn get_dashboard_js() -> &'static str {
    include_str!("templates/live.js")
}
