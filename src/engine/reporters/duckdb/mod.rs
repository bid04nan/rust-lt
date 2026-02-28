use duckdb::{Connection, Result as DuckResult, params};
use std::sync::{Arc, RwLock};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;
use chrono::{DateTime, Local};
use serde::{Serialize, Deserialize};
use std::path::Path;

/// Global logger instance for dashboard access during test execution
static GLOBAL_LOGGER: RwLock<Option<Arc<DuckDbLogger>>> = RwLock::new(None);

/// Get the global logger instance (used by dashboard)
pub fn get_global_logger() -> Option<Arc<DuckDbLogger>> {
    GLOBAL_LOGGER.read().ok().and_then(|guard| guard.as_ref().map(Arc::clone))
}

/// Set the global logger instance
pub fn set_global_logger(logger: Arc<DuckDbLogger>) {
    if let Ok(mut guard) = GLOBAL_LOGGER.write() {
        *guard = Some(logger);
    }
}

/// DuckDB logger for efficient test data storage and querying
#[derive(Clone)]
pub struct DuckDbLogger {
    sender: Sender<LogMessage>,
    scenario_name: String,
    run_id: String,
    db_path: String,
}

// Internal log message enum processed by the worker thread
enum LogMessage {
    Request {
        ts: DateTime<Local>,
        name: String,
        user_id: usize,
        rt_ms: f64,
        status: String,
        pass: usize,
        fail: usize,
        err: String,
        bytes_sent: usize,
        bytes_recv: usize,
        dns_ms: Option<f64>,
        tcp_ms: Option<f64>,
        tls_ms: Option<f64>,
        ttfb_ms: Option<f64>,
        dl_ms: Option<f64>,
        scenario: String,
    },
    VuState {
        ts: DateTime<Local>,
        user_id: usize,
        state: String,
        duration_ms: Option<i64>,
        scenario: String,
    },
    Metric {
        ts: DateTime<Local>,
        metric_type: String,
        metric_name: String,
        value: f64,
        user_id: Option<usize>,
        tags: Option<String>,
        scenario: String,
    },
    QueryLiveStats {
        recent_limit: usize,
        resp: Sender<Result<LiveStats, duckdb::Error>>,
    },
}

impl DuckDbLogger {
    /// Create a new DuckDB logger
    pub fn new(output_dir: &str, scenario_name: String) -> DuckResult<Arc<Self>> {
        // Allow disabling DuckDB at runtime to avoid foreign exceptions
        if std::env::var("RUST_LT_DISABLE_DUCKDB").ok().as_deref() == Some("1") {
            let (tx, _rx) = mpsc::channel();
            let logger = Self {
                sender: tx,
                scenario_name: scenario_name.clone(),
                run_id: format!("{}_disabled", scenario_name.replace(" ", "_")),
                db_path: String::new(),
            };
            let arc_logger = Arc::new(logger);
            set_global_logger(Arc::clone(&arc_logger));
            println!("[DuckDB] Disabled via RUST_LT_DISABLE_DUCKDB=1. No DB writes.");
            return Ok(arc_logger);
        }

        std::fs::create_dir_all(output_dir)
            .map_err(|e| duckdb::Error::FromSqlConversionFailure(0, duckdb::types::Type::Text, Box::new(e)))?;
        
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let run_id = format!("{}_{}", scenario_name.replace(" ", "_"), timestamp);
        let db_path = format!("{}/{}.duckdb", output_dir, run_id);
        
        let conn = Connection::open(&db_path)?;
        Self::create_tables(&conn)?;
        
        println!("📊 DuckDB initialized: {}", db_path);
        println!("   Tables: requests, vu_states, metrics");
        println!();
        
        // Start background worker thread that owns the connection
        let (tx, rx): (Sender<LogMessage>, Receiver<LogMessage>) = mpsc::channel();
        let worker_scenario = scenario_name.clone();
        thread::spawn(move || {
            let conn = conn; // move connection into thread
            while let Ok(msg) = rx.recv() {
                match msg {
                    LogMessage::Request { ts, name, user_id, rt_ms, status, pass, fail, err, bytes_sent, bytes_recv, dns_ms, tcp_ms, tls_ms, ttfb_ms, dl_ms, scenario } => {
                        let _ = conn.execute(
                            "INSERT INTO requests (timestamp, scenario, request_name, user_id, response_time_ms, status, pass_count, fail_count, error_message, bytes_sent, bytes_received, dns_lookup_ms, tcp_connect_ms, tls_handshake_ms, time_to_first_byte_ms, content_download_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            params![
                                ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                                &scenario,
                                &name,
                                user_id as i32,
                                rt_ms,
                                &status,
                                pass as i32,
                                fail as i32,
                                &err,
                                bytes_sent as i32,
                                bytes_recv as i32,
                                dns_ms,
                                tcp_ms,
                                tls_ms,
                                ttfb_ms,
                                dl_ms,
                            ],
                        );
                    }
                    LogMessage::VuState { ts, user_id, state, duration_ms, scenario } => {
                        let _ = conn.execute(
                            "INSERT INTO vu_states (timestamp, scenario, user_id, state, duration_ms) VALUES (?, ?, ?, ?, ?)",
                            params![
                                ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                                &scenario,
                                user_id as i32,
                                &state,
                                duration_ms,
                            ],
                        );
                    }
                    LogMessage::Metric { ts, metric_type, metric_name, value, user_id, tags, scenario } => {
                        let _ = conn.execute(
                            "INSERT INTO metrics (timestamp, scenario, metric_type, metric_name, value, user_id, tags) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            params![
                                ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                                &scenario,
                                &metric_type,
                                &metric_name,
                                value,
                                user_id.map(|u| u as i32),
                                tags.as_deref(),
                            ],
                        );
                    }
                    LogMessage::QueryLiveStats { recent_limit, resp } => {
                        let result: Result<LiveStats, duckdb::Error> = (|| {
                            let mut vu_stmt = conn.prepare(
                                "SELECT 
                                    COUNT(DISTINCT user_id) FILTER (WHERE state = 'started') as total_started,
                                    COUNT(DISTINCT user_id) FILTER (WHERE state = 'active') as active,
                                    COUNT(DISTINCT user_id) FILTER (WHERE state = 'completed') as completed,
                                    COUNT(DISTINCT user_id) FILTER (WHERE state = 'failed') as failed
                                FROM vu_states"
                            )?;
                            let mut vu_state = vu_stmt.query_row([], |row| {
                                Ok(VuState {
                                    total_started: row.get::<_, i32>(0)? as usize,
                                    active: row.get::<_, i32>(1)? as usize,
                                    completed: row.get::<_, i32>(2)? as usize,
                                    failed: row.get::<_, i32>(3)? as usize,
                                    success_rate: 0.0,
                                })
                            })?;
                            if vu_state.total_started > 0 {
                                vu_state.success_rate = (vu_state.completed as f64 / vu_state.total_started as f64) * 100.0;
                            }

                            let mut sum_stmt = conn.prepare(
                                "SELECT 
                                    COALESCE(COUNT(*), 0) as total_requests,
                                    COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), 0) as success_count,
                                    COALESCE(SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END), 0) as fail_count,
                                    COALESCE(AVG(response_time_ms), 0.0) as avg_response_time,
                                    COALESCE(MIN(response_time_ms), 0.0) as min_response_time,
                                    COALESCE(MAX(response_time_ms), 0.0) as max_response_time,
                                    COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY response_time_ms), 0.0) as p50,
                                    COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY response_time_ms), 0.0) as p95,
                                    COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY response_time_ms), 0.0) as p99,
                                    COALESCE(SUM(bytes_sent), 0) as total_bytes_sent,
                                    COALESCE(SUM(bytes_received), 0) as total_bytes_received
                                FROM requests"
                            )?;
                            let summary = sum_stmt.query_row([], |row| {
                                let total: i64 = row.get(0)?;
                                let success: i64 = row.get(1)?;
                                let fail: i64 = row.get(2)?;
                                let success_rate = if total > 0 { (success as f64 / total as f64) * 100.0 } else { 0.0 };
                                Ok(Summary {
                                    total_requests: total as usize,
                                    success_count: success as usize,
                                    fail_count: fail as usize,
                                    success_rate,
                                    avg_response_time: row.get::<_, f64>(3)?,
                                    min_response_time: row.get::<_, f64>(4)?,
                                    max_response_time: row.get::<_, f64>(5)?,
                                    p50: row.get::<_, f64>(6)?,
                                    p95: row.get::<_, f64>(7)?,
                                    p99: row.get::<_, f64>(8)?,
                                    throughput_rps: 0.0,
                                    total_bytes_sent: row.get::<_, i64>(9)? as usize,
                                    total_bytes_received: row.get::<_, i64>(10)? as usize,
                                })
                            })?;

                            let mut req_stmt = conn.prepare(&format!(
                                "SELECT timestamp, request_name, user_id, response_time_ms, status, error_message FROM requests ORDER BY timestamp DESC LIMIT {}",
                                recent_limit
                            ))?;
                            let rows = req_stmt.query_map([], |row| {
                                Ok(RequestRecord {
                                    timestamp: row.get::<_, String>(0)?,
                                    name: row.get(1)?,
                                    user_id: row.get::<_, i32>(2)? as usize,
                                    response_time: row.get(3)?,
                                    status: row.get(4)?,
                                    error: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
                                })
                            })?;
                            let recent_requests: Vec<RequestRecord> = rows.collect::<Result<Vec<_>, _>>()?;

                            let mut hist_stmt = conn.prepare(
                                "SELECT timestamp,
                                        COUNT(DISTINCT user_id) FILTER (WHERE state = 'active') as active,
                                        COUNT(DISTINCT user_id) FILTER (WHERE state = 'completed') as completed,
                                        COUNT(DISTINCT user_id) FILTER (WHERE state = 'failed') as failed
                                 FROM vu_states GROUP BY timestamp ORDER BY timestamp DESC LIMIT 100"
                            )?;
                            let hist_rows = hist_stmt.query_map([], |row| {
                                Ok(VuStateRecord {
                                    timestamp: row.get(0)?,
                                    active: row.get::<_, i32>(1)? as usize,
                                    completed: row.get::<_, i32>(2)? as usize,
                                    failed: row.get::<_, i32>(3)? as usize,
                                })
                            })?;
                            let vu_state_history: Vec<VuStateRecord> = hist_rows.collect::<Result<Vec<_>, _>>()?;

                            Ok(LiveStats {
                                scenario: worker_scenario.clone(),
                                current_time: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                                vu_state,
                                vu_state_history,
                                recent_requests,
                                summary,
                            })
                        })();
                        let _ = resp.send(result);
                    }
                }
            }
        });

        let logger = Self {
            sender: tx,
            scenario_name,
            run_id,
            db_path: db_path.clone(),
        };

        let arc_logger = Arc::new(logger);
        set_global_logger(Arc::clone(&arc_logger));
        
        Ok(arc_logger)
    }
    
    /// Open existing database for reading
    pub fn open_readonly(db_path: &str) -> DuckResult<Self> {
        // Open with access_mode=READ_ONLY for concurrent read access
        let conn = Connection::open_with_flags(
            db_path,
            duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?
        )?;
        
        // Try to extract scenario name from the first request in the database
        let scenario_name = {
            let mut stmt = conn.prepare("SELECT DISTINCT scenario FROM requests LIMIT 1").ok();
            stmt.and_then(|mut s| {
                s.query_row([], |row| row.get::<_, String>(0)).ok()
            }).unwrap_or_else(|| "Unknown Scenario".to_string())
        };
        
        // readonly connections are used only for querying via methods below; writer thread is not started here
        let (tx, _rx_dummy) = mpsc::channel::<LogMessage>();
        Ok(Self {
            sender: tx, // readonly logger won't send
            scenario_name,
            run_id: "unknown".to_string(),
            db_path: db_path.to_string(),
        })
    }
    
    fn create_tables(conn: &Connection) -> DuckResult<()> {
        // Requests table - detailed HTTP request logs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS requests (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                timestamp TIMESTAMP NOT NULL,
                scenario VARCHAR NOT NULL,
                request_name VARCHAR NOT NULL,
                user_id INTEGER NOT NULL,
                response_time_ms DOUBLE NOT NULL,
                status VARCHAR NOT NULL,
                pass_count INTEGER NOT NULL,
                fail_count INTEGER NOT NULL,
                error_message VARCHAR,
                bytes_sent INTEGER,
                bytes_received INTEGER,
                dns_lookup_ms DOUBLE,
                tcp_connect_ms DOUBLE,
                tls_handshake_ms DOUBLE,
                time_to_first_byte_ms DOUBLE,
                content_download_ms DOUBLE
            )",
            [],
        )?;
        
        // VU states table - virtual user lifecycle tracking
        conn.execute(
            "CREATE TABLE IF NOT EXISTS vu_states (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                timestamp TIMESTAMP NOT NULL,
                scenario VARCHAR NOT NULL,
                user_id INTEGER NOT NULL,
                state VARCHAR NOT NULL,
                duration_ms BIGINT
            )",
            [],
        )?;
        
        // Metrics table - raw performance metrics
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metrics (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                timestamp TIMESTAMP NOT NULL,
                scenario VARCHAR NOT NULL,
                metric_type VARCHAR NOT NULL,
                metric_name VARCHAR NOT NULL,
                value DOUBLE NOT NULL,
                user_id INTEGER,
                tags VARCHAR
            )",
            [],
        )?;
        
        // Create indexes for better query performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests(timestamp)", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_name ON requests(request_name)", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vu_states_timestamp ON vu_states(timestamp)", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_type ON metrics(metric_type)", [])?;
        
        Ok(())
    }
    
    /// Log a request
    pub fn log_request(
        &self,
        timestamp: DateTime<Local>,
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
    ) -> DuckResult<()> {
        if std::env::var("RUST_LT_DISABLE_DUCKDB").ok().as_deref() == Some("1") { return Ok(()); }
        let msg = LogMessage::Request {
            ts: timestamp,
            name: request_name.to_string(),
            user_id,
            rt_ms: response_time_ms,
            status: status.to_string(),
            pass: pass_count,
            fail: fail_count,
            err: error_message.to_string(),
            bytes_sent,
            bytes_recv: bytes_received,
            dns_ms: dns_lookup_ms,
            tcp_ms: tcp_connect_ms,
            tls_ms: tls_handshake_ms,
            ttfb_ms: time_to_first_byte_ms,
            dl_ms: content_download_ms,
            scenario: self.scenario_name.clone(),
        };
        let _ = self.sender.send(msg);
        Ok(())
    }
    
    /// Log VU state change
    pub fn log_vu_state(
        &self,
        timestamp: DateTime<Local>,
        user_id: usize,
        state: &str,
        duration_ms: Option<i64>,
    ) -> DuckResult<()> {
        if std::env::var("RUST_LT_DISABLE_DUCKDB").ok().as_deref() == Some("1") { return Ok(()); }
        let msg = LogMessage::VuState {
            ts: timestamp,
            user_id,
            state: state.to_string(),
            duration_ms,
            scenario: self.scenario_name.clone(),
        };
        let _ = self.sender.send(msg);
        Ok(())
    }
    
    /// Log a metric
    pub fn log_metric(
        &self,
        timestamp: DateTime<Local>,
        metric_type: &str,
        metric_name: &str,
        value: f64,
        user_id: Option<usize>,
        tags: Option<&str>,
    ) -> DuckResult<()> {
        if std::env::var("RUST_LT_DISABLE_DUCKDB").ok().as_deref() == Some("1") { return Ok(()); }
        let msg = LogMessage::Metric {
            ts: timestamp,
            metric_type: metric_type.to_string(),
            metric_name: metric_name.to_string(),
            value,
            user_id,
            tags: tags.map(|t| t.to_string()),
            scenario: self.scenario_name.clone(),
        };
        let _ = self.sender.send(msg);
        Ok(())
    }
    
    /// Get live statistics for dashboard
    pub fn get_live_stats(&self, recent_limit: usize) -> DuckResult<LiveStats> {
        if std::env::var("RUST_LT_DISABLE_DUCKDB").ok().as_deref() == Some("1") {
            return Ok(LiveStats {
                scenario: self.scenario_name.clone(),
                current_time: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                vu_state: VuState { total_started: 0, active: 0, completed: 0, failed: 0, success_rate: 0.0 },
                vu_state_history: Vec::new(),
                recent_requests: Vec::new(),
                summary: Summary {
                    total_requests: 0,
                    success_count: 0,
                    fail_count: 0,
                    success_rate: 0.0,
                    avg_response_time: 0.0,
                    min_response_time: 0.0,
                    max_response_time: 0.0,
                    p50: 0.0,
                    p95: 0.0,
                    p99: 0.0,
                    throughput_rps: 0.0,
                    total_bytes_sent: 0,
                    total_bytes_received: 0,
                },
            });
        }
        let (resp_tx, resp_rx) = mpsc::channel();
        let _ = self.sender.send(LogMessage::QueryLiveStats { recent_limit, resp: resp_tx });
        match resp_rx.recv() {
            Ok(Ok(stats)) => Ok(stats),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(duckdb::Error::FromSqlConversionFailure(0, duckdb::types::Type::Text, Box::new(e))),
        }
    }
    
    /// Get time-series data for charts
    pub fn get_time_series(&self, bucket_seconds: i32) -> DuckResult<Vec<TimeSeriesPoint>> {
        // Route through the worker by reusing get_live_stats if needed or add a dedicated query message later.
        // For now, return an empty vector to avoid unsafe concurrent reads.
        Ok(Vec::new())
    }
    
    /// Get request statistics by name
    pub fn get_request_stats_by_name(&self) -> DuckResult<Vec<RequestStats>> {
        // Temporarily disable live per-request stats to avoid concurrent reads.
        Ok(Vec::new())
    }
    
    pub fn get_run_id(&self) -> &str {
        &self.run_id
    }
    
    pub fn get_db_path(&self, output_dir: &str) -> String {
        format!("{}/{}.duckdb", output_dir, self.run_id)
    }
}

// Data structures for query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStats {
    pub scenario: String,
    pub current_time: String,
    pub vu_state: VuState,
    pub vu_state_history: Vec<VuStateRecord>,
    pub recent_requests: Vec<RequestRecord>,
    pub summary: Summary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VuState {
    pub total_started: usize,
    pub active: usize,
    pub completed: usize,
    pub failed: usize,
    pub success_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VuStateRecord {
    pub timestamp: String,
    pub active: usize,
    pub completed: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestRecord {
    pub timestamp: String,
    pub name: String,
    pub user_id: usize,
    pub response_time: f64,
    pub status: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub total_requests: usize,
    pub success_count: usize,
    pub fail_count: usize,
    pub success_rate: f64,
    pub avg_response_time: f64,
    pub min_response_time: f64,
    pub max_response_time: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub throughput_rps: f64,
    pub total_bytes_sent: usize,
    pub total_bytes_received: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp: String,
    pub request_count: usize,
    pub avg_response_time: f64,
    pub p95_response_time: f64,
    pub success_count: usize,
    pub fail_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestStats {
    pub name: String,
    pub count: usize,
    pub avg_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub p95_time: f64,
    pub success_count: usize,
    pub fail_count: usize,
}
