mod engine;

use clap::Parser;
use engine::{Scenario, TestExecutor, ExecutorConfig};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}, Mutex};
use std::thread;
use std::time::Duration;
// Needed for Arrow array null checks in HTTP handlers
use arrow_array::Array;
// Chrono trait needed for timestamp helpers
use chrono::TimeZone;

/// Rust Load Testing Framework
#[derive(Parser, Debug)]
#[command(name = "rust-lt")]
#[command(author = "Rust-LT Team")]
#[command(version = "0.1.0")]
#[command(about = "A load testing framework in Rust", long_about = None)]
struct Args {
    /// YAML scenario files to run (comma-separated for multiple scenarios)
    #[arg(short, long, value_delimiter = ',', default_value = "scenarios/test_scenario.yaml")]
    run: Vec<String>,

    /// Enable live dashboard server
    #[arg(short, long, value_name = "MODE", default_value = "no-open")]
    live: String,

    /// Live dashboard port
    #[arg(long, default_value = "8080")]
    live_port: u16,

    /// Output directory for logs and results
    #[arg(short, long, default_value = "output")]
    output: String,

    /// Stop test on first error
    #[arg(long)]
    stop_on_error: bool,

    /// Disable console progress display
    #[arg(long)]
    no_progress: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("🦀 Rust-LT - Load Testing Framework");
    println!("====================================\n");

    // Create output directory
    if let Err(e) = std::fs::create_dir_all(&args.output) {
        eprintln!("✗ Failed to create output directory: {}", e);
        std::process::exit(1);
    }

    // Start live dashboard server if enabled
    let dashboard_handle = if args.live == "open" {
        println!("📊 Starting live dashboard server...");
        println!("   Port: {}", args.live_port);
        println!("   Output: {}/", args.output);
        println!("\n   🌐 Dashboard URL: \x1b]8;;http://localhost:{}\x1b\\http://localhost:{}\x1b]8;;\x1b\\", args.live_port, args.live_port);
        println!("   👆 Click the link above or copy to browser\n");
        
        Some(start_dashboard_server(
            args.output.clone(),
            args.live_port
        ))
    } else {
        None
    };

    // Run each scenario
    let mut all_success = true;
    for (idx, scenario_file) in args.run.iter().enumerate() {
        if args.run.len() > 1 {
            println!("\n[{}/{}] Running scenario: {}", idx + 1, args.run.len(), scenario_file);
            println!("------------------------------------");
        } else {
            println!("Running scenario: {}", scenario_file);
        }

        match Scenario::from_file(scenario_file).await {
            Ok(scenario) => {
                println!("✓ Scenario loaded: {}\n", scenario.name);

                // Configure executor
                let config = ExecutorConfig {
                    max_concurrent_vus: None,
                    stop_on_error: args.stop_on_error,
                    show_progress: !args.no_progress,
                    log_directory: Some(args.output.clone()),
                };

                // Create and run test
                // Note: Logger is automatically set as global when created
                let executor = TestExecutor::with_config(scenario, config);
                let result = executor.execute().await;

                // Print detailed summary
                result.print_summary();

                // Check result
                if result.success {
                    println!("\n✓ Test completed successfully!");
                } else {
                    println!("\n✗ Test failed!");
                    all_success = false;
                }
            }
            Err(e) => {
                eprintln!("✗ Failed to load scenario '{}': {}", scenario_file, e);
                all_success = false;
                if args.stop_on_error {
                    std::process::exit(1);
                }
            }
        }
    }

    // Keep dashboard running if opened
    if let Some(handle) = dashboard_handle {
        println!("\n====================================");
        println!("📊 Live Dashboard Running");
        println!("====================================");
        println!("   🌐 URL: \x1b]8;;http://localhost:{}\x1b\\http://localhost:{}\x1b]8;;\x1b\\", args.live_port, args.live_port);
        println!("   👆 Click the link to open dashboard");
        println!("\n   Press Ctrl+C to stop the server");
        println!("====================================\n");
        
        // Wait for server thread (blocks until Ctrl+C)
        let _ = handle.join();
    }

    // Exit with appropriate code
    if !all_success {
        std::process::exit(1);
    }
}

/// Start dashboard server in background thread
fn start_dashboard_server(output_dir: String, port: u16) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("dashboard-server".to_string())
        .spawn(move || {
            // Run Tokio runtime in the thread for async DuckDB queries
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
            rt.block_on(async {
                run_dashboard_async(&output_dir, port).await;
            })
        })
        .expect("Failed to spawn dashboard server thread")
}

/// Async dashboard server using Tokio
async fn run_dashboard_async(output_dir: &str, port: u16) {
    use tokio::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use std::fs;

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", port)).await {
        Ok(l) => {
            println!("✓ Dashboard server listening on http://127.0.0.1:{}", port);
            l
        },
        Err(e) => {
            eprintln!("✗ Failed to start dashboard server on port {}: {}", port, e);
            eprintln!("   Try a different port with --live-port");
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                tokio::spawn(async move {
                    let mut buffer = vec![0; 2048];
                    match socket.read(&mut buffer).await {
                        Ok(n) if n > 0 => {
                            buffer.truncate(n);
                            let request = String::from_utf8_lossy(&buffer);
                            let raw_path = request
                                .lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                                .unwrap_or("/");
                            
                            let mut parts = raw_path.splitn(2, '?');
                            let path = parts.next().unwrap_or(raw_path);
                            let query = parts.next();
                            
                            let mut window_secs: i64 = 180;
                            let mut filter_req: Option<String> = None;
                            let mut filter_scenario: Option<String> = None;
                            
                            if let Some(qs) = query {
                                for kv in qs.split('&') {
                                    let mut kvp = kv.splitn(2, '=');
                                    if let (Some(k), Some(v)) = (kvp.next(), kvp.next()) {
                                        match k {
                                            "window_secs" => { if let Ok(ws) = v.parse::<i64>() { window_secs = ws.max(1); } },
                                            "request_name" => { if !v.is_empty() { filter_req = Some(url_decode(v)); } },
                                            "scenario" => { if !v.is_empty() { filter_scenario = Some(url_decode(v)); } },
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            let response_content = match path {
                                "/" | "/index.html" => {
                                    match fs::read_to_string("src/engine/reporters/live/templates/live.hbs") {
                                        Ok(html) => ("HTTP/1.1 200 OK", "text/html; charset=utf-8", html),
                                        Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "Failed to load dashboard".to_string())
                                    }
                                },
                                "/live_stats.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        match logger.get_live_stats() {
                                            Ok(stats) => {
                                                match serde_json::to_string(&stats) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "{}".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "{}".to_string())
                                    }
                                },
                                "/rps.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_rps(segments_dir, filter_scenario.clone(), filter_req.clone()).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "[]".to_string())
                                    }
                                },
                                "/latency_percentiles.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_latency_percentiles(segments_dir, filter_scenario.clone(), filter_req.clone(), window_secs).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "{}".to_string())
                                    }
                                },
                                "/errors_per_sec.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_errors_per_sec(segments_dir, filter_scenario.clone(), filter_req.clone()).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "[]".to_string())
                                    }
                                },
                                "/responses_by_status.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_responses_by_status(segments_dir, filter_scenario.clone(), filter_req.clone()).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "[]".to_string())
                                    }
                                },
                                "/bandwidth.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_bandwidth(segments_dir, filter_scenario.clone(), filter_req.clone()).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "[]".to_string())
                                    }
                                },
                                "/timing_percentiles.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_timing_percentiles(segments_dir, filter_scenario.clone(), filter_req.clone(), window_secs).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "{}".to_string())
                                    }
                                },
                                "/vus.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        match query_vus(segments_dir, filter_scenario.clone()).await {
                                            Ok(data) => {
                                                match serde_json::to_string(&data) {
                                                    Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                                    Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                                }
                                            },
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "[]".to_string())
                                        }
                                    } else {
                                        ("HTTP/1.1 200 OK", "application/json; charset=utf-8", "[]".to_string())
                                    }
                                },
                                "/metrics.json" => {
                                    if let Some(logger) = engine::reporters::get_global_logger() {
                                        let segments_dir = logger.segments_dir().to_string();
                                        let data = query_aggregated_metrics(segments_dir, filter_scenario.clone(), filter_req.clone(), window_secs).await;
                                        match serde_json::to_string(&data) {
                                            Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                        }
                                    } else {
                                        let default = AggregatedMetrics::default();
                                        match serde_json::to_string(&default) {
                                            Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                                            Err(_) => ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", "{}".to_string())
                                        }
                                    }
                                },
                                _ => ("HTTP/1.1 404 NOT FOUND", "text/plain", "404 Not Found".to_string())
                            };

                            let (status, content_type, content) = response_content;
                            let response = format!(
                                "{}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-cache\r\n\r\n{}",
                                status, content_type, content.len(), content
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                            let _ = socket.flush().await;
                        },
                        _ => {}
                    }
                });
            },
            Err(_) => {}
        }
    }
}

// Minimal URL decode for query parameters (handles %20 etc.)
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => { out.push(b' '); i += 1; }
            b'%' if i + 2 < bytes.len() => {
                let h1 = bytes[i+1];
                let h2 = bytes[i+2];
                let v = (hex_val(h1) << 4) | hex_val(h2);
                out.push(v);
                i += 3;
            }
            c => { out.push(c); i += 1; }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => 10 + (b - b'a'),
        b'A'..=b'F' => 10 + (b - b'A'),
        _ => 0,
    }
}

// ============================================================================
// DuckDB Async Query Functions for Live Dashboard Endpoints
// ============================================================================

use duckdb::Connection;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct RpsPoint {
    ts: String,
    rps: f64,
}

#[derive(Serialize, Default)]
struct LatencyPercentiles {
    p50_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Serialize)]
struct StatusPoint {
    ts: String,
    success: usize,
    fail: usize,
}

#[derive(Serialize)]
struct ErrorPoint {
    ts: String,
    errors: usize,
}

#[derive(Serialize)]
struct BandwidthPoint {
    ts: String,
    bytes_sent: i64,
    bytes_received: i64,
}

#[derive(Serialize, Default)]
struct TimingPercentiles {
    ttfb_p50_ms: f64,
    ttfb_p95_ms: f64,
    ttfb_p99_ms: f64,
    dns_p50_ms: f64,
    dns_p95_ms: f64,
    dns_p99_ms: f64,
    tcp_p50_ms: f64,
    tcp_p95_ms: f64,
    tcp_p99_ms: f64,
    tls_p50_ms: f64,
    tls_p95_ms: f64,
    tls_p99_ms: f64,
}

#[derive(Serialize)]
struct VuPoint {
    ts: String,
    arrivals: usize,
    terminations: usize,
    concurrent: isize,
}

/// Aggregated metrics - all data fetched in parallel
#[derive(Serialize, Default)]
struct AggregatedMetrics {
    rps: Vec<RpsPoint>,
    latency_percentiles: LatencyPercentiles,
    errors_per_sec: Vec<ErrorPoint>,
    responses_by_status: Vec<StatusPoint>,
    bandwidth: Vec<BandwidthPoint>,
    timing_percentiles: TimingPercentiles,
    vus: Vec<VuPoint>,
}

/// Helper to build a WHERE clause filter for common request-level filters
fn build_request_filter(scenario: &Option<String>, request_name: &Option<String>) -> String {
    let mut filters = vec!["record_type = 'request'".to_string()];
    if let Some(s) = scenario {
        filters.push(format!("scenario = '{}'", s.replace("'", "''")));
    }
    if let Some(r) = request_name {
        filters.push(format!("request_name = '{}'", r.replace("'", "''")));
    }
    filters.join(" AND ")
}

/// Requests per second over time
async fn query_rps(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
) -> Result<Vec<RpsPoint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let where_clause = build_request_filter(&scenario, &request_name);
    
    // Try to find any parquet files first
    let check_query = format!(
        "SELECT COUNT(*) as cnt FROM read_parquet('{}/*.parquet')",
        segments_dir
    );
    
    let mut check_stmt = conn.prepare(&check_query)?;
    let total_records: i64 = check_stmt.query_row([], |row| row.get(0)).unwrap_or(0);
    
    eprintln!("[DEBUG] RPS query - segments_dir: {}, total_records: {}", segments_dir, total_records);
    
    let query = format!(
        "SELECT 
            CAST((timestamp_ms / 1000) AS VARCHAR) AS ts,
            COUNT(*) AS rps 
         FROM read_parquet('{}/*.parquet') 
         WHERE {} 
         GROUP BY (timestamp_ms / 1000)
         ORDER BY (timestamp_ms / 1000)",
        segments_dir, where_clause
    );

    eprintln!("[DEBUG] RPS WHERE clause: {}", where_clause);

    eprintln!("[DEBUG] Preparing RPS query...");
    let mut stmt = match conn.prepare(&query) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[ERROR] Failed to prepare RPS query: {}", e);
            return Err(Box::new(e));
        }
    };
    
    eprintln!("[DEBUG] Executing RPS query...");
    let results = match stmt.query_map([], |row| {
        Ok(RpsPoint {
            ts: row.get(0)?,
            rps: row.get::<_, i64>(1)? as f64,
        })
    }) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[ERROR] Failed to execute RPS query: {}", e);
            return Err(Box::new(e));
        }
    };

    let mut out = Vec::new();
    for result in results {
        match result {
            Ok(point) => out.push(point),
            Err(e) => {
                eprintln!("[ERROR] Failed to process RPS row: {}", e);
                return Err(Box::new(e));
            }
        }
    }
    eprintln!("[DEBUG] RPS results: {} points", out.len());
    Ok(out)
}

/// Latency percentiles over a time window
async fn query_latency_percentiles(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
    window_secs: i64,
) -> Result<LatencyPercentiles, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let where_clause = build_request_filter(&scenario, &request_name);
    let cutoff_ms = chrono::Local::now().timestamp_millis() - (window_secs * 1000);

    let query = format!(
        "SELECT 
            quantile_cont(response_time_ms, 0.50) FILTER (WHERE response_time_ms IS NOT NULL) AS p50,
            quantile_cont(response_time_ms, 0.90) FILTER (WHERE response_time_ms IS NOT NULL) AS p90,
            quantile_cont(response_time_ms, 0.95) FILTER (WHERE response_time_ms IS NOT NULL) AS p95,
            quantile_cont(response_time_ms, 0.99) FILTER (WHERE response_time_ms IS NOT NULL) AS p99
         FROM read_parquet('{}/*.parquet') 
         WHERE {} AND timestamp_ms >= {}",
        segments_dir, where_clause, cutoff_ms
    );

    let mut stmt = conn.prepare(&query)?;
    let result = stmt.query_row([], |row| {
        Ok(LatencyPercentiles {
            p50_ms: row.get::<_, f64>(0).unwrap_or(0.0),
            p90_ms: row.get::<_, f64>(1).unwrap_or(0.0),
            p95_ms: row.get::<_, f64>(2).unwrap_or(0.0),
            p99_ms: row.get::<_, f64>(3).unwrap_or(0.0),
        })
    })?;

    Ok(result)
}

/// Responses per second bucketed by status
async fn query_responses_by_status(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
) -> Result<Vec<StatusPoint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let where_clause = build_request_filter(&scenario, &request_name);

    // Bucket by second using floor division
    let query = format!(
        "SELECT 
            CAST((timestamp_ms / 1000) AS VARCHAR) AS ts,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as fail
         FROM read_parquet('{}/*.parquet') 
         WHERE {}
         GROUP BY (timestamp_ms / 1000)
         ORDER BY (timestamp_ms / 1000)",
        segments_dir, where_clause
    );

    let mut stmt = conn.prepare(&query)?;
    let results = stmt.query_map([], |row| {
        Ok(StatusPoint {
            ts: row.get(0)?,
            success: row.get::<_, i64>(1).unwrap_or(0) as usize,
            fail: row.get::<_, i64>(2).unwrap_or(0) as usize,
        })
    })?;

    let mut out = Vec::new();
    for result in results {
        out.push(result?);
    }
    Ok(out)
}

/// Errors per second
async fn query_errors_per_sec(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
) -> Result<Vec<ErrorPoint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let where_clause = build_request_filter(&scenario, &request_name);

    let query = format!(
        "SELECT 
            CAST((timestamp_ms / 1000) AS VARCHAR) AS ts,
            COUNT(*) as errors
         FROM read_parquet('{}/*.parquet') 
         WHERE {} AND status = 'error'
         GROUP BY (timestamp_ms / 1000)
         ORDER BY (timestamp_ms / 1000)",
        segments_dir, where_clause
    );

    let mut stmt = conn.prepare(&query)?;
    let results = stmt.query_map([], |row| {
        Ok(ErrorPoint {
            ts: row.get(0)?,
            errors: row.get::<_, i64>(1).unwrap_or(0) as usize,
        })
    })?;

    let mut out = Vec::new();
    for result in results {
        out.push(result?);
    }
    Ok(out)
}

/// Bandwidth per second (bytes sent/received)
async fn query_bandwidth(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
) -> Result<Vec<BandwidthPoint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let where_clause = build_request_filter(&scenario, &request_name);

    let query = format!(
        "SELECT 
            CAST((timestamp_ms / 1000) AS VARCHAR) AS ts,
            SUM(bytes_sent) as bytes_sent, 
            SUM(bytes_received) as bytes_received
         FROM read_parquet('{}/*.parquet') 
         WHERE {}
         GROUP BY (timestamp_ms / 1000)
         ORDER BY (timestamp_ms / 1000)",
        segments_dir, where_clause
    );

    let mut stmt = conn.prepare(&query)?;
    let results = stmt.query_map([], |row| {
        Ok(BandwidthPoint {
            ts: row.get(0)?,
            bytes_sent: row.get::<_, i64>(1).unwrap_or(0),
            bytes_received: row.get::<_, i64>(2).unwrap_or(0),
        })
    })?;

    let mut out = Vec::new();
    for result in results {
        out.push(result?);
    }
    Ok(out)
}

/// Timing percentiles over a window
async fn query_timing_percentiles(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
    window_secs: i64,
) -> Result<TimingPercentiles, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let where_clause = build_request_filter(&scenario, &request_name);
    let cutoff_ms = chrono::Local::now().timestamp_millis() - (window_secs * 1000);

    let query = format!(
        "SELECT 
            quantile_cont(ttfb_ms, 0.50) FILTER (WHERE ttfb_ms IS NOT NULL) AS ttfb_p50,
            quantile_cont(ttfb_ms, 0.95) FILTER (WHERE ttfb_ms IS NOT NULL) AS ttfb_p95,
            quantile_cont(ttfb_ms, 0.99) FILTER (WHERE ttfb_ms IS NOT NULL) AS ttfb_p99,
            quantile_cont(dns_lookup_ms, 0.50) FILTER (WHERE dns_lookup_ms IS NOT NULL) AS dns_p50,
            quantile_cont(dns_lookup_ms, 0.95) FILTER (WHERE dns_lookup_ms IS NOT NULL) AS dns_p95,
            quantile_cont(dns_lookup_ms, 0.99) FILTER (WHERE dns_lookup_ms IS NOT NULL) AS dns_p99,
            quantile_cont(tcp_connect_ms, 0.50) FILTER (WHERE tcp_connect_ms IS NOT NULL) AS tcp_p50,
            quantile_cont(tcp_connect_ms, 0.95) FILTER (WHERE tcp_connect_ms IS NOT NULL) AS tcp_p95,
            quantile_cont(tcp_connect_ms, 0.99) FILTER (WHERE tcp_connect_ms IS NOT NULL) AS tcp_p99,
            quantile_cont(tls_handshake_ms, 0.50) FILTER (WHERE tls_handshake_ms IS NOT NULL) AS tls_p50,
            quantile_cont(tls_handshake_ms, 0.95) FILTER (WHERE tls_handshake_ms IS NOT NULL) AS tls_p95,
            quantile_cont(tls_handshake_ms, 0.99) FILTER (WHERE tls_handshake_ms IS NOT NULL) AS tls_p99
         FROM read_parquet('{}/*.parquet') 
         WHERE {} AND timestamp_ms >= {}",
        segments_dir, where_clause, cutoff_ms
    );

    let mut stmt = conn.prepare(&query)?;
    let result = stmt.query_row([], |row| {
        Ok(TimingPercentiles {
            ttfb_p50_ms: row.get::<_, f64>(0).unwrap_or(0.0),
            ttfb_p95_ms: row.get::<_, f64>(1).unwrap_or(0.0),
            ttfb_p99_ms: row.get::<_, f64>(2).unwrap_or(0.0),
            dns_p50_ms: row.get::<_, f64>(3).unwrap_or(0.0),
            dns_p95_ms: row.get::<_, f64>(4).unwrap_or(0.0),
            dns_p99_ms: row.get::<_, f64>(5).unwrap_or(0.0),
            tcp_p50_ms: row.get::<_, f64>(6).unwrap_or(0.0),
            tcp_p95_ms: row.get::<_, f64>(7).unwrap_or(0.0),
            tcp_p99_ms: row.get::<_, f64>(8).unwrap_or(0.0),
            tls_p50_ms: row.get::<_, f64>(9).unwrap_or(0.0),
            tls_p95_ms: row.get::<_, f64>(10).unwrap_or(0.0),
            tls_p99_ms: row.get::<_, f64>(11).unwrap_or(0.0),
        })
    })?;

    Ok(result)
}

/// VU metrics: arrivals, terminations, concurrent VUs per second
async fn query_vus(
    segments_dir: String,
    scenario: Option<String>,
) -> Result<Vec<VuPoint>, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let mut where_clause = "record_type = 'vu_state'".to_string();
    if let Some(s) = scenario {
        where_clause.push_str(&format!(" AND scenario = '{}'", s.replace("'", "''")));
    }

    let query = format!(
        "SELECT 
            CAST((timestamp_ms / 1000) AS VARCHAR) AS ts,
            SUM(CASE WHEN vu_state = 'started' THEN 1 ELSE 0 END) AS arrivals,
            SUM(CASE WHEN vu_state IN ('completed', 'failed') THEN 1 ELSE 0 END) AS terminations
         FROM read_parquet('{}/*.parquet') 
         WHERE {} 
         GROUP BY (timestamp_ms / 1000)
         ORDER BY (timestamp_ms / 1000)",
        segments_dir, where_clause
    );

    let mut stmt = conn.prepare(&query)?;
    let results = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1).unwrap_or(0),
            row.get::<_, i64>(2).unwrap_or(0),
        ))
    })?;

    let mut out = Vec::new();
    let mut concurrent: isize = 0;
    for result in results {
        let (ts, arrivals, terminations) = result?;
        concurrent += arrivals as isize - terminations as isize;
        out.push(VuPoint {
            ts,
            arrivals: arrivals as usize,
            terminations: terminations as usize,
            concurrent,
        });
    }
    Ok(out)
}

/// Execute all dashboard queries in parallel using tokio::spawn
/// Returns aggregated metrics for comprehensive dashboard view
async fn query_aggregated_metrics(
    segments_dir: String,
    scenario: Option<String>,
    request_name: Option<String>,
    window_secs: i64,
) -> AggregatedMetrics {
    // Execute all 7 query functions concurrently using separate spawns
    let dir1 = segments_dir.clone();
    let s1 = scenario.clone();
    let r1 = request_name.clone();
    let rps_handle = tokio::spawn(async move {
        query_rps(dir1, s1, r1).await.unwrap_or_default()
    });

    let dir2 = segments_dir.clone();
    let s2 = scenario.clone();
    let r2 = request_name.clone();
    let latency_handle = tokio::spawn(async move {
        query_latency_percentiles(dir2, s2, r2, window_secs).await.unwrap_or_default()
    });

    let dir3 = segments_dir.clone();
    let s3 = scenario.clone();
    let r3 = request_name.clone();
    let errors_handle = tokio::spawn(async move {
        query_errors_per_sec(dir3, s3, r3).await.unwrap_or_default()
    });

    let dir4 = segments_dir.clone();
    let s4 = scenario.clone();
    let r4 = request_name.clone();
    let status_handle = tokio::spawn(async move {
        query_responses_by_status(dir4, s4, r4).await.unwrap_or_default()
    });

    let dir5 = segments_dir.clone();
    let s5 = scenario.clone();
    let r5 = request_name.clone();
    let bandwidth_handle = tokio::spawn(async move {
        query_bandwidth(dir5, s5, r5).await.unwrap_or_default()
    });

    let dir6 = segments_dir.clone();
    let s6 = scenario.clone();
    let r6 = request_name.clone();
    let timing_handle = tokio::spawn(async move {
        query_timing_percentiles(dir6, s6, r6, window_secs).await.unwrap_or_default()
    });

    let dir7 = segments_dir.clone();
    let s7 = scenario.clone();
    let vus_handle = tokio::spawn(async move {
        query_vus(dir7, s7).await.unwrap_or_default()
    });

    // Wait for all to complete
    let rps = rps_handle.await.unwrap_or_default();
    let latency_percentiles = latency_handle.await.unwrap_or_default();
    let errors_per_sec = errors_handle.await.unwrap_or_default();
    let responses_by_status = status_handle.await.unwrap_or_default();
    let bandwidth = bandwidth_handle.await.unwrap_or_default();
    let timing_percentiles = timing_handle.await.unwrap_or_default();
    let vus = vus_handle.await.unwrap_or_default();

    AggregatedMetrics {
        rps,
        latency_percentiles,
        errors_per_sec,
        responses_by_status,
        bandwidth,
        timing_percentiles,
        vus,
    }
}
