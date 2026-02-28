/// Live Dashboard Server with Auto-updating JSON
/// 
/// This example runs a simple HTTP server that:
/// 1. Serves dashboard HTML/JS from source templates
/// 2. Auto-generates JSON from CSV logs every 2 seconds (configurable)
/// 3. Serves live JSON data to the dashboard
/// 
/// Usage:
///   cargo run --example dashboard_example
/// 
/// Then run your test (which generates CSV files in output/):
///   cargo run --bin rust-lt
/// 
/// Open browser: http://localhost:8080

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🦀 Rust-LT Live Dashboard Server");
    println!("==================================\n");
    
    // Configuration
    let output_dir = "output";
    let scenario_name = "test_scenario"; // Should match your test scenario name
    let json_refresh_interval_secs = 2; // JSON generation interval
    let port = 8080;
    
    // Step 1: Setup output directory
    println!("1. Setting up output directory: {}...", output_dir);
    std::fs::create_dir_all(output_dir)?;
    println!("   ✓ Output directory created\n");
    
    // Step 2: Start background JSON updater
    println!("2. Starting JSON auto-updater...");
    println!("   Refresh interval: {} seconds", json_refresh_interval_secs);
    let running = Arc::new(AtomicBool::new(true));
    let updater_handle = start_json_updater(
        output_dir,
        scenario_name,
        json_refresh_interval_secs,
        running.clone()
    );
    println!("   ✓ JSON updater running\n");
    
    // Step 3: Start HTTP server
    println!("3. Starting HTTP server on port {}...", port);
    println!("   Dashboard: http://localhost:{}", port);
    println!("   ");
    println!("   Server routes:");
    println!("   GET /              → Dashboard HTML");
    println!("   GET /live.js       → Dashboard JavaScript");
    println!("   GET /live_stats.json → Live JSON data (updates every {}s)", json_refresh_interval_secs);
    println!("   ");
    println!("   📊 Open http://localhost:{} in your browser", port);
    println!("   🧪 Run your test: cargo run --bin rust-lt");
    println!("   ");
    println!("   Press Ctrl+C to stop\n");
    
    // Run server (blocking)
    let server_result = serve_simple_http(output_dir, port);
    
    // Cleanup on exit
    running.store(false, Ordering::Relaxed);
    let _ = updater_handle.join();
    
    server_result?;
    Ok(())
}

/// Start background thread to update JSON file periodically
/// This allows real-time dashboard updates during test execution
fn start_json_updater(
    output_dir: &str,
    scenario: &str,
    interval_secs: u64,
    running: Arc<AtomicBool>
) -> thread::JoinHandle<()> {
    let output_dir = output_dir.to_string();
    let scenario = scenario.to_string();
    
    thread::spawn(move || {
        let mut update_count = 0;
        while running.load(Ordering::Relaxed) {
                        let requests_csv = format!("{}/{}_requests.csv", output_dir, scenario);
                        let vu_states_csv = format!("{}/{}_vu_states.csv", output_dir, scenario);
            
                        // Directly read CSV files and build minimal JSON (replaces missing parser.parse())
                        let requests_content = std::fs::read_to_string(&requests_csv);
                        let vu_states_content = std::fs::read_to_string(&vu_states_csv);
            
                        if let Ok(req) = requests_content {
                            // Count non-empty data lines (skip header)
                            let total_requests = req.lines()
                                .skip(1)
                                .filter(|l| !l.trim().is_empty())
                                .count();
            
                            let active_vus = vu_states_content.ok()
                                .map(|vu| {
                                    vu.lines()
                                        .skip(1)
                                        .filter(|l| !l.trim().is_empty())
                                        .count()
                                })
                                .unwrap_or(0);
            
                            let json_path = format!("{}/live_stats.json", output_dir);
                            let json = format!(r#"{{
                "scenario": "{scenario}",
                "current_time": "",
                "vu_state": {{
                    "total_started": {active_vus},
                    "active": {active_vus},
                    "completed": 0,
                    "failed": 0,
                    "success_rate": 0
                }},
                "vu_state_history": [],
                "recent_requests": [],
                "summary": {{
                    "total_requests": {total_requests},
                    "success_count": 0,
                    "fail_count": 0,
                    "success_rate": 0,
                    "avg_response_time": 0,
                    "min_response_time": 0,
                    "max_response_time": 0,
                    "p50": 0,
                    "p95": 0,
                    "p99": 0,
                    "throughput_rps": 0,
                    "total_bytes_sent": 0,
                    "total_bytes_received": 0
                }}
            }}"#);
            
                            match std::fs::write(&json_path, json) {
                                Ok(_) => {
                                    update_count += 1;
                                    if update_count == 1 {
                                        println!("   ✓ First JSON update complete");
                                    }
                                    if update_count % (30 / interval_secs) == 0 {
                                        println!("   📊 JSON updated: {} requests, {} active VUs",
                                            total_requests,
                                            active_vus
                                        );
                                    }
                                }
                                Err(e) => eprintln!("   ✗ Failed to write JSON: {}", e),
                            }
                        } else {
                            // Waiting for first CSVs
                            if update_count == 0 {
                                println!("   ⏳ Waiting for CSV files (start your test)...");
                                update_count = 1;
                            }
                        }
            
            thread::sleep(Duration::from_secs(interval_secs));
        }
        println!("\n   JSON updater stopped");
    })
}

/// Simple HTTP server for dashboard
fn serve_simple_http(output_dir: &str, port: u16) -> std::io::Result<()> {
    use std::net::TcpListener;
    use std::io::prelude::*;
    use std::fs;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        let mut buffer = [0; 2048];
        
        match stream.read(&mut buffer) {
            Ok(_) => {
                let request = String::from_utf8_lossy(&buffer);
                
                // Parse request path
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                
                let (status, content_type, content) = match path {
                    "/" | "/index.html" | "/live.html" => {
                        // Serve dashboard HTML from source
                        match fs::read_to_string("src/engine/reporters/live/templates/live.hbs") {
                            Ok(html) => ("HTTP/1.1 200 OK", "text/html; charset=utf-8", html),
                            Err(e) => {
                                eprintln!("Failed to read live.hbs: {}", e);
                                ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", 
                                 "Failed to load dashboard HTML".to_string())
                            }
                        }
                    }
                    "/live.js" => {
                        // Serve dashboard JavaScript from source
                        match fs::read_to_string("src/engine/reporters/live/templates/live.js") {
                            Ok(js) => ("HTTP/1.1 200 OK", "application/javascript; charset=utf-8", js),
                            Err(e) => {
                                eprintln!("Failed to read live.js: {}", e);
                                ("HTTP/1.1 500 INTERNAL SERVER ERROR", "text/plain", 
                                 "Failed to load dashboard JavaScript".to_string())
                            }
                        }
                    }
                    "/live_stats.json" => {
                        // Serve auto-generated JSON from output directory
                        let json_path = format!("{}/live_stats.json", output_dir);
                        match fs::read_to_string(&json_path) {
                            Ok(json) => ("HTTP/1.1 200 OK", "application/json; charset=utf-8", json),
                            Err(_) => {
                                // Return empty structure if file doesn't exist yet
                                let empty_json = r#"{
                                    "scenario": "waiting",
                                    "current_time": "",
                                    "vu_state": {"total_started": 0, "active": 0, "completed": 0, "failed": 0, "success_rate": 0},
                                    "vu_state_history": [],
                                    "recent_requests": [],
                                    "summary": {
                                        "total_requests": 0, "success_count": 0, "fail_count": 0,
                                        "success_rate": 0, "avg_response_time": 0, "min_response_time": 0,
                                        "max_response_time": 0, "p50": 0, "p95": 0, "p99": 0,
                                        "throughput_rps": 0, "total_bytes_sent": 0, "total_bytes_received": 0
                                    }
                                }"#;
                                ("HTTP/1.1 200 OK", "application/json; charset=utf-8", empty_json.to_string())
                            }
                        }
                    }
                    _ => {
                        ("HTTP/1.1 404 NOT FOUND", "text/plain", format!("404 Not Found: {}", path))
                    }
                };

                let response = format!(
                    "{}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-cache, no-store, must-revalidate\r\n\r\n{}",
                    status,
                    content_type,
                    content.len(),
                    content
                );

                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
            Err(e) => {
                eprintln!("Failed to read from stream: {}", e);
            }
        }
    }

    Ok(())
}
