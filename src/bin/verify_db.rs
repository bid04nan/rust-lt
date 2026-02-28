use std::env;
use duckdb::{Connection, Result};

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path-to-duckdb-file>", args[0]);
        std::process::exit(1);
    }

    let db_path = &args[1];
    println!("Opening DuckDB file: {}", db_path);
    
    let conn = Connection::open(db_path)?;
    println!("✓ Successfully opened DuckDB file");

    // Check requests table
    println!("\n=== REQUESTS TABLE ===");
    let mut stmt = conn.prepare("SELECT COUNT(*) as count FROM requests")?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    println!("Total requests: {}", count);

    if count > 0 {
        println!("\nSample requests (first 5):");
        let mut stmt = conn.prepare("SELECT timestamp, request_name, response_time_ms, status_code, success FROM requests ORDER BY timestamp LIMIT 5")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, i32>(3)?,
                row.get::<_, bool>(4)?,
            ))
        })?;

        for row in rows {
            let (ts, name, rt, status, success) = row?;
            println!("  {} | {} | {}ms | {} | {}", ts, name, rt, status, if success { "✓" } else { "✗" });
        }

        // Statistics
        println!("\nRequest Statistics:");
        let mut stmt = conn.prepare("SELECT AVG(response_time_ms) as avg_rt, MIN(response_time_ms) as min_rt, MAX(response_time_ms) as max_rt, COUNT(*) FILTER (WHERE success = true) as successful FROM requests")?;
        let stats = stmt.query_row([], |row| {
            Ok((
                row.get::<_, f64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        println!("  Avg Response Time: {:.2}ms", stats.0);
        println!("  Min Response Time: {:.2}ms", stats.1);
        println!("  Max Response Time: {:.2}ms", stats.2);
        println!("  Successful Requests: {}", stats.3);
    }

    // Check vu_states table
    println!("\n=== VU_STATES TABLE ===");
    let mut stmt = conn.prepare("SELECT COUNT(*) as count FROM vu_states")?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    println!("Total VU state records: {}", count);

    if count > 0 {
        println!("\nVU State Summary:");
        let mut stmt = conn.prepare("SELECT state, COUNT(*) as count FROM vu_states GROUP BY state")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        for row in rows {
            let (state, count) = row?;
            println!("  {}: {}", state, count);
        }
    }

    // Check metrics table
    println!("\n=== METRICS TABLE ===");
    let mut stmt = conn.prepare("SELECT COUNT(*) as count FROM metrics")?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    println!("Total metric records: {}", count);

    if count > 0 {
        println!("\nMetric types:");
        let mut stmt = conn.prepare("SELECT metric_type, COUNT(*) as count FROM metrics GROUP BY metric_type")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        for row in rows {
            let (metric_type, count) = row?;
            println!("  {}: {}", metric_type, count);
        }
    }

    println!("\n✓ DuckDB file validation complete!");
    
    Ok(())
}
