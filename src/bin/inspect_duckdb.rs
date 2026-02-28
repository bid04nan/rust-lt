use std::path::PathBuf;
use duckdb::{Connection, Result as DuckResult};
use std::fs;

fn latest_duckdb_file(dir: &str) -> Option<PathBuf> {
    fs::read_dir(dir).ok()?.filter_map(|e| e.ok()).filter(|e| {
        e.path().extension().and_then(|ext| ext.to_str()) == Some("duckdb")
    }).max_by_key(|e| e.metadata().ok().and_then(|m| m.modified().ok()))
      .map(|e| e.path())
}

fn print_table_count(conn: &Connection, table: &str) -> DuckResult<()> {
    let mut stmt = conn.prepare(&format!("SELECT COUNT(*) FROM {}", table))?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    println!("Table {:<10} rows: {}", table, count);
    Ok(())
}

fn print_sample_rows(conn: &Connection, table: &str, cols: &[&str]) -> DuckResult<()> {
    let columns = cols.join(", ");
    let sql = format!("SELECT {} FROM {} LIMIT 5", columns, table);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    println!("Sample from {}:", table);
    while let Some(row) = rows.next()? {
        let mut values = Vec::new();
        for idx in 0..cols.len() {
            let v: DuckResult<String> = row.get(idx);
            values.push(match v { Ok(s) => s, Err(_) => "<unrepr>".to_string() });
        }
        println!("  {}", values.join(" | "));
    }
    Ok(())
}

fn main() -> DuckResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let path = if args.len() > 1 { PathBuf::from(&args[1]) } else { latest_duckdb_file("output").expect("No duckdb files found") };
    println!("Inspecting DuckDB file: {}", path.display());
    let conn = Connection::open(&path)?;

    // Basic tables assumed
    for table in ["requests", "vu_states", "metrics"].iter() {
        if let Err(e) = print_table_count(&conn, table) {
            println!("Table {} not found or error: {}", table, e);
        }
    }

    // Samples (choose likely columns, ignore errors if columns absent)
    let _ = print_sample_rows(&conn, "requests", &["scenario", "request_name", "status_code", "response_time_ms"]);
    let _ = print_sample_rows(&conn, "vu_states", &["scenario", "vu_id", "event", "timestamp_ms"]);
    let _ = print_sample_rows(&conn, "metrics", &["metric_type", "value", "timestamp_ms"]);

    Ok(())
}
