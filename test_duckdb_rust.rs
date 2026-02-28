use duckdb::Connection;

fn main() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Test with latest segments directory
    let segments_dir = r"./test_output/Simple_Two-Step_API_Test_20251206_191109/segments";
    
    let query = format!(
        "SELECT 
            strftime(to_timestamp(timestamp_ms / 1000)::TIMESTAMP, '%H:%M:%S') AS ts,
            COUNT(*) AS rps 
         FROM read_parquet('{}/*.parquet') 
         WHERE record_type = 'request'
         GROUP BY (timestamp_ms / 1000)
         ORDER BY (timestamp_ms / 1000)
         LIMIT 5",
        segments_dir
    );
    
    println!("Query: {}", query);
    
    match conn.prepare(&query) {
        Ok(mut stmt) => {
            match stmt.query_map([], |row| {
                let ts: String = row.get(0)?;
                let rps: i64 = row.get(1)?;
                Ok((ts, rps))
            }) {
                Ok(results) => {
                    println!("Results:");
                    for result in results {
                        match result {
                            Ok((ts, rps)) => println!("  {} -> {}", ts, rps),
                            Err(e) => eprintln!("Error processing row: {}", e)
                        }
                    }
                },
                Err(e) => eprintln!("Query execution error: {}", e)
            }
        },
        Err(e) => eprintln!("Query preparation error: {}", e)
    }
}
