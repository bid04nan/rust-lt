import duckdb

conn = duckdb.connect()

# Check record_type values
result = conn.execute("""
    SELECT record_type, COUNT(*) 
    FROM read_parquet('./test_output/Dashboard_Test_-_Quick_Demo_20251206_190053/segments/*.parquet')
    GROUP BY record_type
""").fetchall()

print("Record types:", result)

# Test the RPS query with cast
rps_query = """
    SELECT 
        strftime(to_timestamp(timestamp_ms / 1000)::TIMESTAMP, '%H:%M:%S') AS ts,
        COUNT(*) AS rps 
    FROM read_parquet('./test_output/Dashboard_Test_-_Quick_Demo_20251206_190053/segments/*.parquet')
    WHERE record_type = 'request'
    GROUP BY (timestamp_ms / 1000)
    ORDER BY (timestamp_ms / 1000)
"""

rps_result = conn.execute(rps_query).fetchall()

print(f"\nRPS query result: {len(rps_result)} time buckets")
for row in rps_result:
    print(f"  {row[0]}: {row[1]} requests")
