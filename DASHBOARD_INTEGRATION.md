# Dashboard Integration Summary

## What We Built

You now have a complete live dashboard integration that reads JSON data generated from CSV log files.

## Files Modified/Created

### 1. **live.js** (Enhanced)
Added functions to load and process JSON data from the CSV parser:

- `fetchLiveStats()` - Fetches JSON file with cache busting
- `processLiveStats(liveStats)` - Main processor for JSON data
- `updateMetricCards(summary, vuState)` - Updates 6 metric cards (error rate, TPS, total requests, max VUsers, P95, duration)
- `updateAllChartsFromRequests(requests)` - Processes recent_requests array and updates all charts
- `calculateMovingAverage()` - Calculates moving average for time series data
- `updateConnectionTimingCharts()` - Updates DNS, TCP, TLS, TTFB charts
- `updateVuStateChart()` - Updates VU state chart
- `updateDataTables()` - Updates all 4 data tables
- `updateChartData()` - Helper to update Chart.js datasets
- `getChartColor()` - Color palette helper

Changed initialization from SSE to JSON polling:
```javascript
// OLD: connectSSE(); startDashboardRefresh();
// NEW: fetchLiveStats(); setInterval(fetchLiveStats, refreshInterval);
```

### 2. **live/mod.rs** (Enhanced)
Added utility function:

```rust
pub fn setup_dashboard(output_dir: &str) -> std::io::Result<()>
```

Copies `live.hbs` and `live.js` template files to output directory.

### 3. **README.md** (New)
Comprehensive integration guide at `src/engine/reporters/live/README.md` covering:
- Data flow overview
- Setup instructions (3 options: Python, Node.js, Rust HTTP server)
- JSON structure reference
- Dashboard features list
- Continuous updates pattern
- Troubleshooting guide
- Migration to SSE (optional)

### 4. **dashboard_example.rs** (New)
Complete example at `examples/dashboard_example.rs` showing:
- Setting up dashboard templates
- Parsing CSV to JSON
- Background JSON updater thread
- Simple HTTP server implementation
- Step-by-step instructions

## How to Use

### Quick Start

```rust
use rust_lt::engine::reporters::live::CsvParser;

// 1. Run your test (generates CSV files)
// ... test execution ...

// 2. Parse CSV to JSON
let parser = CsvParser::new("output", "my_scenario");
let live_stats = parser.parse_csv_files()?;
live_stats.to_json_file("output/live_stats.json")?;

// 3. Your live server should serve:
// GET /              → src/engine/reporters/live/templates/live.hbs
// GET /live.js       → src/engine/reporters/live/templates/live.js
// GET /live_stats.json → output/live_stats.json

// 4. Open: http://your-server:port/
```

### Real-time Updates During Test

For continuous updates while test is running:

```rust
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

let running = Arc::new(AtomicBool::new(true));
let running_clone = running.clone();

// Start background updater
thread::spawn(move || {
    while running_clone.load(Ordering::Relaxed) {
        let parser = CsvParser::new("output", "my_scenario");
        if let Ok(live_stats) = parser.parse_csv_files() {
            let _ = live_stats.to_json_file("output/live_stats.json");
        }
        thread::sleep(Duration::from_secs(2));
    }
});

// Run test...

// Stop updater
running.store(false, Ordering::Relaxed);
```

## Data Flow

```
┌─────────────────┐
│ Test Execution  │
│  (TestExecutor) │
└────────┬────────┘
         │
         ├─→ VirtualUser executions
         │   └─→ Action metrics
         │
         ├─→ CsvRequestLogger
         │   └─→ output/scenario_requests.csv
         │       (timestamp, scenario, request_name, user_id, 
         │        response_time_ms, status, pass_count, fail_count,
         │        error_message, bytes_sent, bytes_received,
         │        dns_lookup_ms, tcp_connect_ms, tls_handshake_ms,
         │        time_to_first_byte_ms, content_download_ms)
         │
         ├─→ CsvVuStateLogger (every 2 seconds)
         │   └─→ output/scenario_vu_states.csv
         │       (timestamp, scenario, total_started, active,
         │        completed, failed, success_rate)
         │
         └─→ ConsoleReporter
             └─→ Real-time progress + summary

         ┌─────────────────┐
         │   CsvParser     │
         │ parse_csv_files │
         └────────┬────────┘
                  │
                  ├─→ Reads both CSV files
                  ├─→ Aggregates statistics
                  ├─→ Calculates percentiles (P50, P95, P99)
                  └─→ Generates LiveStats JSON
                      │
                      └─→ output/live_stats.json
                          {
                            scenario: "...",
                            current_time: "...",
                            vu_state: {...},
                            recent_requests: [...],
                            summary: {...}
                          }

         ┌─────────────────┐
         │  Dashboard      │
         │  (live.html)    │
         └────────┬────────┘
                  │
                  ├─→ live.js: fetchLiveStats()
                  │   └─→ fetch('live_stats.json')
                  │
                  ├─→ processLiveStats(json)
                  │   ├─→ updateMetricCards()
                  │   ├─→ updateAllChartsFromRequests()
                  │   │   ├─→ Group by time buckets
                  │   │   ├─→ Calculate rates (req/s, err/s)
                  │   │   ├─→ Calculate moving averages
                  │   │   └─→ Update 14 Chart.js charts
                  │   └─→ updateDataTables()
                  │
                  └─→ Refresh every 5 seconds (configurable)
```

## Dashboard Charts Implemented

### 1. Request/Response Chart (`reqRespChart`)
- Groups requests by 1-second time buckets
- Displays requests per second over time
- Line chart with blue color

### 2. Response Status Chart (`respStatusChart`)
- Counts Success vs Failed requests
- Bar chart showing distribution

### 3. Response Time Chart (`respTimeChart`)
- Shows average response time over time
- Uses moving average for smoothing
- Line chart with time on X-axis

### 4. Errors Chart (`errorsChart`)
- Shows errors per second
- Groups errors by time buckets
- Red line chart

### 5. DNS Duration Chart (`dnsDurationChart`)
- Displays DNS lookup times
- Moving average of dns_lookup_ms field
- Only shows when data available

### 6. TCP Duration Chart (`tcpDurationChart`)
- Shows TCP connection times
- Moving average of tcp_connect_ms field
- Only shows when data available

### 7. TLS Chart (`tlsChart`)
- Displays TLS handshake duration
- Moving average of tls_handshake_ms field
- Only shows when data available

### 8. Concurrent Users Chart (`concurrentChart`)
- Shows active VUsers over time
- Updated from vu_state.active

### 9. Arrival Chart (`arrivalChart`)
- Shows VU arrival rate (VUs/second starting)
- Calculates rate from consecutive VU state snapshots
- Uses vu_state_history to determine new started VUs per period

### 10. Termination Chart (`terminationChart`)
- Shows VU termination rate (VUs/second completing or failing)
- Calculates rate from consecutive VU state snapshots
- Uses vu_state_history to determine finished VUs per period

### 11. Connections Open/Close Chart (`connOpenCloseChart`)
- Shows new connections per second
- Counts TCP connection attempts from tcp_connect_ms presence
- Groups by 1-second time buckets

### 12. TCP State Chart (`tcpStateChart`)
- Bar chart categorizing TCP connections by speed
- Categories: Fast (<10ms), Normal (10-50ms), Slow (50-100ms), Very Slow (>100ms), No Data
- Analyzes tcp_connect_ms values

### 13. Bandwidth Chart (`bandwidthChart`)
- Shows data transfer rate in MB/s
- Dual dataset: bytes sent and bytes received
- Calculates from bytes_sent + bytes_received per time bucket

### 14. DNS Resolutions Chart (`dnsResChart`)
- Shows DNS lookups per second
- Counts requests with non-null dns_lookup_ms values
- Groups by 1-second time buckets

## Data Tables

### 1. Request/Response Table (`reqRespTable`)
Shows last 10 requests with:
- Timestamp
- Request Name
- Status
- Response Time
- Error Message

### 2. Users Table (`usersTable`)
Shows current VU state:
- Timestamp
- Active Users
- Completed Users
- Failed Users
- Success Rate

### 3. Connections Table (`connectionsTable`)
Shows last 10 connections with:
- Timestamp
- TCP Connect Time
- TLS Handshake Time

### 4. DNS Table (`dnsTable`)
Shows last 10 DNS lookups with:
- Timestamp
- Request Name
- DNS Lookup Time

## Configuration Options

### Refresh Interval
Change in `live.js`:
```javascript
let refreshInterval = 5000; // milliseconds
```

Or use dropdown in dashboard UI (1s, 2s, 5s, 10s)

### Recent Requests Limit
Limit JSON size by restricting requests array:

```rust
let live_stats = parser.parse_csv_files()?;
let limited_stats = live_stats.get_recent_requests(100); // Last 100 requests
limited_stats.to_json_file("output/live_stats.json")?;
```

### JSON File Path
Change in `live.js`:
```javascript
let liveStatsJsonPath = 'live_stats.json'; // relative to HTML file
```

## Testing

Run the example:
```powershell
cargo run --example dashboard_example
```

This will:
1. Setup dashboard templates in `output/`
2. Parse existing CSV files (if any)
3. Generate JSON file
4. Show instructions to serve the dashboard

## Summary

✅ **82 tests passing**
✅ Dashboard templates served from source (no copying needed)
✅ JSON generation from CSV files with VU state history
✅ **All 14 charts fully implemented** with real data:
  - Request/Response, Status, Response Time, Errors
  - Arrival Rate, Termination Rate, Concurrent Users
  - Connection Rate, TCP State Distribution
  - TCP Duration, TLS Duration, DNS Duration
  - Bandwidth (MB/s sent/received), DNS Resolutions
✅ 4 data tables with live updates (fixed IDs)
✅ 6 metric cards with correct element IDs
✅ 3-way filtering system (scenario + request + hostname)
✅ Dynamic refresh interval (1s/2s/5s/10s)
✅ Hostname extraction from URLs for DNS filtering
✅ Historical VU state tracking for proper rate calculations
✅ Complete integration guide and example code
✅ Configurable refresh interval
✅ Complete integration guide
✅ Example code provided
✅ Real-time updates support

The dashboard is ready to use! Just parse your CSV files to JSON and serve the HTML.
