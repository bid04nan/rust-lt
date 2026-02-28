# Rust-LT Architecture

## Folder Structure (Gatling-inspired)

```
Project Root/
├── scenarios/              # Test scenario definitions (YAML)
│   ├── checkout_test.yaml
│   ├── api_load_test.yaml
│   └── stress_test.yaml
│
├── data/                   # Test data for feeders
│   ├── users.csv
│   ├── products.json
│   └── names.csv
│
├── results/                # Test execution results
│   ├── 2025-11-29_14-30-45/        # Timestamped run folder
│   │   ├── index.html              # HTML report
│   │   ├── simulation.log          # Raw simulation log
│   │   ├── stats.json              # Aggregated statistics
│   │   └── charts/                 # Chart assets
│   └── latest -> 2025-11-29_14-30-45  # Symlink to latest run
│
├── src/
├── engine/
│   ├── mod.rs              # Main orchestration
│   │
│   ├── core/               # Core engine types
│   │   ├── mod.rs
│   │   ├── scenario/       # Scenario definitions
│   │   │   ├── mod.rs
│   │   │   └── load_model.rs    # LoadModel enum (Constant, RampUp, etc)
│   │   ├── session/        # Session management (per-VU state)
│   │   │   ├── mod.rs
│   │   │   ├── variables.rs     # Variable storage and resolution
│   │   │   └── context.rs       # Loop context, connection state
│   │   ├── action/         # Action/FlowStep definitions
│   │   │   ├── mod.rs
│   │   │   ├── flow.rs          # FlowStep enum
│   │   │   ├── pause.rs         # Wait/pause logic
│   │   │   └── control.rs       # Repeat, During, etc
│   │   └── injector/       # User injection profiles
│   │       ├── mod.rs
│   │       ├── closed.rs        # AtOnce, RampUsers
│   │       └── open.rs          # ConstantUsersPerSec, RampUsersPerSec
│   │
│   ├── protocols/          # Protocol-specific implementations
│   │   ├── mod.rs
│   │   ├── http/           # HTTP protocol
│   │   │   ├── mod.rs
│   │   │   ├── client.rs        # HTTP client wrapper
│   │   │   ├── request.rs       # HttpRequest definition
│   │   │   ├── response.rs      # HttpResponse handling
│   │   │   └── config.rs        # HTTP protocol config
│   │   ├── ws/             # WebSocket (future)
│   │   │   └── mod.rs
│   │   ├── jms/            # JMS (future)
│   │   │   └── mod.rs
│   │   └── jdbc/           # JDBC (future)
│   │       └── mod.rs
│   │
│   ├── feeders/            # Data feeders
│   │   ├── mod.rs
│   │   ├── csv.rs               # CSV feeder
│   │   ├── json.rs              # JSON feeder (future)
│   │   └── strategies.rs        # Sequential, Random, Cyclic
│   │
│   ├── checks/             # Validation & extraction
│   │   ├── mod.rs
│   │   ├── validation.rs        # Validation types
│   │   └── extraction.rs        # Extraction types
│   │
│   ├── stats/              # Statistics & aggregation
│   │   ├── mod.rs
│   │   ├── collector.rs         # StatsCollector
│   │   ├── aggregator.rs        # StatisticsAggregator
│   │   └── metrics.rs           # VuMetric, timing types
│   │
│   ├── reporters/          # Reporting & output
│   │   ├── mod.rs
│   │   ├── console/             # Console reporter
│   │   │   ├── mod.rs
│   │   │   └── progress.rs      # Progress bar logic
│   │   ├── html/                # HTML report generation
│   │   │   ├── mod.rs
│   │   │   ├── generator.rs
│   │   │   └── templates/       # Handlebars templates
│   │   │       ├── report.hbs
│   │   │       └── live.hbs
│   │   ├── live/                # Live SSE server
│   │   │   ├── mod.rs
│   │   │   ├── server.rs        # SSE server
│   │   │   └── transform.rs     # Dashboard transformation
│   │   └── simulation_log/      # Gatling-style simulation.log
│   │       └── mod.rs
│   │
│   └── runner/             # VU execution
│       ├── mod.rs
│       └── vu.rs                # VirtualUser implementation
│
├── config/                 # Configuration
│   ├── mod.rs
│   └── app.rs                   # AppConfig
│
└── main.rs
```

## Module Responsibilities

### `core/`
- **scenario/**: YAML schema types (Scenario, LoadModel)
- **session/**: Per-VU session management (variables, state, connections)
- **action/**: FlowStep definitions (HttpRequest, Wait, Repeat, During, etc)
- **injector/**: User injection profiles and scheduling logic

### `protocols/`
- **http/**: HTTP client, request/response types, protocol configuration
- Future: ws, jms, jdbc, redis for additional protocol support

### `feeders/`
- Feeder trait and implementations (CSV, JSON)
- Strategy types (Sequential, Random, Cyclic)

### `checks/`
- Validation and extraction logic
- Status code checks, JSONPath, regex, etc

### `stats/`
- Metric collection and aggregation
- Percentile calculations, windowing

### `reporters/`
- **console/**: Terminal progress and summary
- **html/**: Static HTML report generation
- **live/**: SSE server for real-time dashboard
- **simulation_log/**: Gatling-compatible log output

### `runner/`
- VirtualUser (VU) execution logic
- Per-VU state management

## Top-Level Folders

### `scenarios/`
YAML test scenario definitions. Each file describes:
- Scenario name and base URL
- Load model (constant, ramp_up, constant_users_per_sec, etc)
- Flow steps (HTTP requests, waits, loops)
- Feeders, validations, extractions

**Example**: `scenarios/checkout_test.yaml`

### `data/`
Test data files consumed by feeders:
- CSV files for user data, product catalogs
- JSON files for complex payloads
- Any static test data referenced in scenarios

**Example**: `data/users.csv`, `data/products.json`

### `results/`
Test execution output organized by timestamp:
- **HTML reports**: Interactive dashboards with charts
- **simulation.log**: Gatling-compatible raw log for analysis
- **stats.json**: Aggregated metrics (percentiles, throughput, errors)
- **charts/**: Generated chart images and assets
- **latest symlink**: Points to most recent run for quick access

**Structure**: `results/YYYY-MM-DD_HH-MM-SS/index.html`

## Benefits

1. **Clear separation of concerns**: Each module has a single, well-defined responsibility
2. **Scalability**: Easy to add new protocols (ws, jms) or feeders (db, kafka) without touching core
3. **Testability**: Each module can be tested in isolation
4. **Gatling parity**: Structure mirrors Gatling's organization for easier feature mapping
5. **Maintainability**: Related code lives together; changes are localized

## Next Steps

1. Create folder hierarchy with mod.rs files
2. Move existing types into appropriate modules
3. Update all import paths
4. Add protocol/feeder extensibility through traits
5. Implement additional load profiles and checks
