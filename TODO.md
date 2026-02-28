# Rust-LT TODO - Future Work

## ✅ Completed Features

### Core Framework
- [x] Virtual User execution engine with lifecycle tracking
- [x] Session management with variable templating
- [x] HTTP protocol with full connection timing breakdown
- [x] Kafka protocol (basic implementation with tests)
- [x] 8 load models (AtOnce, Constant, RampUp, RampRate, ConstantRate, Heaviside, Stages, Poisson)
- [x] User injection scheduling
- [x] CSV/JSON feeders with strategies (Sequential, Random, Cyclic)
- [x] Stats collection with connection timings (DNS, TCP, TLS, TTFB, Content Download)
- [x] **Stats collection optimization - Incremental aggregation (99.9% memory reduction)**
- [x] Console reporter with real-time progress
- [x] CSV logging (requests + VU states every 2s)
- [x] Live dashboard with 14 charts
- [x] 82 unit tests passing

### Live Dashboard
- [x] All 14 charts fully implemented:
  - Request/Response rate, Status distribution, Response time, Errors
  - VU arrival rate, VU termination rate, Concurrent users
  - Connection rate, TCP state distribution
  - TCP/TLS/DNS duration charts
  - Bandwidth (MB/s), DNS resolutions per second
- [x] 3-way filtering (scenario + request + hostname)
- [x] Dynamic refresh interval (1s/2s/5s/10s)
- [x] 6 metric cards with real-time updates
- [x] 4 data tables (last 10 entries)
- [x] Hostname extraction from URLs
- [x] VU state history tracking
- [x] JSON generation from CSV logs
- [x] Complete integration guide

## 🔧 Protocol Implementations (Future Work)

### WebSocket Protocol
**Status:** Placeholder with API structure  
**Priority:** Medium  
**Dependencies:** `tokio-tungstenite` (already in Cargo.toml)

**Tasks:**
- [ ] Implement actual WebSocket connection in `WsClient::connect()`
- [ ] Add WebSocket stream handling in `WsConnection`
- [ ] Implement `send_text()` and `send_binary()` methods
- [ ] Implement `receive()` method with message parsing
- [ ] Add connection state tracking
- [ ] Add ping/pong handling for keep-alive
- [ ] Implement proper close handshake
- [ ] Add WebSocket-specific metrics (frames sent/received, connection duration)
- [ ] Add WebSocket action to `FlowStep` enum in scenario module
- [ ] Create WebSocket-specific checks and extractions
- [ ] Add WebSocket examples to `scenarios/websocket_chat.yaml`
- [ ] Add integration tests with mock WebSocket server

**Files to modify:**
- `src/engine/protocols/ws/mod.rs`
- `src/engine/core/action/mod.rs` (add WS action executor)
- `src/engine/core/scenario/mod.rs` (add WS flow step)
- `scenarios/websocket_chat.yaml` (already has example structure)

### JDBC/Database Protocol
**Status:** Placeholder with API structure  
**Priority:** Medium  
**Dependencies:** Need to enable `sqlx` in Cargo.toml (currently commented out)

**Tasks:**
- [ ] Uncomment and configure `sqlx` in Cargo.toml
- [ ] Choose supported databases (PostgreSQL, MySQL, SQLite)
- [ ] Implement `JdbcConnection::connect()` using sqlx
- [ ] Add connection pooling for better performance
- [ ] Implement `execute_query()` for SELECT statements
- [ ] Implement `execute_update()` for INSERT/UPDATE/DELETE
- [ ] Implement `execute_prepared()` for parameterized queries
- [ ] Add transaction support (begin, commit, rollback)
- [ ] Add result set parsing to HashMap
- [ ] Add JDBC-specific metrics (query time, rows affected, connection time)
- [ ] Implement JDBC feeder in `feeders/jdbc.rs` (currently has TODO)
- [ ] Add JDBC action to `FlowStep` enum
- [ ] Create JDBC-specific checks (rows affected, result set validation)
- [ ] Add integration tests with test database
- [ ] Add examples for different database types

**Files to modify:**
- `Cargo.toml` (uncomment sqlx with features)
- `src/engine/protocols/jdbc/mod.rs`
- `src/engine/feeders/jdbc.rs` (remove TODO, implement actual DB query)
- `src/engine/core/action/mod.rs` (add JDBC action executor)
- `src/engine/core/scenario/mod.rs` (add JDBC flow step)
- `scenarios/database_jdbc.yaml` (already has example structure)

### JMS Protocol
**Status:** Placeholder with API structure  
**Priority:** Low  
**Dependencies:** Need Java bindings or pure Rust implementation

**Tasks:**
- [ ] Research JMS implementation options:
  - Option 1: JNI bindings to Java ActiveMQ/RabbitMQ
  - Option 2: Use AMQP protocol directly (RabbitMQ)
  - Option 3: Use STOMP protocol (ActiveMQ compatible)
- [ ] Add appropriate dependency to Cargo.toml
- [ ] Implement `JmsConnectionFactory::create_connection()`
- [ ] Implement `JmsConnection::create_session()`
- [ ] Add queue operations (send, receive)
- [ ] Add topic operations (publish, subscribe)
- [ ] Add message types (Text, Bytes, Object, Map)
- [ ] Add JMS-specific metrics (message latency, queue depth)
- [ ] Add JMS action to `FlowStep` enum
- [ ] Create JMS-specific checks
- [ ] Add integration tests
- [ ] Add examples

**Files to modify:**
- `Cargo.toml` (add JMS/AMQP/STOMP dependency)
- `src/engine/protocols/jms/mod.rs`
- `src/engine/core/action/mod.rs` (add JMS action executor)
- `src/engine/core/scenario/mod.rs` (add JMS flow step)
- `scenarios/jms_queue.yaml` (already has example structure)

## 📊 Reporting Enhancements

### HTML Static Report Generator
**Priority:** Medium

**Tasks:**
- [ ] Create HTML report template with embedded CSS/JS
- [ ] Generate static HTML file from TestResult
- [ ] Include all 14 charts as static images or Chart.js
- [ ] Add summary tables (requests, users, connections, DNS)
- [ ] Add test configuration section
- [ ] Add error details section
- [ ] Add percentile distribution charts (P50, P75, P90, P95, P99)
- [ ] Support multiple scenario comparison
- [ ] Add export to PDF functionality

**Files to create:**
- `src/engine/reporters/html/mod.rs`
- `src/engine/reporters/html/templates/report.html`
- `src/engine/reporters/html/templates/report.css`

### Gatling-Compatible Simulation Log
**Priority:** Low

**Tasks:**
- [ ] Implement simulation.log format writer
- [ ] Support Gatling log format specification
- [ ] Enable import into Gatling reports
- [ ] Add timestamp conversion utilities

**Files to create:**
- `src/engine/reporters/gatling/mod.rs`

### Live Dashboard Server Integration
**Priority:** High

**Tasks:**
- [ ] Create embedded HTTP server for dashboard
- [ ] Add Server-Sent Events (SSE) support as alternative to polling
- [ ] Auto-update JSON during test execution
- [ ] Add WebSocket support for real-time updates
- [ ] Add authentication/authorization
- [ ] Support multiple concurrent test sessions
- [ ] Add test control API (start/stop/pause)

**Files to create:**
- `src/engine/reporters/live/server.rs`
- Update `examples/dashboard_example.rs` with embedded server

## 🔌 Feeder Enhancements

### Redis Feeder
**Status:** Placeholder structure exists  
**Priority:** Low  
**Dependencies:** Need to enable `redis` in Cargo.toml (currently commented out)

**Tasks:**
- [ ] Uncomment redis dependency in Cargo.toml
- [ ] Implement `RedisFeeder::load()` in `feeders/redis.rs`
- [ ] Add Redis connection pool
- [ ] Support Redis data structures (strings, lists, sets, hashes)
- [ ] Add Redis commands (GET, LRANGE, SMEMBERS, HGETALL)
- [ ] Add connection timeout handling
- [ ] Add integration tests with Redis mock

**Files to modify:**
- `Cargo.toml` (uncomment redis)
- `src/engine/feeders/redis.rs`

### Kafka Feeder
**Status:** Placeholder structure exists  
**Priority:** Low  
**Dependencies:** Need to enable `rdkafka` in Cargo.toml (currently commented out)

**Tasks:**
- [ ] Uncomment rdkafka dependency in Cargo.toml
- [ ] Implement `KafkaFeeder::load()` in `feeders/kafka.rs`
- [ ] Add Kafka consumer configuration
- [ ] Support multiple topic subscription
- [ ] Add offset management
- [ ] Add consumer group support
- [ ] Add integration tests

**Files to modify:**
- `Cargo.toml` (uncomment rdkafka)
- `src/engine/feeders/kafka.rs`

### Additional Feeder Types
**Priority:** Low

**Tasks:**
- [ ] Add HTTP API feeder (fetch data from REST endpoint)
- [ ] Add gRPC feeder
- [ ] Add MongoDB feeder
- [ ] Add Elasticsearch feeder
- [ ] Add random data generator feeder

## 🎯 Action & Check Enhancements

### Additional HTTP Features
**Priority:** Medium

**Tasks:**
- [ ] Add multipart/form-data support
- [ ] Add file upload support
- [ ] Add cookie management improvements
- [ ] Add OAuth 2.0 flow support
- [ ] Add JWT token handling
- [ ] Add rate limiting simulation
- [ ] Add retry logic with exponential backoff
- [ ] Add circuit breaker pattern

### Response Validation Enhancements
**Priority:** Medium

**Tasks:**
- [ ] Add JSONPath check support (currently has structure but not implemented)
- [ ] Add XPath check support for XML responses
- [ ] Add regex capture groups for extraction
- [ ] Add CSS selector support for HTML responses
- [ ] Add schema validation (JSON Schema, XML Schema)
- [ ] Add custom validation functions

## 🧪 Testing & Quality

### Test Coverage
**Priority:** High

**Tasks:**
- [ ] Add integration tests for end-to-end scenarios
- [ ] Add performance benchmarks
- [ ] Add load tests for the framework itself (meta-testing)
- [ ] Increase unit test coverage to 90%+
- [ ] Add property-based testing with proptest
- [ ] Add chaos testing scenarios

### CI/CD Pipeline
**Priority:** High

**Tasks:**
- [ ] Setup GitHub Actions workflow
- [ ] Add automated testing on push
- [ ] Add code coverage reporting
- [ ] Add linting (clippy) enforcement
- [ ] Add formatting (rustfmt) checks
- [ ] Add security audit (cargo-audit)
- [ ] Add dependency update checks (dependabot)
- [ ] Setup automated release process

## 📚 Documentation

### User Documentation
**Priority:** High

**Tasks:**
- [ ] Create comprehensive README.md with quick start guide
- [ ] Add API documentation with rustdoc
- [ ] Create user guide with examples
- [ ] Add tutorial series (beginner to advanced)
- [ ] Create scenario authoring guide
- [ ] Add best practices guide
- [ ] Create FAQ section
- [ ] Add troubleshooting guide

### Developer Documentation
**Priority:** Medium

**Tasks:**
- [ ] Document architecture decisions (ADR)
- [ ] Create contribution guidelines
- [ ] Add code style guide
- [ ] Document testing strategy
- [ ] Create release process documentation
- [ ] Add module-level documentation

## 🚀 Performance & Optimization

### Runtime Performance
**Priority:** Medium

**Tasks:**
- [ ] Profile CPU usage under load
- [ ] Optimize memory allocations
- [ ] Add connection pooling for HTTP client
- [ ] Implement request batching where applicable
- [ ] Add zero-copy optimizations
- [ ] Consider using `async-std` as alternative to tokio
- [ ] Benchmark against Gatling and K6

### Scalability
**Priority:** Medium

**Tasks:**
- [ ] Add distributed execution support
- [ ] Implement master-worker architecture
- [ ] Add result aggregation from multiple workers
- [ ] Support cloud deployment (AWS, GCP, Azure)
- [ ] Add container orchestration support (Kubernetes)

## 🔒 Security

**Priority:** Medium

**Tasks:**
- [ ] Add TLS certificate validation options
- [ ] Add client certificate support
- [ ] Add secrets management integration
- [ ] Implement secure credential storage
- [ ] Add audit logging
- [ ] Security hardening review

## 🛠️ CLI & Usability

### Command Line Interface
**Priority:** High

**Tasks:**
- [ ] Enhance CLI with clap (already in dependencies)
- [ ] Add subcommands (run, validate, report, dashboard)
- [ ] Add verbose/quiet logging options
- [ ] Add dry-run mode
- [ ] Add scenario validation without execution
- [ ] Add interactive mode
- [ ] Add shell completion scripts

### Configuration
**Priority:** Medium

**Tasks:**
- [ ] Add global configuration file support
- [ ] Add environment variable support
- [ ] Add configuration validation
- [ ] Add configuration templates
- [ ] Support configuration inheritance

## 📦 Packaging & Distribution

**Priority:** Medium

**Tasks:**
- [ ] Publish to crates.io
- [ ] Create binary releases for Windows/Linux/macOS
- [ ] Add Homebrew formula
- [ ] Add Chocolatey package
- [ ] Add Docker image
- [ ] Create VS Code extension for scenario editing
- [ ] Create IntelliJ IDEA plugin

## 🔄 Backwards Compatibility

**Priority:** Low

**Tasks:**
- [ ] Add Gatling scenario import
- [ ] Add JMeter scenario import
- [ ] Add K6 scenario import
- [ ] Add result export formats (JUnit XML, CSV, JSON)

## 📝 Known Issues & Bugs

**Current Issues:**
- None reported

**Technical Debt:**
- Clean up unused imports (37 warnings in last test run)
- Remove dead code warnings (fields never read)
- Add proper error types instead of Box<dyn Error>
- Consider replacing anyhow with thiserror throughout
- Standardize on one async trait pattern

## 🎓 Learning & Examples

**Priority:** High

**Tasks:**
- [ ] Create example scenarios for common use cases:
  - REST API testing
  - GraphQL testing
  - gRPC testing
  - WebSocket real-time apps
  - Database load testing
  - Message queue testing
- [ ] Add video tutorials
- [ ] Create blog posts
- [ ] Present at conferences/meetups

---

## Priority Legend
- **High**: Critical for v1.0 release
- **Medium**: Important but can be deferred
- **Low**: Nice to have, future consideration

## Version Roadmap

### v0.1.0 (Current - MVP)
- ✅ Core VU execution
- ✅ HTTP protocol
- ✅ Basic stats & reporting
- ✅ CSV feeders
- ✅ Console output
- ✅ Live dashboard with 14 charts

### v0.2.0 (Next Release)
- [ ] Enhanced CLI
- [ ] HTML static reports
- [ ] WebSocket protocol
- [ ] Embedded dashboard server
- [ ] Comprehensive documentation
- [ ] CI/CD pipeline

### v0.3.0
- [ ] JDBC protocol
- [ ] Redis/Kafka feeders
- [ ] Distributed execution
- [ ] Performance optimizations

### v1.0.0
- [ ] Production-ready
- [ ] All core protocols implemented
- [ ] Full documentation
- [ ] Published to crates.io
- [ ] Binary releases

---

**Last Updated:** 2025-11-30  
**Status:** 82/82 tests passing, dashboard 100% complete
