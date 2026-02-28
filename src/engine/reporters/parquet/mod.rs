use std::sync::{mpsc::{self, Sender, Receiver}, Arc, RwLock};
use std::thread;
use chrono::{DateTime, Local};
use serde::{Serialize, Deserialize};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use arrow_schema::{Schema, Field, DataType};
use arrow_array::{RecordBatch, ArrayRef, Int64Array, Float64Array, StringArray};
use arrow_array::builder::{Int64Builder, Float64Builder, StringBuilder, Int32Builder};
use arrow_array::Array;
use std::fs::File;
// removed std::io::Write import (no longer needed)

#[derive(Debug)]
pub enum ParquetError {
    Io(std::io::Error),
    Arrow(arrow_schema::ArrowError),
    Parquet(parquet::errors::ParquetError),
}

pub type PqResult<T> = Result<T, ParquetError>;

impl From<std::io::Error> for ParquetError { fn from(e: std::io::Error) -> Self { ParquetError::Io(e) } }
impl From<arrow_schema::ArrowError> for ParquetError { fn from(e: arrow_schema::ArrowError) -> Self { ParquetError::Arrow(e) } }
impl From<parquet::errors::ParquetError> for ParquetError { fn from(e: parquet::errors::ParquetError) -> Self { ParquetError::Parquet(e) } }

impl std::fmt::Display for ParquetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetError::Io(e) => write!(f, "IO error: {}", e),
            ParquetError::Arrow(e) => write!(f, "Arrow error: {}", e),
            ParquetError::Parquet(e) => write!(f, "Parquet error: {}", e),
        }
    }
}

impl std::error::Error for ParquetError {}

#[derive(Clone)]
pub struct ParquetLogger {
    sender: Sender<LogMessage>,
    scenario_name: String,
    run_id: String,
    /// Directory containing rotated segment parquet files
    segments_dir: String,
}

// Global logger (mirrors previous DuckDB interface)
static GLOBAL_LOGGER: RwLock<Option<Arc<ParquetLogger>>> = RwLock::new(None);

pub fn get_global_logger() -> Option<Arc<ParquetLogger>> {
    GLOBAL_LOGGER.read().ok().and_then(|g| g.as_ref().map(Arc::clone))
}

pub fn set_global_logger(logger: Arc<ParquetLogger>) {
    if let Ok(mut g) = GLOBAL_LOGGER.write() { *g = Some(logger); }
}

#[derive(Debug)]
enum LogMessage {
    Request {
        ts: DateTime<Local>,
        name: String,
        hostname: Option<String>,
        user_id: usize,
        rt_ms: f64,
        ttfb_ms: Option<f64>,
        dns_lookup_ms: Option<f64>,
        tcp_connect_ms: Option<f64>,
        tls_handshake_ms: Option<f64>,
        status: String,
        status_code: Option<i32>,
        err: String,
        bytes_sent: usize,
        bytes_recv: usize,
    },
    VuState {
        ts: DateTime<Local>,
        user_id: usize,
        state: String,
        duration_ms: Option<i64>,
    },
    Metric {
        ts: DateTime<Local>,
        metric_type: String,
        metric_name: String,
        value: f64,
        user_id: Option<usize>,
        tags: Option<String>,
    },
    Flush,
}

fn build_schema() -> Schema {
    Schema::new(vec![
        Field::new("record_type", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("scenario", DataType::Utf8, false),
        // request fields
        Field::new("request_name", DataType::Utf8, true),
        Field::new("hostname", DataType::Utf8, true),
        Field::new("user_id", DataType::Int32, true),
        Field::new("response_time_ms", DataType::Float64, true),
        Field::new("ttfb_ms", DataType::Float64, true),
        Field::new("dns_lookup_ms", DataType::Float64, true),
        Field::new("tcp_connect_ms", DataType::Float64, true),
        Field::new("tls_handshake_ms", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("status_code", DataType::Int32, true),
        Field::new("error_message", DataType::Utf8, true),
        Field::new("bytes_sent", DataType::Int32, true),
        Field::new("bytes_received", DataType::Int32, true),
        // vu state
        Field::new("vu_state", DataType::Utf8, true),
        Field::new("vu_duration_ms", DataType::Int64, true),
        // metrics
        Field::new("metric_type", DataType::Utf8, true),
        Field::new("metric_name", DataType::Utf8, true),
        Field::new("metric_value", DataType::Float64, true),
        Field::new("metric_user_id", DataType::Int32, true),
        Field::new("metric_tags", DataType::Utf8, true),
    ])
}

impl ParquetLogger {
    pub fn new(output_dir: &str, scenario_name: String) -> PqResult<Arc<Self>> {
        std::fs::create_dir_all(output_dir)?;
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let run_id = format!("{}_{}", scenario_name.replace(" ", "_"), timestamp);
        let run_dir = format!("{}/{}_{}", output_dir, scenario_name.replace(" ", "_"), timestamp);
        let segments_dir = format!("{}/segments", run_dir);
        std::fs::create_dir_all(&segments_dir)?;

        let (tx, rx): (Sender<LogMessage>, Receiver<LogMessage>) = mpsc::channel();
        let scenario_clone = scenario_name.clone();
        let segments_dir_clone = segments_dir.clone();
        // rotation state (segment counter)
        let mut segment_counter: u64 = 0;
        let rotate_interval_ms: u64 = 2000; // 2s rotation
        thread::spawn(move || {
            let schema = build_schema();
            let mut writer: Option<ArrowWriter<File>> = None;
            let mut active_segment_path: Option<String> = None;
            let mut last_rotate = std::time::Instant::now();

            // open initial segment
            segment_counter += 1;
            {
                let segment_name = format!("segment_{}_{}.parquet", segment_counter, Local::now().format("%H%M%S"));
                let final_path = format!("{}/{}", segments_dir_clone, segment_name);
                let temp_path = format!("{}.tmp", final_path);
                if let Ok(file) = File::create(&temp_path) {
                    let props = WriterProperties::builder().build();
                    active_segment_path = Some(final_path.clone());
                    writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).ok();
                }
            }

            // builders for batch buffering
            let mut record_type = StringBuilder::new();
            let mut timestamp_ms = Int64Builder::new();
            let mut scenario = StringBuilder::new();
            let mut request_name = StringBuilder::new();
            let mut hostname = StringBuilder::new();
            let mut user_id = Int32Builder::new();
            let mut response_time_ms = Float64Builder::new();
            let mut ttfb_ms = Float64Builder::new();
            let mut dns_lookup_ms = Float64Builder::new();
            let mut tcp_connect_ms = Float64Builder::new();
            let mut tls_handshake_ms = Float64Builder::new();
            let mut status = StringBuilder::new();
            let mut status_code = Int32Builder::new();
            let mut error_message = StringBuilder::new();
            let mut bytes_sent = Int32Builder::new();
            let mut bytes_received = Int32Builder::new();
            let mut vu_state = StringBuilder::new();
            let mut vu_duration_ms = Int64Builder::new();
            let mut metric_type = StringBuilder::new();
            let mut metric_name = StringBuilder::new();
            let mut metric_value = Float64Builder::new();
            let mut metric_user_id = Int32Builder::new();
            let mut metric_tags = StringBuilder::new();

            let mut count: usize = 0;
            const FLUSH_EVERY: usize = 10; // small batch for responsiveness
            const WRITE_INTERVAL_MS: u64 = 1000; // time-based flush every 1s
            let mut last_write = std::time::Instant::now();

            while let Ok(msg) = rx.recv() {
                let is_flush = matches!(msg, LogMessage::Flush);
                match msg {
                    LogMessage::Request { ts, name, hostname: host_opt, user_id: uid, rt_ms, ttfb_ms: ttfb_opt, dns_lookup_ms: dns_opt, tcp_connect_ms: tcp_opt, tls_handshake_ms: tls_opt, status: st, status_code: sc_opt, err, bytes_sent: bs, bytes_recv: br } => {
                        record_type.append_value("request");
                        timestamp_ms.append_value(ts.timestamp_millis());
                        scenario.append_value(&scenario_clone);
                        request_name.append_value(&name);
                        match host_opt { Some(h) => hostname.append_value(&h), None => hostname.append_null() };
                        user_id.append_value(uid as i32);
                        response_time_ms.append_value(rt_ms);
                        match ttfb_opt { Some(v) => ttfb_ms.append_value(v), None => ttfb_ms.append_null() };
                        match dns_opt { Some(v) => dns_lookup_ms.append_value(v), None => dns_lookup_ms.append_null() };
                        match tcp_opt { Some(v) => tcp_connect_ms.append_value(v), None => tcp_connect_ms.append_null() };
                        match tls_opt { Some(v) => tls_handshake_ms.append_value(v), None => tls_handshake_ms.append_null() };
                        status.append_value(&st);
                        match sc_opt { Some(v) => status_code.append_value(v), None => status_code.append_null() };
                        error_message.append_value(&err);
                        bytes_sent.append_value(bs as i32);
                        bytes_received.append_value(br as i32);
                        vu_state.append_null();
                        vu_duration_ms.append_null();
                        metric_type.append_null();
                        metric_name.append_null();
                        metric_value.append_null();
                        metric_user_id.append_null();
                        metric_tags.append_null();
                        count += 1;
                    }
                    LogMessage::VuState { ts, user_id: uid, state, duration_ms } => {
                        record_type.append_value("vu_state");
                        timestamp_ms.append_value(ts.timestamp_millis());
                        scenario.append_value(&scenario_clone);
                        request_name.append_null();
                        hostname.append_null();
                        user_id.append_value(uid as i32);
                        response_time_ms.append_null();
                        ttfb_ms.append_null();
                        dns_lookup_ms.append_null();
                        tcp_connect_ms.append_null();
                        tls_handshake_ms.append_null();
                        status.append_null();
                        status_code.append_null();
                        error_message.append_null();
                        bytes_sent.append_null();
                        bytes_received.append_null();
                        vu_state.append_value(&state);
                        match duration_ms { Some(v) => vu_duration_ms.append_value(v), None => vu_duration_ms.append_null() };
                        metric_type.append_null();
                        metric_name.append_null();
                        metric_value.append_null();
                        metric_user_id.append_null();
                        metric_tags.append_null();
                        count += 1;
                    }
                    LogMessage::Metric { ts, metric_type: mt, metric_name: mn, value, user_id: uid, tags } => {
                        record_type.append_value("metric");
                        timestamp_ms.append_value(ts.timestamp_millis());
                        scenario.append_value(&scenario_clone);
                        request_name.append_null();
                        hostname.append_null();
                        match uid { Some(u) => user_id.append_value(u as i32), None => user_id.append_null() };
                        response_time_ms.append_null();
                        ttfb_ms.append_null();
                        dns_lookup_ms.append_null();
                        tcp_connect_ms.append_null();
                        tls_handshake_ms.append_null();
                        status.append_null();
                        status_code.append_null();
                        error_message.append_null();
                        bytes_sent.append_null();
                        bytes_received.append_null();
                        vu_state.append_null();
                        vu_duration_ms.append_null();
                        metric_type.append_value(&mt);
                        metric_name.append_value(&mn);
                        metric_value.append_value(value);
                        match tags { Some(t) => metric_tags.append_value(&t), None => metric_tags.append_null() };
                        metric_user_id.append_null();
                        count += 1;
                    }
                    LogMessage::Flush => {}
                }

                let should_time_flush = last_write.elapsed().as_millis() as u64 >= WRITE_INTERVAL_MS;
                let should_rotate = last_rotate.elapsed().as_millis() as u64 >= rotate_interval_ms;
                if count >= FLUSH_EVERY || should_time_flush || is_flush {
                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(record_type.finish()),
                        Arc::new(timestamp_ms.finish()),
                        Arc::new(scenario.finish()),
                        Arc::new(request_name.finish()),
                        Arc::new(hostname.finish()),
                        Arc::new(user_id.finish()),
                        Arc::new(response_time_ms.finish()),
                        Arc::new(ttfb_ms.finish()),
                        Arc::new(dns_lookup_ms.finish()),
                        Arc::new(tcp_connect_ms.finish()),
                        Arc::new(tls_handshake_ms.finish()),
                        Arc::new(status.finish()),
                        Arc::new(status_code.finish()),
                        Arc::new(error_message.finish()),
                        Arc::new(bytes_sent.finish()),
                        Arc::new(bytes_received.finish()),
                        Arc::new(vu_state.finish()),
                        Arc::new(vu_duration_ms.finish()),
                        Arc::new(metric_type.finish()),
                        Arc::new(metric_name.finish()),
                        Arc::new(metric_value.finish()),
                        Arc::new(metric_user_id.finish()),
                        Arc::new(metric_tags.finish()),
                    ];
                    if let Some(ref mut w) = writer {
                        if let Ok(batch) = RecordBatch::try_new(Arc::new(schema.clone()), arrays) {
                            let _ = w.write(&batch);
                            if let Some(p) = &active_segment_path {
                                eprintln!("[parquet] wrote batch {} rows -> {}", batch.num_rows(), p);
                            }
                        }
                    }
                    // rebuild builders
                    record_type = StringBuilder::new();
                    timestamp_ms = Int64Builder::new();
                    scenario = StringBuilder::new();
                    request_name = StringBuilder::new();
                    hostname = StringBuilder::new();
                    user_id = Int32Builder::new();
                    response_time_ms = Float64Builder::new();
                    ttfb_ms = Float64Builder::new();
                    dns_lookup_ms = Float64Builder::new();
                    tcp_connect_ms = Float64Builder::new();
                    tls_handshake_ms = Float64Builder::new();
                    status = StringBuilder::new();
                    status_code = Int32Builder::new();
                    error_message = StringBuilder::new();
                    bytes_sent = Int32Builder::new();
                    bytes_received = Int32Builder::new();
                    vu_state = StringBuilder::new();
                    vu_duration_ms = Int64Builder::new();
                    metric_type = StringBuilder::new();
                    metric_name = StringBuilder::new();
                    metric_value = Float64Builder::new();
                    metric_user_id = Int32Builder::new();
                    metric_tags = StringBuilder::new();
                    count = 0;
                    last_write = std::time::Instant::now();

                    // Handle rotation (finalize current segment, rename temp -> final)
                    if should_rotate && !is_flush {
                        if let Some(w) = writer.take() {
                            let _ = w.close();
                            if let Some(ref final_path) = active_segment_path {
                                let temp_path = format!("{}.tmp", final_path);
                                let _ = std::fs::rename(&temp_path, final_path);
                                eprintln!("[parquet] rotated segment finalized: {}", final_path);
                            }
                        }
                        // open next segment
                        segment_counter += 1;
                        let segment_name = format!("segment_{}_{}.parquet", segment_counter, Local::now().format("%H%M%S"));
                        let final_path = format!("{}/{}", segments_dir_clone, segment_name);
                        let temp_path = format!("{}.tmp", final_path);
                        if let Ok(file) = File::create(&temp_path) {
                            let props = WriterProperties::builder().build();
                            active_segment_path = Some(final_path.clone());
                            writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props)).ok();
                        }
                        last_rotate = std::time::Instant::now();
                    }

                    // On explicit Flush, finalize current segment and exit
                    if is_flush {
                        if let Some(w) = writer.take() {
                            let _ = w.close();
                            if let Some(ref final_path) = active_segment_path {
                                let temp_path = format!("{}.tmp", final_path);
                                let _ = std::fs::rename(&temp_path, final_path);
                                eprintln!("[parquet] final flush segment: {}", final_path);
                            }
                        }
                        break;
                    }
                }
            }

            // Ensure writer is closed & segment moved if channel closed
            if let Some(w) = writer {
                let _ = w.close();
                if let Some(ref final_path) = active_segment_path {
                    let temp_path = format!("{}.tmp", final_path);
                    let _ = std::fs::rename(&temp_path, final_path);
                    eprintln!("[parquet] channel closed, finalized segment: {}", final_path);
                }
            }
        });

        let logger = Self { sender: tx, scenario_name: scenario_name.clone(), run_id: run_id.clone(), segments_dir: segments_dir.clone() };
        let arc_logger = Arc::new(logger);
        set_global_logger(Arc::clone(&arc_logger));
        Ok(arc_logger)
    }

    pub fn segments_dir(&self) -> &str { &self.segments_dir }
    pub fn run_id(&self) -> &str { &self.run_id }

    pub fn log_request(&self,
        timestamp: DateTime<Local>,
        request_name: &str,
        hostname: Option<&str>,
        user_id: usize,
        response_time_ms: f64,
        ttfb_ms: Option<f64>,
        dns_lookup_ms: Option<f64>,
        tcp_connect_ms: Option<f64>,
        tls_handshake_ms: Option<f64>,
        status: &str,
        status_code: Option<i32>,
        error_message: &str,
        bytes_sent: usize,
        bytes_received: usize,
    ) -> PqResult<()> {
        let _ = self.sender.send(LogMessage::Request {
            ts: timestamp,
            name: request_name.to_string(),
            hostname: hostname.map(|h| h.to_string()),
            user_id,
            rt_ms: response_time_ms,
            ttfb_ms,
            dns_lookup_ms,
            tcp_connect_ms,
            tls_handshake_ms,
            status: status.to_string(),
            status_code,
            err: error_message.to_string(),
            bytes_sent,
            bytes_recv: bytes_received,
        });
        Ok(())
    }

    pub fn log_vu_state(&self,
        timestamp: DateTime<Local>,
        user_id: usize,
        state: &str,
        duration_ms: Option<i64>,
    ) -> PqResult<()> {
        let _ = self.sender.send(LogMessage::VuState {
            ts: timestamp,
            user_id,
            state: state.to_string(),
            duration_ms,
        });
        Ok(())
    }

    pub fn log_metric(&self,
        timestamp: DateTime<Local>,
        metric_type: &str,
        metric_name: &str,
        value: f64,
        user_id: Option<usize>,
        tags: Option<&str>,
    ) -> PqResult<()> {
        let _ = self.sender.send(LogMessage::Metric {
            ts: timestamp,
            metric_type: metric_type.to_string(),
            metric_name: metric_name.to_string(),
            value,
            user_id,
            tags: tags.map(|t| t.to_string()),
        });
        Ok(())
    }

    pub fn flush(&self) { let _ = self.sender.send(LogMessage::Flush); }
}

// Minimal in-Rust live stats reader without DuckDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStats {
    pub scenario: String,
    pub current_time: String,
    pub total_requests: usize,
    pub success_count: usize,
    pub fail_count: usize,
    pub avg_response_time: f64,
    pub throughput_rps: f64,
    pub requests_per_minute: f64,
    pub active_vus: usize,
    pub total_vus_started: usize,
    pub total_vus_completed: usize,
    pub success_rate: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

pub fn read_live_stats(segments_dir: &str) -> PqResult<LiveStats> {
    // Aggregate across all finalized segment parquet files in directory
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut total = 0usize;
    let mut success = 0usize;
    let mut fail = 0usize;
    let mut sum_rt = 0.0f64;
    let mut digest = tdigest::TDigest::new_with_size(100);
    let mut first_ts: Option<i64> = None;
    let mut last_ts: Option<i64> = None;
    let mut active_vus = 0usize;
    let mut vus_started = 0usize;
    let mut vus_completed = 0usize;

    // Iterate segment files
    if let Ok(entries) = std::fs::read_dir(segments_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() || path.extension().and_then(|s| s.to_str()) != Some("parquet") { continue; }
            if let Ok(file) = File::open(&path) {
                if let Ok(builder) = ParquetRecordBatchReaderBuilder::try_new(file) {
                    if let Ok(mut reader) = builder.build() {
                        while let Some(Ok(batch)) = reader.next() {
                            let cols = batch.columns();
                            let record_type = cols[0].as_any().downcast_ref::<StringArray>().unwrap();
                            let status = cols[10].as_any().downcast_ref::<StringArray>().unwrap();
                            let rt = cols[5].as_any().downcast_ref::<Float64Array>().unwrap(); // response_time_ms
                            let ts_col = cols[1].as_any().downcast_ref::<Int64Array>().unwrap();
                            let vu_state_col = cols[15].as_any().downcast_ref::<StringArray>().unwrap();
                            for i in 0..batch.num_rows() {
                                match record_type.value(i) {
                                    "request" => {
                                        total += 1;
                                        let st = if status.is_null(i) { "" } else { status.value(i) };
                                        if st == "success" { success += 1; } else { fail += 1; }
                                        if !rt.is_null(i) {
                                            let v = rt.value(i);
                                            sum_rt += v;
                                            digest = digest.merge_unsorted(vec![v]);
                                        }
                                        let ts_v = ts_col.value(i);
                                        if first_ts.is_none() { first_ts = Some(ts_v); }
                                        last_ts = Some(ts_v);
                                    }
                                    "vu_state" => {
                                        if !vu_state_col.is_null(i) {
                                            let st = vu_state_col.value(i);
                                            match st {
                                                "started" => { active_vus += 1; vus_started += 1; }
                                                "completed" | "failed" => { if active_vus > 0 { active_vus -= 1; } vus_completed += 1; }
                                                _ => {}
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let avg = if total > 0 { sum_rt / total as f64 } else { 0.0 };
    let non_empty = !digest.is_empty();
    let p50 = if non_empty { digest.estimate_quantile(0.50) } else { 0.0 };
    let p95 = if non_empty { digest.estimate_quantile(0.95) } else { 0.0 };
    let p99 = if non_empty { digest.estimate_quantile(0.99) } else { 0.0 };
    let duration_secs = if let (Some(f), Some(l)) = (first_ts, last_ts) { ((l - f) as f64 / 1000.0).max(0.001) } else { 0.0 };
    let throughput = if duration_secs > 0.0 { total as f64 / duration_secs } else { 0.0 };
    let requests_per_minute = throughput * 60.0;
    let success_rate = if total > 0 { (success as f64 / total as f64) * 100.0 } else { 0.0 };
    Ok(LiveStats {
        scenario: "".into(),
        current_time: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        total_requests: total,
        success_count: success,
        fail_count: fail,
        avg_response_time: avg,
        throughput_rps: throughput,
        requests_per_minute,
        active_vus,
        total_vus_started: vus_started,
        total_vus_completed: vus_completed,
        success_rate,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
    })
}

impl ParquetLogger {
    pub fn get_live_stats(&self) -> PqResult<LiveStats> {
        let mut stats = read_live_stats(&self.segments_dir)?;
        stats.scenario = self.scenario_name.clone();
        Ok(stats)
    }
}
