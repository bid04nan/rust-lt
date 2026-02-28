use super::session::Session;
use super::scenario::{FlowStep, Check, Extraction};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use async_trait::async_trait;

/// Result of executing an action
#[derive(Debug, Clone)]
pub struct ActionResult {
    /// Action name
    pub name: String,
    /// Whether the action succeeded
    pub success: bool,
    /// Response time in milliseconds
    pub response_time_ms: u64,
    /// HTTP status code (if applicable)
    pub status_code: Option<u16>,
    /// Time to first byte (TTFB) in ms
    pub ttfb_ms: Option<u64>,
    /// DNS lookup time in ms
    pub dns_lookup_ms: Option<u64>,
    /// TCP connect time in ms
    pub tcp_connect_ms: Option<u64>,
    /// TLS handshake time in ms
    pub tls_handshake_ms: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
    /// Response body (truncated for logging)
    pub response_body: Option<String>,
    /// Number of bytes sent
    pub bytes_sent: usize,
    /// Number of bytes received
    pub bytes_received: usize,
    /// Hostname extracted from URL (if applicable)
    pub hostname: Option<String>,
}

impl ActionResult {
    pub fn success(name: String, response_time_ms: u64) -> Self {
        Self {
            name,
            success: true,
            response_time_ms,
            status_code: None,
            ttfb_ms: None,
            dns_lookup_ms: None,
            tcp_connect_ms: None,
            tls_handshake_ms: None,
            error: None,
            response_body: None,
            bytes_sent: 0,
            bytes_received: 0,
            hostname: None,
        }
    }

    pub fn failure(name: String, error: String, response_time_ms: u64) -> Self {
        Self {
            name,
            success: false,
            response_time_ms,
            status_code: None,
            ttfb_ms: None,
            dns_lookup_ms: None,
            tcp_connect_ms: None,
            tls_handshake_ms: None,
            error: Some(error),
            response_body: None,
            bytes_sent: 0,
            bytes_received: 0,
            hostname: None,
        }
    }
}

/// Trait for executing actions
#[async_trait]
pub trait ActionExecutor: Send + Sync {
    async fn execute(&self, session: &mut Session) -> Result<ActionResult, Box<dyn std::error::Error>>;
}

/// HTTP request executor
pub struct HttpRequestExecutor {
    pub name: String,
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
    pub checks: Vec<Check>,
    pub extractions: Vec<Extraction>,
}

#[async_trait]
impl ActionExecutor for HttpRequestExecutor {
    async fn execute(&self, session: &mut Session) -> Result<ActionResult, Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        // Resolve template variables in URL
        let resolved_url = session.resolve_template(&self.url);
        
        // Build HTTP client (reuse or create new)
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        
        // Build request
        let mut request = match self.method.to_uppercase().as_str() {
            "GET" => client.get(&resolved_url),
            "POST" => client.post(&resolved_url),
            "PUT" => client.put(&resolved_url),
            "DELETE" => client.delete(&resolved_url),
            "PATCH" => client.patch(&resolved_url),
            "HEAD" => client.head(&resolved_url),
            _ => return Err(format!("Unsupported HTTP method: {}", self.method).into()),
        };
        
        // Add headers
        for (key, value) in &self.headers {
            let resolved_value = session.resolve_template(value);
            request = request.header(key, resolved_value);
        }
        
        // Add body if present
        let bytes_sent = if let Some(body) = &self.body {
            let resolved_body = session.resolve_template(body);
            let body_bytes = resolved_body.len();
            request = request.body(resolved_body);
            body_bytes
        } else {
            0
        };
        
        // Execute request
        let response = request.send().await?;
        let status = response.status();
        let status_code = status.as_u16();
        
        // Read response body
        // measure TTFB roughly as time to first body chunk
        let ttfb = start.elapsed().as_millis() as u64; // coarse; refined tracing can replace
        let response_text = response.text().await?;
        let bytes_received = response_text.len();
        
        let elapsed = start.elapsed().as_millis() as u64;
        
        // Run checks
        let mut check_failed = false;
        let mut check_error = None;
        
        for check in &self.checks {
            match check {
                Check::Status { expected } => {
                    if status_code != *expected {
                        check_failed = true;
                        check_error = Some(format!("Status check failed: expected {}, got {}", expected, status_code));
                        break;
                    }
                }
                Check::BodyContains { text } => {
                    let resolved_text = session.resolve_template(text);
                    if !response_text.contains(&resolved_text) {
                        check_failed = true;
                        check_error = Some(format!("Body does not contain: {}", resolved_text));
                        break;
                    }
                }
                Check::BodyMatches { pattern } => {
                    let regex = regex::Regex::new(pattern)?;
                    if !regex.is_match(&response_text) {
                        check_failed = true;
                        check_error = Some(format!("Body does not match pattern: {}", pattern));
                        break;
                    }
                }
                Check::ResponseTime { max_ms } => {
                    if elapsed > *max_ms {
                        check_failed = true;
                        check_error = Some(format!("Response time {}ms exceeds max {}ms", elapsed, max_ms));
                        break;
                    }
                }
                Check::HeaderEquals { name, value } => {
                    check_failed = true;
                    check_error = Some(format!("Header check not implemented: {} = {}", name, value));
                    break;
                }
                Check::JsonPath { path, expected } => {
                    check_failed = true;
                    check_error = Some(format!("JSONPath check not implemented: {} = {}", path, expected));
                    break;
                }
            }
        }
        
        // Run extractions
        if !check_failed {
            for extraction in &self.extractions {
                match extraction {
                    Extraction::Body { save_as } => {
                        session.set(save_as.clone(), response_text.clone());
                    }
                    Extraction::JsonPath { path, save_as } => {
                        // Placeholder: would use jsonpath library
                        // For now, just log that extraction is not fully implemented
                        eprintln!("JSONPath extraction not fully implemented: {} -> {}", path, save_as);
                    }
                    Extraction::Regex { pattern, group, save_as } => {
                        let regex = regex::Regex::new(pattern)?;
                        if let Some(captures) = regex.captures(&response_text) {
                            if let Some(matched) = captures.get(*group) {
                                session.set(save_as.clone(), matched.as_str().to_string());
                            }
                        }
                    }
                    Extraction::Header { name, save_as } => {
                        eprintln!("Header extraction not implemented: {} -> {}", name, save_as);
                    }
                    Extraction::CssSelector { selector, attribute, save_as } => {
                        eprintln!("CSS selector extraction not implemented: {} ({:?}) -> {}", selector, attribute, save_as);
                    }
                }
            }
        }
        
        // Extract hostname from resolved URL without external crates
        fn extract_hostname(u: &str) -> Option<String> {
            // Expect patterns like scheme://host[:port]/path or scheme://host
            let s = u;
            let scheme_sep = s.find("://")?;
            let after = &s[scheme_sep + 3..];
            let end = after.find(['/','?','#'].as_ref()).unwrap_or(after.len());
            let host_port = &after[..end];
            let host_end = host_port.find(':').unwrap_or(host_port.len());
            let host = &host_port[..host_end];
            if host.is_empty() { None } else { Some(host.to_string()) }
        }
        let hostname = extract_hostname(&resolved_url);

        Ok(ActionResult {
            name: self.name.clone(),
            success: !check_failed,
            response_time_ms: elapsed,
            status_code: Some(status_code),
            ttfb_ms: Some(ttfb),
            dns_lookup_ms: None,
            tcp_connect_ms: None,
            tls_handshake_ms: None,
            error: check_error,
            response_body: Some(response_text[..response_text.len().min(1000)].to_string()),
            bytes_sent,
            bytes_received,
            hostname,
        })
    }
}

/// Wait executor
pub struct WaitExecutor {
    pub duration_ms: u64,
}

#[async_trait]
impl ActionExecutor for WaitExecutor {
    async fn execute(&self, _session: &mut Session) -> Result<ActionResult, Box<dyn std::error::Error>> {
        tokio::time::sleep(Duration::from_millis(self.duration_ms)).await;
        Ok(ActionResult::success("wait".to_string(), self.duration_ms))
    }
}

/// Repeat executor
pub struct RepeatExecutor {
    pub times: usize,
    pub steps: Vec<FlowStep>,
}

#[async_trait]
impl ActionExecutor for RepeatExecutor {
    async fn execute(&self, session: &mut Session) -> Result<ActionResult, Box<dyn std::error::Error>> {
        let start = Instant::now();
        
        for i in 0..self.times {
            for step in &self.steps {
                let executor = create_executor(step)?;
                let result = executor.execute(session).await?;
                
                if !result.success {
                    let elapsed = start.elapsed().as_millis() as u64;
                    return Ok(ActionResult::failure(
                        format!("repeat (iteration {})", i + 1),
                        result.error.unwrap_or_else(|| "Unknown error".to_string()),
                        elapsed,
                    ));
                }
            }
        }
        
        let elapsed = start.elapsed().as_millis() as u64;
        Ok(ActionResult::success("repeat".to_string(), elapsed))
    }
}

/// During executor (time-based loop)
pub struct DuringExecutor {
    pub duration_sec: u64,
    pub steps: Vec<FlowStep>,
}

#[async_trait]
impl ActionExecutor for DuringExecutor {
    async fn execute(&self, session: &mut Session) -> Result<ActionResult, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let duration = Duration::from_secs(self.duration_sec);
        
        while start.elapsed() < duration {
            for step in &self.steps {
                let executor = create_executor(step)?;
                let result = executor.execute(session).await?;
                
                if !result.success {
                    let elapsed = start.elapsed().as_millis() as u64;
                    return Ok(ActionResult::failure(
                        "during".to_string(),
                        result.error.unwrap_or_else(|| "Unknown error".to_string()),
                        elapsed,
                    ));
                }
                
                // Check if time is up
                if start.elapsed() >= duration {
                    break;
                }
            }
        }
        
        let elapsed = start.elapsed().as_millis() as u64;
        Ok(ActionResult::success("during".to_string(), elapsed))
    }
}

/// Feed executor (load data from feeder)
pub struct FeedExecutor {
    pub feeder_name: String,
}

#[async_trait]
impl ActionExecutor for FeedExecutor {
    async fn execute(&self, session: &mut Session) -> Result<ActionResult, Box<dyn std::error::Error>> {
        // Placeholder: would look up feeder from scenario and get next row
        // For now, just indicate that this needs to be wired up
        Ok(ActionResult::failure(
            "feed".to_string(),
            format!("Feeder '{}' not yet wired up - requires scenario context", self.feeder_name),
            0,
        ))
    }
}

/// Create an executor from a FlowStep
pub fn create_executor(step: &FlowStep) -> Result<Box<dyn ActionExecutor>, Box<dyn std::error::Error>> {
    match step {
        FlowStep::Request { protocol, name, method, url, headers, body, checks, extractions } => {
            // Currently support only HTTP protocol for request
            if protocol == "http_request" {
                Ok(Box::new(HttpRequestExecutor {
                    name: name.clone(),
                    method: method.clone(),
                    url: url.clone(),
                    headers: headers.clone(),
                    body: body.clone(),
                    checks: checks.clone(),
                    extractions: extractions.clone(),
                }))
            } else {
                Err(format!("Unsupported request protocol: {}", protocol).into())
            }
        }
        FlowStep::HttpRequest { name, method, url, headers, body, checks, extractions } => {
            Ok(Box::new(HttpRequestExecutor {
                name: name.clone(),
                method: method.clone(),
                url: url.clone(),
                headers: headers.clone(),
                body: body.clone(),
                checks: checks.clone(),
                extractions: extractions.clone(),
            }))
        }
        FlowStep::Wait { duration } => {
            Ok(Box::new(WaitExecutor {
                duration_ms: *duration,
            }))
        }
        FlowStep::Repeat { times, steps } => {
            Ok(Box::new(RepeatExecutor {
                times: *times,
                steps: steps.clone(),
            }))
        }
        FlowStep::During { duration, steps } => {
            Ok(Box::new(DuringExecutor {
                duration_sec: *duration,
                steps: steps.clone(),
            }))
        }
        FlowStep::Feed { feeder } => {
            Ok(Box::new(FeedExecutor {
                feeder_name: feeder.clone(),
            }))
        }
        _ => {
            Err(format!("Executor not implemented for step type: {:?}", step).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_executor() {
        let mut session = Session::new(1, "test".to_string());
        let executor = WaitExecutor { duration_ms: 100 };
        
        let start = Instant::now();
        let result = executor.execute(&mut session).await.unwrap();
        let elapsed = start.elapsed().as_millis() as u64;
        
        assert!(result.success);
        assert!(elapsed >= 100);
        assert!(elapsed < 200); // Should be reasonably close
    }

    #[tokio::test]
    async fn test_http_executor_with_template() {
        let mut session = Session::new(1, "test".to_string());
        session.set("user_id".to_string(), "12345".to_string());
        
        let executor = HttpRequestExecutor {
            name: "Get User".to_string(),
            method: "GET".to_string(),
            url: "https://httpbin.org/status/{{status}}".to_string(),
            headers: HashMap::new(),
            body: None,
            checks: vec![],
            extractions: vec![],
        };
        
        session.set("status".to_string(), "200".to_string());
        let result = executor.execute(&mut session).await;
        
        // May fail if no network, but should at least resolve template
        if let Ok(r) = result {
            println!("HTTP request result: {:?}", r);
        }
    }

    #[tokio::test]
    async fn test_repeat_executor() {
        let mut session = Session::new(1, "test".to_string());
        
        let executor = RepeatExecutor {
            times: 3,
            steps: vec![
                FlowStep::Wait { duration: 10 },
            ],
        };
        
        let start = Instant::now();
        let result = executor.execute(&mut session).await.unwrap();
        let elapsed = start.elapsed().as_millis() as u64;
        
        assert!(result.success);
        assert!(elapsed >= 30); // 3 iterations × 10ms
    }

    #[tokio::test]
    async fn test_during_executor() {
        let mut session = Session::new(1, "test".to_string());
        
        let executor = DuringExecutor {
            duration_sec: 1,
            steps: vec![
                FlowStep::Wait { duration: 100 },
            ],
        };
        
        let start = Instant::now();
        let result = executor.execute(&mut session).await.unwrap();
        let elapsed = start.elapsed().as_millis() as u64;
        
        assert!(result.success);
        assert!(elapsed >= 1000); // Should run for at least 1 second
        assert!(elapsed < 1500); // But not too much longer
    }

    #[test]
    fn test_action_result_creation() {
        let success = ActionResult::success("test".to_string(), 100);
        assert!(success.success);
        assert_eq!(success.response_time_ms, 100);
        assert!(success.error.is_none());
        
        let failure = ActionResult::failure("test".to_string(), "error".to_string(), 50);
        assert!(!failure.success);
        assert_eq!(failure.error, Some("error".to_string()));
    }
}
