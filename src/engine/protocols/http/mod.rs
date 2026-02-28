use async_trait::async_trait;
use reqwest::{Client, Response, Method, StatusCode};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

/// Connection timing breakdown for detailed performance analysis
#[derive(Debug, Clone, Default)]
pub struct ConnectionTimings {
    /// DNS lookup time in milliseconds
    pub dns_lookup_ms: Option<u64>,
    /// TCP connection time in milliseconds
    pub tcp_connect_ms: Option<u64>,
    /// TLS handshake time in milliseconds (HTTPS only)
    pub tls_handshake_ms: Option<u64>,
    /// Time to first byte in milliseconds
    pub time_to_first_byte_ms: Option<u64>,
    /// Content download time in milliseconds
    pub content_download_ms: Option<u64>,
}

/// HTTP client wrapper for load testing
pub struct HttpClient {
    client: Client,
    base_url: Option<String>,
    default_headers: HashMap<String, String>,
}

impl HttpClient {
    /// Create a new HTTP client
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .build()?;
        
        Ok(Self {
            client,
            base_url: None,
            default_headers: HashMap::new(),
        })
    }

    /// Create a new HTTP client with custom configuration
    pub fn with_config(config: HttpClientConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .pool_max_idle_per_host(config.max_idle_per_host);

        if !config.follow_redirects {
            builder = builder.redirect(reqwest::redirect::Policy::none());
        }

        if config.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        let client = builder.build()?;

        Ok(Self {
            client,
            base_url: config.base_url,
            default_headers: config.default_headers,
        })
    }

    /// Set base URL for relative requests
    pub fn set_base_url(&mut self, base_url: String) {
        self.base_url = Some(base_url);
    }

    /// Add a default header
    pub fn add_default_header(&mut self, key: String, value: String) {
        self.default_headers.insert(key, value);
    }

    /// Execute an HTTP request
    pub async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, Box<dyn std::error::Error>> {
        let start = Instant::now();

        // Resolve URL
        let url = if let Some(base) = &self.base_url {
            if request.url.starts_with("http://") || request.url.starts_with("https://") {
                request.url.clone()
            } else {
                format!("{}{}", base.trim_end_matches('/'), request.url)
            }
        } else {
            request.url.clone()
        };

        // Build request
        let method = match request.method.to_uppercase().as_str() {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            "PATCH" => Method::PATCH,
            "HEAD" => Method::HEAD,
            "OPTIONS" => Method::OPTIONS,
            _ => return Err(format!("Unsupported HTTP method: {}", request.method).into()),
        };

        let mut req_builder = self.client.request(method, &url);

        // Add default headers
        for (key, value) in &self.default_headers {
            req_builder = req_builder.header(key, value);
        }

        // Add request headers (override defaults)
        for (key, value) in &request.headers {
            req_builder = req_builder.header(key, value);
        }

        // Add query parameters
        if !request.query_params.is_empty() {
            req_builder = req_builder.query(&request.query_params);
        }

        // Add body
        let bytes_sent = if let Some(body) = &request.body {
            let body_bytes = body.len();
            req_builder = req_builder.body(body.clone());
            body_bytes
        } else {
            0
        };

        // Execute request
        let response = req_builder.send().await?;
        let status = response.status();
        let status_code = status.as_u16();
        
        // Collect response headers
        let mut response_headers = HashMap::new();
        for (key, value) in response.headers() {
            if let Ok(v) = value.to_str() {
                response_headers.insert(key.as_str().to_string(), v.to_string());
            }
        }

        // Read response body
        let response_text = response.text().await?;
        let bytes_received = response_text.len();

        let elapsed = start.elapsed();
        let total_time_ms = elapsed.as_millis() as u64;

        // Note: reqwest doesn't expose detailed connection timings
        // These would need custom instrumentation or a different HTTP client
        // For now, we'll estimate time_to_first_byte and content_download
        let timings = ConnectionTimings {
            dns_lookup_ms: None,
            tcp_connect_ms: None,
            tls_handshake_ms: None,
            time_to_first_byte_ms: Some(total_time_ms / 2), // Rough estimate
            content_download_ms: Some(total_time_ms / 2),    // Rough estimate
        };

        Ok(HttpResponse {
            status_code,
            status_text: status.canonical_reason().unwrap_or("").to_string(),
            headers: response_headers,
            body: response_text,
            response_time_ms: total_time_ms,
            bytes_sent,
            bytes_received,
            timings,
        })
    }

    /// Execute a GET request
    pub async fn get(&self, url: &str) -> Result<HttpResponse, Box<dyn std::error::Error>> {
        self.execute(HttpRequest::get(url)).await
    }

    /// Execute a POST request
    pub async fn post(&self, url: &str, body: String) -> Result<HttpResponse, Box<dyn std::error::Error>> {
        self.execute(HttpRequest::post(url, body)).await
    }

    /// Execute a PUT request
    pub async fn put(&self, url: &str, body: String) -> Result<HttpResponse, Box<dyn std::error::Error>> {
        self.execute(HttpRequest::put(url, body)).await
    }

    /// Execute a DELETE request
    pub async fn delete(&self, url: &str) -> Result<HttpResponse, Box<dyn std::error::Error>> {
        self.execute(HttpRequest::delete(url)).await
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new().expect("Failed to create default HTTP client")
    }
}

/// HTTP client configuration
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Maximum idle connections per host
    pub max_idle_per_host: usize,
    /// Whether to follow redirects
    pub follow_redirects: bool,
    /// Whether to accept invalid SSL certificates
    pub accept_invalid_certs: bool,
    /// Base URL for relative requests
    pub base_url: Option<String>,
    /// Default headers to include in all requests
    pub default_headers: HashMap<String, String>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            timeout_secs: 30,
            max_idle_per_host: 10,
            follow_redirects: true,
            accept_invalid_certs: false,
            base_url: None,
            default_headers: HashMap::new(),
        }
    }
}

/// HTTP request
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: HashMap<String, String>,
    pub query_params: HashMap<String, String>,
    pub body: Option<String>,
}

impl HttpRequest {
    pub fn new(method: &str, url: &str) -> Self {
        Self {
            method: method.to_string(),
            url: url.to_string(),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body: None,
        }
    }

    pub fn get(url: &str) -> Self {
        Self::new("GET", url)
    }

    pub fn post(url: &str, body: String) -> Self {
        Self {
            method: "POST".to_string(),
            url: url.to_string(),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body: Some(body),
        }
    }

    pub fn put(url: &str, body: String) -> Self {
        Self {
            method: "PUT".to_string(),
            url: url.to_string(),
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body: Some(body),
        }
    }

    pub fn delete(url: &str) -> Self {
        Self::new("DELETE", url)
    }

    pub fn with_header(mut self, key: String, value: String) -> Self {
        self.headers.insert(key, value);
        self
    }

    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers.extend(headers);
        self
    }

    pub fn with_query_param(mut self, key: String, value: String) -> Self {
        self.query_params.insert(key, value);
        self
    }

    pub fn with_body(mut self, body: String) -> Self {
        self.body = Some(body);
        self
    }
}

/// HTTP response
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub response_time_ms: u64,
    pub bytes_sent: usize,
    pub bytes_received: usize,
    /// Detailed connection timing breakdown
    pub timings: ConnectionTimings,
}

impl HttpResponse {
    /// Check if the status code is successful (2xx)
    pub fn is_success(&self) -> bool {
        self.status_code >= 200 && self.status_code < 300
    }

    /// Check if the status code is a client error (4xx)
    pub fn is_client_error(&self) -> bool {
        self.status_code >= 400 && self.status_code < 500
    }

    /// Check if the status code is a server error (5xx)
    pub fn is_server_error(&self) -> bool {
        self.status_code >= 500 && self.status_code < 600
    }

    /// Get a header value
    pub fn get_header(&self, name: &str) -> Option<&String> {
        self.headers.get(&name.to_lowercase())
    }

    /// Parse JSON body
    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.body)
    }

    /// Check if body contains text
    pub fn body_contains(&self, text: &str) -> bool {
        self.body.contains(text)
    }

    /// Check if body matches regex
    pub fn body_matches(&self, pattern: &str) -> Result<bool, regex::Error> {
        let regex = regex::Regex::new(pattern)?;
        Ok(regex.is_match(&self.body))
    }

    /// Extract text using regex
    pub fn extract_regex(&self, pattern: &str, group: usize) -> Result<Option<String>, regex::Error> {
        let regex = regex::Regex::new(pattern)?;
        Ok(regex.captures(&self.body)
            .and_then(|caps| caps.get(group))
            .map(|m| m.as_str().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_request_builder() {
        let req = HttpRequest::get("https://example.com")
            .with_header("Authorization".to_string(), "Bearer token".to_string())
            .with_query_param("page".to_string(), "1".to_string());

        assert_eq!(req.method, "GET");
        assert_eq!(req.url, "https://example.com");
        assert_eq!(req.headers.get("Authorization"), Some(&"Bearer token".to_string()));
        assert_eq!(req.query_params.get("page"), Some(&"1".to_string()));
    }

    #[test]
    fn test_http_request_post() {
        let req = HttpRequest::post("https://api.example.com/users", r#"{"name":"test"}"#.to_string());

        assert_eq!(req.method, "POST");
        assert_eq!(req.url, "https://api.example.com/users");
        assert_eq!(req.body, Some(r#"{"name":"test"}"#.to_string()));
    }

    #[test]
    fn test_http_response_status_checks() {
        let resp = HttpResponse {
            status_code: 200,
            status_text: "OK".to_string(),
            headers: HashMap::new(),
            body: "test".to_string(),
            response_time_ms: 100,
            bytes_sent: 0,
            bytes_received: 4,
            timings: ConnectionTimings::default(),
        };

        assert!(resp.is_success());
        assert!(!resp.is_client_error());
        assert!(!resp.is_server_error());

        let err_resp = HttpResponse {
            status_code: 404,
            status_text: "Not Found".to_string(),
            headers: HashMap::new(),
            body: "".to_string(),
            response_time_ms: 50,
            bytes_sent: 0,
            bytes_received: 0,
            timings: ConnectionTimings::default(),
        };

        assert!(!err_resp.is_success());
        assert!(err_resp.is_client_error());
        assert!(!err_resp.is_server_error());
    }

    #[test]
    fn test_http_response_body_contains() {
        let resp = HttpResponse {
            status_code: 200,
            status_text: "OK".to_string(),
            headers: HashMap::new(),
            body: "Hello World".to_string(),
            response_time_ms: 100,
            bytes_sent: 0,
            bytes_received: 11,
            timings: ConnectionTimings::default(),
        };

        assert!(resp.body_contains("Hello"));
        assert!(resp.body_contains("World"));
        assert!(!resp.body_contains("Goodbye"));
    }

    #[test]
    fn test_http_client_config() {
        let config = HttpClientConfig {
            timeout_secs: 60,
            max_idle_per_host: 20,
            follow_redirects: false,
            accept_invalid_certs: true,
            base_url: Some("https://api.example.com".to_string()),
            default_headers: HashMap::new(),
        };

        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.max_idle_per_host, 20);
        assert!(!config.follow_redirects);
        assert!(config.accept_invalid_certs);
    }

    #[tokio::test]
    async fn test_http_client_creation() {
        let client = HttpClient::new();
        assert!(client.is_ok());

        let config = HttpClientConfig::default();
        let client_with_config = HttpClient::with_config(config);
        assert!(client_with_config.is_ok());
    }
}
