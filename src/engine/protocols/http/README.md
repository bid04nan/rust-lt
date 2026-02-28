# HTTP Protocol Examples

This module provides HTTP client functionality for load testing.

## Basic Usage

### Simple GET Request

```rust
use rust_lt::protocols::{HttpClient, HttpRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new()?;
    let response = client.get("https://api.example.com/users").await?;
    
    println!("Status: {}", response.status_code);
    println!("Response time: {}ms", response.response_time_ms);
    println!("Body: {}", response.body);
    
    Ok(())
}
```

### POST Request with JSON

```rust
use rust_lt::protocols::{HttpClient, HttpRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new()?;
    
    let request = HttpRequest::post(
        "https://api.example.com/users",
        r#"{"name": "John Doe", "email": "john@example.com"}"#.to_string()
    )
    .with_header("Content-Type".to_string(), "application/json".to_string());
    
    let response = client.execute(request).await?;
    
    if response.is_success() {
        println!("User created successfully!");
    }
    
    Ok(())
}
```

### Using Base URL and Default Headers

```rust
use rust_lt::protocols::{HttpClient, HttpClientConfig};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut default_headers = HashMap::new();
    default_headers.insert("Authorization".to_string(), "Bearer TOKEN".to_string());
    default_headers.insert("User-Agent".to_string(), "rust-lt/0.1".to_string());
    
    let config = HttpClientConfig {
        timeout_secs: 60,
        base_url: Some("https://api.example.com".to_string()),
        default_headers,
        ..Default::default()
    };
    
    let client = HttpClient::with_config(config)?;
    
    // This will request https://api.example.com/users/123
    let response = client.get("/users/123").await?;
    
    println!("Response: {}", response.body);
    
    Ok(())
}
```

### Response Validation

```rust
use rust_lt::protocols::{HttpClient, HttpRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new()?;
    let response = client.get("https://api.example.com/data").await?;
    
    // Check status
    assert!(response.is_success());
    
    // Check body contains text
    assert!(response.body_contains("expected_value"));
    
    // Check with regex
    assert!(response.body_matches(r"\d{3}-\d{4}")?);
    
    // Extract data with regex
    if let Some(id) = response.extract_regex(r"id:\s*(\d+)", 1)? {
        println!("Extracted ID: {}", id);
    }
    
    // Get header
    if let Some(content_type) = response.get_header("content-type") {
        println!("Content-Type: {}", content_type);
    }
    
    Ok(())
}
```

### Custom Configuration

```rust
use rust_lt::protocols::{HttpClient, HttpClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = HttpClientConfig {
        timeout_secs: 120,
        max_idle_per_host: 20,
        follow_redirects: false,
        accept_invalid_certs: true,  // For testing environments
        ..Default::default()
    };
    
    let client = HttpClient::with_config(config)?;
    let response = client.get("https://localhost:8443/api").await?;
    
    println!("Status: {}", response.status_code);
    
    Ok(())
}
```

### Using in Load Testing Scenarios

```rust
use rust_lt::protocols::{HttpClient, HttpRequest};
use rust_lt::Session;

async fn test_user_flow() -> Result<(), Box<dyn std::error::Error>> {
    let client = HttpClient::new()?;
    let mut session = Session::new(1, "test_scenario".to_string());
    
    // Step 1: Login
    let login_req = HttpRequest::post(
        "https://api.example.com/login",
        r#"{"username": "test", "password": "pass"}"#.to_string()
    )
    .with_header("Content-Type".to_string(), "application/json".to_string());
    
    let login_resp = client.execute(login_req).await?;
    
    // Extract token
    if let Some(token) = login_resp.extract_regex(r#""token":\s*"([^"]+)"#, 1)? {
        session.set("auth_token".to_string(), token);
    }
    
    // Step 2: Use authenticated request
    let token = session.get("auth_token").unwrap();
    let data_req = HttpRequest::get("https://api.example.com/data")
        .with_header("Authorization".to_string(), format!("Bearer {}", token));
    
    let data_resp = client.execute(data_req).await?;
    
    println!("Data retrieved: {} bytes in {}ms", 
        data_resp.bytes_received, 
        data_resp.response_time_ms
    );
    
    Ok(())
}
```

## Features

- **Connection Pooling**: Automatic connection reuse for better performance
- **Timeout Control**: Configure per-request or global timeouts
- **Header Management**: Set default headers and override per-request
- **Base URL Support**: Define base URL for relative paths
- **Response Validation**: Built-in methods for status checks, body validation, regex extraction
- **Metrics Collection**: Automatic tracking of response times, bytes sent/received
- **SSL/TLS Support**: Option to accept invalid certificates for testing

## Protocol Support

- HTTP/1.1 ✅
- HTTPS ✅
- HTTP/2 ✅ (via reqwest)
- WebSocket ⚠️ (placeholder)
- JDBC ⚠️ (placeholder)
- JMS ⚠️ (placeholder)
