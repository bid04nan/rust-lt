# Kafka Protocol Examples

This module provides Kafka client functionality for load testing message streaming applications.

## Features

- **Producer**: Send messages to Kafka topics with configurable acknowledgments and batching
- **Consumer**: Poll messages from Kafka topics with consumer group support
- **Admin**: Create, delete, and list Kafka topics
- **Configuration**: Full control over producer/consumer settings
- **Headers & Keys**: Support for message keys and custom headers
- **Metrics**: Track latency, throughput, and message sizes

## Basic Usage

### Producer - Send Messages

```rust
use rust_lt::protocols::{KafkaProducer, KafkaMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::new("localhost:9092".to_string());
    
    let message = KafkaMessage::from_string("Hello Kafka!".to_string());
    
    let result = producer.send("my-topic", message).await?;
    
    println!("Message sent to partition {} at offset {}", 
        result.partition, result.offset);
    println!("Latency: {}ms", result.latency_ms);
    
    Ok(())
}
```

### Producer - Send with Key and Headers

```rust
use rust_lt::protocols::{KafkaProducer, KafkaMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::new("localhost:9092".to_string());
    
    let message = KafkaMessage::from_string(r#"{"user_id": 123, "action": "login"}"#.to_string())
        .with_key("user-123".to_string())
        .with_header("trace-id".to_string(), "abc-xyz-123".to_string())
        .with_header("source".to_string(), "load-test".to_string());
    
    let result = producer.send("user-events", message).await?;
    
    println!("Event sent: partition={}, offset={}, latency={}ms",
        result.partition, result.offset, result.latency_ms);
    
    Ok(())
}
```

### Producer - Batch Send

```rust
use rust_lt::protocols::{KafkaProducer, KafkaMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::new("localhost:9092".to_string());
    
    let messages: Vec<KafkaMessage> = (0..100)
        .map(|i| KafkaMessage::from_string(format!("Message {}", i))
            .with_key(format!("key-{}", i)))
        .collect();
    
    let results = producer.send_batch("my-topic", messages).await?;
    
    println!("Sent {} messages", results.len());
    let total_latency: u64 = results.iter().map(|r| r.latency_ms).sum();
    println!("Average latency: {}ms", total_latency / results.len() as u64);
    
    Ok(())
}
```

### Producer - Custom Configuration

```rust
use rust_lt::protocols::{KafkaProducer, KafkaProducerConfig, KafkaMessage};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = KafkaProducerConfig {
        timeout_ms: 10000,
        acks: -1,  // Wait for all replicas
        compression_type: "gzip".to_string(),
        batch_size: 32768,
        linger_ms: 10,  // Wait up to 10ms to batch messages
        additional_config: HashMap::new(),
    };
    
    config.additional_config.insert(
        "max.in.flight.requests.per.connection".to_string(),
        "5".to_string()
    );
    
    let producer = KafkaProducer::with_config("localhost:9092".to_string(), config);
    
    let message = KafkaMessage::from_string("High-throughput message".to_string());
    let result = producer.send("high-volume-topic", message).await?;
    
    println!("Message sent with latency: {}ms", result.latency_ms);
    
    Ok(())
}
```

### Consumer - Subscribe and Poll

```rust
use rust_lt::protocols::KafkaConsumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer = KafkaConsumer::new(
        "localhost:9092".to_string(),
        "my-consumer-group".to_string()
    );
    
    consumer.subscribe(vec!["my-topic".to_string()]).await?;
    
    // Poll for messages
    for _ in 0..10 {
        if let Some(message) = consumer.poll(5000).await? {
            println!("Received message from topic: {} partition: {} offset: {}",
                message.topic, message.partition, message.offset);
            
            if let Ok(payload) = message.payload_as_string() {
                println!("Payload: {}", payload);
            }
            
            if let Some(Ok(key)) = message.key_as_string() {
                println!("Key: {}", key);
            }
        }
    }
    
    Ok(())
}
```

### Consumer - Batch Polling

```rust
use rust_lt::protocols::KafkaConsumer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer = KafkaConsumer::new(
        "localhost:9092".to_string(),
        "batch-consumer-group".to_string()
    );
    
    consumer.subscribe(vec!["events".to_string()]).await?;
    
    // Poll up to 100 messages with 10 second timeout
    let messages = consumer.poll_batch(100, 10000).await?;
    
    println!("Received {} messages", messages.len());
    
    for msg in messages {
        println!("Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
            msg.topic, msg.partition, msg.offset, msg.timestamp);
    }
    
    Ok(())
}
```

### Consumer - Custom Configuration

```rust
use rust_lt::protocols::{KafkaConsumer, KafkaConsumerConfig};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = KafkaConsumerConfig {
        auto_offset_reset: "earliest".to_string(),  // Start from beginning
        enable_auto_commit: false,  // Manual commit control
        auto_commit_interval_ms: 1000,
        session_timeout_ms: 30000,
        max_poll_records: 1000,
        additional_config: HashMap::new(),
    };
    
    config.additional_config.insert(
        "fetch.min.bytes".to_string(),
        "1024".to_string()
    );
    
    let consumer = KafkaConsumer::with_config(
        "localhost:9092".to_string(),
        "custom-group".to_string(),
        config
    );
    
    consumer.subscribe(vec!["data-topic".to_string()]).await?;
    
    if let Some(message) = consumer.poll(5000).await? {
        println!("Consumed message with {} bytes", message.payload.len());
    }
    
    Ok(())
}
```

### Admin - Topic Management

```rust
use rust_lt::protocols::KafkaAdmin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let admin = KafkaAdmin::new("localhost:9092".to_string());
    
    // Create a topic
    admin.create_topic("new-topic", 3, 1).await?;
    println!("Topic created with 3 partitions");
    
    // List all topics
    let topics = admin.list_topics().await?;
    println!("Available topics: {:?}", topics);
    
    // Delete a topic
    admin.delete_topic("old-topic").await?;
    println!("Topic deleted");
    
    Ok(())
}
```

## Load Testing Scenarios

### Throughput Test

```rust
use rust_lt::protocols::{KafkaProducer, KafkaMessage};
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::new("localhost:9092".to_string());
    
    let message_count = 10000;
    let start = Instant::now();
    
    for i in 0..message_count {
        let message = KafkaMessage::from_string(format!("Message {}", i))
            .with_key(format!("key-{}", i % 100));  // 100 unique keys
        
        producer.send("throughput-test", message).await?;
    }
    
    let elapsed = start.elapsed();
    let throughput = message_count as f64 / elapsed.as_secs_f64();
    
    println!("Sent {} messages in {:?}", message_count, elapsed);
    println!("Throughput: {:.2} messages/sec", throughput);
    
    Ok(())
}
```

### Latency Test

```rust
use rust_lt::protocols::{KafkaProducer, KafkaMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::new("localhost:9092".to_string());
    
    let mut latencies = Vec::new();
    
    for i in 0..1000 {
        let message = KafkaMessage::from_string(format!("Latency test {}", i));
        let result = producer.send("latency-test", message).await?;
        latencies.push(result.latency_ms);
    }
    
    latencies.sort();
    let avg = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    
    println!("Latency statistics:");
    println!("  Average: {}ms", avg);
    println!("  P50: {}ms", p50);
    println!("  P95: {}ms", p95);
    println!("  P99: {}ms", p99);
    
    Ok(())
}
```

### Consumer Performance Test

```rust
use rust_lt::protocols::KafkaConsumer;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer = KafkaConsumer::new(
        "localhost:9092".to_string(),
        "perf-test-group".to_string()
    );
    
    consumer.subscribe(vec!["test-topic".to_string()]).await?;
    
    let mut message_count = 0;
    let mut total_bytes = 0;
    let start = Instant::now();
    let test_duration_secs = 60;
    
    while start.elapsed().as_secs() < test_duration_secs {
        if let Some(message) = consumer.poll(1000).await? {
            message_count += 1;
            total_bytes += message.payload.len();
        }
    }
    
    let elapsed = start.elapsed();
    let throughput = message_count as f64 / elapsed.as_secs_f64();
    let bandwidth_mbps = (total_bytes as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0);
    
    println!("Consumed {} messages ({} bytes) in {:?}", message_count, total_bytes, elapsed);
    println!("Throughput: {:.2} messages/sec", throughput);
    println!("Bandwidth: {:.2} MB/s", bandwidth_mbps);
    
    Ok(())
}
```

## Integration with Load Testing Framework

```rust
use rust_lt::{Session, KafkaProducer, KafkaMessage};

async fn kafka_load_test_scenario(session: &mut Session) -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::new("localhost:9092".to_string());
    
    // Get user-specific data from session
    let user_id = session.get("user_id").unwrap_or(&"unknown".to_string()).clone();
    
    // Send message with user context
    let message = KafkaMessage::from_string(
        format!(r#"{{"user_id": "{}", "event": "page_view"}}"#, user_id)
    )
    .with_key(user_id.clone())
    .with_header("session-id".to_string(), session.user_id.to_string());
    
    let result = producer.send("user-events", message).await?;
    
    // Store result in session for later use
    session.set("last_offset".to_string(), result.offset.to_string());
    session.set("last_latency".to_string(), result.latency_ms.to_string());
    
    println!("User {} sent event: partition={}, offset={}, latency={}ms",
        user_id, result.partition, result.offset, result.latency_ms);
    
    Ok(())
}
```

## Configuration Notes

### Enabling Full Kafka Support

To use the actual Kafka client (rdkafka), enable the `kafka` feature in Cargo.toml:

```toml
[dependencies]
rdkafka = { version = "0.36", features = ["tokio"] }
```

Without this feature, the module provides simulated responses for testing the framework itself.

### Performance Tuning

**Producer:**
- `acks=1`: Wait for leader only (faster, less durable)
- `acks=-1`: Wait for all replicas (slower, more durable)
- `batch_size`: Larger batches = higher throughput
- `linger_ms`: Wait time for batching = trade latency for throughput
- `compression_type`: gzip/snappy/lz4/zstd for bandwidth efficiency

**Consumer:**
- `max_poll_records`: More records per poll = higher throughput
- `fetch.min.bytes`: Wait for more data before returning
- `auto_offset_reset`: earliest/latest for consumer positioning

## Supported Operations

- ✅ Producer send (single message)
- ✅ Producer batch send
- ✅ Consumer poll (single/batch)
- ✅ Message keys and headers
- ✅ Topic management (create/delete/list)
- ✅ Custom producer/consumer configuration
- ✅ Metrics collection (latency, throughput)
- ⚠️ Transactional producer (requires rdkafka feature)
- ⚠️ Consumer commit control (requires rdkafka feature)
