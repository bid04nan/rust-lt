use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod csv;
pub mod json;
pub mod jdbc;
pub mod redis;
pub mod kafka;
pub mod strategies;

pub use csv::CsvFeeder;
pub use json::JsonFeeder;
pub use jdbc::JdbcFeeder;
pub use redis::RedisFeeder;
pub use kafka::KafkaFeeder;
pub use strategies::FeederStrategy;

/// A row of data returned by a feeder (key-value pairs)
pub type DataRow = HashMap<String, String>;

/// Core Feeder trait that all feeder implementations must satisfy
#[async_trait]
pub trait Feeder: Send + Sync {
    /// Gets the next data row based on the feeder's strategy.
    /// Returns None when no more data is available (for sequential strategy).
    async fn next_row(&mut self) -> Option<DataRow>;
    
    /// Returns the column headers/keys available in this feeder.
    /// Useful for validation and debugging.
    fn get_headers(&self) -> Vec<String>;
    
    /// Resets the feeder cursor to the beginning.
    /// Used when implementing cyclic or restart behavior.
    fn reset(&mut self);
}

/// Feeder configuration from YAML
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FeederConfig {
    Csv {
        name: String,
        path: String,
        strategy: FeederStrategy,
    },
    Json {
        name: String,
        path: String,
        strategy: FeederStrategy,
    },
    Jdbc {
        name: String,
        connection_string: String,
        query: String,
        strategy: FeederStrategy,
        #[serde(default)]
        username: Option<String>,
        #[serde(default)]
        password: Option<String>,
    },
    Redis {
        name: String,
        url: String,
        key_pattern: String,
        strategy: FeederStrategy,
        #[serde(default = "default_redis_db")]
        db: i64,
    },
    Kafka {
        name: String,
        brokers: Vec<String>,
        topic: String,
        group_id: String,
        strategy: FeederStrategy,
        #[serde(default = "default_kafka_timeout")]
        timeout_ms: u64,
    },
}

impl FeederConfig {
    /// Get the feeder name for lookup in the feeder map
    pub fn name(&self) -> &str {
        match self {
            FeederConfig::Csv { name, .. } => name,
            FeederConfig::Json { name, .. } => name,
            FeederConfig::Jdbc { name, .. } => name,
            FeederConfig::Redis { name, .. } => name,
            FeederConfig::Kafka { name, .. } => name,
        }
    }
}

fn default_redis_db() -> i64 {
    0
}

fn default_kafka_timeout() -> u64 {
    5000
}
