use async_trait::async_trait;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::error::Error;

use super::{DataRow, Feeder, FeederConfig, FeederStrategy};

/// JSON Feeder implementation
/// Expects a JSON file containing an array of objects
pub struct JsonFeeder {
    data: Vec<DataRow>,
    strategy: FeederStrategy,
    cursor: usize,
    headers: Vec<String>,
}

impl JsonFeeder {
    /// Load a JSON feeder from a file path
    pub async fn load(config: &FeederConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (path, strategy) = match config {
            FeederConfig::Json { path, strategy, .. } => (path.clone(), *strategy),
            _ => return Err("Invalid feeder config for JSON".into()),
        };

        // Read file contents
        let file_content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| format!("Failed to read JSON file {}: {}", path, e))?;

        // Parse JSON array
        let json_array: Vec<serde_json::Value> = serde_json::from_str(&file_content)
            .map_err(|e| format!("Failed to parse JSON file {}: {}", path, e))?;

        if json_array.is_empty() {
            return Err("JSON file contains empty array".into());
        }

        // Extract headers from first object
        let headers = if let Some(serde_json::Value::Object(first_obj)) = json_array.first() {
            let mut h: Vec<String> = first_obj.keys().cloned().collect();
            h.sort(); // Sort for consistent ordering
            h
        } else {
            return Err("JSON array must contain objects".into());
        };

        // Convert JSON objects to DataRow format
        let mut data = Vec::new();
        for (idx, item) in json_array.iter().enumerate() {
            if let serde_json::Value::Object(obj) = item {
                let mut row = HashMap::new();
                for key in &headers {
                    let value = obj
                        .get(key)
                        .map(|v| json_value_to_string(v))
                        .unwrap_or_default();
                    row.insert(key.clone(), value);
                }
                data.push(row);
            } else {
                return Err(format!("JSON array element {} is not an object", idx).into());
            }
        }

        Ok(JsonFeeder {
            data,
            strategy,
            cursor: 0,
            headers,
        })
    }
}

/// Convert JSON value to string representation
fn json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

#[async_trait]
impl Feeder for JsonFeeder {
    async fn next_row(&mut self) -> Option<DataRow> {
        if self.data.is_empty() {
            return None;
        }

        match self.strategy {
            FeederStrategy::Sequential => {
                if self.cursor >= self.data.len() {
                    None
                } else {
                    let row = self.data.get(self.cursor).cloned();
                    self.cursor += 1;
                    row
                }
            }
            FeederStrategy::Random => {
                let mut rng = rand::thread_rng();
                self.data.choose(&mut rng).cloned()
            }
            FeederStrategy::Cyclic => {
                let row = self.data.get(self.cursor).cloned();
                self.cursor = (self.cursor + 1) % self.data.len();
                row
            }
        }
    }

    fn get_headers(&self) -> Vec<String> {
        self.headers.clone()
    }

    fn reset(&mut self) {
        self.cursor = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_json_value_conversion() {
        assert_eq!(json_value_to_string(&serde_json::json!("test")), "test");
        assert_eq!(json_value_to_string(&serde_json::json!(42)), "42");
        assert_eq!(json_value_to_string(&serde_json::json!(3.14)), "3.14");
        assert_eq!(json_value_to_string(&serde_json::json!(true)), "true");
        assert_eq!(json_value_to_string(&serde_json::json!(null)), "");
    }
}
