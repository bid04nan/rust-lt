use async_trait::async_trait;
use rand::Rng;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{DataRow, Feeder, FeederConfig, FeederStrategy};

/// CSV Feeder implementation
pub struct CsvFeeder {
    data: Vec<DataRow>,
    strategy: FeederStrategy,
    cursor: usize,
    /// Atomic cursor for lock-free cyclic/sequential access
    atomic_cursor: AtomicUsize,
    headers: Vec<String>,
}

impl CsvFeeder {
    /// Load a CSV feeder from a file path
    pub async fn load(config: &FeederConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (path, strategy) = match config {
            FeederConfig::Csv { path, strategy, .. } => (path.clone(), *strategy),
            _ => return Err("Invalid feeder config for CSV".into()),
        };

        // Read file contents
        let file_content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| format!("Failed to read CSV file {}: {}", path, e))?;

        let mut lines = file_content.lines();

        // Parse headers
        let headers: Vec<String> = lines
            .next()
            .ok_or("CSV file is empty or missing headers")?
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        if headers.is_empty() {
            return Err("CSV file has no headers".into());
        }

        // Parse data rows
        let mut data = Vec::new();
        for (line_num, line) in lines.enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue; // Skip empty lines
            }

            let values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            
            if values.len() != headers.len() {
                return Err(format!(
                    "CSV line {} has {} columns, expected {}",
                    line_num + 2, // +2 for header line and 0-based index
                    values.len(),
                    headers.len()
                ).into());
            }

            let mut row = HashMap::new();
            for (i, header) in headers.iter().enumerate() {
                row.insert(header.clone(), values[i].to_string());
            }
            data.push(row);
        }

        if data.is_empty() {
            return Err("CSV file has no data rows".into());
        }

        Ok(CsvFeeder {
            data,
            strategy,
            cursor: 0,
            atomic_cursor: AtomicUsize::new(0),
            headers,
        })
    }
}
#[async_trait]
impl Feeder for CsvFeeder {
    async fn next_row(&mut self) -> Option<DataRow> {
        if self.data.is_empty() {
            return None;
        }

        match self.strategy {
            FeederStrategy::Sequential => {
                // Use atomic for lock-free sequential access
                let idx = self.atomic_cursor.fetch_add(1, Ordering::Relaxed);
                if idx >= self.data.len() {
                    None
                } else {
                    self.data.get(idx).cloned()
                }
            }
            FeederStrategy::Random => {
                // OPTIMIZED: Use faster random index generation
                // Avoids SliceRandom overhead and potential lock contention
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..self.data.len());
                self.data.get(idx).cloned()
            }
            FeederStrategy::Cyclic => {
                // Use atomic for lock-free cyclic access
                let idx = self.atomic_cursor.fetch_add(1, Ordering::Relaxed) % self.data.len();
                self.data.get(idx).cloned()
            }
        }
    }

    fn get_headers(&self) -> Vec<String> {
        self.headers.clone()
    }

    fn reset(&mut self) {
        self.cursor = 0;
        self.atomic_cursor.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_data() -> Vec<DataRow> {
        vec![
            [("name", "Alice"), ("age", "25")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            [("name", "Bob"), ("age", "30")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            [("name", "Charlie"), ("age", "35")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ]
    }

    #[tokio::test]
    async fn test_sequential_strategy() {
        let mut feeder = CsvFeeder {
            data: create_test_data(),
            strategy: FeederStrategy::Sequential,
            cursor: 0,
            atomic_cursor: AtomicUsize::new(0),
            headers: vec!["name".to_string(), "age".to_string()],
        };

        assert_eq!(feeder.next_row().await.unwrap()["name"], "Alice");
        assert_eq!(feeder.next_row().await.unwrap()["name"], "Bob");
        assert_eq!(feeder.next_row().await.unwrap()["name"], "Charlie");
        assert!(feeder.next_row().await.is_none());
    }

    #[tokio::test]
    async fn test_cyclic_strategy() {
        let mut feeder = CsvFeeder {
            data: create_test_data(),
            strategy: FeederStrategy::Cyclic,
            cursor: 0,
            atomic_cursor: AtomicUsize::new(0),
            headers: vec!["name".to_string(), "age".to_string()],
        };

        assert_eq!(feeder.next_row().await.unwrap()["name"], "Alice");
        assert_eq!(feeder.next_row().await.unwrap()["name"], "Bob");
        assert_eq!(feeder.next_row().await.unwrap()["name"], "Charlie");
        assert_eq!(feeder.next_row().await.unwrap()["name"], "Alice"); // Cycles back
    }

    #[tokio::test]
    async fn test_random_strategy() {
        let mut feeder = CsvFeeder {
            data: create_test_data(),
            strategy: FeederStrategy::Random,
            cursor: 0,
            atomic_cursor: AtomicUsize::new(0),
            headers: vec!["name".to_string(), "age".to_string()],
        };

        // Random should always return some row
        for _ in 0..10 {
            assert!(feeder.next_row().await.is_some());
        }
    }

    #[tokio::test]
    async fn test_reset() {
        let mut feeder = CsvFeeder {
            data: create_test_data(),
            strategy: FeederStrategy::Sequential,
            cursor: 0,
            atomic_cursor: AtomicUsize::new(0),
            headers: vec!["name".to_string(), "age".to_string()],
        };

        feeder.next_row().await;
        feeder.next_row().await;
        assert_eq!(feeder.atomic_cursor.load(Ordering::Relaxed), 2);

        feeder.reset();
        assert_eq!(feeder.atomic_cursor.load(Ordering::Relaxed), 0);
        assert_eq!(feeder.next_row().await.unwrap()["name"], "Alice");
    }
}
