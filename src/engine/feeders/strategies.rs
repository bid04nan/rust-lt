use serde::{Deserialize, Serialize};

/// Strategy for iterating through feeder data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeederStrategy {
    /// Sequential: Each VU gets the next row. Stops when data exhausted.
    /// VU1 → row0, VU2 → row1, VU3 → row2, ... VUn → None
    Sequential,
    
    /// Random: Each VU gets a random row. Never exhausts.
    /// VU1 → random, VU2 → random, VU3 → random, ...
    Random,
    
    /// Cyclic: Each VU gets the next row, wrapping around. Never exhausts.
    /// VU1 → row0, VU2 → row1, ..., VUn → row0 (cycles)
    Cyclic,
    
    // Future strategies:
    // Shuffle: Randomize once at start, then sequential
    // Queue: Thread-safe queue, each row consumed once across all VUs
}

impl Default for FeederStrategy {
    fn default() -> Self {
        FeederStrategy::Cyclic
    }
}
