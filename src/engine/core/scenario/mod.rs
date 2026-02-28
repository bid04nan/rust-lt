use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Import FeederConfig
use super::super::feeders::FeederConfig;

/// Load model determines how virtual users are injected into the test
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LoadModel {
    /// Closed model: Inject a fixed number of users at once
    /// Example: at_once_users: { users: 100 }
    AtOnceUsers {
        users: usize,
    },

    /// Closed model: Inject users with constant rate over duration
    /// Example: constant: { users: 50, duration: 60 }
    Constant {
        users: usize,
        #[serde(default)]
        duration: Option<u64>, // seconds, None = run indefinitely
    },

    /// Closed model: Gradually increase users from 0 to target over duration
    /// Example: ramp_up: { users: 100, duration: 300 }
    RampUp {
        users: usize,
        duration: u64, // seconds
    },

    /// Closed model: Ramp down from current users to target over duration
    /// Example: ramp_down: { users: 10, duration: 120 }
    RampDown {
        users: usize,
        duration: u64, // seconds
    },

    /// Open model: Inject users at constant rate (users per second)
    /// Example: constant_users_per_sec: { rate: 10, duration: 300 }
    ConstantUsersPerSec {
        rate: f64, // users per second
        duration: u64, // seconds
    },

    /// Open model: Gradually increase injection rate from start to end
    /// Example: ramp_users_per_sec: { start_rate: 1, end_rate: 50, duration: 600 }
    RampUsersPerSec {
        start_rate: f64,
        end_rate: f64,
        duration: u64, // seconds
    },

    /// Closed model: Heaviside step function (sudden increase)
    /// Ramps from 0 to target, holds, then continues
    /// Example: heaviside_users: { users: 200, ramp_duration: 30, hold_duration: 300 }
    HeavisideUsers {
        users: usize,
        ramp_duration: u64,
        hold_duration: u64,
    },

    /// Open model: Inject users following a custom profile defined by steps
    /// Example: stages: [{ duration: 60, rate: 5 }, { duration: 120, rate: 20 }]
    Stages {
        stages: Vec<Stage>,
    },
}

/// A stage in a multi-stage load profile
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Stage {
    pub duration: u64, // seconds
    pub rate: f64,     // users per second (for open model) or target users (for closed)
}

/// Flow control step in a scenario
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum FlowStep {
    /// Generic request wrapper with protocol
    Request {
        protocol: String,
        name: String,
        method: String,
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
        #[serde(default)]
        body: Option<String>,
        #[serde(default)]
        checks: Vec<Check>,
        #[serde(default)]
        extractions: Vec<Extraction>,
    },
    /// HTTP request
    HttpRequest {
        name: String,
        method: String,
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
        #[serde(default)]
        body: Option<String>,
        #[serde(default)]
        checks: Vec<Check>,
        #[serde(default)]
        extractions: Vec<Extraction>,
    },

    /// WebSocket operations
    WebSocket {
        name: String,
        operation: WsOperation,
    },

    /// Database query
    JdbcQuery {
        name: String,
        connection: String,
        query: String,
        #[serde(default)]
        parameters: Vec<String>,
        #[serde(default)]
        extractions: Vec<Extraction>,
    },

    /// JMS operations
    JmsMessage {
        name: String,
        destination: String,
        message: String,
        #[serde(default)]
        message_type: JmsMessageType,
    },

    /// Kafka producer
    KafkaPublish {
        name: String,
        topic: String,
        key: Option<String>,
        message: String,
        #[serde(default)]
        partition: Option<i32>,
    },

    /// Pause execution
    Wait {
        duration: u64, // milliseconds
    },

    /// Repeat a block of steps N times
    Repeat {
        times: usize,
        steps: Vec<FlowStep>,
    },

    /// Execute steps for a duration (time-based loop)
    During {
        duration: u64, // seconds
        steps: Vec<FlowStep>,
    },

    /// Conditional execution
    If {
        condition: String, // e.g., "{{status}} == 200"
        then_steps: Vec<FlowStep>,
        #[serde(default)]
        else_steps: Vec<FlowStep>,
    },

    /// Feed data from feeder into session
    Feed {
        feeder: String, // reference to named feeder
    },
}

/// WebSocket operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsOperation {
    Connect { url: String },
    Send { message: String },
    Receive { 
        #[serde(default)]
        timeout_ms: Option<u64>,
        #[serde(default)]
        extractions: Vec<Extraction>,
    },
    Close,
}

/// JMS message types
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JmsMessageType {
    #[default]
    Text,
    Bytes,
    Object,
}

/// Response validation check
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Check {
    /// Check HTTP status code
    Status { 
        expected: u16,
    },

    /// Check response body contains text
    BodyContains { 
        text: String,
    },

    /// Check response body matches regex
    BodyMatches { 
        pattern: String,
    },

    /// Check header value
    HeaderEquals { 
        name: String,
        value: String,
    },

    /// Check JSON path value
    JsonPath { 
        path: String,
        expected: String,
    },

    /// Check response time threshold
    ResponseTime { 
        max_ms: u64,
    },
}

/// Extract data from response into session
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Extraction {
    /// Extract from JSON response using JSONPath
    JsonPath { 
        path: String,
        save_as: String,
    },

    /// Extract using regex with capture group
    Regex { 
        pattern: String,
        group: usize,
        save_as: String,
    },

    /// Extract response header
    Header { 
        name: String,
        save_as: String,
    },

    /// Extract entire response body
    Body { 
        save_as: String,
    },

    /// Extract CSS selector (for HTML responses)
    CssSelector { 
        selector: String,
        attribute: Option<String>,
        save_as: String,
    },
}

/// Complete scenario definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    /// Unique scenario name
    pub name: String,

    /// Optional description
    #[serde(default)]
    pub description: Option<String>,

    /// Load profile configuration
    pub load_model: LoadModel,

    /// Named feeders that can be referenced in steps
    #[serde(default)]
    pub feeders: HashMap<String, FeederConfig>,

    /// Global variables available to all virtual users
    #[serde(default)]
    pub variables: HashMap<String, String>,

    /// Scenario execution steps
    pub steps: Vec<FlowStep>,

    /// Maximum duration for the entire scenario (seconds)
    #[serde(default)]
    pub max_duration: Option<u64>,

    /// Think time between steps (milliseconds)
    #[serde(default)]
    pub think_time: Option<u64>,
}

impl Scenario {
    /// Load a scenario from a YAML file
    pub async fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = tokio::fs::read_to_string(path).await?;
        // Support both legacy root and new `scenario:` wrapper
        // Try deserializing as wrapped first, then fallback
        #[derive(Deserialize)]
        struct Wrapped { scenario: Scenario }
        if let Ok(wrapped) = serde_yaml::from_str::<Wrapped>(&content) {
            Ok(wrapped.scenario)
        } else {
            let scenario: Scenario = serde_yaml::from_str(&content)?;
            Ok(scenario)
        }
    }

    /// Get total expected users based on load model
    pub fn expected_users(&self) -> Option<usize> {
        match &self.load_model {
            LoadModel::AtOnceUsers { users } => Some(*users),
            LoadModel::Constant { users, .. } => Some(*users),
            LoadModel::RampUp { users, .. } => Some(*users),
            LoadModel::RampDown { users, .. } => Some(*users),
            LoadModel::HeavisideUsers { users, .. } => Some(*users),
            // Open models don't have a fixed user count
            LoadModel::ConstantUsersPerSec { rate, duration } => {
                Some((rate * (*duration as f64)) as usize)
            }
            LoadModel::RampUsersPerSec { start_rate, end_rate, duration } => {
                // Average rate over duration
                let avg_rate = (start_rate + end_rate) / 2.0;
                Some((avg_rate * (*duration as f64)) as usize)
            }
            LoadModel::Stages { stages } => {
                let total: f64 = stages.iter()
                    .map(|s| s.rate * s.duration as f64)
                    .sum();
                Some(total as usize)
            }
        }
    }

    /// Get scenario duration in seconds
    pub fn duration(&self) -> Option<u64> {
        if let Some(max) = self.max_duration {
            return Some(max);
        }

        match &self.load_model {
            LoadModel::AtOnceUsers { .. } => None,
            LoadModel::Constant { duration, .. } => *duration,
            LoadModel::RampUp { duration, .. } => Some(*duration),
            LoadModel::RampDown { duration, .. } => Some(*duration),
            LoadModel::ConstantUsersPerSec { duration, .. } => Some(*duration),
            LoadModel::RampUsersPerSec { duration, .. } => Some(*duration),
            LoadModel::HeavisideUsers { ramp_duration, hold_duration, .. } => {
                Some(ramp_duration + hold_duration)
            }
            LoadModel::Stages { stages } => {
                Some(stages.iter().map(|s| s.duration).sum())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_model_deserialization() {
        // Test AtOnceUsers
        let yaml = r#"
type: at_once_users
users: 100
"#;
        let model: LoadModel = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(model, LoadModel::AtOnceUsers { users: 100 });

        // Test RampUp
        let yaml = r#"
type: ramp_up
users: 50
duration: 300
"#;
        let model: LoadModel = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(model, LoadModel::RampUp { users: 50, duration: 300 });

        // Test ConstantUsersPerSec
        let yaml = r#"
type: constant_users_per_sec
rate: 10.5
duration: 60
"#;
        let model: LoadModel = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(model, LoadModel::ConstantUsersPerSec { rate: 10.5, duration: 60 });
    }

    #[test]
    fn test_scenario_expected_users() {
        let scenario = Scenario {
            name: "test".to_string(),
            description: None,
            load_model: LoadModel::Constant { users: 50, duration: Some(120) },
            feeders: HashMap::new(),
            variables: HashMap::new(),
            steps: vec![],
            max_duration: None,
            think_time: None,
        };

        assert_eq!(scenario.expected_users(), Some(50));
    }

    #[test]
    fn test_scenario_duration() {
        let scenario = Scenario {
            name: "test".to_string(),
            description: None,
            load_model: LoadModel::RampUp { users: 100, duration: 300 },
            feeders: HashMap::new(),
            variables: HashMap::new(),
            steps: vec![],
            max_duration: Some(600),
            think_time: None,
        };

        // max_duration takes precedence
        assert_eq!(scenario.duration(), Some(600));
    }

    #[test]
    fn test_flow_step_deserialization() {
        let yaml = r#"
action: http_request
name: Get User
method: GET
url: https://api.example.com/users/{{user_id}}
headers:
  Authorization: Bearer {{token}}
checks:
  - type: status
    expected: 200
extractions:
  - type: json_path
    path: $.id
    save_as: user_id
"#;
        let step: FlowStep = serde_yaml::from_str(yaml).unwrap();
        
        match step {
            FlowStep::HttpRequest { name, method, url, headers, checks, extractions, .. } => {
                assert_eq!(name, "Get User");
                assert_eq!(method, "GET");
                assert!(url.contains("{{user_id}}"));
                assert_eq!(headers.len(), 1);
                assert_eq!(checks.len(), 1);
                assert_eq!(extractions.len(), 1);
            }
            _ => panic!("Expected HttpRequest"),
        }
    }

    #[test]
    fn test_check_types() {
        let yaml = r#"
- type: status
  expected: 200
- type: body_contains
  text: "success"
- type: response_time
  max_ms: 1000
"#;
        let checks: Vec<Check> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(checks.len(), 3);
    }

    #[test]
    fn test_extraction_types() {
        let yaml = r#"
- type: json_path
  path: $.data.id
  save_as: user_id
- type: header
  name: X-Request-ID
  save_as: request_id
- type: regex
  pattern: "token=([a-zA-Z0-9]+)"
  group: 1
  save_as: auth_token
"#;
        let extractions: Vec<Extraction> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(extractions.len(), 3);
    }

    #[tokio::test]
    async fn test_load_scenario_from_file() {
        // Load the test scenario we created
        let result = Scenario::from_file("scenarios/test_scenario.yaml").await;
        assert!(result.is_ok(), "Failed to load test scenario: {:?}", result.err());
        
        let scenario = result.unwrap();
        assert_eq!(scenario.name, "Simple Test Scenario");
        assert_eq!(scenario.description, Some("Basic scenario for unit testing".to_string()));
        
        // Verify load model
        match scenario.load_model {
            LoadModel::RampUp { users, duration } => {
                assert_eq!(users, 10);
                assert_eq!(duration, 60);
            }
            _ => panic!("Expected RampUp load model"),
        }
        
        // Verify feeders
        assert_eq!(scenario.feeders.len(), 1);
        assert!(scenario.feeders.contains_key("test_users"));
        
        // Verify variables
        assert_eq!(scenario.variables.get("base_url"), Some(&"https://api.example.com".to_string()));
        
        // Verify steps
        assert!(scenario.steps.len() > 0);
        
        // Verify duration calculations
        assert_eq!(scenario.duration(), Some(300));
        assert_eq!(scenario.expected_users(), Some(10));
        assert_eq!(scenario.think_time, Some(500));
    }
}
