use std::collections::HashMap;
use std::sync::Arc;
use std::borrow::Cow;
use regex::Regex;
use tokio::sync::Mutex;

/// Represents a virtual user's session with isolated state
/// Each VU has its own Session instance for the duration of the scenario execution
#[derive(Clone, Debug)]
pub struct Session {
    /// User ID for this virtual user
    pub user_id: usize,
    
    /// Session-scoped variables (extracted values, feeder data, etc.)
    /// OPTIMIZED: Using Cow to avoid unnecessary string clones for static/readonly values
    variables: HashMap<String, Cow<'static, str>>,
    
    /// Connection pool or state that can be shared across requests
    /// Each protocol can store its own connection state here
    connections: HashMap<String, Arc<Mutex<Box<dyn std::any::Any + Send + Sync>>>>,
    
    /// Scenario name for logging/reporting
    pub scenario_name: String,
    
    /// Start time of this session (Unix timestamp in milliseconds)
    pub start_time: i64,
}

impl Session {
    /// Create a new session for a virtual user
    pub fn new(user_id: usize, scenario_name: String) -> Self {
        Self {
            user_id,
            variables: HashMap::new(),
            connections: HashMap::new(),
            scenario_name,
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        }
    }

    /// Set a variable in the session (owned string)
    pub fn set(&mut self, key: String, value: String) {
        self.variables.insert(key, Cow::Owned(value));
    }
    
    /// Set a static variable (zero-copy for constants)
    pub fn set_static(&mut self, key: String, value: &'static str) {
        self.variables.insert(key, Cow::Borrowed(value));
    }

    /// Get a variable from the session
    pub fn get(&self, key: &str) -> Option<&str> {
        self.variables.get(key).map(|v| v.as_ref())
    }

    /// Set multiple variables at once (useful for feeder data)
    pub fn set_all(&mut self, data: HashMap<String, String>) {
        for (key, value) in data {
            self.variables.insert(key, Cow::Owned(value));
        }
    }

    /// Resolve template strings with variable substitution
    /// Example: "Hello {{name}}" with name="World" -> "Hello World"
    pub fn resolve_template(&self, template: &str) -> String {
        let re = Regex::new(r"\{\{([a-zA-Z0-9_]+)\}\}").unwrap();
        let result = re.replace_all(template, |caps: &regex::Captures| {
            let var_name = &caps[1];
            self.variables
                .get(var_name)
                .map(|v| v.as_ref())
                .unwrap_or_else(|| {
                    eprintln!("Warning: Variable '{}' not found in session", var_name);
                    ""
                })
        });
        result.to_string()
    }

    /// Store a connection for reuse (e.g., HTTP client, DB connection)
    pub fn set_connection<T: 'static + Send + Sync>(
        &mut self,
        name: String,
        connection: T,
    ) {
        let boxed: Box<dyn std::any::Any + Send + Sync> = Box::new(connection);
        self.connections.insert(name, Arc::new(Mutex::new(boxed)));
    }

    /// Get a connection by name (async version)
    pub async fn get_connection<T: 'static + Clone>(&self, name: &str) -> Option<T> {
        if let Some(conn) = self.connections.get(name) {
            let guard = conn.lock().await;
            guard.downcast_ref::<T>().cloned()
        } else {
            None
        }
    }
    
    /// Get a connection by name (synchronous version for non-async contexts)
    /// Note: This should only be used when absolutely necessary as it may block
    pub fn get_connection_blocking<T: 'static + Clone>(&self, name: &str) -> Option<T> {
        self.connections.get(name).and_then(|conn| {
            // Try to lock synchronously - may block
            if let Ok(guard) = conn.try_lock() {
                guard.downcast_ref::<T>().cloned()
            } else {
                None
            }
        })
    }

    /// Clear all session variables (useful for debugging/testing)
    pub fn clear_variables(&mut self) {
        self.variables.clear();
    }

    /// Get all variables as a reference (useful for logging/debugging)
    pub fn get_all_variables(&self) -> &HashMap<String, Cow<'static, str>> {
        &self.variables
    }

    /// Check if a variable exists
    pub fn has_variable(&self, key: &str) -> bool {
        self.variables.contains_key(key)
    }

    /// Remove a variable from the session
    pub fn remove(&mut self, key: &str) -> Option<Cow<'static, str>> {
        self.variables.remove(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_creation() {
        let session = Session::new(1, "test_scenario".to_string());
        assert_eq!(session.user_id, 1);
        assert_eq!(session.scenario_name, "test_scenario");
        assert!(session.start_time > 0);
    }

    #[test]
    fn test_variable_operations() {
        let mut session = Session::new(1, "test".to_string());
        
        // Set and get
        session.set("username".to_string(), "alice".to_string());
        assert_eq!(session.get("username"), Some("alice"));
        
        // Has variable
        assert!(session.has_variable("username"));
        assert!(!session.has_variable("password"));
        
        // Remove
        let removed = session.remove("username");
        assert_eq!(removed.as_ref().map(|s| s.as_ref()), Some("alice"));
        assert!(!session.has_variable("username"));
    }

    #[test]
    fn test_set_all() {
        let mut session = Session::new(1, "test".to_string());
        
        let mut data = HashMap::new();
        data.insert("user".to_string(), "bob".to_string());
        data.insert("email".to_string(), "bob@example.com".to_string());
        
        session.set_all(data);
        
        assert_eq!(session.get("user"), Some("bob"));
        assert_eq!(session.get("email"), Some("bob@example.com"));
    }

    #[test]
    fn test_template_resolution() {
        let mut session = Session::new(1, "test".to_string());
        session.set("name".to_string(), "Alice".to_string());
        session.set("age".to_string(), "30".to_string());
        
        // Simple substitution
        let result = session.resolve_template("Hello {{name}}!");
        assert_eq!(result, "Hello Alice!");
        
        // Multiple variables
        let result = session.resolve_template("{{name}} is {{age}} years old");
        assert_eq!(result, "Alice is 30 years old");
        
        // Missing variable (should replace with empty string)
        let result = session.resolve_template("City: {{city}}");
        assert_eq!(result, "City: ");
        
        // No variables
        let result = session.resolve_template("No variables here");
        assert_eq!(result, "No variables here");
    }

    #[test]
    fn test_clear_variables() {
        let mut session = Session::new(1, "test".to_string());
        session.set("key1".to_string(), "value1".to_string());
        session.set("key2".to_string(), "value2".to_string());
        
        assert_eq!(session.get_all_variables().len(), 2);
        
        session.clear_variables();
        assert_eq!(session.get_all_variables().len(), 0);
    }

    #[tokio::test]
    async fn test_connection_storage() {
        let mut session = Session::new(1, "test".to_string());
        
        // Store a connection
        let http_client = "http_client_instance".to_string();
        session.set_connection("http".to_string(), http_client.clone());
        
        // Retrieve the connection (async)
        let retrieved: Option<String> = session.get_connection("http").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), "http_client_instance");
        
        // Also test blocking version
        let retrieved_blocking: Option<String> = session.get_connection_blocking("http");
        assert!(retrieved_blocking.is_some());
        assert_eq!(retrieved_blocking.unwrap(), "http_client_instance");
    }
}
