# Session Architecture

## Overview

Yes! Each Virtual User (VU) has its **own isolated Session** that maintains state throughout the test execution. This is exactly how Gatling works.

## Session Per Virtual User

```
VU #1 → Session #1 (username=alice, auth_token=xyz, order_id=123, ...)
VU #2 → Session #2 (username=bob, auth_token=abc, order_id=456, ...)
VU #3 → Session #3 (username=charlie, auth_token=def, order_id=789, ...)
```

Each session is completely isolated and maintains:

1. **Feeder Data**: Variables loaded from CSV/JSON (username, password, product_id, etc.)
2. **Extracted Values**: Data extracted from responses (auth_token, order_id, session_id, etc.)
3. **Loop Context**: Current iteration count for repeat/during loops
4. **Protocol State**: Connections, cookies, WebSocket handles per user

## How It Works

### 1. Feeder Data Per VU

When a VU starts, it gets its own row from the feeder based on strategy:

**Cyclic Strategy:**
```
VU #1 → Row 1: alice, Pass123!
VU #2 → Row 2: bob, SecureP@ss
VU #3 → Row 3: charlie, MyP@ssw0rd
VU #4 → Row 1: alice, Pass123!  (cycles back)
```

**Random Strategy:**
```
VU #1 → Random row: diana, Test1234!
VU #2 → Random row: alice, Pass123!
VU #3 → Random row: bob, SecureP@ss
```

**Sequential Strategy:**
```
VU #1 → Row 1
VU #2 → Row 2
VU #3 → Row 3
VU #4 → (no more data, VU stops)
```

### 2. Variable Scoping

Variables are **scoped to each VU's session**:

```yaml
flow:
  # VU #1 session: username=alice
  # VU #2 session: username=bob
  
  - type: http_request
    name: Login
    path: /auth/login
    body:
      username: "{{username}}"  # Each VU uses its own username
    extractions:
      - type: json_path
        path: $.token
        target_var: auth_token    # Stored in THIS VU's session only
  
  # VU #1 session: auth_token=token_for_alice
  # VU #2 session: auth_token=token_for_bob
  
  - type: http_request
    name: Get Profile
    path: /profile
    headers:
      Authorization: Bearer {{auth_token}}  # Each VU uses its own token
```

### 3. State Isolation Example

**E-commerce Flow (2 concurrent VUs):**

```
Time  VU #1 (alice)                          VU #2 (bob)
----  ------------------------------------   ------------------------------------
0s    Load feeder: username=alice            Load feeder: username=bob
1s    Login → extract auth_token=ABC         Login → extract auth_token=XYZ
2s    Browse product_id=PROD-001             Browse product_id=PROD-005
3s    Add to cart (token=ABC, prod=001)      Add to cart (token=XYZ, prod=005)
4s    Checkout → extract order_id=ORD-123    Checkout → extract order_id=ORD-456
5s    Verify order ORD-123                   Verify order ORD-456
```

**No interference**: VU #1's variables never affect VU #2's variables.

### 4. Loop State Per Session

```yaml
- type: repeat
  times: 5
  steps:
    - type: http_request
      name: Get Product
      path: /products/{{product_id}}
```

**VU #1's loop state:**
- Iteration 1: product_id from feeder row 1
- Iteration 2: product_id from feeder row 1 (same VU, same session)
- ...

**VU #2's loop state:**
- Iteration 1: product_id from feeder row 2
- Iteration 2: product_id from feeder row 2
- ...

### 5. WebSocket Session Example

```yaml
- type: ws_connect
  name: Connect
  # Each VU maintains its own WebSocket connection

- type: ws_send
  name: Auth
  message:
    userId: "{{user_id}}"      # VU #1: WS-U001, VU #2: WS-U002
    token: "{{auth_token}}"

- type: ws_receive
  name: Get Session
  extractions:
    - type: json_path
      path: $.sessionId
      target_var: session_id    # VU #1: session-abc, VU #2: session-xyz

- type: ws_send
  name: Join Room
  message:
    sessionId: "{{session_id}}"  # Each VU uses its own session ID
```

## Implementation Architecture

### Session Struct
```rust
pub struct Session {
    vu_id: usize,
    variables: HashMap<String, String>,  // Feeder + extracted values
    connections: HashMap<String, Connection>,  // HTTP client, WS handles, DB connections
    loop_stack: Vec<LoopContext>,  // Nested loop state
}
```

### Variable Resolution
```rust
impl Session {
    pub fn set(&mut self, key: &str, value: String) {
        self.variables.insert(key.to_string(), value);
    }
    
    pub fn get(&self, key: &str) -> Option<&String> {
        self.variables.get(key)
    }
    
    pub fn resolve_template(&self, template: &str) -> String {
        // Replace {{var}} with session.get("var")
        // e.g., "/users/{{user_id}}" → "/users/1001"
    }
}
```

### VU Execution
```rust
impl VirtualUser {
    pub async fn run(&mut self, scenario: Arc<Scenario>, feeders: Arc<FeedersMap>) {
        // 1. Create isolated session for this VU
        let mut session = Session::new(self.vu_id);
        
        // 2. Load feeder data into session
        for feeder_name in &scenario.feeders {
            if let Some(row) = feeders.get(feeder_name).next_row().await {
                for (key, value) in row {
                    session.set(&key, value);
                }
            }
        }
        
        // 3. Execute flow steps with this session
        for step in &scenario.flow {
            self.execute_step(step, &mut session).await;
        }
    }
    
    async fn execute_step(&self, step: &FlowStep, session: &mut Session) {
        match step {
            FlowStep::HttpRequest(req) => {
                // Resolve path template with session variables
                let path = session.resolve_template(&req.path);
                
                // Make request
                let response = self.client.execute(&req.method, &path, ...).await;
                
                // Extract values into THIS session
                for extraction in &req.extractors {
                    let value = extract_from_response(&response, extraction);
                    session.set(&extraction.target_var, value);
                }
            }
            // ... other step types
        }
    }
}
```

## Key Benefits

✅ **Isolation**: Each user's data never interferes with others
✅ **Realistic**: Simulates real user sessions with state
✅ **Flexible**: Variables can be chained (extract from request 1, use in request 2)
✅ **Safe**: Concurrent execution without race conditions
✅ **Testable**: Each VU can be debugged independently

## Example Session Lifecycle

```
1. VU #42 spawns
2. Create Session { vu_id: 42, variables: {} }
3. Load feeder: session.set("username", "alice")
4. Execute Login → extract auth_token → session.set("auth_token", "xyz123")
5. Execute Get Cart → use {{auth_token}} from session
6. Execute Add Item → extract cart_id → session.set("cart_id", "456")
7. Execute Checkout → use {{cart_id}} from session
8. VU #42 completes, session destroyed
```

Session lives for the entire VU execution, maintaining all state throughout the user journey.
