# Domain Implementation Pattern

Every domain in Fitz follows the same structure. This guide shows how to implement a new domain or modify existing ones.

## Directory Structure

```
src/core/DOMAIN_NAME/
├── mod.rs           - Domain struct and impl Domain trait
├── handler.rs       - Parse TLV, call service, build response
├── service.rs       - Business logic
├── types.rs         - Operation enums, data structures
└── [optional]
    ├── store.rs     - Storage-specific code
    └── client.rs    - In-process client helpers (for RPC)
```

## The Domain Trait

All domains implement this trait (defined in `src/core/domain.rs`):

```rust
#[async_trait]
pub trait Domain: Send + Sync {
    /// Core request handler - REQUIRED
    async fn handle(&self, request: DomainContext) -> DomainResponse;
    
    /// Cleanup when channel closes - optional (default: no-op)
    async fn cleanup_channel(&self, rf: RouteFamilyId, channel_id: u32) {
        // Default: do nothing
    }
    
    /// Subscribe to route - optional (default: error)
    async fn subscribe(
        &self,
        _route: String,
        _channel_id: u32,
        _sender: SubSender,
    ) -> Result<u64, String> {
        Err("not implemented".to_string())
    }
    
    /// Unsubscribe from route - optional (default: false)
    async fn unsubscribe(&self, _id: u64) -> bool {
        false
    }
}
```

## The Flow Pattern

Every domain request follows this pattern:

```
1. Transport builds TLV payload
   └─ TAG_ROUTE, TAG_ID, TAG_BODY, TAG_LEASE, etc.

2. Session calls engine.dispatch(route, payload, channel_id)

3. Engine routes to domain.handle(request)
   ├─ request.route_str: full route "notice://topic"
   ├─ request.route: parsed route struct
   ├─ request.payload: TLV bytes
   └─ request.channel_id: for subscriptions

4. Handler (handler.rs):
   ├─ Parse TLV payload into typed request
   ├─ Extract operation type
   ├─ Call service method
   ├─ Get result
   └─ Build TLV response

5. Return DomainResponse:
   ├─ Ok(empty) → empty response
   ├─ Frame(bytes) → TLV response
   └─ Error(msg) → error response
```

## Example: Notice Domain

### 1. mod.rs - Domain struct and trait impl

```rust
// src/core/notice/mod.rs

use async_trait::async_trait;
use crate::core::domain::{Domain, DomainContext, DomainResponse};
use std::sync::Arc;

pub use self::handler::NoticeHandler as NoticeDomain;
pub use self::service::NoticeService;

mod handler;
mod service;
mod types;

pub use self::types::*;

// Factory function
impl NoticeDomain {
    pub fn new() -> Self {
        let service = Arc::new(NoticeService::new());
        Self { service }
    }
    
    pub fn get_service(&self) -> Arc<NoticeService> {
        Arc::clone(&self.service)
    }
}
```

### 2. types.rs - Operations and data structures

```rust
// src/core/notice/types.rs

#[derive(Debug, Clone, Copy)]
pub enum NoticeOp {
    Publish,      // Send message to subscribers
    Subscribe,    // Register as subscriber
}

impl NoticeOp {
    pub fn parse(op_str: Option<&str>) -> Result<Self, String> {
        match op_str {
            Some("pub") => Ok(NoticeOp::Publish),
            Some("sub") => Ok(NoticeOp::Subscribe),
            _ => Err("unknown operation".to_string()),
        }
    }
}

pub struct Notification {
    pub id: String,
    pub body: Vec<u8>,
}
```

### 3. handler.rs - Parse TLV, call service, build response

```rust
// src/core/notice/handler.rs

use crate::protocol::frame::{build_tlv, find_tlv};
use crate::protocol::tags::*;
use crate::core::domain::{Domain, DomainContext, DomainResponse};

#[derive(Clone)]
pub struct NoticeHandler {
    service: Arc<NoticeService>,
}

#[async_trait]
impl Domain for NoticeHandler {
    async fn handle(&self, request: DomainContext) -> DomainResponse {
        // 1. Parse operation from route
        let operation = match NoticeOp::parse(request.route.operation.as_deref()) {
            Ok(op) => op,
            Err(e) => return DomainResponse::Error(e),
        };

        // 2. Route to handler based on operation
        match operation {
            NoticeOp::Publish => {
                // Parse TLV payload
                let id = find_tlv(&request.payload, TAG_ID)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                
                let body = find_tlv(&request.payload, TAG_BODY)
                    .map(|b| b.to_vec())
                    .unwrap_or_default();

                // Call service
                let (delivered, failed) = self.service.publish(&request.route_str, id, body);

                // Build response TLV
                let mut response = Vec::new();
                build_tlv(TAG_NOTIFICATION, &[], &mut response);
                build_tlv(TAG_DELIVERED, &delivered.to_be_bytes(), &mut response);
                build_tlv(TAG_FAILED, &failed.to_be_bytes(), &mut response);

                DomainResponse::Frame(PooledFrame::from_vec(response))
            }
            
            NoticeOp::Subscribe => {
                // Extract route for subscription
                let sub_route = /* extract from payload */;
                
                // Call service (inherited from handler)
                let id = self.service.subscribe(sub_route, request.channel_id, sender);
                
                // Response is implicit in subscription
                DomainResponse::Ok
            }
        }
    }

    async fn subscribe(
        &self,
        route: String,
        channel_id: u32,
        sender: SubSender,
    ) -> Result<u64, String> {
        self.service.subscribe(route, channel_id, sender).await
    }

    async fn unsubscribe(&self, id: u64) -> bool {
        self.service.unsubscribe(id).await
    }
}
```

### 4. service.rs - Business logic

```rust
// src/core/notice/service.rs

use std::sync::Arc;
use crate::routing::RouteTable;

pub struct NoticeService {
    route_table: Arc<RouteTable>,
}

impl NoticeService {
    pub fn new() -> Self {
        Self {
            route_table: Arc::new(RouteTable::new()),
        }
    }

    pub fn publish(&self, route: &str, msg_id: String, body: Vec<u8>) -> (u32, u32) {
        // Business logic: publish to all subscribers on this route
        self.route_table.notify(route, msg_id, body)
    }

    pub async fn subscribe(
        &self,
        route: String,
        channel_id: u32,
        sender: SubSender,
    ) -> Result<u64, String> {
        // Business logic: register subscriber
        let id = self.route_table.subscribe(route, channel_id, sender);
        Ok(id)
    }

    pub async fn unsubscribe(&self, id: u64) -> bool {
        // Business logic: remove subscriber
        self.route_table.unsubscribe(id)
    }
}
```

## Key Patterns

### 1. TLV Parsing Pattern

```rust
// From handler.rs - parse incoming TLV
let my_field = find_tlv(&request.payload, TAG_MY_FIELD)
    .and_then(|b| std::str::from_utf8(b).ok())
    .map(|s| s.to_string())
    .unwrap_or_default();
```

### 2. TLV Building Pattern

```rust
// From handler.rs - build outgoing TLV
let mut response = Vec::new();
build_tlv(TAG_RESULT, result.as_bytes(), &mut response);
build_tlv(TAG_BODY, body, &mut response);

DomainResponse::Frame(PooledFrame::from_vec(response))
```

### 3. Async Service Pattern

```rust
// Service calls are async (might hit storage)
pub async fn my_operation(&self, param: String) -> Result<String, String> {
    // Do async work
    Ok(result)
}

// Called from handler
let result = self.service.my_operation(param).await?;
```

### 4. Error Handling Pattern

```rust
// Parse failures immediately return error
let value: u32 = find_tlv(&request.payload, TAG_VALUE)
    .and_then(|b| /* parse */)
    .ok_or("missing or invalid TAG_VALUE")?;

// Service errors propagate
let result = self.service.operation().await?;
```

## Adding a New Domain

1. **Create directory:** `src/core/mydom/`
2. **Create files:** `mod.rs`, `types.rs`, `handler.rs`, `service.rs`
3. **Implement Domain trait** in handler.rs
4. **Register in engine:** `src/core/engine.rs`
   - Create domain: `let my_domain = Arc::new(MyDomain::new());`
   - Insert: `domains.insert("myscheme", Arc::clone(&my_domain) as Arc<dyn Domain>);`
5. **Add to main:** `src/core/mod.rs`
   - Export: `pub use mydom::MyDomain;`

## Transport Routes

The route format is:
```
scheme://resource/path

Examples:
- notice://alerts/system/startup
- rpc://api/users/list
- queue://jobs/process
- lease://items/123
- kv://users/john
- stream://events/2024
- control://manage
```

The handler parses:
- `scheme` - Determines which domain handles it
- `resource` - Primary target (topic, queue name, key, etc.)
- `path` - Optional hierarchical path for filtering

## Response Types

```rust
enum DomainResponse {
    Ok,                      // Empty success response
    Frame(PooledFrame),      // TLV response
    Error(String),           // Error message (sent as FRAME_ERR)
}
```

## Common TLV Tags

From `src/protocol/tags.rs`:
```rust
TAG_ROUTE       = 0x01  // Route/topic
TAG_ID          = 0x02  // Message/resource ID
TAG_BODY        = 0x22  // Message body
TAG_SEQ         = 0x24  // Sequence number
TAG_LEASE       = 0x26  // Lease time
TAG_DELIVERY_TOKEN = 0x27  // Token for consume
TAG_ROUTE_REPLY = 0x2E  // Reply-to route
TAG_STREAM_END  = 0x25  // End-of-stream marker
TAG_METADATA    = 0xA3  // Metadata
TAG_NOTIFICATION = 0x50  // Notification marker
...
```

## Testing Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_handle_publish_operation() {
        // Arrange
        let handler = NoticeHandler::new();
        let mut payload = Vec::new();
        build_tlv(TAG_ID, b"msg123", &mut payload);
        build_tlv(TAG_BODY, b"hello", &mut payload);
        
        // Act
        let response = handler.handle(/* request */).await;
        
        // Assert
        assert!(matches!(response, DomainResponse::Frame(_)));
    }
}
```

## Debugging Tips

1. **Payload parsing fails?** Check TAG values in `src/protocol/tags.rs`
2. **Response not returned?** Ensure `DomainResponse::Frame(...)` wraps result
3. **Subscription not working?** Verify `subscribe()` method is implemented
4. **Route not found?** Check domain is registered in `engine.rs`
5. **Permissions denied?** Error happens in `session/mod.rs`, not domain

## References

- Domain trait: `src/core/domain.rs`
- Notice domain (reference): `src/core/notice/`
- RPC domain (reference): `src/core/rpc/`
- Engine dispatcher: `src/core/engine.rs`
- Session handler: `src/transport/session/mod.rs`
