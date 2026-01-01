# Sessions Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Ephemeral (lost on restart)  
**Last Updated:** December 11, 2025  

---

## Overview

The Sessions domain manages WebSocket connection lifecycle, identity binding, and message routing coordination. Each active WebSocket connection has associated ephemeral state tracked by SessionActor.

### Key Features

- **Connection lifecycle**: Connect, authenticate, disconnect
- **Identity binding**: Associate JWT/token with connection
- **Message correlation**: Track request/response pairs
- **Route-family isolation**: Enforce route family boundaries per connection
- **Heartbeat/keepalive**: Detect dead connections
- **Graceful shutdown**: Clean session termination

### Ephemeral Characteristics

- **Not persisted**: Session state lost on restart
- **Reconstructed**: Clients reconnect and re-authenticate
- **Connection-scoped**: State lives only as long as WebSocket
- **No replay**: Inflight messages not recovered

### Use Cases

- WebSocket connection management
- JWT validation and identity caching
- Per-connection message routing
- Connection-level quotas and rate limiting
- Session diagnostics and monitoring

---

## Route Format

Sessions use internal routing (not client-facing):

```
session://{session_id}/{operation}
```

### Examples (Internal)
- `session://sess_abc123/send` - Send message to session
- `session://sess_abc123/disconnect` - Terminate session
- `session://sess_abc123/heartbeat` - Connection keepalive

---

## Core Operations

### 1. Register Session

Called when WebSocket connection established.

**Internal Message:**
```rust
SessionMsg::Register {
    session_id: String,
    conn_tx: mpsc::Sender<Vec<u8>>,
    remote_addr: SocketAddr,
    reply_to: ActorRef<SessionReply>,
}
```

**State Created:**
```rust
struct SessionState {
    session_id: String,
    conn_tx: mpsc::Sender<Vec<u8>>,
    remote_addr: SocketAddr,
    route_family: Option<String>,
    identity: Option<Identity>,
    connected_at: Instant,
    last_activity: Instant,
    message_count: u64,
}
```

---

### 2. Authenticate Session

Validate JWT and bind identity to session.

**Internal Message:**
```rust
SessionMsg::Authenticate {
    session_id: String,
    token: String,
    reply_to: ActorRef<SessionReply>,
}
```

**Flow:**
1. SessionActor receives authenticate request
2. SessionActor forwards to AuthEvalActor for validation
3. AuthEvalActor validates JWT, extracts claims
4. SessionActor updates session state with identity

**State Update:**
```rust
struct Identity {
    subject: String,           // JWT "sub" claim
    route_family: String,      // JWT "route_family" claim
    roles: Vec<String>,        // JWT "roles" claim
    permissions: Vec<String>,  // Resolved from roles
    expires_at: Instant,       // JWT "exp" claim
}
```

---

### 3. Route Message

Forward parsed message to appropriate domain.

**Internal Message:**
```rust
SessionMsg::RouteMessage {
    session_id: String,
    frame: TlvFrame,
    reply_to: ActorRef<SessionReply>,
}
```

**Flow:**
1. SessionActor receives parsed TLV frame
2. Extract route from frame
3. Check authorization (via AuthEvalActor)
4. Forward to appropriate domain actor
5. Track correlation for reply routing

---

### 4. Send to Session

Send response/notification to client.

**Internal Message:**
```rust
SessionMsg::Send {
    session_id: String,
    frame: Vec<u8>,
}
```

**Behavior:**
- Look up session state
- Write frame bytes to conn_tx channel
- Update last_activity timestamp
- Increment message_count

---

### 5. Disconnect Session

Gracefully terminate connection.

**Internal Message:**
```rust
SessionMsg::Disconnect {
    session_id: String,
    reason: String,
}
```

**Cleanup:**
- Remove session state
- Close conn_tx channel
- Notify RealmActor (decrement connection count)
- Cancel any pending requests

---

### 6. Heartbeat

Update session liveness.

**Internal Message:**
```rust
SessionMsg::Heartbeat {
    session_id: String,
}
```

**Behavior:**
- Update last_activity timestamp
- Reset idle timeout
- Send heartbeat response to client

---

## Actor Implementation

### SessionActor State

```rust
pub struct SessionActor {
    /// Active sessions keyed by session_id
    sessions: DashMap<String, SessionState>,
    
    /// References to other actors
    router: ActorRef<RoutingMsg>,
    auth_eval: ActorRef<AuthEvalMsg>,
    realm: ActorRef<RealmMsg>,
    
    /// Configuration
    idle_timeout: Duration,
    max_message_size: usize,
}

struct SessionState {
    session_id: String,
    conn_tx: mpsc::Sender<Vec<u8>>,
    remote_addr: SocketAddr,
    route_family: Option<String>,
    identity: Option<Identity>,
    connected_at: Instant,
    last_activity: Instant,
    message_count: u64,
    pending_requests: HashMap<u32, ActorRef<SessionReply>>,
}
```

---

### Message Handler

```rust
impl Actor for SessionActor {
    type Message = SessionMsg;
    
    fn on_message(&mut self, msg: Self::Message, ctx: &ActorContext<Self>) {
        match msg {
            SessionMsg::Register { session_id, conn_tx, remote_addr, reply_to } => {
                let state = SessionState {
                    session_id: session_id.clone(),
                    conn_tx,
                    remote_addr,
                    route_family: None,
                    identity: None,
                    connected_at: Instant::now(),
                    last_activity: Instant::now(),
                    message_count: 0,
                    pending_requests: HashMap::new(),
                };
                
                self.sessions.insert(session_id.clone(), state);
                
                reply_to.send(SessionReply::Registered { session_id });
            }
            
            SessionMsg::Authenticate { session_id, token, reply_to } => {
                // Forward to AuthEvalActor
                self.auth_eval.send(AuthEvalMsg::ValidateToken {
                    token,
                    reply_to: ctx.actor_ref(),
                });
                
                // Store reply_to for async response (needs correlation)
            }
            
            SessionMsg::RouteMessage { session_id, frame, reply_to } => {
                let session = match self.sessions.get(&session_id) {
                    Some(s) => s,
                    None => {
                        reply_to.send(SessionReply::Error("session_not_found".to_string()));
                        return;
                    }
                };
                
                // Extract route from frame
                let route = extract_route_from_frame(&frame);
                
                // Check authorization
                if let Some(identity) = &session.identity {
                    self.auth_eval.send(AuthEvalMsg::CheckPermission {
                        identity: identity.clone(),
                        route: route.clone(),
                        operation: extract_operation(&route),
                        reply_to: ctx.actor_ref(),
                    });
                } else {
                    reply_to.send(SessionReply::Error("not_authenticated".to_string()));
                }
                
                // On authz success, forward to router
                self.router.send(RoutingMsg::Dispatch {
                    route,
                    frame,
                    session_id,
                    reply_to: ctx.actor_ref(),
                });
            }
            
            SessionMsg::Send { session_id, frame } => {
                if let Some(mut session) = self.sessions.get_mut(&session_id) {
                    session.last_activity = Instant::now();
                    session.message_count += 1;
                    
                    let _ = session.conn_tx.try_send(frame);
                }
            }
            
            SessionMsg::Disconnect { session_id, reason } => {
                if let Some((_, session)) = self.sessions.remove(&session_id) {
                    // Notify realm
                    if let Some(realm) = &session.realm {
                        self.realm.send(RealmMsg::ConnectionClosed {
                            realm: realm.clone(),
                            session_id: session_id.clone(),
                        });
                    }
                    
                    // Close channel (drops conn_tx)
                }
            }
            
            SessionMsg::Heartbeat { session_id } => {
                if let Some(mut session) = self.sessions.get_mut(&session_id) {
                    session.last_activity = Instant::now();
                    
                    // Send heartbeat response
                    let heartbeat_frame = build_heartbeat_frame();
                    let _ = session.conn_tx.try_send(heartbeat_frame);
                }
            }
        }
    }
}
```

---

### Idle Timeout Handling

Periodic task to detect idle sessions:

```rust
impl SessionActor {
    fn start_idle_checker(&self, ctx: &ActorContext<Self>) {
        let actor_ref = ctx.actor_ref();
        let timeout = self.idle_timeout;
        
        ctx.schedule_recurring(Duration::from_secs(30), move || {
            actor_ref.send(SessionMsg::CheckIdle);
        });
    }
    
    fn check_idle_sessions(&mut self) {
        let now = Instant::now();
        let timeout = self.idle_timeout;
        
        let idle_sessions: Vec<String> = self.sessions
            .iter()
            .filter(|entry| now.duration_since(entry.value().last_activity) > timeout)
            .map(|entry| entry.key().clone())
            .collect();
        
        for session_id in idle_sessions {
            self.sessions.remove(&session_id);
            // Send close frame to client
        }
    }
}
```

---

## Integration with Transport

WebSocket handler creates session:

```rust
async fn handle_websocket(socket: WebSocket, session_actor: ActorRef<SessionMsg>) {
    let session_id = generate_session_id();
    let (conn_tx, mut conn_rx) = mpsc::channel(100);
    
    // Register session
    let reply = session_actor.send_and_wait(SessionMsg::Register {
        session_id: session_id.clone(),
        conn_tx,
        remote_addr: socket.remote_addr(),
        reply_to: ...,
    }).await;
    
    // Spawn read task
    tokio::spawn(async move {
        while let Some(msg) = socket.next().await {
            let frame = parse_tlv_frame(msg?)?;
            session_actor.send(SessionMsg::RouteMessage {
                session_id: session_id.clone(),
                frame,
                reply_to: ...,
            });
        }
    });
    
    // Spawn write task
    tokio::spawn(async move {
        while let Some(frame) = conn_rx.recv().await {
            socket.send(frame).await?;
        }
    });
}
```

---

## Error Handling

### Error Codes

- `SESSION_NOT_FOUND` - Invalid session_id
- `NOT_AUTHENTICATED` - No identity bound to session
- `AUTHORIZATION_FAILED` - Permission denied
- `SESSION_EXPIRED` - Idle timeout exceeded
- `MESSAGE_TOO_LARGE` - Frame exceeds size limit

### Recovery

- **Connection loss**: Session automatically cleaned up
- **Authentication failure**: Session remains unauthenticated
- **Authorization failure**: Return error, don't terminate session

---

## Performance Characteristics

### Latency

- **Message routing**: <10µs (in-memory dispatch)
- **Authorization check**: <50µs (cached identity)
- **Send to session**: <5µs (channel send)

### Throughput

- **Concurrent sessions**: 10,000+ per instance
- **Messages per session**: 1,000+ msg/sec
- **Memory per session**: ~500 bytes

### Scalability

- DashMap for lock-free session lookup
- MPSC channels for async write
- No blocking operations in hot path

---

## Testing Strategy

### Unit Tests

- Session registration and cleanup
- Identity binding
- Message routing
- Idle timeout detection
- Heartbeat handling

### Integration Tests

- End-to-end WebSocket flow
- Authentication and authorization
- Session lifecycle
- Concurrent sessions

### Benchmarks

- Session creation/destruction
- Message routing throughput
- Memory usage with N sessions

---

## References

- [Routing Domain](ROUTING.md)
- [Auth Evaluation Domain](AUTH_EVAL.md)
- [Realm Domain](REALMS.md)
- [Transport Layer](../../src/transport/)
