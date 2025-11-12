# Fitz System Architecture - Visual Guide

## Complete Request/Response Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CLIENT (Browser, SDK, etc.)                                                 │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                          WebSocket upgrade
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ TRANSPORT (ws.rs / tcp.rs / http.rs)                                        │
│ • Accept connection                                                          │
│ • TLS termination                                                            │
│ • Parse wire format (frames)                                                │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    ┌────────────▼───────────┐
                    │      Muxer (mux.rs)    │
                    │ Demultiplex by channel │
                    └────────────┬───────────┘
                                 │
                          channel_id = 1
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ SESSION HANDLER (session/mod.rs)                                            │
│                                                                              │
│  1. Client sends: FRAME_AUTH with credentials                              │
│     → Validates token                                                       │
│     → Stores tenant in SessionState.auth_state                             │
│     → Sends FRAME_ACK                                                       │
│                                                                              │
│  2. Client sends: FRAME_PUB with notice://topic and body                   │
│     → Extracts: route = "notice://topic", payload = TLV bytes              │
│     → Checks: permissions.has_permission(&tenant, &route, Action::Write)   │
│     → Builds: Calls engine.dispatch(route, payload, channel_id)            │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    dispatch(route, payload, 1)
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ ENGINE (engine.rs) - Actor Loop                                             │
│                                                                              │
│  • Receives: EngineCommand::Dispatch { route, payload, channel_id, resp }  │
│  • Parses: route "notice://topic" → scheme = "notice"                      │
│  • Lookup: domain = domains.get("notice") → NoticeDomain                    │
│  • Creates: DomainContext { route, route_str, payload, channel_id }        │
│  • Calls: domain.handle(request).await                                      │
│  • Waits: for DomainResponse                                                │
│  • Sends: response back to session handler via oneshot channel              │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                    handle(DomainContext)
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ NOTICE HANDLER (notice/handler.rs)                                          │
│                                                                              │
│  1. Parses TLV payload:                                                     │
│     • find_tlv(payload, TAG_ID) → "msg123"                                 │
│     • find_tlv(payload, TAG_BODY) → b"hello"                               │
│                                                                              │
│  2. Calls: self.service.publish("notice://topic", "msg123", b"hello")     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ NOTICE SERVICE (notice/service.rs) - Business Logic                         │
│                                                                              │
│  • Updates internal route_table                                             │
│  • Finds all subscribers on "notice://topic"                                │
│  • Sends notification to each subscriber's channel                          │
│  • Returns: (delivered, failed) counts                                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ NOTICE HANDLER (notice/handler.rs) - Response Building                      │
│                                                                              │
│  • Builds response TLV:                                                      │
│    • build_tlv(TAG_NOTIFICATION, &[])                                       │
│    • build_tlv(TAG_DELIVERED, &delivered.to_be_bytes())                    │
│    • build_tlv(TAG_FAILED, &failed.to_be_bytes())                          │
│                                                                              │
│  • Returns: DomainResponse::Frame(PooledFrame::from_vec(response_bytes))   │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                   DomainResponse::Frame(bytes)
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ ENGINE (engine.rs) - Response Send                                          │
│                                                                              │
│  • Receives DomainResponse::Frame(...)                                      │
│  • Converts to PooledFrame                                                   │
│  • Sends via oneshot channel back to session handler                         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                     Ok(response_bytes)
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ SESSION HANDLER (session/mod.rs) - Response Send                            │
│                                                                              │
│  • Builds FRAME_DAT with channel_id = 1                                     │
│  • Calls: mux.send_on_channel(frame)                                       │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ MUXER (mux.rs)                                                              │
│                                                                              │
│  • Receives: FRAME_DAT with channel_id = 1                                  │
│  • Looks up: channel handlers[1]                                             │
│  • Sends: frame to WebSocket channel                                         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────┐
│ WEBSOCKET (ws.rs)                                                           │
│                                                                              │
│  • Sends: FRAME_DAT bytes to client                                         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                          ┌──────▼──────┐
                          │    CLIENT   │
                          │  receives   │
                          │  response   │
                          └─────────────┘
```

## Concurrent Operations Example

```
Time →

Client 1 (channel_id=1)                  Client 2 (channel_id=2)
    │                                            │
    │ FRAME_AUTH                                │ FRAME_AUTH
    ├──→ Session 1                             └──→ Session 2
    │    (auth tenant A)                           (auth tenant B)
    │
    │ FRAME_PUB (notice://alerts)                 FRAME_PUB (queue://jobs)
    ├──→ Session 1                                ├──→ Session 2
    │    → permission check tenant A              │    → permission check tenant B
    │    → engine.dispatch(...)                   │    → engine.dispatch(...)
    │                                              │
    ├─────────────────────────────────────────────┼─────────────────────
    │  [Engine routes notice://alerts → Notice]   │  [Engine routes queue://jobs → Queue]
    │  [Concurrent: Both domains handle in parallel]
    │                                              │
    │ [Notice service notifies subscribers]        [Queue service reserves item]
    │                                              │
    └──→ Response on channel_id=1                 └──→ Response on channel_id=2
         FRAME_DAT back to Client 1                  FRAME_DAT back to Client 2

Key points:
• Two clients have separate sessions (separate channel_ids)
• Two domains (Notice, Queue) execute concurrently
• Each response goes to correct client via channel_id
• Auth/permissions are per-session
```

## Subscription Flow

```
CLIENT 1: Subscribe to notice://alerts
  │
  └→ FRAME_REG (subscribe)
     │
     └→ Session Handler
        • Checks auth/permissions
        └→ engine.subscribe("notice://alerts", sender, channel_id=1)
           │
           └→ Engine routes to NoticeDomain
              │
              └→ NoticeDomain.subscribe("notice://alerts", channel_id=1, sender)
                 │
                 └→ NoticeService
                    • Register channel_id=1 as subscriber on "notice://alerts"
                    • Store sender channel for notifications
                    │
                    └→ Return subscription_id = 42

CLIENT 1 receives: ACK with subscription_id

Now when ANOTHER CLIENT publishes to notice://alerts:
  │
  └→ engine.dispatch("notice://alerts", body)
     │
     └→ NoticeService.publish(...)
        • Find all subscribers on "notice://alerts"
        • For each subscriber:
          - Find sender for subscription
          - Send notification on that sender
        │
        └→ Client 1 receives: FRAME_DAT (notification)
```

## Domain Handler Pattern (All domains follow this)

```
Every domain has:

mod.rs
  ├─ pub struct MyDomain
  ├─ impl Domain for MyDomain
  │  ├─ async fn handle(&self, request) → DomainResponse
  │  ├─ async fn subscribe(...) → Result<u64, String>  [if pub/sub]
  │  └─ async fn unsubscribe(&self, id) → bool         [if pub/sub]
  │
  handler.rs
  │  • Parse TLV from request.payload
  │  • Call self.service.operation(...)
  │  • Build response TLV
  │  • Return DomainResponse::Frame(...)
  │
  service.rs
  │  • Business logic
  │  • Storage access (midge)
  │  • Return results
  │
  types.rs
     • Enum MyOp { Operation1, Operation2, ... }
     • Data structures

Example: Notice domain
  notice/
  ├─ mod.rs (domain struct, impl Domain)
  ├─ handler.rs (parse/build TLV, call service)
  ├─ service.rs (publish/subscribe logic)
  └─ types.rs (NoticeOp enum, Notification struct)
```

## Data Flow by Layer

```
┌──────────────────────────────────────────────────────┐
│ TRANSPORT LAYER (ws.rs, tcp.rs, http.rs)            │
│ Responsibility: Accept connections, frame handling  │
│ Protocol: WebSocket / TCP / HTTP + TLZ frame format │
│ NOT responsible: TLV building (moved to session)    │
│ NOT responsible: Business logic (in domains)        │
│ NOT responsible: Routing (in engine)                │
└─────────────────────┬────────────────────────────────┘
                      │ Per-connection
                      │ (mux + session)
│
├─ SessionState
│  ├─ channel_id: u32 (bi-directional ID)
│  ├─ auth_state: Tenant  (who is client)
│  ├─ engine: EngineHandle (dispatcher)
│  └─ mux: Arc<Muxer> (send responses)
│
└──────────────────────────────────────────────────────┐
│ SESSION LAYER (session/mod.rs)                       │
│ Responsibility: Auth, permissions, TLV building     │
│ Builds: TLV payloads from frame data                │
│ Checks: Permissions before dispatch                 │
│ NOT responsible: Routing (delegated to engine)      │
│ NOT responsible: Business logic (delegated to domain│
└─────────────────────┬────────────────────────────────┘
                      │ dispatch(route, payload)
                      │
└──────────────────────────────────────────────────────┐
│ ENGINE LAYER (engine.rs)                             │
│ Responsibility: Route to correct domain              │
│ Routes: Based on scheme (notice, rpc, queue, ...)   │
│ Manages: Subscriptions, channel cleanup              │
│ NOT responsible: TLV handling (in session/domain)   │
│ NOT responsible: Business logic (in domain)         │
└─────────────────────┬────────────────────────────────┘
                      │ handle(request)
                      │
└──────────────────────────────────────────────────────┐
│ DOMAIN LAYER (core/*/handler.rs + service.rs)       │
│ Responsibility: Parse TLV, execute operation        │
│ Handler: Parse TLV → Call service → Build response  │
│ Service: Business logic + storage                   │
│ NOT responsible: Routing (already done by engine)   │
│ NOT responsible: Auth (already done by session)     │
└──────────────────────────────────────────────────────┘
```

## Key Responsibilities

```
Who builds TLV?
  └─ Session (for requests) + Domain (for responses)
  └─ NOT Engine (clean separation!)

Who routes?
  └─ Engine (based on scheme)

Who does business logic?
  └─ Domain Service

Who manages channel_id?
  └─ Session (creates) + Mux (routes) + Engine (cleanup)

Who checks permissions?
  └─ Session (before dispatch)

Who manages subscriptions?
  └─ Individual domains (Notice, RPC)

Who handles storage?
  └─ Domain Service (via midge adapter)
```

## Testing

```
Unit tests:
  • handler.rs: Test TLV parsing (input) and building (output)
  • service.rs: Test business logic in isolation

Integration tests:
  • Create domain → call handle() with full request
  • Mock storage → verify service behavior

End-to-end tests:
  • WebSocket connection → auth → request → response
  • Multiple clients → verify isolation
  • Subscriptions → verify notifications
```

## Summary

The architecture is a **clean layering of concerns**:
- **Transport** accepts connections and frames
- **Session** handles auth, permissions, TLV protocol
- **Engine** routes to appropriate domain
- **Domain** executes business logic
- **Storage** persists state (midge)

Each layer knows only what it needs to know, communicates via clean interfaces (channel_id for routing, TLV for protocol), and stays focused on its responsibility.
