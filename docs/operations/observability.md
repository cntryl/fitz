## Observability Implementation Guide for Fitz

This guide explains how to instrument each layer of Fitz with comprehensive observability (logging, metrics, distributed tracing).

### Quick Reference

**Sampling Strategy:**
- **Hot paths** (routing, TLV codec, frame I/O): 0.1% sampling (1 in 1000)
- **Critical paths** (auth, permission checks, request boundaries): 100% sampling
- **Debug paths** (scheduler, internal operations): Debug level (hidden by default)

**Metric Types:**
- **Counters**: Increment-only (frames_received, errors_total, operations_total)
- **Gauges**: Point-in-time values (active_connections, mailbox_depth, pending_operations)
- **Histograms**: Latency distributions (message_latency_ms, codec_latency_us, operation_latency_ms)

**Key Attributes to Record:**
- `message_id`, `parent_message_id` (causation chains)
- `route`, `domain`, `realm`, `area` (routing context)
- `session_id`, `connection_id` (session context)
- `operation`, `error_type` (operation context)

---

## Layer-by-Layer Instrumentation

### Layer 1: API/Transport (Async)

**Files:** `src/api/tcp.rs`, `src/api/ws.rs`, `src/api/ingress.rs`

**Goal:** Track connection lifecycle, frame I/O, and protocol errors

**Template:**

```rust
// On connection accept (AlwaysSample = 100%)
#[tracing::instrument(skip_all, fields(peer_addr = %peer_addr))]
async fn handle_connection(socket: TcpStream, peer_addr: SocketAddr) {
    let span = tracing::Span::current();
    METRICS.counter_inc(observability::METRIC_CONNECTIONS_OPENED);
    
    // Inside connection loop:
    loop {
        let instant = std::time::Instant::now();
        
        // Frame read (hot path, sample at 0.1%)
        if should_sample_hot_path() {
            let read_span = tracing::debug_span!("frame::read", size = ?frame.len());
            read_span.in_scope(|| {
                // read frame
            });
        }
        
        METRICS.histogram_observe_us(
            observability::METRIC_FRAME_READ_LATENCY,
            instant.elapsed().as_micros() as u64
        );
        
        // Wrap frame in span context if sampled
        let envelope = parse_frame(frame)?;
    }
    
    METRICS.counter_inc(observability::METRIC_CONNECTIONS_CLOSED);
}
```

**Key Metrics:**
- `fitz_connections_opened_total` (counter)
- `fitz_connections_closed_total` (counter)
- `fitz_frames_received_total` (counter)
- `fitz_frames_sent_total` (counter)
- `fitz_frames_malformed_total` (counter)

---

### Layer 2: Session (Sync)

**Files:** `src/session/session.rs`, `src/session/permissions.rs`, `src/api/runtime_ingress.rs`

**Goal:** Track authentication, authorization, and frame processing

**Template:**

```rust
/// Process an inbound frame (AlwaysSample = 100%)
#[tracing::instrument(skip_all, fields(session_id = %session.id(), frame_size = frame.len()))]
pub fn process_frame(session: &Session, frame: &[u8]) -> Result<Message> {
    // TLV decode (hot path, sample at 0.1%)
    let instant = std::time::Instant::now();
    let message = if should_sample_hot_path() {
        let decode_span = tracing::debug_span!("tlv::decode");
        decode_span.in_scope(|| tlv::decode(frame))
    } else {
        tlv::decode(frame)
    }?;
    
    METRICS.histogram_observe_us(
        observability::METRIC_TLV_CODEC_LATENCY,
        instant.elapsed().as_micros() as u64
    );
    
    // Permission check (AlwaysSample - critical for security)
    let perm_instant = std::time::Instant::now();
    let _perm_span = tracing::info_span!(
        observability::SPAN_PERMISSION_CHECK,
        route = %message.route(),
        auth_method = %session.auth_method()
    );
    
    if !session.has_permission(&message.route()) {
        METRICS.counter_inc(observability::METRIC_PERMISSION_DENIALS);
        tracing::warn!("Permission denied for route");
        return Err(SessionError::PermissionDenied);
    }
    
    METRICS.histogram_observe_us(
        observability::METRIC_PERMISSION_CHECK_LATENCY,
        perm_instant.elapsed().as_micros() as u64
    );
    
    Ok(message)
}
```

**Key Metrics:**
- `fitz_sessions_created_total` (counter)
- `fitz_sessions_closed_total` (counter)
- `fitz_tlv_decode_errors_total` (counter)
- `fitz_auth_failures_total` (counter)
- `fitz_permission_denials_total` (counter)
- `fitz_permission_check_latency_us` (histogram)

---

### Layer 3: Runtime/Router (Sync)

**Files:** `src/runtime/router.rs`, `src/runtime/scheduler.rs`

**Goal:** Track message routing, delivery, and actor scheduling

**Template:**

```rust
impl Router {
    /// Route an envelope (hot path, sample at 0.1%)
    pub fn route(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();
        let start = std::time::Instant::now();
        
        // Only create span if sampled
        if should_sample_hot_path() {
            let span = tracing::debug_span!(
                observability::SPAN_ROUTE_MATCH,
                route = %dest,
                domain = %dest.domain()
            );
            span.in_scope(|| self._route_impl(envelope))
        } else {
            self._route_impl(envelope)
        }?;
        
        METRICS.histogram_observe_us(
            observability::METRIC_ROUTE_MATCH_LATENCY,
            start.elapsed().as_micros() as u64
        );
        
        Ok(())
    }
    
    fn _route_impl(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();
        
        // Try exact route match
        let sink = if let Some(sink) = self.registry.get(&dest) {
            sink
        } else {
            // Domain fallback
            self.registry.get_by_domain(&dest.domain())
                .ok_or_else(|| {
                    METRICS.counter_inc(observability::METRIC_ROUTE_MISMATCHES);
                    RouteError::RouteNotFound(dest.clone())
                })?
        };
        
        match sink.deliver(envelope) {
            Ok(()) => {
                METRICS.counter_inc(observability::METRIC_MESSAGES_DELIVERED);
                Ok(())
            }
            Err(e) => {
                METRICS.counter_inc(observability::METRIC_DELIVERY_FAILURES);
                Err(RouteError::DeliveryFailed(dest, e))
            }
        }
    }
}
```

**Key Metrics:**
- `fitz_route_mismatches_total` (counter)
- `fitz_delivery_failures_total` (counter)
- `fitz_messages_pending` (gauge - mailbox depth)
- `fitz_route_match_latency_us` (histogram)

---

### Layer 4: Protocol/Codecs (Sync)

**Files:** `src/protocol/tlv.rs`, `src/protocol/{domain}_codec.rs`

**Goal:** Track codec performance and errors

**Template:**

```rust
impl MyDomainCodec {
    pub fn encode_message(msg: &MyMessage) -> Result<Vec<u8>> {
        let start = std::time::Instant::now();
        
        let result = if should_sample_hot_path() {
            let span = tracing::debug_span!("tlv::encode", domain = "my_domain");
            span.in_scope(|| Self::_encode_impl(msg))
        } else {
            Self::_encode_impl(msg)
        };
        
        if result.is_ok() {
            METRICS.histogram_observe_us(
                observability::METRIC_TLV_CODEC_LATENCY,
                start.elapsed().as_micros() as u64
            );
        } else {
            METRICS.counter_inc(observability::METRIC_TLV_ENCODE_ERRORS);
        }
        
        result
    }
}
```

**Key Metrics:**
- `fitz_tlv_encode_errors_total` (counter)
- `fitz_tlv_decode_errors_total` (counter)
- `fitz_tlv_codec_latency_us` (histogram)

---

### Layer 5: Domains (Sync)

**Files:** `src/domains/{kv,lease,notice,rpc,queue,stream,schedule}/mod.rs`

**Goal:** Track domain-specific operations and business logic

**Template:**

```rust
impl Actor for MyDomainActor {
    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) -> Self::Response {
        let operation_name = format!("{:?}", msg).split('(').next().unwrap_or("unknown");
        let start = std::time::Instant::now();
        
        // AlwaysSample domain operations
        let _span = tracing::info_span!(
            observability::SPAN_DOMAIN_OPERATION,
            domain = env!("CARGO_PKG_NAME"),
            operation = %operation_name,
            realm = %ctx.route().realm(),
            actor_id = ?ctx.actor_id()
        );
        
        let result = match msg {
            MyMessage::Operation1 { param } => {
                // Business logic here
                self.handle_operation_1(param, ctx)
            }
            MyMessage::Operation2 { param } => {
                self.handle_operation_2(param, ctx)
            }
        };
        
        // Record latency and success/failure
        let latency_ms = start.elapsed().as_millis() as u64;
        METRICS.histogram_observe_ms(
            observability::METRIC_DOMAIN_OPERATION_LATENCY,
            latency_ms
        );
        
        if result.is_error() {
            METRICS.counter_inc(observability::METRIC_DOMAIN_ERRORS);
        } else {
            METRICS.counter_inc(observability::METRIC_DOMAIN_OPERATIONS);
        }
        
        result
    }
}
```

**Key Metrics (per domain):**
- `fitz_domain_operations_total` (counter, labeled by domain)
- `fitz_domain_errors_total` (counter, labeled by domain + error_type)
- `fitz_domain_operation_latency_ms` (histogram, labeled by domain)

---

## Practical Implementation Steps

### Step 1: Start with a Single Layer (e.g., Router)

1. Read the code path you want to instrument
2. Identify the hot path (small, frequently called), critical path (security/correctness), and debug path
3. Add `#[tracing::instrument]` or manual `let _span = tracing::info_span!(...)` at appropriate places
4. Add metric counters/histograms for success/failure/latency
5. Test with: `FITZ_LOG_LEVEL=debug OTEL_ENABLED=false cargo test`

### Step 2: Verify JSON Logging (Opt-In)

By default, Fitz emits human-readable, colorized text logs. Set `FITZ_LOG_FORMAT=json` to opt into structured JSON output.

```bash
FITZ_LOG_FORMAT=json FITZ_LOG_LEVEL=info ./target/debug/fitz > /tmp/fitz.log 2>&1 &
# Makes a request (or run integration test)
cat /tmp/fitz.log | jq '.' | head -20
```

Expected output:
```json
{
  "timestamp": "2026-02-21T12:34:56.789Z",
  "level": "INFO",
  "message": "Starting Fitz broker",
  "target": "fitz::boot",
  "module_path": "fitz::boot",
  "span": {
    "name": "boot"
  }
}
```

### Step 3: Check Metrics Endpoint

Once the `/metrics` endpoint is added (see next section):

```bash
curl http://localhost:9090/metrics | head -30
```

Expected output:
```
# HELP fitz_connections_opened_total Fitz counter metrics
# TYPE fitz_connections_opened_total counter
fitz_connections_opened_total 5

# HELP fitz_messages_delivered_total Fitz counter metrics
# TYPE fitz_messages_delivered_total counter
fitz_messages_delivered_total 1024
```

### Step 4: Progressive Rollout

Instrument layers in this order:
1. **Runtime/Router** (highest impact, hot path)
2. **Session** (security-critical)
3. **API/Transport** (connection lifecycle)
4. **Protocol/Codecs** (performance debugging)
5. **Domains** (per-domain observability)

---

## Common Patterns

### Record with Optional Metric

```rust
// Safe to use even if metric export isn't set up yet
METRICS.counter_inc("my_counter"); // No-op if collector not initialized
```

### Conditional Sampling for Hot Paths

```rust
fn should_sample_hot_path() -> bool {
    // This could be thread-local or RNG-based
    // For now, simple fixed ratio:
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_nanos() as u64) % 1000 == 0)
        .unwrap_or(false)
}
```

### Link Parent-Child Message Causation

```rust
// In envelope creation or routing:
#[tracing::instrument(skip_all, fields(message_id = %msg_id, parent_message_id = ?parent_id))]
fn route_with_causation(parent_id: Option<MessageId>, msg_id: MessageId) {
    // Spans are automatically linked by tracing
}
```

### Record Complex Attributes

```rust
let span = tracing::info_span!(
    "operation",
    route = %route,
    realm = %realm,
    operation = op,
    error_type = tracing::field::Empty,  // Fill in later if error
);

span.in_scope(|| {
    let result = do_work();
    if let Err(e) = &result {
        span.record("error_type", &e.kind());
    }
    result
});
```

---

## Testing Observability

See `tests/observability_spans.rs` and `tests/observability_metrics.rs` for comprehensive tests.

Quick local test:

```bash
# Run a single integration test with JSON logging enabled (opt-in)
FITZ_LOG_FORMAT=json FITZ_LOG_LEVEL=debug cargo test kv_basics -- --nocapture
```

---

## Next Steps

1. **Implement Layer 1 (API)** - Track connections and frames
2. **Implement Layer 2 (Session)** - Track auth and TLV codec
3. **Implement Layer 3 (Router)** - Track routing and delivery (highest ROI)
4. **Add `POST /metrics` endpoint** - Export metrics in Prometheus format
5. **Add distributed tracing** - Link parent/child messages via OpenTelemetry
6. **Configure Datadog/Prometheus** - Connect external observability platform
