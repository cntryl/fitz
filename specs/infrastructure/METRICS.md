# Metrics Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Optional Midge persistence  
**Last Updated:** December 11, 2025  

---

## Overview

The Metrics domain provides time-series collection, aggregation, and optional persistence of system and application metrics. Metrics are aggregated in-memory with periodic flushing to Midge for durability and historical analysis.

### Key Features

- **Multiple metric types**: Counters, gauges, histograms, timers
- **In-memory aggregation**: Low-latency updates with batched persistence
- **Optional durability**: Configure per-metric persistence policies
- **Realm isolation**: Metrics namespaced by realm/area
- **Downsampling**: Automatic aggregation over time windows
- **Efficient encoding**: Compact storage format for time-series data

### Use Cases

- Request latency tracking
- Throughput monitoring
- Resource utilization (memory, connections, queue depths)
- Business metrics (orders, users, transactions)
- SLA tracking and alerting

---

## Route Format

Metrics routes follow the standard Fitz format:

```
metrics://{realm}/{area}/{metric_name}/{operation}
```

### Examples
- `metrics://acme/api/request_latency/record` - Record latency sample
- `metrics://acme/system/memory_used/set` - Set gauge value
- `metrics://acme/orders/count/increment` - Increment counter
- `metrics://acme/api/*/query` - Query multiple metrics

---

## Core Operations

### 1. Counter Increment

Atomically increment a counter metric.

**Route:** `metrics://{realm}/{area}/{counter}/increment`

**Request (TLV):**
```
Type: 0x0700 (Metrics Request)
Tags:
  0x01 (realm)        → "acme"
  0x02 (area)         → "api"
  0x03 (resource)     → "requests_total"
  0x04 (operation)    → "increment"
  0x10 (delta)        → varint(1)  # optional, defaults to 1
  0x11 (labels)       → ["endpoint=/users", "method=GET"]  # optional
```

**Response:**
```
Type: 0x0701 (Metrics Response)
Tags:
  0x01 (status)       → "ok"
  0x10 (new_value)    → varint(12345)  # current counter value
```

**Errors:**
- `INVALID_METRIC_TYPE` - Counter operation on non-counter metric
- `REALM_NOT_FOUND` - Realm doesn't exist

---

### 2. Gauge Set

Set a gauge to a specific value.

**Route:** `metrics://{realm}/{area}/{gauge}/set`

**Request:**
```
Type: 0x0700
Tags:
  0x03 (resource)     → "memory_used_bytes"
  0x04 (operation)    → "set"
  0x10 (value)        → double(123456789.0)
  0x11 (labels)       → ["host=server1"]  # optional
```

**Response:**
```
Type: 0x0701
Tags:
  0x01 (status)       → "ok"
```

---

### 3. Histogram Record

Record a sample in a histogram.

**Route:** `metrics://{realm}/{area}/{histogram}/record`

**Request:**
```
Type: 0x0700
Tags:
  0x03 (resource)     → "request_duration_ms"
  0x04 (operation)    → "record"
  0x10 (value)        → double(42.5)
  0x11 (labels)       → ["endpoint=/api/users"]  # optional
```

**Response:**
```
Type: 0x0701
Tags:
  0x01 (status)       → "ok"
  0x10 (bucket_count) → varint(1024)  # samples recorded
```

---

### 4. Query Metrics

Query current metric values (in-memory only).

**Route:** `metrics://{realm}/{area}/{pattern}/query`

**Request:**
```
Type: 0x0700
Tags:
  0x03 (resource)     → "request_*"  # glob pattern
  0x04 (operation)    → "query"
  0x10 (label_filter) → ["endpoint=/api/*"]  # optional
```

**Response:**
```
Type: 0x0701
Tags:
  0x01 (status)       → "ok"
  0x10 (metrics)      → [
    {
      "name": "request_latency_ms",
      "type": "histogram",
      "labels": {"endpoint": "/api/users"},
      "values": {"p50": 12.5, "p99": 45.2, "count": 1000}
    },
    {
      "name": "request_total",
      "type": "counter",
      "labels": {"endpoint": "/api/users"},
      "value": 1000
    }
  ]
```

---

### 5. Flush to Storage

Manually trigger persistence of in-memory metrics to Midge.

**Route:** `metrics://{realm}/{area}/*/flush`

**Request:**
```
Type: 0x0700
Tags:
  0x04 (operation)    → "flush"
  0x10 (force)        → bool(true)  # optional
```

**Response:**
```
Type: 0x0701
Tags:
  0x01 (status)       → "ok"
  0x10 (flushed_count) → varint(42)
  0x11 (bytes_written) → varint(4096)
```

---

## Metric Types

### Counter
- Monotonically increasing integer
- Supports: increment, reset
- Example: request_total, errors_count

### Gauge
- Arbitrary floating-point value
- Supports: set, increment, decrement
- Example: memory_used, active_connections

### Histogram
- Distribution of values with buckets
- Supports: record (sample)
- Reports: count, sum, min, max, percentiles
- Example: request_latency, payload_size

### Timer
- Specialized histogram for duration measurements
- Supports: start, stop (auto-calculates duration)
- Example: operation_duration, lock_hold_time

---

## State Model

### MetricsActor State

```rust
pub struct MetricsActor {
    /// In-memory aggregated metrics
    counters: DashMap<MetricKey, AtomicI64>,
    gauges: DashMap<MetricKey, AtomicF64>,
    histograms: DashMap<MetricKey, Histogram>,
    timers: DashMap<MetricKey, ActiveTimer>,
    
    /// Persistence configuration
    flush_interval: Duration,
    persist_policy: HashMap<String, PersistPolicy>,
    
    /// Storage bridge
    midge: ActorRef<MidgeMsg>,
}

#[derive(Hash, Eq, PartialEq)]
struct MetricKey {
    realm: InternedString,
    area: InternedString,
    name: InternedString,
    labels: Vec<(InternedString, InternedString)>,
}

enum PersistPolicy {
    Never,                      // Ephemeral only
    OnFlush,                    // Periodic flush
    OnUpdate,                   // Every update (high cost)
    Retention(Duration),        // Flush with retention
}
```

---

## Durability Strategy

### Aggregation Flow

```
1. Metric update arrives → update in-memory atomic
2. Check persist policy:
   - Never: done
   - OnFlush: mark dirty, batch write later
   - OnUpdate: immediate write to Midge
3. Background flush task:
   - Every flush_interval (e.g., 30s)
   - Batch all dirty metrics
   - Send to MidgeActor as FlushMetrics message
```

### Storage Format

Metrics stored in Midge as time-series blocks:

```
Key: metrics/{realm}/{area}/{metric_name}/{timestamp_window}
Value: {
  "type": "counter",
  "samples": [
    {"ts": 1702300800, "value": 100, "labels": {...}},
    {"ts": 1702300805, "value": 105, "labels": {...}}
  ]
}
```

### Retention

- Configurable per-metric or per-realm
- Automatic downsampling (1s → 1m → 1h → 1d)
- Background compaction deletes old windows

---

## Actor Implementation

### Message Handler

```rust
impl Actor for MetricsActor {
    type Message = MetricsMsg;
    
    fn on_message(&mut self, msg: Self::Message, ctx: &ActorContext<Self>) {
        match msg {
            MetricsMsg::IncrementCounter { realm, area, name, delta, labels, reply_to } => {
                let key = self.make_key(realm, area, name, labels);
                let counter = self.counters.entry(key).or_insert(AtomicI64::new(0));
                let new_val = counter.fetch_add(delta, Ordering::Relaxed) + delta;
                
                self.mark_dirty(&key);
                reply_to.send(MetricsReply::CounterValue(new_val));
            }
            
            MetricsMsg::SetGauge { realm, area, name, value, labels, reply_to } => {
                let key = self.make_key(realm, area, name, labels);
                let gauge = self.gauges.entry(key).or_insert(AtomicF64::new(0.0));
                gauge.store(value, Ordering::Relaxed);
                
                self.mark_dirty(&key);
                reply_to.send(MetricsReply::Ok);
            }
            
            MetricsMsg::RecordHistogram { realm, area, name, value, labels, reply_to } => {
                let key = self.make_key(realm, area, name, labels);
                let histogram = self.histograms.entry(key).or_insert_with(Histogram::new);
                histogram.record(value);
                
                self.mark_dirty(&key);
                reply_to.send(MetricsReply::Ok);
            }
            
            MetricsMsg::Flush { force, reply_to } => {
                let flushed = self.flush_dirty_metrics();
                reply_to.send(MetricsReply::Flushed { count: flushed });
            }
        }
    }
}
```

### Periodic Flush Task

```rust
impl MetricsActor {
    fn start_flush_task(&self, ctx: &ActorContext<Self>) {
        let actor_ref = ctx.actor_ref();
        let interval = self.flush_interval;
        
        ctx.schedule_recurring(interval, move || {
            actor_ref.send(MetricsMsg::Flush { force: false, reply_to: ActorRef::dead() });
        });
    }
    
    fn flush_dirty_metrics(&mut self) -> usize {
        let mut batch = vec![];
        
        for (key, counter) in &self.counters {
            if self.is_dirty(key) {
                let value = counter.load(Ordering::Relaxed);
                batch.push((key.clone(), MetricValue::Counter(value)));
            }
        }
        
        // Same for gauges, histograms...
        
        if !batch.is_empty() {
            self.midge.send(MidgeMsg::FlushMetrics {
                metrics: batch,
                reply_to: ActorRef::dead(),
            });
        }
        
        self.clear_dirty();
        batch.len()
    }
}
```

---

## Error Handling

### Error Codes

- `INVALID_METRIC_TYPE` - Operation doesn't match metric type
- `INVALID_LABELS` - Label format error
- `REALM_NOT_FOUND` - Realm doesn't exist
- `STORAGE_ERROR` - Midge write failure (non-fatal for in-memory)

### Recovery

- **In-memory failures**: Never fail metric updates; log and continue
- **Persistence failures**: Retry flush on next interval; metric remains in-memory
- **Label cardinality explosion**: Warn and drop labels exceeding threshold

---

## Performance Characteristics

### Latency

- **Counter increment**: <100ns (atomic operation)
- **Gauge set**: <100ns (atomic operation)
- **Histogram record**: <500ns (lock-free bucket)
- **Query**: <10µs per metric
- **Flush**: Async, non-blocking

### Throughput

- **Updates**: >10M ops/sec per core
- **Flush**: 1000s of metrics per batch
- **Storage**: Limited by Midge write throughput

### Memory

- **Counter**: 8 bytes + key overhead
- **Gauge**: 8 bytes + key overhead
- **Histogram**: ~4KB per histogram (configurable buckets)

---

## Testing Strategy

### Unit Tests

```rust
#[test]
fn should_increment_counter() {
    // Arrange
    let mut actor = MetricsActor::new(midge_ref);
    
    // Act
    let reply = actor.increment_counter("realm", "area", "requests", 1, None);
    
    // Assert
    assert_eq!(reply, MetricsReply::CounterValue(1));
}

#[test]
fn should_flush_dirty_metrics_to_midge() {
    // Arrange
    let mut actor = MetricsActor::new(midge_ref);
    actor.increment_counter("realm", "area", "requests", 5, None);
    
    // Act
    let flushed = actor.flush_dirty_metrics();
    
    // Assert
    assert_eq!(flushed, 1);
}
```

### Integration Tests

- End-to-end metric recording and query
- Midge persistence and retrieval
- Label cardinality limits
- Flush interval timing

### Benchmarks

- Counter increment throughput
- Histogram recording latency
- Concurrent metric updates
- Flush batch size and duration

---

## Implementation Notes

1. **Lock-free operations**: Use atomics for counters/gauges
2. **Label interning**: Reuse strings via routing interner
3. **Histogram implementation**: Use HDR Histogram or similar
4. **Cardinality limits**: Enforce max unique label combinations
5. **Prometheus compatibility**: Consider exporter format

---

## References

- [MidgeActor Specification](../durable/MIDGE.md)
- [Prometheus Data Model](https://prometheus.io/docs/concepts/data_model/)
- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
