# Fitz Stream Implementation Plan

## Current Status

**Tests:** ✅ Fully stubbed in `tests/stream.rs` (90+ test scenarios)
**Implementation:** ❌ Not yet started
**Design:** ✅ Complete (see `stream_design_final.md`)

---

## Test File Summary

The `tests/stream.rs` file contains **comprehensive test coverage** for the new streaming model:

### Test Categories

1. **Basic Append (3 tests)** - Client-controlled resource sequences
2. **Read Operations (4 tests)** - Resource-level and area-level reads  
3. **Consume/Prefix (5 tests)** - Hierarchical consumption (deprecated in new model)
4. **Concurrent Batches (9 tests)** - Watermark and visibility control
5. **Gap Handling (11 tests)** - Resource gaps (rejected) vs area gaps (tolerated)
6. **Edge Cases (10 tests)** - Ordering, large data, idempotency
7. **Negative Tests (15 tests)** - Error scenarios and validation

**Total:** 90+ test scenarios documenting expected behavior

---

## ⚠️ **Important: Tests Define Target API**

The test file currently **will NOT compile** because it uses the new API signatures that don't exist yet:

```rust
// NEW API (defined in tests, not yet implemented):
handle.stream_append(route, resource_seq, body, metadata, is_end)
  → Returns: AppendResult { resource_seq, area_seq_range }

handle.stream_read(route, from_seq, limit)
  → Returns: Vec<StreamEvent>

handle.stream_read_area(realm, area, from_seq, limit)
  → Returns: AreaReadResponse { events, watermark, has_more }
```

```rust
// OLD API (currently exists):
handle.stream_append(route, id, body, metadata, expected_revision)
  → Returns: u64 (just the sequence)

handle.stream_peek(route, from_seq, limit)
  → Returns: Vec<(u64, Vec<u8>)>

handle.stream_consume_prefix(prefix, from_seq, limit)
  → Returns: Vec<(String, u64, Vec<u8>)>
```

---

## Implementation Roadmap

### Phase 1: Core Data Structures

**Files to modify:**
- `src/storage/mem.rs` - Add dual-index storage
- `src/core/stream.rs` - Update API surface

**New types needed:**

```rust
// In storage/mem.rs
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub route: String,
    pub resource_seq: u64,           // Client-controlled
    pub area_seq: Option<u64>,       // Server-assigned (None until finalized)
    pub body: Arc<Vec<u8>>,
    pub metadata: Option<Arc<Vec<u8>>>,
    pub is_end: bool,
    pub created_at: u64,
}

#[derive(Debug)]
pub struct AppendResult {
    pub resource_seq: u64,
    pub area_seq_range: Option<Range<u64>>,
}

#[derive(Debug)]
pub struct AreaReadResponse {
    pub events: Vec<StreamEvent>,
    pub current_watermark: u64,
    pub has_more: bool,
}

#[derive(Debug)]
struct AreaStreamState {
    next_seq: u64,
    low_watermark: u64,
    reserved_ranges: BTreeMap<u64, ReservationStatus>,
}

#[derive(Debug)]
enum ReservationStatus {
    Reserved,
    Committed,
    Rolledback,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamError {
    SequenceGap { expected: u64, received: u64 },
    SequenceConflict { seq: u64 },
    StreamClosed { route: String },
    InvalidRoute(String),
    PayloadTooLarge { size: usize, max: usize },
    RouteNotFound(String),
    AreaNotFound(String, String),
    Internal(String),
}
```

**Storage additions:**

```rust
struct MemStore {
    // Existing fields...
    
    // NEW: Dual-index storage
    resource_streams: Mutex<HashMap<String, Vec<StreamEvent>>>,
    area_streams: Mutex<HashMap<(String, String), Vec<StreamEvent>>>,
    area_states: Mutex<HashMap<(String, String), AreaStreamState>>,
}
```

### Phase 2: Route Parsing

```rust
fn parse_stream_route(route: &str) -> Result<(String, String, String), StreamError> {
    // "stream://payments/transactions/batch_123"
    // Returns: (realm, area, resource)
    
    let parts: Vec<&str> = route
        .strip_prefix("stream://")
        .ok_or(StreamError::InvalidRoute("Missing stream:// prefix".into()))?
        .split('/')
        .collect();
    
    if parts.len() != 3 {
        return Err(StreamError::InvalidRoute(
            "Expected format: stream://{realm}/{area}/{resource}".into()
        ));
    }
    
    Ok((
        parts[0].to_string(),  // realm
        parts[1].to_string(),  // area
        parts[2].to_string(),  // resource
    ))
}
```

### Phase 3: Append Implementation

```rust
pub async fn stream_append(
    &self,
    route: String,
    resource_seq: u64,
    body: Vec<u8>,
    metadata: Option<Vec<u8>>,
    is_end: bool,
) -> Result<AppendResult, StreamError> {
    let (realm, area, resource) = parse_stream_route(&route)?;
    
    // Step 1: Validate and write to resource index
    {
        let mut streams = self.resource_streams.lock().await;
        let stream = streams.entry(route.clone()).or_default();
        
        // Gap detection
        let expected = stream.last().map(|e| e.resource_seq + 1).unwrap_or(0);
        if resource_seq != expected {
            return Err(StreamError::SequenceGap { expected, received: resource_seq });
        }
        
        // Check if stream closed
        if stream.last().map(|e| e.is_end).unwrap_or(false) {
            return Err(StreamError::StreamClosed { route: route.clone() });
        }
        
        // Idempotency check
        if let Some(existing) = stream.iter().find(|e| e.resource_seq == resource_seq) {
            if existing.body.as_ref() == &body {
                return Ok(AppendResult {
                    resource_seq,
                    area_seq_range: existing.area_seq.map(|s| s..(s+1)),
                });
            } else {
                return Err(StreamError::SequenceConflict { seq: resource_seq });
            }
        }
        
        // Write event
        let event = StreamEvent {
            route: route.clone(),
            resource_seq,
            area_seq: None,
            body: Arc::new(body),
            metadata: metadata.map(Arc::new),
            is_end,
            created_at: now(),
        };
        stream.push(event);
    }
    
    // Step 2: If is_end=true, finalize to area index
    if is_end {
        match self.finalize_stream_to_area(route, realm, area).await {
            Ok(area_seq_range) => {
                return Ok(AppendResult {
                    resource_seq,
                    area_seq_range: Some(area_seq_range),
                });
            }
            Err(e) => {
                eprintln!("Finalization failed: {:?}", e);
                return Ok(AppendResult {
                    resource_seq,
                    area_seq_range: None,
                });
            }
        }
    }
    
    Ok(AppendResult {
        resource_seq,
        area_seq_range: None,
    })
}
```

### Phase 4: Finalization Logic

```rust
async fn finalize_stream_to_area(
    &self,
    route: String,
    realm: String,
    area: String,
) -> Result<Range<u64>, StreamError> {
    // 1. Get events from resource stream
    let events = {
        let streams = self.resource_streams.lock().await;
        streams.get(&route)
            .ok_or(StreamError::RouteNotFound(route.clone()))?
            .clone()
    };
    
    // 2. Reserve area_seq range
    let area_seq_range = {
        let mut states = self.area_states.lock().await;
        let state = states.entry((realm.clone(), area.clone())).or_default();
        
        let start = state.next_seq;
        let end = start + events.len() as u64;
        
        for seq in start..end {
            state.reserved_ranges.insert(seq, ReservationStatus::Reserved);
        }
        
        state.next_seq = end;
        start..end
    };
    
    // 3. Write to area index
    let write_result = {
        let mut resource_streams = self.resource_streams.lock().await;
        let mut area_streams = self.area_streams.lock().await;
        
        let events_mut = resource_streams.get_mut(&route).unwrap();
        let area_vec = area_streams.entry((realm.clone(), area.clone()))
            .or_default();
        
        for (i, event) in events_mut.iter_mut().enumerate() {
            let area_seq = area_seq_range.start + i as u64;
            event.area_seq = Some(area_seq);
            area_vec.push(event.clone());
        }
        
        Ok(())
    };
    
    // 4. Commit or rollback
    match write_result {
        Ok(()) => {
            let mut states = self.area_states.lock().await;
            let state = states.get_mut(&(realm, area)).unwrap();
            state.commit_sequences(area_seq_range.clone());
            Ok(area_seq_range)
        }
        Err(e) => {
            let mut states = self.area_states.lock().await;
            let state = states.get_mut(&(realm, area)).unwrap();
            state.rollback_sequences(area_seq_range);
            Err(e)
        }
    }
}
```

### Phase 5: Read Implementation

```rust
pub async fn stream_read(
    &self,
    route: String,
    from_seq: u64,
    limit: usize,
) -> Result<Vec<StreamEvent>, StreamError> {
    let streams = self.resource_streams.lock().await;
    let events = streams.get(&route)
        .ok_or(StreamError::RouteNotFound(route))?;
    
    Ok(events.iter()
        .filter(|e| e.resource_seq >= from_seq)
        .take(limit)
        .cloned()
        .collect())
}

pub async fn stream_read_area(
    &self,
    realm: String,
    area: String,
    from_area_seq: u64,
    limit: usize,
) -> Result<AreaReadResponse, StreamError> {
    let watermark = {
        let states = self.area_states.lock().await;
        states.get(&(realm.clone(), area.clone()))
            .map(|s| s.low_watermark)
            .unwrap_or(0)
    };
    
    let streams = self.area_streams.lock().await;
    let events = streams.get(&(realm.clone(), area.clone()))
        .ok_or(StreamError::AreaNotFound(realm, area))?;
    
    let visible_events: Vec<StreamEvent> = events.iter()
        .filter(|e| {
            e.area_seq.is_some() &&
            e.area_seq.unwrap() >= from_area_seq &&
            e.area_seq.unwrap() < watermark
        })
        .take(limit)
        .cloned()
        .collect();
    
    let has_more = events.iter()
        .any(|e| e.area_seq.is_some() && e.area_seq.unwrap() >= watermark);
    
    Ok(AreaReadResponse {
        events: visible_events,
        current_watermark: watermark,
        has_more,
    })
}
```

### Phase 6: Watermark Logic

```rust
impl AreaStreamState {
    fn commit_sequences(&mut self, range: Range<u64>) {
        for seq in range {
            self.reserved_ranges.insert(seq, ReservationStatus::Committed);
        }
        
        // Advance watermark
        while let Some((&seq, status)) = self.reserved_ranges
            .range(self.low_watermark..)
            .next() 
        {
            if seq != self.low_watermark {
                break; // Gap found
            }
            
            match status {
                ReservationStatus::Committed | ReservationStatus::Rolledback => {
                    self.low_watermark += 1;
                    self.reserved_ranges.remove(&seq);
                }
                ReservationStatus::Reserved => {
                    break; // Still waiting
                }
            }
        }
    }
    
    fn rollback_sequences(&mut self, range: Range<u64>) {
        for seq in range {
            self.reserved_ranges.insert(seq, ReservationStatus::Rolledback);
        }
        // Watermark advancement will skip rolledback sequences
    }
}
```

### Phase 7: Engine Integration

**Files to modify:**
- `src/core/engine.rs` - Update EngineCommand enum
- `src/core/engine.rs` - Update EngineHandle methods
- `src/core/stream.rs` - Update public API

**Update EngineCommand:**

```rust
pub enum EngineCommand {
    // ... existing commands ...
    
    StreamAppend {
        route: String,
        resource_seq: u64,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        is_end: bool,
        resp: oneshot::Sender<Result<AppendResult, StreamError>>,
    },
    StreamRead {
        route: String,
        from_seq: u64,
        limit: usize,
        resp: oneshot::Sender<Result<Vec<StreamEvent>, StreamError>>,
    },
    StreamReadArea {
        realm: String,
        area: String,
        from_seq: u64,
        limit: usize,
        resp: oneshot::Sender<Result<AreaReadResponse, StreamError>>,
    },
}
```

### Phase 8: Test Execution

Once implementation is complete, run:

```bash
cargo test stream --lib
```

Expected results:
- ✅ All 90+ tests should pass
- ✅ No compilation errors
- ✅ Full test coverage of streaming behavior

---

## Test-Driven Development Workflow

1. **Pick one test category** (e.g., "Basic Append")
2. **Implement minimal code** to make those tests compile
3. **Run tests** - they will fail (expected)
4. **Implement feature** until tests pass
5. **Refactor** if needed
6. **Move to next category**

### Suggested Order

1. ✅ Phase 1-2: Data structures + route parsing
2. ✅ Phase 3: Basic append (resource index only)
3. ✅ Phase 5: Resource read
4. ✅ Phase 4: Finalization logic
5. ✅ Phase 6: Watermark + area reads
6. ✅ Phase 7: Engine integration
7. ✅ Run full test suite

---

## Success Criteria

- [ ] All tests in `tests/stream.rs` compile
- [ ] All tests pass
- [ ] No gaps in resource sequences enforced
- [ ] Gaps in area sequences tolerated and skipped
- [ ] Watermark advances correctly
- [ ] Idempotency works (retry safety)
- [ ] Finalization is atomic
- [ ] Dual-index queries work efficiently

---

## Next Steps

**Immediate:** Begin Phase 1 - Update data structures in `src/storage/mem.rs`

**Command to start:**
```bash
# Open the storage file
code src/storage/mem.rs

# Start implementing StreamEvent, AppendResult, etc.
```

---

End of implementation plan.
