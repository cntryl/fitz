# Lease Domain Specification

**Version:** 1.0  
**Status:** Implementation Complete  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz Leases provide distributed coordination primitives for exclusive resource access. Leases enable exactly-one semantics for resource ownership with automatic expiry, supporting patterns like leader election, distributed locking, and work distribution.

### Key Features

- **Exclusive access**: Exactly-one holder per resource
- **Automatic expiry**: Leases expire if not renewed
- **Wait queues**: FIFO queuing for contended resources
- **Hierarchical namespacing**: `realm/area/resource` organization
- **Lease tokens**: Secure, non-forgeable access tokens
- **Graceful handover**: Renew/release operations

### Use Cases

- Distributed locking
- Leader election
- Job scheduling and work distribution
- Resource allocation
- Configuration management coordination

---

## Route Format

Lease routes follow the standard Fitz format:

```
lease://{realm}/{area}/{resource}[/{operation}]
```

### Examples
- `lease://acme/locks/database/migration` - Database migration lock
- `lease://acme/election/coordinator/primary` - Primary coordinator election
- `lease://acme/jobs/queue/processor` - Job queue processor lease

---

## Core Operations

### 1. Acquire Lease

**Route Operation:** `lease://{realm}/{area}/{resource}/acquire`  
**TLV Tags:** `TAG_ROUTE`, `TAG_LEASE_SECS` (TTL), `TAG_BODY` (optional data)

**Behavior:**
- Attempts to acquire exclusive lease on resource
- If resource is held, client joins wait queue
- Returns lease token on success

**Response TLV:** `TAG_ID` (lease token), `TAG_BODY` (optional data)

### 2. Renew Lease

**Route Operation:** `lease://{realm}/{area}/{resource}/renew`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (lease token), `TAG_LEASE_SECS` (new TTL)

**Behavior:**
- Extends lease expiry time
- Only current holder can renew
- Maintains exclusive access

**Response TLV:** Success acknowledgment

### 3. Release Lease

**Route Operation:** `lease://{realm}/{area}/{resource}/release`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (lease token)

**Behavior:**
- Voluntarily releases lease
- Allows next waiter to acquire
- Cleans up lease state

**Response TLV:** Success acknowledgment

---

## Lease Semantics

### Exclusive Access

Each resource can have at most one active lease:

```rust
#[derive(Debug)]
pub struct LeaseEntry {
    pub id: String,           // Current holder identifier
    pub token: String,        // Secure access token
    pub expiry: Instant,      // When lease expires
    pub body: Option<Vec<u8>>, // Optional associated data
    pub waiters: VecDeque<Pending>, // FIFO wait queue
}
```

### Lease Lifecycle

1. **Free State**: No active lease, resource available
2. **Held State**: Single client holds lease with expiry
3. **Expired State**: Lease timed out, next waiter granted lease
4. **Wait State**: Multiple clients waiting in FIFO queue

### Automatic Expiry

Leases expire automatically if not renewed:

```rust
impl LeaseEntry {
    fn is_expired(&self, now: Instant) -> bool {
        now >= self.expiry
    }

    fn is_active(&self, now: Instant) -> bool {
        !self.id.is_empty() && !self.is_expired(now)
    }
}
```

---

## Wait Queues

### FIFO Ordering

Contended resources maintain fair ordering:

```rust
#[derive(Debug)]
pub struct Pending {
    pub requested_ttl: u32,
    pub responder: oneshot::Sender<Result<LeaseGrant, String>>,
}
```

### Grant Process

When lease becomes available:

```rust
async fn grant_next_lease(&self, resource: &str) -> Result<(), LeaseError> {
    let mut entry = self.get_lease_entry(resource).write().await;

    if let Some(pending) = entry.waiters.pop_front() {
        // Generate secure token
        let token = generate_secure_token();

        // Grant lease to next waiter
        let grant = LeaseGrant {
            id: resource.to_string(),
            body: entry.body.clone(),
            token: token.clone(),
            ttl_secs: pending.requested_ttl,
        };

        // Update lease entry
        entry.id = grant.id.clone();
        entry.token = token;
        entry.expiry = Instant::now() + Duration::from_secs(pending.requested_ttl as u64);

        // Notify waiter
        let _ = pending.responder.send(Ok(grant));
    }

    Ok(())
}
```

---

## Data Model

### Lease Grant

```rust
#[derive(Debug, Clone)]
pub struct LeaseGrant {
    pub id: String,                    // Resource identifier
    pub body: Option<Vec<u8>>,         // Associated data
    pub token: String,                 // Secure access token
    pub ttl_secs: u32,                 // Time-to-live in seconds
}
```

### Lease Storage

Hierarchical storage with realm/area/resource organization:

```rust
type LeaseLock = Arc<RwLock<LeaseEntry>>;
type ResourceMap = DashMap<String, LeaseLock>;
type AreaMap = DashMap<String, Arc<ResourceMap>>;
type RealmMap = DashMap<String, Arc<AreaMap>>;
```

### Secure Tokens

Lease tokens are cryptographically secure:

```rust
fn generate_secure_token() -> String {
    use rand::{thread_rng, Rng};
    use base64::{Engine as _, engine::general_purpose};

    let mut rng = thread_rng();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);

    general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
}
```

---

## Lease Operations

### Acquire with Waiting

```rust
async fn acquire_lease(
    &self,
    realm: &str,
    area: &str,
    resource: &str,
    ttl_secs: u32,
    body: Option<Vec<u8>>,
) -> Result<LeaseGrant, LeaseError> {
    let resource_key = format!("{}/{}/{}", realm, area, resource);
    let mut entry = self.get_lease_entry(&resource_key).write().await;

    let now = Instant::now();

    // Check if lease is available
    if entry.is_active(now) {
        // Resource held - join wait queue
        let (tx, rx) = oneshot::channel();

        entry.waiters.push_back(Pending {
            requested_ttl: ttl_secs,
            responder: tx,
        });

        // Release lock while waiting
        drop(entry);

        // Wait for grant or timeout
        match tokio::time::timeout(Duration::from_secs(300), rx).await {
            Ok(Ok(grant)) => Ok(grant),
            Ok(Err(e)) => Err(LeaseError::AcquireFailed(e)),
            Err(_) => Err(LeaseError::Timeout),
        }
    } else {
        // Resource free - grant immediately
        let token = generate_secure_token();

        entry.id = resource_key.clone();
        entry.token = token.clone();
        entry.expiry = now + Duration::from_secs(ttl_secs as u64);
        entry.body = body.clone();

        Ok(LeaseGrant {
            id: resource_key,
            body,
            token,
            ttl_secs,
        })
    }
}
```

### Renew Lease

```rust
async fn renew_lease(
    &self,
    realm: &str,
    area: &str,
    resource: &str,
    token: &str,
    new_ttl_secs: u32,
) -> Result<(), LeaseError> {
    let resource_key = format!("{}/{}/{}", realm, area, resource);
    let mut entry = self.get_lease_entry(&resource_key).write().await;

    // Verify token ownership
    if entry.token != token {
        return Err(LeaseError::InvalidToken);
    }

    // Extend expiry
    entry.expiry = Instant::now() + Duration::from_secs(new_ttl_secs as u64);

    Ok(())
}
```

### Release Lease

```rust
async fn release_lease(
    &self,
    realm: &str,
    area: &str,
    resource: &str,
    token: &str,
) -> Result<(), LeaseError> {
    let resource_key = format!("{}/{}/{}", realm, area, resource);
    let mut entry = self.get_lease_entry(&resource_key).write().await;

    // Verify token ownership
    if entry.token != token {
        return Err(LeaseError::InvalidToken);
    }

    // Clear lease
    entry.id.clear();
    entry.token.clear();
    entry.body = None;

    // Grant to next waiter if any
    drop(entry); // Release lock before granting
    self.grant_next_lease(&resource_key).await?;

    Ok(())
}
```

---

## TLV Framing Details

### Acquire Lease
```
DAT Frame:
- TAG_ROUTE (0x20): "lease://acme/locks/database/migration/acquire"
- TAG_LEASE_SECS (0x??): 300
- TAG_BODY (0x22): <optional associated data>

Response:
- TAG_ID (0x??): "lease_token_abc123"
- TAG_BODY (0x22): <associated data>
```

### Renew Lease
```
DAT Frame:
- TAG_ROUTE (0x20): "lease://acme/locks/database/migration/renew"
- TAG_ID (0x??): "lease_token_abc123"
- TAG_LEASE_SECS (0x??): 300
```

### Release Lease
```
DAT Frame:
- TAG_ROUTE (0x20): "lease://acme/locks/database/migration/release"
- TAG_ID (0x??): "lease_token_abc123"
```

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 7001 | ERR_LEASE_HELD | Resource already leased | Wait or join queue |
| 7002 | ERR_INVALID_TOKEN | Token doesn't match current lease | Check token validity |
| 7003 | ERR_LEASE_EXPIRED | Lease expired before operation | Re-acquire lease |
| 7004 | ERR_TIMEOUT | Operation timed out | Retry with longer timeout |
| 7005 | ERR_RESOURCE_NOT_FOUND | Invalid resource path | Check route format |
| 7006 | ERR_LEASE_NOT_HELD | No active lease on resource | Acquire lease first |

### Token Validation

```rust
fn validate_lease_token(&self, resource: &str, token: &str) -> Result<(), LeaseError> {
    let entry = self.get_lease_entry(resource).read().await;

    if entry.token != token {
        return Err(LeaseError::InvalidToken);
    }

    if !entry.is_active(Instant::now()) {
        return Err(LeaseError::LeaseExpired);
    }

    Ok(())
}
```

---

## Configuration

### Lease Settings

```yaml
lease:
  # Global settings
  default_ttl_seconds: 300          # 5 minutes
  max_ttl_seconds: 3600             # 1 hour
  renewal_grace_period_seconds: 30  # Grace period for renewals

  # Per-area settings
  areas:
    "acme/locks":
      default_ttl_seconds: 600      # 10 minutes for locks
      max_concurrent_waits: 50

    "acme/election":
      default_ttl_seconds: 30       # 30 seconds for elections
      renewal_grace_period_seconds: 5

  # Security
  token_length: 32
  token_entropy_bits: 256
```

### Monitoring

```yaml
monitoring:
  lease:
    metrics_interval_seconds: 60
    alert_on_expired_leases: true
    alert_on_long_wait_queues: true
```

---

## Observability

### Metrics

- `lease_operations_total{operation,result}`
- `lease_active_leases{area}`
- `lease_wait_queue_length{area}`
- `lease_expirations_total{area}`
- `lease_renewals_total{area}`
- `lease_acquire_duration_seconds`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "lease_acquired",
  "resource": "acme/locks/database/migration",
  "holder": "worker-01",
  "ttl_seconds": 300
}
```

```json
{
  "timestamp": "2025-11-15T10:30:05Z",
  "level": "warn",
  "message": "lease_expired",
  "resource": "acme/election/coordinator",
  "previous_holder": "node-02",
  "waiters": 3
}
```

---

## Implementation Status

### ✅ Completed
- Lease acquire/renew/release operations
- FIFO wait queues for contended resources
- Automatic lease expiry
- Secure token generation
- Hierarchical resource organization
- TLV framing and parsing

### 🚧 In Progress
- Lease persistence across restarts
- Cross-node lease coordination
- Lease transfer operations
- Advanced conflict resolution

### 📋 TODO
- Lease monitoring and alerting
- Lease hierarchy and delegation
- Time-based lease scheduling
- Lease dependency chains
- Administrative lease operations

---

## Testing Requirements

### Unit Tests
- Lease lifecycle operations
- Wait queue FIFO ordering
- Token validation and security
- Automatic expiry handling
- TLV parsing and framing
- Error condition handling

### Integration Tests
- End-to-end lease operations
- Concurrent access patterns
- Lease expiry and renewal
- Wait queue fairness
- Cross-client coordination

### Performance Benchmarks
- Lease acquisition latency
- Concurrent lease contention
- Wait queue throughput
- Memory usage scaling
- Token generation performance

---

## Usage Patterns

### Distributed Locking

```rust
// Acquire database migration lock
let grant = lease_client.acquire(
    "acme/locks/database/migration",
    600, // 10 minutes
    Some(b"migration_v2.1".to_vec())
).await?;

// Perform migration
run_database_migration().await?;

// Release lock
lease_client.release("acme/locks/database/migration", &grant.token).await?;
```

### Leader Election

```rust
// Attempt to become primary coordinator
async fn attempt_leadership() -> Result<(), LeaseError> {
    match lease_client.acquire("acme/election/coordinator/primary", 30, None).await {
        Ok(grant) => {
            // We are now the leader
            become_leader().await?;

            // Maintain leadership with renewal loop
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                loop {
                    interval.tick().await;
                    if let Err(_) = lease_client.renew(
                        "acme/election/coordinator/primary",
                        &grant.token,
                        30
                    ).await {
                        // Lost leadership
                        step_down().await;
                        break;
                    }
                }
            });

            Ok(())
        }
        Err(LeaseError::Held) => {
            // Someone else is leader, wait and retry
            tokio::time::sleep(Duration::from_secs(35)).await;
            attempt_leadership().await
        }
        Err(e) => Err(e),
    }
}
```

### Job Processing

```rust
// Worker acquires job processing lease
async fn process_jobs() {
    loop {
        match lease_client.acquire("acme/jobs/processor", 300, None).await {
            Ok(grant) => {
                // Process jobs while holding lease
                while let Some(job) = get_next_job().await {
                    process_job(job).await?;
                }

                // Renew lease periodically
                lease_client.renew("acme/jobs/processor", &grant.token, 300).await?;
            }
            Err(_) => {
                // Could not acquire lease, wait and retry
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
    }
}
```

### Resource Allocation

```rust
// Allocate GPU resource
let gpu_grant = lease_client.acquire(
    "acme/resources/gpu/0",
    3600, // 1 hour
    Some(b"training_job_123".to_vec())
).await?;

// Use GPU for computation
run_ml_training(gpu_grant.body.unwrap()).await?;

// Release GPU
lease_client.release("acme/resources/gpu/0", &gpu_grant.token).await?;
```

---

*See OVERVIEW.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\LEASE_SPEC.md