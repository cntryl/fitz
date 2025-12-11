# Control Domain Specification

**Version:** 1.0  
**Status:** Implementation In Progress  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz Control provides system management and observability capabilities for broker nodes. The control domain enables communication between broker instances and a central control plane for heartbeats, metrics collection, configuration management, and graceful shutdown coordination.

### Key Features

- **Heartbeats**: Periodic liveness signals to control plane
- **Metrics**: System health and performance data reporting
- **Configuration**: Runtime configuration updates from control plane
- **Shutdown**: Coordinated graceful shutdown notifications
- **Pub/Sub Integration**: Control operations use notice service for broadcasting

### Use Cases

- Service discovery and registration
- Health monitoring and alerting
- Centralized configuration management
- Coordinated deployment and shutdown
- Performance monitoring and metrics collection

---

## Route Format

Control routes follow the system-scoped format (no realm required):

```
control://{area}/{resource}[/{operation}]
```

### Examples
- `control://broker/heartbeat` - Node heartbeat
- `control://broker/shutdown` - Shutdown notification
- `control://config/update` - Configuration update
- `control://metrics/system` - System metrics

---

## Core Operations

### 1. Heartbeat

**Route Operation:** `control://broker/heartbeat`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`

**Behavior:**
- Periodic liveness signal sent by broker nodes
- Contains node ID, timestamp, and optional health data
- Control plane monitors for missed heartbeats

**Response TLV:** Success acknowledgment (echoed body for pub/sub)

### 2. Shutdown

**Route Operation:** `control://broker/shutdown`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`

**Behavior:**
- Graceful shutdown notification before node termination
- Includes shutdown reason and timing information
- Allows control plane to update service registry

**Response TLV:** Success acknowledgment

### 3. Metrics

**Route Operation:** `control://metrics/{category}`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`

**Behavior:**
- Periodic system health and performance data
- Extensible metrics schema (JSON/CBOR)
- Categories: system, connections, throughput, storage

**Response TLV:** Success acknowledgment

### 4. Configuration Update

**Route Operation:** `control://config/update`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY`

**Behavior:**
- Runtime configuration updates from control plane
- Updates JWT validators, feature flags, limits
- Hot-reloadable without restart

**Response TLV:** Success acknowledgment

---

## Data Model

### Heartbeat Message

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub node_id: String,
    pub timestamp: u64,
    pub version: String,
    pub status: NodeStatus,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus {
    Starting,
    Healthy,
    Degraded,
    Unhealthy,
}
```

### Shutdown Message

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownMessage {
    pub node_id: String,
    pub reason: ShutdownReason,
    pub timestamp: u64,
    pub graceful_timeout_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShutdownReason {
    Maintenance,
    Deployment,
    Error(String),
    Manual,
}
```

### Metrics Message

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsMessage {
    pub node_id: String,
    pub timestamp: u64,
    pub category: String,
    pub metrics: HashMap<String, MetricValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(Vec<f64>),
}
```

### Configuration Message

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigMessage {
    pub version: String,
    pub timestamp: u64,
    pub jwt_config: Option<JwtConfig>,
    pub feature_flags: HashMap<String, bool>,
    pub limits: SystemLimits,
    pub ack_window: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemLimits {
    pub max_connections: u32,
    pub max_message_size: u32,
    pub rate_limit_per_second: u32,
}
```

---

## Control Operations

### Heartbeat Lifecycle

**Registration:**
```rust
// Node registers with control plane
let heartbeat = HeartbeatMessage {
    node_id: "broker-01".to_string(),
    timestamp: now(),
    version: env!("CARGO_PKG_VERSION").to_string(),
    status: NodeStatus::Starting,
    uptime_seconds: 0,
};
control_service.send_heartbeat(heartbeat).await?;
```

**Periodic Heartbeats:**
```rust
// Send heartbeats at configured interval
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        let heartbeat = create_heartbeat();
        control_service.send_heartbeat(heartbeat).await?;
    }
});
```

### Metrics Collection

**System Metrics:**
```rust
let metrics = MetricsMessage {
    node_id: node_id.clone(),
    timestamp: now(),
    category: "system".to_string(),
    metrics: hashmap! {
        "cpu_usage_percent" => MetricValue::Gauge(45.2),
        "memory_usage_bytes" => MetricValue::Gauge(1_500_000_000.0),
        "active_connections" => MetricValue::Gauge(150.0),
    },
};
control_service.send_metrics(metrics).await?;
```

**Throughput Metrics:**
```rust
let metrics = MetricsMessage {
    node_id: node_id.clone(),
    timestamp: now(),
    category: "throughput".to_string(),
    metrics: hashmap! {
        "messages_per_second" => MetricValue::Gauge(1250.0),
        "bytes_per_second" => MetricValue::Gauge(2_500_000.0),
    },
};
```

### Configuration Updates

**Receiving Config:**
```rust
// Control plane sends configuration update
let config = ConfigMessage {
    version: "1.2.3".to_string(),
    timestamp: now(),
    jwt_config: Some(JwtConfig { ... }),
    feature_flags: hashmap! { "new_feature" => true },
    limits: SystemLimits {
        max_connections: 1000,
        max_message_size: 1048576,
        rate_limit_per_second: 1000,
    },
    ack_window: Some(100),
};
control_service.update_config(config).await?;
```

### Graceful Shutdown

**Shutdown Sequence:**
```rust
// Node initiates graceful shutdown
let shutdown = ShutdownMessage {
    node_id: node_id.clone(),
    reason: ShutdownReason::Maintenance,
    timestamp: now(),
    graceful_timeout_seconds: 30,
};
control_service.send_shutdown(shutdown).await?;

// Wait for connections to drain
tokio::time::sleep(Duration::from_secs(30)).await;

// Force shutdown remaining connections
force_shutdown().await;
```

---

## TLV Framing Details

### Heartbeat Message
```
DAT Frame:
- TAG_ROUTE (0x20): "control://broker/heartbeat"
- TAG_BODY (0x22): <JSON/CBOR encoded heartbeat data>
```

### Metrics Message
```
DAT Frame:
- TAG_ROUTE (0x20): "control://metrics/system"
- TAG_BODY (0x22): <JSON/CBOR encoded metrics data>
```

### Configuration Update
```
DAT Frame:
- TAG_ROUTE (0x20): "control://config/update"
- TAG_BODY (0x22): <JSON/CBOR encoded config data>
```

### Shutdown Notification
```
DAT Frame:
- TAG_ROUTE (0x20): "control://broker/shutdown"
- TAG_BODY (0x22): <JSON/CBOR encoded shutdown data>
```

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 5001 | ERR_INVALID_HEARTBEAT | Malformed heartbeat data | Check heartbeat format |
| 5002 | ERR_METRICS_TOO_LARGE | Metrics payload exceeds limit | Reduce metrics size |
| 5003 | ERR_INVALID_CONFIG | Malformed configuration | Check config schema |
| 5004 | ERR_SHUTDOWN_IN_PROGRESS | Shutdown already initiated | Wait for completion |
| 5005 | ERR_CONTROL_PLANE_UNAVAILABLE | Cannot reach control plane | Retry with backoff |

### Validation

```rust
fn validate_heartbeat(body: &[u8]) -> Result<HeartbeatMessage, ControlError> {
    let heartbeat: HeartbeatMessage = serde_json::from_slice(body)
        .map_err(|_| ControlError::InvalidHeartbeat)?;

    if heartbeat.node_id.is_empty() {
        return Err(ControlError::InvalidHeartbeat);
    }

    Ok(heartbeat)
}
```

---

## Configuration

### Control Plane Settings

```yaml
control_plane:
  # Control plane connection
  url: "wss://control.dev1.mesh.local"
  reconnect_interval_seconds: 30
  max_reconnect_attempts: 10

  # Heartbeat configuration
  heartbeat_interval_seconds: 30
  heartbeat_timeout_seconds: 90  # Mark unhealthy after 3 missed

  # Metrics configuration
  metrics_interval_seconds: 60
  metrics_categories: ["system", "connections", "throughput"]

  # Shutdown configuration
  graceful_shutdown_timeout_seconds: 30
  force_shutdown_timeout_seconds: 10
```

### Node Configuration

```yaml
node:
  id: "broker-01"
  region: "us-west-2"
  zone: "us-west-2a"
  version: "1.0.0"

  # Control domain settings
  control:
    enabled: true
    buffer_size: 100
    retry_attempts: 3
```

---

## Observability

### Metrics

- `control_heartbeats_sent_total{node_id}`
- `control_heartbeats_failed_total{node_id}`
- `control_metrics_sent_total{node_id,category}`
- `control_config_updates_received_total{node_id}`
- `control_shutdown_notifications_sent_total{node_id}`
- `control_connection_status{node_id,status}`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "heartbeat_sent",
  "node_id": "broker-01",
  "control_plane_url": "wss://control.dev1.mesh.local"
}
```

```json
{
  "timestamp": "2025-11-15T10:30:05Z",
  "level": "info",
  "message": "config_updated",
  "node_id": "broker-01",
  "config_version": "1.2.3",
  "changes": ["jwt_config", "feature_flags"]
}
```

---

## Implementation Status

### ✅ Completed
- Control domain handler with TLV parsing
- Basic operation types (heartbeat, shutdown, metrics, config)
- Notice service integration for pub/sub pattern
- TLV response building and error handling
- Basic service structure with operation routing

### 🚧 In Progress
- Control plane connectivity and forwarding
- Configuration application and hot-reloading
- Metrics aggregation and collection
- Graceful shutdown coordination
- JWT validator updates from config

### 📋 TODO
- Control plane URL configuration
- Heartbeat monitoring and health checks
- Metrics schema standardization
- Configuration validation and rollback
- Cross-node coordination for deployments
- Control plane authentication

---

## Testing Requirements

### Unit Tests
- TLV parsing and response building
- Operation type detection from routes
- Body validation for each operation type
- Error response generation
- Notice service integration

### Integration Tests
- End-to-end heartbeat with control plane
- Configuration update application
- Metrics collection and forwarding
- Graceful shutdown sequence
- Control plane disconnection handling

### Performance Benchmarks
- Heartbeat throughput and latency
- Metrics serialization performance
- Configuration update application time
- Memory usage during high-frequency operations

---

## Usage Patterns

### Node Registration

```rust
// Node starts up and registers
async fn register_with_control_plane() {
    let control = ControlService::new(node_id);

    // Send initial heartbeat
    let heartbeat = HeartbeatMessage {
        node_id: node_id.clone(),
        timestamp: now(),
        version: get_version(),
        status: NodeStatus::Starting,
        uptime_seconds: 0,
    };

    control.send_heartbeat(heartbeat).await?;

    // Start periodic heartbeats
    start_heartbeat_loop(control.clone());
}
```

### Metrics Reporting

```rust
// Collect and send system metrics
async fn report_system_metrics(control: &ControlService) {
    let metrics = collect_system_metrics();
    let message = MetricsMessage {
        node_id: get_node_id(),
        timestamp: now(),
        category: "system".to_string(),
        metrics,
    };

    control.send_metrics(message).await?;
}
```

### Configuration Management

```rust
// Apply configuration update
async fn apply_config_update(config: ConfigMessage) -> Result<(), ControlError> {
    // Update JWT configuration
    if let Some(jwt_config) = config.jwt_config {
        update_jwt_validator(jwt_config).await?;
    }

    // Apply feature flags
    for (flag, enabled) in config.feature_flags {
        set_feature_flag(&flag, enabled).await?;
    }

    // Update system limits
    apply_system_limits(config.limits).await?;

    Ok(())
}
```

---

*See OVERVIEW.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\CONTROL_SPEC.md