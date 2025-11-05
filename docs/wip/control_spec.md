# Control Domain Specification

## Overview
Control plane integration for heartbeats, metrics, configuration, and lifecycle management.

## Test Coverage

### Heartbeats
- `should_send_periodic_heartbeats_to_control_plane`
- `should_include_node_id_in_heartbeat`
- `should_send_heartbeats_at_configured_interval`
- `should_continue_heartbeats_indefinitely`

### Shutdown
- `should_send_shutdown_signal_on_graceful_shutdown`
- `should_include_shutdown_reason_when_available`
- `should_send_shutdown_before_closing_connections`

### Metrics
- `should_send_periodic_metrics_to_control_plane`
- `should_include_connection_count_in_metrics`
- `should_include_message_throughput_in_metrics`
- `should_include_storage_usage_in_metrics`
- `should_support_extensible_metrics_schema`
- `should_send_metrics_at_configured_interval`
- `should_allow_different_intervals_for_heartbeat_and_metrics`

### Configuration
- `should_receive_config_from_control_plane`
- `should_include_jwt_validation_config`
- `should_update_jwt_validator_when_config_changes`
- `should_include_feature_flags_in_config`
- `should_include_limits_in_config`
- `should_apply_ack_window_from_config`
- `should_support_extensible_config_schema`

## Protocol Details

### Operations
- **Heartbeat**: Periodic liveness signal
- **Metrics**: System health and performance data
- **Config Update**: Receive configuration from control plane
- **Shutdown**: Graceful shutdown notification

### Key Concepts
- **Node ID**: Unique identifier for each node
- **Intervals**: Configurable heartbeat and metrics frequencies
- **Dynamic Config**: Runtime configuration updates
- **JWT Validation**: Token validation config from control plane
