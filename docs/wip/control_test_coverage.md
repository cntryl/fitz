# Control Domain - Test Coverage

## Overview
Comprehensive test coverage for control plane operations extracted from `tests/control.rs`.

## Test Inventory (21 tests)

### Heartbeats (4 tests)
- ✅ `should_send_periodic_heartbeats_to_control_plane`
- ✅ `should_include_node_id_in_heartbeat`
- ✅ `should_send_heartbeats_at_configured_interval`
- ✅ `should_continue_heartbeats_indefinitely`

### Shutdown (3 tests)
- ✅ `should_send_shutdown_signal_on_graceful_shutdown`
- ✅ `should_include_shutdown_reason_when_available`
- ✅ `should_send_shutdown_before_closing_connections`

### Metrics (7 tests)
- ✅ `should_send_periodic_metrics_to_control_plane`
- ✅ `should_include_connection_count_in_metrics`
- ✅ `should_include_message_throughput_in_metrics`
- ✅ `should_include_storage_usage_in_metrics`
- ✅ `should_support_extensible_metrics_schema`
- ✅ `should_send_metrics_at_configured_interval`
- ✅ `should_allow_different_intervals_for_heartbeat_and_metrics`

### Configuration (7 tests)
- ✅ `should_receive_config_from_control_plane`
- ✅ `should_include_jwt_validation_config`
- ✅ `should_update_jwt_validator_when_config_changes`
- ✅ `should_include_feature_flags_in_config`
- ✅ `should_include_limits_in_config`
- ✅ `should_apply_ack_window_from_config`
- ✅ `should_support_extensible_config_schema`

## Implementation Status
- **Total Tests**: 21
- **Passing**: 0 (domain handler stubbed with panic!)
- **Blocked**: All tests blocked on domain implementation

## Special Considerations
- Control domain coordinates system-wide concerns
- Integrates with authz for JWT config updates
- Requires background tasks for heartbeat/metrics
- Config updates affect runtime behavior

## Next Steps
1. Implement ControlDomain::handle() to parse TLV and route to operations
2. Implement background heartbeat/metrics tasks
3. Wire up config updates to system components
4. Update tests to work with new architecture
