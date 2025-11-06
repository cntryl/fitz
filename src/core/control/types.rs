//! Control domain types
//!
//! Control domain uses raw body bytes (often JSON payloads in tests, but the
//! domain itself treats them as opaque bytes). The domain handler processes
//! TLV-encoded frames and the body contains the actual control data.

/// Control operation types based on routes
#[derive(Debug, Clone)]
pub enum ControlOperation {
    /// Heartbeat - periodic liveness signal
    /// Body contains arbitrary data (often JSON with nodeId, timestamp)
    Heartbeat,
    /// Shutdown - graceful shutdown notification
    /// Body contains arbitrary data (often JSON with nodeId, optional reason)
    Shutdown,
    /// Metrics - system health and performance data
    /// Body contains arbitrary data (often JSON with nodeId and metrics)
    Metrics,
    /// Config - receive configuration from control plane
    /// Body contains arbitrary data (often JSON configuration)
    Config,
}

impl ControlOperation {
    /// Determine operation from route string
    pub fn from_route(route: &str) -> Result<Self, String> {
        if route.contains("heartbeat") {
            Ok(ControlOperation::Heartbeat)
        } else if route.contains("shutdown") {
            Ok(ControlOperation::Shutdown)
        } else if route.contains("metrics") {
            Ok(ControlOperation::Metrics)
        } else if route.contains("config") {
            Ok(ControlOperation::Config)
        } else {
            Err(format!("Unknown control operation: {}", route))
        }
    }
}
