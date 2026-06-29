// LAYER: RUNTIME
//! Client frame metadata passed through the synchronous runtime.
//!
//! Protocol codecs at the transport edge map wire-specific frame structures
//! into this small runtime-owned shape before routing into domain sinks.

use crate::runtime::routing::RouteFamily;
use bytes::Bytes;

/// Logical client channel carried with a routed client frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientChannel {
    Control,
    Pub,
    Sub,
    Rpc,
    Lease,
    Internal,
}

/// Runtime-owned metadata for one client frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientFrameMeta {
    pub session_id: u64,
    pub channel: ClientChannel,
    pub message_type: u16,
    pub route_family: RouteFamily,
}

impl ClientFrameMeta {
    #[must_use]
    pub fn new(
        session_id: u64,
        channel: ClientChannel,
        message_type: u16,
        route_family: RouteFamily,
    ) -> Self {
        Self {
            session_id,
            channel,
            message_type,
            route_family,
        }
    }
}

/// Already-encoded payload plus runtime client frame metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedClientFrame {
    pub meta: ClientFrameMeta,
    pub payload: Bytes,
}

impl EncodedClientFrame {
    pub fn new(meta: ClientFrameMeta, payload: Bytes) -> Self {
        Self { meta, payload }
    }
}
