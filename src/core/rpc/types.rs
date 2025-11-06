//! RPC domain types

use std::fmt;

/// RPC request handle returned to client
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RpcRequestId(pub String);

impl fmt::Display for RpcRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// RPC reply message
#[derive(Debug, Clone)]
pub struct RpcReply {
    pub correlation_id: String,
    pub body: Vec<u8>,
    pub seq: Option<u64>,
    pub is_stream_end: bool,
}

/// RPC error types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    Timeout,
    NotFound,
    PermissionDenied,
    Backpressure,
    InvalidToken,
    MalformedPayload(String),
    InboxNotFound,
    Canceled,
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => write!(f, "Request timed out"),
            Self::NotFound => write!(f, "Route not found"),
            Self::PermissionDenied => write!(f, "Permission denied"),
            Self::Backpressure => write!(f, "Broker backpressure"),
            Self::InvalidToken => write!(f, "Invalid delivery token"),
            Self::MalformedPayload(msg) => write!(f, "Malformed payload: {}", msg),
            Self::InboxNotFound => write!(f, "Inbox not found"),
            Self::Canceled => write!(f, "Request canceled"),
        }
    }
}

impl std::error::Error for RpcError {}
