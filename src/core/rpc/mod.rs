//! RPC domain - request/reply messaging

pub mod client;
mod handler;
mod service;
mod types;

// Re-export public API
pub use client::RpcClient;
pub use handler::RpcDomain;
pub use service::RpcService;
pub use types::*;
