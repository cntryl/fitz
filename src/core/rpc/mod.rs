//! RPC domain - request/reply messaging

pub mod client;
mod handler;

// Re-export public API
pub use client::RpcClient;
pub use handler::RpcDomain;
