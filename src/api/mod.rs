//! Public APIs for SDK and external clients.
//!
//! Clean, language-agnostic API surface:
//! - fitz.publish(...)
//! - fitz.subscribe(...)
//! - fitz.rpc(...)
//! - fitz.queue.push(...)
//! - fitz.stream.append(...)
//! - fitz.kv.put(...)

pub mod client_api;
pub mod server_api;
pub mod stream_api;
pub mod queue_api;
pub mod rpc_api;
pub mod kv_api;
pub mod conn_options;
