//! Broker wire contracts and domain-owned payload codecs.

pub use fitz_core::protocol::*;
pub use fitz_kv::kv_codec;
pub use fitz_lease::lease_codec;
pub use fitz_notice::notice_codec;
pub use fitz_queue::queue_codec;
pub use fitz_rpc::rpc_codec;
pub use fitz_schedule::schedule_codec;
pub use fitz_stream::stream_codec;
pub use kv_codec as kv;
pub use lease_codec as lease;
pub use notice_codec as notice;
pub use queue_codec as queue;
pub use rpc_codec as rpc;
pub use schedule_codec as schedule;
pub use stream_codec as stream;

#[cfg(test)]
mod auth_route_tests;
