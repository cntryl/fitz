mod frame_and_routes;
mod mutation_parsers;

pub use crate::domain::protocol::KvClientFrame as ParsedKvFrame;
pub use frame_and_routes::{
    begin_mode, encode_response, extract_auth_route, msg_type, parse_frame, parse_request,
};
pub use mutation_parsers::{encode_notify, SCAN_OPTION_REVERSE, SCAN_OPTION_START_EXCLUSIVE};

#[cfg(test)]
mod drop_tests;
#[cfg(test)]
mod tests;

#[path = "kv_envelope.rs"]
mod envelope_build;

pub use envelope_build::build_envelope;
