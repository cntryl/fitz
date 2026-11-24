//! Session identity shared by the broker and domains.

use std::sync::atomic::{AtomicU64, Ordering};

pub mod id_generator;
pub use id_generator::generate as generate_session_id;

/// Unique session identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique session ID across transports
pub fn next_session_id() -> u64 {
    SESSION_COUNTER.fetch_add(1, Ordering::SeqCst)
}
