//! Fitz Queue domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod queue_codec;

pub use domain::*;

use fitz_core::observability;
pub mod utils;

#[doc(hidden)]
pub mod wire {
    pub use crate::queue_codec;
    pub use fitz_core::protocol::*;
    pub use queue_codec as queue;
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;
