//! Fitz Schedule domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod schedule_codec;

pub use domain::*;

#[doc(hidden)]
pub mod wire {
    pub use crate::schedule_codec;
    pub use fitz_core::protocol::*;
    pub use schedule_codec as schedule;
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;
