//! Fitz Lease domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod lease_codec;

pub use domain::*;

#[doc(hidden)]
pub mod wire {
    pub use crate::lease_codec;
    pub use fitz_core::protocol::*;
    pub use lease_codec as lease;
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;
