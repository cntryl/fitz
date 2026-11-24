//! Fitz Stream domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod stream_codec;

pub use domain::*;

#[doc(hidden)]
pub mod wire {
    pub use crate::stream_codec;
    pub use fitz_core::protocol::*;
    pub use stream_codec as stream;
}

pub mod snapshot;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;

#[cfg(feature = "benchkit")]
#[doc(hidden)]
pub mod stream_dispatch;
