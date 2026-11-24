//! Fitz Notice domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod notice_codec;

pub use domain::*;

#[doc(hidden)]
pub mod wire {
    pub use crate::notice_codec;
    pub use fitz_core::protocol::*;
    pub use notice_codec as notice;
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;
