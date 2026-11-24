//! Fitz Rpc domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod rpc_codec;

pub use domain::*;

#[doc(hidden)]
pub mod wire {
    pub use crate::rpc_codec;
    pub use fitz_core::protocol::*;
    pub use rpc_codec as rpc;
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;
