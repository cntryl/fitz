//! Fitz Kv domain and payload codec.

#[doc(hidden)]
pub mod control;
pub mod domain;
pub mod kv_codec;

pub use domain::*;

#[doc(hidden)]
pub mod wire {
    pub use crate::kv_codec;
    pub use fitz_core::protocol::*;
    pub use kv_codec as kv;
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod testkit;
mod prelude {
    pub use fitz_core::runtime::*;
}
pub mod snapshot;
