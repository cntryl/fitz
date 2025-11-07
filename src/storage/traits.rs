//! Storage backend trait: domain-level storage built on KvStore
//!
//! Re-exports Midge's KvStore and KvTransaction traits which provide
//! the low-level key-value storage interface used throughout Fitz.

// Re-export Midge's storage traits and types
pub use midge::{KvStore, KvTransaction, MidgeError, MidgeResult};

/// Helper to convert MidgeError to String for legacy code
pub fn midge_error_to_string(err: MidgeError) -> String {
    format!("{:?}", err)
}

/// Helper trait for converting MidgeResult to Result<T, String>
pub trait MidgeResultExt<T> {
    fn map_err_string(self) -> Result<T, String>;
}

impl<T> MidgeResultExt<T> for MidgeResult<T> {
    fn map_err_string(self) -> Result<T, String> {
        self.map_err(midge_error_to_string)
    }
}
