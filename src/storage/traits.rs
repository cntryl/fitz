//! Storage backend trait: domain-level storage built on KvStore
//!
//! The storage layer provides domain-friendly abstractions (routes, records, metadata)
//! built on top of a low-level KvStore trait. This allows domain services to work with
//! logical concepts while the actual persistence is delegated to the KvStore implementation.

// ============================================================================
// KV STORE TRAIT (Foundation Layer)
// ============================================================================
// This is the low-level key-value store interface that the real storage
// implementation will provide. Our StorageBackend is built on top of this.

use bytes::Bytes;

/// Simple key-value store interface.
/// The real implementation comes from another project (Shale).
pub trait KvStore: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String>;
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, String>;
    fn delete(&self, key: &[u8]) -> Result<(), String>;
    fn put_batch(&self, writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String>;
    fn delete_batch(&self, keys: Vec<Vec<u8>>) -> Result<(), String>;
    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String>;
    fn flush(&self) -> Result<(), String>;

    /// Begin a new transaction with snapshot isolation.
    fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String>;
}

/// Transaction with snapshot isolation and ACID guarantees.
pub trait KvTransaction: Send {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String>;
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, String>;
    fn delete(&mut self, key: &[u8]) -> Result<(), String>;
    fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String>;

    /// Commit the transaction. Returns error if conflicts detected.
    fn commit(self: Box<Self>) -> Result<(), String>;

    /// Rollback the transaction.
    fn rollback(self: Box<Self>) -> Result<(), String>;
}
