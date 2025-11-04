//! Test harness helpers for end-to-end integration tests.
//!
//! Keep test utilities here so they can be imported by integration test files
//! using `mod harness; use harness::common::...`.

use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use fitz::core::engine::{start_engine, EngineHandle};
use fitz::storage::mem::MemStore;
use tokio::task::JoinHandle;

/// Message tuple sent to subscribers by the Router/Engine.
pub type SubscriberMsg = (
    String,
    Option<String>,
    Vec<u8>,
    Option<String>,
    Option<u32>,
    bool,
);

/// Start an in-process engine backed by an in-memory store.
/// Returns the `EngineHandle` and the `Arc<Mutex<MemStore>>` backing it so
/// tests may seed or inspect the store directly.
pub fn start_test_engine() -> (EngineHandle, Arc<Mutex<MemStore>>) {
    let store = Arc::new(Mutex::new(MemStore::new()));
    let handle = start_engine(store.clone());
    (handle, store)
}

/// Start test engine and return the `EngineHandle`, store, and the JoinHandle for the engine task.
pub fn start_test_engine_with_join() -> (EngineHandle, Arc<Mutex<MemStore>>, JoinHandle<()>) {
    let store = Arc::new(Mutex::new(MemStore::new()));
    let (handle, jh) = fitz::core::engine::start_engine_with_join(store.clone());
    (handle, store, jh)
}

/// Create a subscriber channel with the given capacity and return the sender
/// that can be passed to `EngineHandle::subscribe` and the receiver the test
/// can await on.
pub fn create_sub_channel(cap: usize) -> (fitz::core::engine::SubSender, mpsc::Receiver<SubscriberMsg>) {
    let (tx, rx) = mpsc::channel::<SubscriberMsg>(cap);
    (tx, rx)
}

/// Default small channel capacity used in many tests.
pub fn default_sub_capacity() -> usize {
    4
}
