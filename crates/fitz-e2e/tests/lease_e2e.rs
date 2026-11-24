//! Lease end-to-end transport tests
//!
//! Tests full lease domain functionality across TCP and WebSocket transports.
//! Split by scenario group to stay under the repository's file-size limit:
//! `common` (shared fixtures), `core` (acquire/renew/release/query lifecycle,
//! tokens, disconnect, restart), `waiters` (FIFO wait queue), `subscriptions`
//! (exact/wildcard watch notifications), and `list` (patterned inventory).

mod fixtures;

mod lease_e2e {
    mod common;
    mod core;
    mod list;
    mod subscriptions;
    mod waiters;
}
