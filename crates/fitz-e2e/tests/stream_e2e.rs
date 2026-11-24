//! Stream domain end-to-end tests.
//! Tests both TCP and WebSocket transports.

mod fixtures;

mod stream_e2e {
    mod common;
    mod event_sourcing;
    mod restart_storage;
    mod subscriptions_disconnect;
    mod transport_cases;
    mod transport_reading;
    mod wildcard_metadata;
}
