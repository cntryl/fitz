//! RPC domain end-to-end tests.
//! Tests both TCP and WebSocket transports.

mod fixtures;

mod rpc_e2e {
    mod common;
    mod request_failures;
    mod response_errors;
    mod restart;
}
