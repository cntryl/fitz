//! Consolidated KV end-to-end tests - transport, codec, and integration.

mod fixtures;

mod kv_e2e {
    mod scenarios;
    mod tcp;
    mod websocket;
}
