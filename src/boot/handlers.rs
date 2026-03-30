//! Transport handlers: TCP and WebSocket

use crate::observability as obs;

mod http_listener;
mod tcp_listener;
mod tcp_session;
mod websocket;

pub use http_listener::{spawn_http_listener, spawn_http_listener_with_bound_socket};
pub use tcp_listener::{spawn_tcp_listener, spawn_tcp_listener_with_bound_socket};

pub struct ListenerHandle {
    pub ready: tokio::sync::oneshot::Receiver<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
}

fn record_connection_opened() {
    if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_OPENED);
    }
}

fn record_connection_closed() {
    if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_CLOSED);
    }
}
