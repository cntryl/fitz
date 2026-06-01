//! Transport handlers: TCP and WebSocket

use crate::observability as obs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};

mod http_listener;
mod tcp_listener;
mod tcp_session;
mod websocket;

pub use http_listener::{spawn_http_listener, spawn_http_listener_with_bound_socket};
pub use tcp_listener::{spawn_tcp_listener, spawn_tcp_listener_with_bound_socket};

pub struct ListenerHandle {
    pub ready: tokio::sync::oneshot::Receiver<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
    pub join: JoinHandle<()>,
}

pub(super) type SessionTasks = Arc<Mutex<JoinSet<()>>>;

pub(super) fn session_tasks() -> SessionTasks {
    Arc::new(Mutex::new(JoinSet::new()))
}

pub(super) async fn drain_session_tasks(label: &'static str, task_sets: Vec<SessionTasks>) {
    let drain = async {
        for tasks in &task_sets {
            let mut tasks = tasks.lock().await;
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    tracing::debug!(listener = label, error = %error, "Session task ended with join error");
                }
            }
        }
    };

    if tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .is_err()
    {
        tracing::warn!(
            listener = label,
            "Session drain timed out; aborting remaining tasks"
        );
        for tasks in task_sets {
            let mut tasks = tasks.lock().await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }
}

fn record_connection_opened() {
    if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_OPENED);
    }
}

fn record_connection_closed() {
    if let Ok(collector) = std::panic::catch_unwind(crate::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_CLOSED);
    }
}
