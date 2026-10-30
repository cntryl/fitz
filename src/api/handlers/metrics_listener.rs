use super::{drain_session_tasks, reap_session_tasks, session_tasks, ListenerHandle};
use crate::api::http::Request;
use crate::boot::{BootConfig, BootResult, Runtime};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::Method;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

const MAX_METRICS_CONNECTIONS: usize = 16;

/// Spawn the unauthenticated Prometheus listener.
///
/// # Errors
///
/// Returns an error if the configured listener address cannot be bound.
pub async fn spawn_metrics_listener(
    config: &BootConfig,
    runtime: Runtime,
) -> BootResult<ListenerHandle> {
    let metrics_addr = format!("{}:{}", config.metrics_bind_addr, config.metrics_port);
    let metrics_listener = TcpListener::bind(&metrics_addr).await?;
    info!(
        "Metrics endpoint listening on {}",
        metrics_listener.local_addr()?
    );

    spawn_metrics_listener_with_bound_socket(metrics_listener, runtime)
}

/// Spawn the Prometheus listener with a pre-bound socket.
///
/// # Errors
///
/// Returns an error if the listener task cannot be created.
pub fn spawn_metrics_listener_with_bound_socket(
    metrics_listener: TcpListener,
    runtime: Runtime,
) -> BootResult<ListenerHandle> {
    let runtime = Arc::new(runtime);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    let join = tokio::spawn(async move {
        let connections = session_tasks();
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(MAX_METRICS_CONNECTIONS));
        let mut reap_interval = tokio::time::interval(std::time::Duration::from_millis(250));
        reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let _ = ready_tx.send(());

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    tracing::info!("Metrics listener shutdown requested");
                    break;
                }
                accept_result = metrics_listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            let Ok(connection_permit) = connection_limit.clone().try_acquire_owned() else {
                                tracing::debug!(%peer_addr, "Metrics connection capacity reached; dropping connection");
                                continue;
                            };
                            let runtime = runtime.clone();
                            connections.lock().await.spawn(async move {
                                let _connection_permit = connection_permit;
                                if let Err(error) = handle_metrics_connection(stream, runtime).await {
                                    tracing::debug!(%peer_addr, %error, "Metrics connection ended with an error");
                                }
                            });
                        }
                        Err(error) => {
                            tracing::error!(%error, "Metrics accept error");
                        }
                    }
                }
                _ = reap_interval.tick() => {
                    reap_session_tasks("metrics", &connections).await;
                }
            }
        }

        drain_session_tasks("metrics", vec![connections]).await;
    });

    Ok(ListenerHandle {
        ready: ready_rx,
        shutdown: shutdown_tx,
        join,
    })
}

async fn handle_metrics_connection(
    stream: TcpStream,
    runtime: Arc<Runtime>,
) -> Result<(), hyper::Error> {
    let service = service_fn(move |req: Request| {
        let runtime = runtime.clone();
        async move {
            let response = if req.method() == Method::GET && req.uri().path() == "/metrics" {
                handle_metrics_observation(runtime).await
            } else {
                hyper::http::Response::builder()
                    .status(hyper::StatusCode::NOT_FOUND)
                    .body(crate::api::http::Body::from("Not Found"))
                    .expect("static metrics 404 response is valid")
            };
            Ok::<_, std::convert::Infallible>(response)
        }
    });

    http1::Builder::new()
        .keep_alive(true)
        .serve_connection(TokioIo::new(stream), service)
        .await
}

async fn handle_metrics_observation(runtime: Arc<Runtime>) -> crate::api::http::Response {
    let Some(permit) = runtime.try_acquire_admin_blocking_permit() else {
        crate::api::admin::observation_metrics::record_overload_rejection();
        return hyper::http::Response::builder()
            .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
            .body(crate::api::http::Body::from(
                "Admin observation capacity is full",
            ))
            .expect("static metrics overload response is valid");
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        crate::api::admin::metrics::handle_metrics(runtime.as_ref())
    })
    .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(transport = "prometheus", operation = "metrics_observation", failure_class = "worker_join", %error, "Metrics observation worker failed");
            hyper::http::Response::builder()
                .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
                .body(crate::api::http::Body::from("Metrics observation failed"))
                .expect("static metrics failure response is valid")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Router;

    #[tokio::test]
    async fn should_reject_metrics_observation_when_admin_capacity_is_exhausted() {
        // Arrange
        let runtime = Arc::new(Runtime::new(Arc::new(Router::new())));
        let permits = (0..2)
            .map(|_| runtime.try_acquire_admin_blocking_permit().unwrap())
            .collect::<Vec<_>>();

        // Act
        let response = handle_metrics_observation(runtime).await;

        // Assert
        assert_eq!(response.status(), hyper::StatusCode::SERVICE_UNAVAILABLE);
        drop(permits);
    }
}
