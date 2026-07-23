use crate::api::handlers::{ConnectionLifecycle, SessionCloseFinalizer};
use crate::api::http::{Body, Request, Response};
use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::Ingress;
use crate::session::{
    generate_session_id, CloseReason, Session, SessionMetadata, SessionPermissions, TransportKind,
};
use bytes::Bytes;
use hyper_tungstenite::tungstenite::error::ProtocolError;
use hyper_tungstenite::tungstenite::protocol::WebSocketConfig;
use hyper_tungstenite::tungstenite::Error as WsError;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;

fn websocket_session_frame_error_reason(error: &crate::session::SessionError) -> String {
    format!("session frame error: {error:?}")
}

fn websocket_close_reason(result: &Result<(), String>) -> CloseReason {
    match result {
        Ok(()) => CloseReason::ClientClose,
        Err(reason) if reason.contains("pong timeout") => CloseReason::Timeout,
        Err(reason) => CloseReason::Error(reason.clone()),
    }
}

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn is_normal_websocket_disconnect(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    )
}

fn bounded_websocket_config(max_frame_size: usize) -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(max_frame_size))
        .max_frame_size(Some(max_frame_size))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_websocket(
    req: Request,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    websocket_tasks: super::SessionTasks,
    ws_allowed_origins: Arc<Vec<crate::api::origin::ExactOrigin>>,
    connection_lifecycle: ConnectionLifecycle,
    accepted_at: tokio::time::Instant,
) -> Result<Response, std::convert::Infallible> {
    // Note: increment_connections / decrement_connections are handled by the
    // HTTP listener wrapper (handle_http_upgrade) — no additional counter here.
    if !runtime.is_ready_for_traffic() {
        tracing::warn!(
            lifecycle = runtime.lifecycle_state().as_str(),
            storage_ready = runtime.is_storage_ready(),
            domains_ready = runtime.are_domains_ready(),
            startup_complete = runtime.is_startup_complete(),
            "WebSocket upgrade rejected: broker data plane is not ready"
        );
        return Ok(hyper::http::Response::builder()
            .status(503)
            .header("Retry-After", "1")
            .body(Body::from("Fitz broker data plane is not ready"))
            .unwrap());
    }

    if !websocket_origin_allowed(&req, ws_allowed_origins.as_slice()) {
        tracing::warn!("WebSocket upgrade rejected: origin not allowed");
        return Ok(hyper::http::Response::builder()
            .status(403)
            .body(Body::from("WebSocket origin not allowed"))
            .unwrap());
    }

    let websocket_config = bounded_websocket_config(config.max_frame_size);
    match hyper_tungstenite::upgrade(req, Some(websocket_config)) {
        Ok((response, websocket_fut)) => {
            let runtime_clone = runtime.clone();
            let router = runtime.router.clone();
            connection_lifecycle.handoff_to_websocket();
            let websocket_lifecycle = connection_lifecycle.websocket_owner();
            websocket_tasks.lock().await.spawn(async move {
                match websocket_fut.await {
                    Ok(ws_stream) => {
                        tracing::info!("WebSocket upgrade completed");
                        if let Err(e) = run_websocket_session(
                            ws_stream,
                            ingress,
                            config,
                            runtime_clone,
                            router,
                            websocket_lifecycle,
                            accepted_at,
                        )
                        .await
                        {
                            tracing::error!("WebSocket session error: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("WebSocket upgrade error: {}", e);
                    }
                }
            });

            Ok(response)
        }
        Err(e) => {
            tracing::debug!("WebSocket upgrade rejected: {}", e);
            Ok(hyper::http::Response::builder()
                .status(400)
                .body(Body::from("Bad WebSocket upgrade request"))
                .unwrap())
        }
    }
}

fn websocket_origin_allowed<B>(
    req: &hyper::Request<B>,
    allowed_origins: &[crate::api::origin::ExactOrigin],
) -> bool {
    if allowed_origins.is_empty() {
        return true;
    }

    let mut origin_values = req.headers().get_all("origin").iter();
    let Some(origin_value) = origin_values.next() else {
        return true;
    };
    if origin_values.next().is_some() {
        return false;
    }
    let Some(origin) = origin_value
        .to_str()
        .ok()
        .and_then(|value| crate::api::origin::parse_exact_origin(value).ok())
    else {
        return false;
    };

    allowed_origins
        .iter()
        .any(|allowed| allowed.same_origin(&origin))
}

async fn run_websocket_writer<S>(
    mut ws_sender: futures_util::stream::SplitSink<
        hyper_tungstenite::WebSocketStream<S>,
        hyper_tungstenite::tungstenite::Message,
    >,
    mut outbound_rx: tokio::sync::mpsc::Receiver<Bytes>,
    ws_session_id: u64,
    runtime_for_writes: Arc<crate::boot::Runtime>,
    ingress_for_writes: Arc<dyn Ingress>,
    mut control_rx: tokio::sync::mpsc::Receiver<hyper_tungstenite::tungstenite::Message>,
    last_pong: Arc<AtomicU64>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::SinkExt;
    use hyper_tungstenite::tungstenite::Message;

    tracing::debug!(
        session_id = ws_session_id,
        "WS outbound writer task started"
    );
    let mut ping_interval = tokio::time::interval(crate::api::ingress::WEBSOCKET_PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut outbound_open = true;
    let mut control_open = true;
    let mut ping_sent = false;

    loop {
        tokio::select! {
            frame = outbound_rx.recv(), if outbound_open => {
                match frame {
                    Some(frame) => {
                        tracing::debug!(session_id = ws_session_id, frame_len = frame.len(), "WS outbound: sending frame to wire");
                        ws_sender.send(Message::Binary(frame)).await.map_err(|error| format!("WebSocket writer error: {error}"))?;
                        runtime_for_writes.increment_messages_sent();
                        ingress_for_writes.record_frame_sent(ws_session_id);
                    }
                    None => outbound_open = false,
                }
            }
            control = control_rx.recv(), if control_open => {
                match control {
                    Some(control) => ws_sender.send(control).await.map_err(|error| format!("WebSocket control writer error: {error}"))?,
                    None => control_open = false,
                }
            }
            _ = ping_interval.tick() => {
                let elapsed_ms = epoch_millis().saturating_sub(last_pong.load(Ordering::Acquire));
                if ping_sent
                    && elapsed_ms
                        >= crate::api::ingress::WEBSOCKET_PONG_TIMEOUT
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX)
                {
                    return Err("websocket pong timeout".to_string());
                }
                ws_sender.send(Message::Ping(bytes::Bytes::new())).await.map_err(|error| format!("WebSocket ping error: {error}"))?;
                ping_sent = true;
            }
            else => break,
        }

        if !outbound_open && !control_open {
            break;
        }
    }
    tracing::debug!(session_id = ws_session_id, "WS outbound writer task ended");
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_websocket_session<S>(
    ws_stream: hyper_tungstenite::WebSocketStream<S>,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    router: Arc<crate::runtime::Router>,
    connection_lifecycle: ConnectionLifecycle,
    accepted_at: tokio::time::Instant,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::StreamExt;

    let session_id = generate_session_id();

    let session_config = crate::session::NewSessionConfig::unauthenticated(
        TransportKind::WebSocket,
        None,
        SessionPermissions::empty(),
        SessionMetadata::new(),
        config.channel_capacity,
        None,
        crate::runtime::routing::RouteFamily::new(1),
    );
    let mut session = Session::new(session_id, session_config);

    let session_id = ingress.on_open(session.info()).await?;

    info!(
        session_id = session_id,
        "WebSocket session accepted by ingress"
    );

    let (ws_sender, mut ws_receiver) = ws_stream.split();
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<Bytes>(config.channel_capacity);
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(8);
    let last_pong = Arc::new(AtomicU64::new(epoch_millis()));

    let sink = std::sync::Arc::new(crate::api::outbound::SessionOutboundSink::new(
        outbound_tx.clone(),
    ));
    let mut inbox_route =
        crate::runtime::routing::session_inbox_address(session.info().route_family, session_id);
    tracing::debug!(
        session_id = session_id,
        inbox = %inbox_route,
        "Registering outbound sink at inbox route"
    );
    router.register(
        inbox_route.clone(),
        sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
    );
    let finalizer = Arc::new(SessionCloseFinalizer::new(
        ingress.clone(),
        router.clone(),
        session_id,
        inbox_route.clone(),
    ));

    let ws_session_id = session_id;
    let runtime_for_writes = runtime.clone();
    let ingress_for_writes = ingress.clone();
    // Capture the handle so we can await graceful drain when the session closes.
    let writer_last_pong = last_pong.clone();
    let writer_handle = tokio::spawn(async move {
        run_websocket_writer(
            ws_sender,
            outbound_rx,
            ws_session_id,
            runtime_for_writes,
            ingress_for_writes,
            control_rx,
            writer_last_pong,
        )
        .await
    });

    let mut writer_handle = writer_handle;
    let runtime_for_shutdown = runtime.clone();
    let result = tokio::select! {
        result = process_websocket_frames(
            &mut ws_receiver,
            WebSocketFrameContext {
                session: &mut session,
                config: &config,
                ingress: ingress.as_ref(),
                runtime: &runtime,
                router: &router,
                session_id,
                sink: &sink,
                inbox_route: &mut inbox_route,
                finalizer: &finalizer,
                control_tx: &control_tx,
                last_pong: &last_pong,
                connect_deadline: accepted_at + crate::api::ingress::CONNECT_DEADLINE,
            },
        ) => {
            writer_handle.abort();
            result
        }
        writer_result = &mut writer_handle => {
            match writer_result {
                Ok(Ok(())) => Err("WebSocket writer stopped".to_string()),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(format!("WebSocket writer task failed: {error}")),
            }
        }
        () = async {
            while !runtime_for_shutdown.is_shutting_down() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        } => {
            writer_handle.abort();
            Err("server shutdown".to_string())
        }
    };

    let close_reason = websocket_close_reason(&result);
    finalizer.finish(close_reason).await;

    drop(sink);
    drop(outbound_tx);
    drop(control_tx);
    if let Err(e) = tokio::time::timeout(Duration::from_secs(1), writer_handle).await {
        tracing::warn!(session_id = session_id, error = %e, "WS writer task did not terminate in time");
    }

    connection_lifecycle.finish();

    if result.is_ok() {
        info!("WebSocket connection closed, session {}", session_id);
    }
    result
}

struct WebSocketFrameContext<'a> {
    session: &'a mut Session,
    config: &'a IngressConfig,
    ingress: &'a dyn Ingress,
    runtime: &'a Arc<crate::boot::Runtime>,
    router: &'a Arc<crate::runtime::Router>,
    session_id: u64,
    sink: &'a Arc<crate::api::outbound::SessionOutboundSink>,
    inbox_route: &'a mut crate::runtime::routing::RouteAddress,
    finalizer: &'a Arc<SessionCloseFinalizer>,
    control_tx: &'a tokio::sync::mpsc::Sender<hyper_tungstenite::tungstenite::Message>,
    last_pong: &'a AtomicU64,
    connect_deadline: tokio::time::Instant,
}

#[allow(clippy::too_many_lines)]
async fn process_websocket_frames<S>(
    ws_receiver: &mut futures_util::stream::SplitStream<hyper_tungstenite::WebSocketStream<S>>,
    context: WebSocketFrameContext<'_>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::StreamExt;
    use hyper_tungstenite::tungstenite::Message;
    loop {
        let next_message = if context.session.info().authenticated {
            ws_receiver.next().await
        } else {
            let remaining = context
                .connect_deadline
                .saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break Err("CONNECT deadline exceeded".to_string());
            }
            match tokio::time::timeout(remaining, ws_receiver.next()).await {
                Ok(message) => message,
                Err(_) => break Err("CONNECT deadline exceeded".to_string()),
            }
        };

        let Some(msg_result) = next_message else {
            break Ok(());
        };

        match msg_result {
            Ok(Message::Binary(data)) => {
                let frame = data;
                context.runtime.increment_messages_received();
                context.ingress.record_frame_received(context.session_id);
                tracing::debug!(
                    session_id = context.session_id,
                    frame_len = frame.len(),
                    "WS inbound: received binary frame"
                );

                if frame.len() > context.config.max_frame_size {
                    let reason = format!(
                        "frame too large: {} > {}",
                        frame.len(),
                        context.config.max_frame_size
                    );
                    tracing::warn!(
                        session_id = context.session_id,
                        frame_len = frame.len(),
                        max = context.config.max_frame_size,
                        "WS frame too large"
                    );
                    break Err(reason);
                }

                if let Err(error) = crate::api::session::process_session_frame(
                    context.session,
                    frame,
                    context.ingress,
                )
                .await
                {
                    let reason = websocket_session_frame_error_reason(&error);
                    tracing::error!(session_id = context.session_id, error = %reason, "WS session frame processing error");
                    break Err(reason);
                }

                if let Some(updated_route_family) =
                    context.ingress.get_route_family(context.session_id)
                {
                    if updated_route_family != *context.inbox_route.family() {
                        context.router.unregister(context.inbox_route);
                        *context.inbox_route = crate::runtime::routing::session_inbox_address(
                            updated_route_family,
                            context.session_id,
                        );
                        context.router.register(
                            context.inbox_route.clone(),
                            context.sink.clone()
                                as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
                        );
                        context.finalizer.add_route(context.inbox_route.clone());
                    }
                }
                tracing::trace!(
                    session_id = context.session_id,
                    "WS inbound frame processed successfully"
                );
            }
            Ok(Message::Close(_)) => {
                tracing::debug!(session_id = context.session_id, "WS received Close frame");
                break Ok(());
            }
            Ok(Message::Ping(payload)) => {
                context
                    .control_tx
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| "WebSocket control channel closed".to_string())?;
            }
            Ok(Message::Pong(_)) => {
                context.last_pong.store(epoch_millis(), Ordering::Release);
            }
            Ok(Message::Text(_)) => {
                tracing::trace!(
                    session_id = context.session_id,
                    "WS received non-binary frame (ignored)"
                );
            }
            Ok(_) => {}
            Err(e) => {
                if is_normal_websocket_disconnect(&e) {
                    tracing::info!(session_id = context.session_id, error = %e, "WS connection terminated by client without a close handshake");
                    break Ok(());
                }
                tracing::error!(session_id = context.session_id, error = %e, "WS session error");
                break Err(format!("WebSocket error: {e}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bounded_websocket_config, is_normal_websocket_disconnect, websocket_close_reason,
        websocket_origin_allowed, websocket_session_frame_error_reason,
    };
    use crate::protocol::frame::ChannelId;
    use crate::session::{CloseReason, SessionError};
    use hyper::Request;
    use hyper_tungstenite::tungstenite::error::ProtocolError;
    use hyper_tungstenite::tungstenite::Error as WsError;

    #[test]
    fn should_treat_websocket_backpressure_as_terminal_session_error() {
        // Arrange
        let reason =
            websocket_session_frame_error_reason(&SessionError::Backpressure(ChannelId::Control));
        let result = Err(reason.clone());

        // Act
        let close_reason = websocket_close_reason(&result);

        // Assert
        assert_eq!(reason, "session frame error: Backpressure(Control)");
        assert!(matches!(
            close_reason,
            CloseReason::Error(message) if message == reason
        ));
    }

    #[test]
    fn should_treat_connection_reset_without_close_handshake_as_graceful_disconnect() {
        // Arrange
        let error = WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake);

        // Act
        let result = is_normal_websocket_disconnect(&error);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_bound_websocket_decoder_to_ingress_frame_limit() {
        // Arrange
        let max_frame_size = 512 * 1024;

        // Act
        let config = bounded_websocket_config(max_frame_size);

        // Assert
        assert_eq!(config.max_message_size, Some(max_frame_size));
        assert_eq!(config.max_frame_size, Some(max_frame_size));
    }

    #[test]
    fn should_allow_configured_websocket_origin() {
        // Arrange
        let allowed = vec![
            crate::api::origin::parse_exact_origin("https://app.example.com").expect("origin"),
        ];
        let request = Request::builder()
            .header("Origin", "https://app.example.com")
            .body(crate::api::http::Body::default())
            .expect("request");

        // Act
        let result = websocket_origin_allowed(&request, &allowed);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_allow_missing_websocket_origin_when_origins_are_configured() {
        // Arrange
        let allowed = vec![
            crate::api::origin::parse_exact_origin("https://app.example.com").expect("origin"),
        ];
        let request = Request::builder()
            .body(crate::api::http::Body::default())
            .expect("request");

        // Act
        let result = websocket_origin_allowed(&request, &allowed);

        // Assert
        assert!(result);
    }
}
