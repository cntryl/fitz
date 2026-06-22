use crate::api::http::{Body, Request, Response};
use crate::api::ingress::IngressConfig;
use crate::session::manager::Ingress;
use crate::session::{
    generate_session_id, CloseReason, Session, SessionMetadata, SessionPermissions, TransportKind,
};
use bytes::Bytes;
use hyper_tungstenite::tungstenite::error::ProtocolError;
use hyper_tungstenite::tungstenite::Error as WsError;
use std::sync::Arc;
use tracing::info;

fn websocket_session_frame_error_reason(error: &crate::session::SessionError) -> String {
    format!("session frame error: {:?}", error)
}

fn websocket_close_reason(result: &Result<(), String>) -> CloseReason {
    match result {
        Ok(()) => CloseReason::ClientClose,
        Err(reason) => CloseReason::Error(reason.clone()),
    }
}

fn is_normal_websocket_disconnect(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    )
}

pub(super) async fn handle_websocket(
    req: Request,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    websocket_tasks: super::SessionTasks,
    ws_allowed_origins: Arc<Vec<crate::api::origin::ExactOrigin>>,
) -> Result<Response, std::convert::Infallible> {
    // Note: increment_connections / decrement_connections are handled by the
    // HTTP listener wrapper (handle_http_upgrade) — no additional counter here.
    if !runtime.is_accepting_traffic() {
        tracing::warn!("WebSocket upgrade rejected: broker is draining");
        return Ok(hyper::http::Response::builder()
            .status(503)
            .body(Body::from("Fitz broker is draining"))
            .unwrap());
    }

    if !websocket_origin_allowed(&req, ws_allowed_origins.as_slice()) {
        tracing::warn!("WebSocket upgrade rejected: origin not allowed");
        return Ok(hyper::http::Response::builder()
            .status(403)
            .body(Body::from("WebSocket origin not allowed"))
            .unwrap());
    }

    match hyper_tungstenite::upgrade(req, None) {
        Ok((response, websocket_fut)) => {
            let runtime_clone = runtime.clone();
            let router = runtime.router.clone();
            websocket_tasks.lock().await.spawn(async move {
                match websocket_fut.await {
                    Ok(ws_stream) => {
                        tracing::info!("WebSocket upgrade completed");
                        if let Err(e) =
                            run_websocket_session(ws_stream, ingress, config, runtime_clone, router)
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
    };
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

async fn run_websocket_session<S>(
    ws_stream: hyper_tungstenite::WebSocketStream<S>,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    router: Arc<crate::runtime::Router>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use hyper_tungstenite::tungstenite::Message;

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

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<Bytes>(config.channel_capacity);

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

    let ws_session_id = session_id;
    let runtime_for_writes = runtime.clone();
    let ingress_for_writes = ingress.clone();
    // Capture the handle so we can await graceful drain when the session closes.
    let writer_handle = tokio::spawn(async move {
        tracing::debug!(
            session_id = ws_session_id,
            "WS outbound writer task started"
        );
        while let Some(frame) = outbound_rx.recv().await {
            tracing::debug!(
                session_id = ws_session_id,
                frame_len = frame.len(),
                "WS outbound: sending frame to wire"
            );
            if let Err(e) = ws_sender.send(Message::Binary(frame)).await {
                tracing::error!(session_id = ws_session_id, error = %e, "WS outbound send error");
                break;
            }
            runtime_for_writes.increment_messages_sent();
            ingress_for_writes.record_frame_sent(ws_session_id);
        }
        tracing::debug!(session_id = ws_session_id, "WS outbound writer task ended");
    });

    let result = loop {
        let Some(msg_result) = ws_receiver.next().await else {
            break Ok(());
        };

        match msg_result {
            Ok(Message::Binary(data)) => {
                let frame = data;
                runtime.increment_messages_received();
                ingress.record_frame_received(session_id);
                tracing::debug!(
                    session_id = session_id,
                    frame_len = frame.len(),
                    "WS inbound: received binary frame"
                );

                if frame.len() > config.max_frame_size {
                    let reason = format!(
                        "frame too large: {} > {}",
                        frame.len(),
                        config.max_frame_size
                    );
                    tracing::warn!(
                        session_id = session_id,
                        frame_len = frame.len(),
                        max = config.max_frame_size,
                        "WS frame too large"
                    );
                    break Err(reason);
                }

                if let Err(error) = crate::api::session::process_session_frame(
                    &mut session,
                    frame,
                    ingress.as_ref(),
                )
                .await
                {
                    let reason = websocket_session_frame_error_reason(&error);
                    tracing::error!(session_id = session_id, error = %reason, "WS session frame processing error");
                    break Err(reason);
                }

                if let Some(updated_route_family) = ingress.get_route_family(session_id) {
                    if updated_route_family != *inbox_route.family() {
                        router.unregister(&inbox_route);
                        inbox_route = crate::runtime::routing::session_inbox_address(
                            updated_route_family,
                            session_id,
                        );
                        router.register(
                            inbox_route.clone(),
                            sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
                        );
                    }
                }
                tracing::trace!(
                    session_id = session_id,
                    "WS inbound frame processed successfully"
                );
            }
            Ok(Message::Close(_)) => {
                tracing::debug!(session_id = session_id, "WS received Close frame");
                break Ok(());
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Text(_)) => {
                tracing::trace!(
                    session_id = session_id,
                    "WS received non-binary frame (ignored)"
                );
            }
            Ok(_) => {}
            Err(e) => {
                if is_normal_websocket_disconnect(&e) {
                    tracing::info!(session_id = session_id, error = %e, "WS connection terminated by client without a close handshake");
                    break Ok(());
                }
                tracing::error!(session_id = session_id, error = %e, "WS session error");
                break Err(format!("WebSocket error: {}", e));
            }
        }
    };

    let close_reason = websocket_close_reason(&result);
    ingress.on_close(session_id, close_reason).await;
    router.unregister(&inbox_route);

    // Drop the sender-side of the outbound channel so the writer task sees EOF
    // and stops. Then wait up to 1 s for it to finish flushing remaining frames.
    drop(sink);
    drop(outbound_tx);
    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(1), writer_handle).await {
        tracing::warn!(
            session_id = session_id,
            error = %e,
            "WS writer task did not terminate in time"
        );
    }

    if result.is_ok() {
        info!("WebSocket connection closed, session {}", session_id);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{
        is_normal_websocket_disconnect, websocket_close_reason, websocket_origin_allowed,
        websocket_session_frame_error_reason,
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
