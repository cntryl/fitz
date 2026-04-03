use crate::api::ingress::IngressConfig;
use crate::session::manager::Ingress;
use crate::session::{
    generate_session_id, CloseReason, Session, SessionMetadata, SessionPermissions, TransportKind,
};
use bytes::Bytes;
use std::sync::Arc;
use tracing::info;

pub(super) async fn handle_websocket(
    req: hyper::Request<hyper::Body>,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
) -> Result<hyper::Response<hyper::Body>, std::convert::Infallible> {
    runtime.increment_sessions();

    match hyper_tungstenite::upgrade(req, None) {
        Ok((response, websocket_fut)) => {
            let runtime_clone = runtime.clone();
            let router = runtime.router.clone();
            tokio::spawn(async move {
                match websocket_fut.await {
                    Ok(ws_stream) => {
                        tracing::info!("WebSocket upgrade completed");
                        if let Err(e) = run_websocket_session(
                            ws_stream,
                            ingress,
                            config,
                            runtime_clone.clone(),
                            router,
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
                runtime_clone.decrement_sessions();
            });

            Ok(response)
        }
        Err(e) => {
            tracing::debug!("WebSocket upgrade rejected: {}", e);
            runtime.decrement_sessions();
            Ok(hyper::Response::builder()
                .status(400)
                .body(hyper::Body::from("Bad WebSocket upgrade request"))
                .unwrap())
        }
    }
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
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel::<Bytes>(config.channel_capacity);

    let sink = std::sync::Arc::new(crate::session::outbound::SessionOutboundSink::new(
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
    tokio::spawn(async move {
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
            if let Err(e) = ws_sender.send(Message::Binary(frame.to_vec())).await {
                tracing::error!(session_id = ws_session_id, error = %e, "WS outbound send error");
                break;
            }
            runtime_for_writes.increment_messages_sent();
        }
        tracing::debug!(session_id = ws_session_id, "WS outbound writer task ended");
    });

    let result = loop {
        let Some(msg_result) = ws_receiver.next().await else {
            break Ok(());
        };

        match msg_result {
            Ok(Message::Binary(data)) => {
                let frame = Bytes::from(data);
                runtime.increment_messages_received();
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

                if let Err(e) = session.on_frame(frame, ingress.as_ref()).await {
                    let reason = format!("session frame error: {:?}", e);
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
                break Err(format!("WebSocket error: {}", e));
            }
        }
    };

    let close_reason = match &result {
        Ok(()) => CloseReason::ClientClose,
        Err(reason) => CloseReason::Error(reason.clone()),
    };
    ingress.on_close(session_id, close_reason).await;
    router.unregister(&inbox_route);

    if result.is_ok() {
        info!("WebSocket connection closed, session {}", session_id);
    }
    result
}
