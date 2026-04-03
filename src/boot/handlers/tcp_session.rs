use super::record_connection_closed;
use crate::api::ingress::IngressConfig;
use crate::session::manager::Ingress;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Handle an incoming TCP connection with outbound response support.
pub(super) async fn handle_tcp_connection(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::api::tcp::create_session;

    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let (handler, write_half) =
        create_session(ingress.clone(), config.clone(), stream, frame_tx.clone()).await?;
    runtime.increment_connections();

    let session_id = handler.session_id;

    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel::<Bytes>(config.channel_capacity);

    let sink = std::sync::Arc::new(crate::session::outbound::SessionOutboundSink::new(
        outbound_tx.clone(),
    ));
    let initial_family = ingress
        .get_route_family(session_id)
        .unwrap_or_else(|| crate::runtime::routing::RouteFamily::new(1));
    let inbox_route = crate::runtime::routing::session_inbox_address(initial_family, session_id);
    let current_inbox = Arc::new(Mutex::new(inbox_route.clone()));
    let registered_inboxes = Arc::new(Mutex::new(vec![inbox_route.clone()]));
    tracing::debug!(
        session_id = session_id,
        inbox = %inbox_route,
        "Registering TCP outbound sink at inbox route"
    );
    runtime.router.register(
        inbox_route.clone(),
        sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
    );

    let tcp_session_id = session_id;
    let runtime_for_writes = runtime.clone();
    let mut write_half = write_half;
    let writer_handle = tokio::spawn(async move {
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task started"
        );
        while let Some(frame) = outbound_rx.recv().await {
            tracing::debug!(
                session_id = tcp_session_id,
                frame_len = frame.len(),
                "TCP outbound: sending frame to wire"
            );
            let len = frame.len() as u32;
            if let Err(e) = write_half.write_all(&len.to_be_bytes()).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error (header)");
                break;
            }
            if let Err(e) = write_half.write_all(frame.as_ref()).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error (payload)");
                break;
            }
            if let Err(e) = write_half.flush().await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound flush error");
                break;
            }
            runtime_for_writes.increment_messages_sent();
        }
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task ended"
        );
    });

    let ingress_clone = ingress.clone();
    let config_clone = config.clone();
    let runtime_for_frames = runtime.clone();
    let outbound_sink = sink.clone();
    let current_inbox_for_frames = current_inbox.clone();
    let registered_inboxes_for_frames = registered_inboxes.clone();
    let mut frame_handle = tokio::spawn(async move {
        let session_config = crate::session::NewSessionConfig::unauthenticated(
            crate::session::TransportKind::Tcp,
            None,
            crate::session::SessionPermissions::empty(),
            crate::session::SessionMetadata::new(),
            config_clone.channel_capacity,
            None,
            crate::runtime::routing::RouteFamily::new(1),
        );

        let mut session = crate::session::Session::new(session_id, session_config);
        let mut frame_rx = frame_rx;
        let mut registered_inbox =
            crate::runtime::routing::session_inbox_address(initial_family, session_id);

        while let Some((_sid, frame)) = frame_rx.recv().await {
            runtime_for_frames.increment_messages_received();
            if let Err(e) = session.on_frame(frame, ingress_clone.as_ref()).await {
                tracing::error!(session_id = session_id, error = %e, "TCP frame processing error");
                ingress_clone
                    .on_close(
                        session_id,
                        crate::session::CloseReason::Error(format!("{:?}", e)),
                    )
                    .await;
                return Err(format!("{e}"));
            }

            if let Some(updated_route_family) = ingress_clone.get_route_family(session_id) {
                if updated_route_family != *registered_inbox.family() {
                    runtime_for_frames.router.unregister(&registered_inbox);
                    registered_inbox = crate::runtime::routing::session_inbox_address(
                        updated_route_family,
                        session_id,
                    );
                    runtime_for_frames.router.register(
                        registered_inbox.clone(),
                        outbound_sink.clone()
                            as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
                    );
                    *current_inbox_for_frames.lock() = registered_inbox.clone();
                    let mut inboxes = registered_inboxes_for_frames.lock();
                    if !inboxes.contains(&registered_inbox) {
                        inboxes.push(registered_inbox.clone());
                    }
                }
            }
        }
        Ok::<(), String>(())
    });

    let mut handler_task = tokio::spawn(async move { handler.run().await });
    let run_result = tokio::select! {
        res = &mut handler_task => {
            drop(frame_tx);
            if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(1), &mut frame_handle).await {
                tracing::warn!(session_id = session_id, error = %e, "TCP frame task did not terminate in time");
            }
            match res {
                Ok(result) => result,
                Err(e) => Err(format!("tcp handler task join error: {e}")),
            }
        }
        res = &mut frame_handle => {
            drop(frame_tx);
            match res {
                Ok(Ok(())) => Ok(()),
                Ok(Err(reason)) => {
                    tracing::debug!(
                        session_id = session_id,
                        reason = %reason,
                        "TCP frame task requested transport close"
                    );
                    handler_task.abort();
                    Err(reason)
                }
                Err(e) => {
                    handler_task.abort();
                    Err(format!("tcp frame task join error: {e}"))
                }
            }
        }
    };

    let inboxes = registered_inboxes.lock().clone();
    for inbox in &inboxes {
        runtime.router.unregister(inbox);
    }

    drop(current_inbox);
    drop(sink);
    drop(outbound_tx);

    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(1), writer_handle).await {
        tracing::warn!(session_id = session_id, error = %e, "TCP writer task did not terminate in time");
    }

    record_connection_closed();
    runtime.decrement_connections();

    run_result
        .map_err(std::io::Error::other)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}
