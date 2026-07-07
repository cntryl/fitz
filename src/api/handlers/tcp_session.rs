use super::record_connection_closed;
use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::Ingress;
use bytes::Bytes;
use parking_lot::Mutex;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

// Initial capacity for the per-connection write buffer (length prefix + typical frame).
// Grows automatically for larger frames; the allocation is reused across all frames
// on the same connection so there is at most one heap allocation per connection.
const WRITE_BUF_INIT_CAPACITY: usize = 512;

fn should_ignore_unknown_session_error(error: &crate::session::SessionError) -> bool {
    matches!(
        error,
        crate::session::SessionError::IngressClose(reason)
            if reason.starts_with("unknown session:")
    )
}

async fn close_tcp_session_on_frame_error(
    ingress: &dyn Ingress,
    session_id: u64,
    error: crate::session::SessionError,
) -> String {
    let reason = error.to_string();
    ingress
        .on_close(
            session_id,
            crate::session::CloseReason::Error(format!("{error:?}")),
        )
        .await;
    reason
}

fn frame_length_prefix(frame_len: usize) -> Result<[u8; 4], std::num::TryFromIntError> {
    u32::try_from(frame_len).map(u32::to_be_bytes)
}

async fn write_tcp_frame(
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    write_buf: &mut Vec<u8>,
    frame: Bytes,
) -> Result<(), std::io::Error> {
    write_buf.clear();
    write_buf
        .extend_from_slice(&frame_length_prefix(frame.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large")
        })?);
    write_buf.extend_from_slice(&frame);
    write_half.write_all(write_buf).await?;
    write_half.flush().await
}

struct TcpFrameTaskContext {
    ingress: Arc<dyn Ingress>,
    channel_capacity: usize,
    runtime: Arc<crate::boot::Runtime>,
    outbound_sink: Arc<crate::api::outbound::SessionOutboundSink>,
    registered_inboxes: Arc<Mutex<Vec<crate::runtime::routing::RouteAddress>>>,
    initial_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
}

fn spawn_tcp_frame_task(
    frame_rx: tokio::sync::mpsc::Receiver<(u64, bytes::Bytes)>,
    context: TcpFrameTaskContext,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let session_config = crate::session::NewSessionConfig::unauthenticated(
            crate::session::TransportKind::Tcp,
            None,
            crate::session::SessionPermissions::empty(),
            crate::session::SessionMetadata::new(),
            context.channel_capacity,
            None,
            crate::runtime::routing::RouteFamily::new(1),
        );

        let mut session = crate::session::Session::new(context.session_id, session_config);
        let mut frame_rx = frame_rx;
        let mut registered_inbox = crate::runtime::routing::session_inbox_address(
            context.initial_family,
            context.session_id,
        );

        while let Some((_sid, frame)) = frame_rx.recv().await {
            context.runtime.increment_messages_received();
            context.ingress.record_frame_received(context.session_id);
            if let Err(error) = crate::api::session::process_session_frame(
                &mut session,
                frame,
                context.ingress.as_ref(),
            )
            .await
            {
                if should_ignore_unknown_session_error(&error) {
                    tracing::debug!(
                        session_id = context.session_id,
                        error = %error,
                        "TCP frame processing encountered a stale frame for an already-closed session"
                    );
                    return Ok(());
                }
                tracing::error!(session_id = context.session_id, error = %error, "TCP frame processing error");
                let reason = close_tcp_session_on_frame_error(
                    context.ingress.as_ref(),
                    context.session_id,
                    error,
                )
                .await;
                return Err(reason);
            }

            maybe_rebind_tcp_inbox(
                context.ingress.as_ref(),
                &context.runtime,
                context.session_id,
                &context.outbound_sink,
                &context.registered_inboxes,
                &mut registered_inbox,
            );
        }
        Ok(())
    })
}

fn maybe_rebind_tcp_inbox(
    ingress: &dyn Ingress,
    runtime: &crate::boot::Runtime,
    session_id: u64,
    outbound_sink: &Arc<crate::api::outbound::SessionOutboundSink>,
    registered_inboxes: &Arc<Mutex<Vec<crate::runtime::routing::RouteAddress>>>,
    registered_inbox: &mut crate::runtime::routing::RouteAddress,
) {
    if let Some(updated_route_family) = ingress.get_route_family(session_id) {
        if updated_route_family != *registered_inbox.family() {
            runtime.router.unregister(registered_inbox);
            *registered_inbox =
                crate::runtime::routing::session_inbox_address(updated_route_family, session_id);
            runtime.router.register(
                registered_inbox.clone(),
                outbound_sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
            );
            let mut inboxes = registered_inboxes.lock();
            if !inboxes.contains(registered_inbox) {
                inboxes.push(registered_inbox.clone());
            }
        }
    }
}

async fn resolve_tcp_run_result(
    frame_tx: tokio::sync::mpsc::Sender<(u64, bytes::Bytes)>,
    mut frame_handle: tokio::task::JoinHandle<Result<(), String>>,
    mut handler_task: tokio::task::JoinHandle<Result<(), String>>,
    session_id: u64,
) -> Result<(), String> {
    tokio::select! {
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
    }
}

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

    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<Bytes>(config.channel_capacity);

    let sink = std::sync::Arc::new(crate::api::outbound::SessionOutboundSink::new(
        outbound_tx.clone(),
    ));
    let initial_family = ingress
        .get_route_family(session_id)
        .unwrap_or_else(|| crate::runtime::routing::RouteFamily::new(1));
    let inbox_route = crate::runtime::routing::session_inbox_address(initial_family, session_id);
    // Tracks the pre-auth inbox plus any CONNECT-time rebind so shutdown can
    // unregister both if authentication moves the session to a new route family.
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
    let ingress_for_writes = ingress.clone();
    let mut write_half = write_half;
    let writer_handle = tokio::spawn(async move {
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task started"
        );
        let mut write_buf: Vec<u8> = Vec::with_capacity(WRITE_BUF_INIT_CAPACITY);
        while let Some(frame) = outbound_rx.recv().await {
            tracing::debug!(
                session_id = tcp_session_id,
                frame_len = frame.len(),
                "TCP outbound: sending frame to wire"
            );
            if let Err(e) = write_tcp_frame(&mut write_half, &mut write_buf, frame).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error");
                break;
            }
            runtime_for_writes.increment_messages_sent();
            ingress_for_writes.record_frame_sent(tcp_session_id);
        }
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task ended"
        );
    });

    let frame_handle = spawn_tcp_frame_task(
        frame_rx,
        TcpFrameTaskContext {
            ingress: ingress.clone(),
            channel_capacity: config.channel_capacity,
            runtime: runtime.clone(),
            outbound_sink: sink.clone(),
            registered_inboxes: registered_inboxes.clone(),
            initial_family,
            session_id,
        },
    );

    let handler_task = tokio::spawn(async move { handler.run().await });
    let run_result = resolve_tcp_run_result(frame_tx, frame_handle, handler_task, session_id).await;

    let inboxes = registered_inboxes.lock().clone();
    for inbox in &inboxes {
        runtime.router.unregister(inbox);
    }

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

#[cfg(test)]
mod tests {
    use super::{close_tcp_session_on_frame_error, should_ignore_unknown_session_error};
    use crate::api::runtime_ingress::{Ingress, IngressDecision};
    use crate::protocol::frame::ChannelId;
    use crate::session::{CloseReason, SessionError, SessionInfo};
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};

    struct RecordingIngress {
        closes: Arc<Mutex<Vec<CloseReason>>>,
    }

    #[async_trait::async_trait]
    impl Ingress for RecordingIngress {
        async fn on_open(&self, _session: SessionInfo) -> Result<u64, String> {
            Ok(1)
        }

        async fn on_frame(
            &self,
            _session_id: u64,
            _channel_id: ChannelId,
            _msg_type: crate::protocol::tlv::MessageType,
            _message_payload: Bytes,
        ) -> IngressDecision {
            IngressDecision::Accept
        }

        async fn on_close(&self, _session_id: u64, reason: CloseReason) {
            self.closes.lock().unwrap().push(reason);
        }
    }

    #[tokio::test]
    async fn should_close_tcp_session_given_backpressure_session_error() {
        // Arrange
        let closes = Arc::new(Mutex::new(Vec::new()));
        let ingress = RecordingIngress {
            closes: closes.clone(),
        };

        // Act
        let reason = close_tcp_session_on_frame_error(
            &ingress,
            7,
            SessionError::Backpressure(ChannelId::Control),
        )
        .await;

        // Assert
        assert_eq!(reason, "backpressure on channel control");
        let closes = closes.lock().unwrap();
        assert_eq!(closes.len(), 1);
        assert!(matches!(
            &closes[0],
            CloseReason::Error(message) if message.contains("Backpressure")
        ));
    }

    #[test]
    fn should_ignore_unknown_session_ingress_close_errors() {
        // Arrange
        let error = SessionError::IngressClose("unknown session: 42".to_string());

        // Act
        let result = should_ignore_unknown_session_error(&error);

        // Assert
        assert!(result);
    }
}
