use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::Ingress;
use bytes::Bytes;
use parking_lot::Mutex;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

// Initial capacity for the per-connection write buffer (length prefix + typical frame).
// Grows automatically for larger frames; the allocation is reused across all frames
// on the same connection so there is at most one heap allocation per connection.
const WRITE_BUF_INIT_CAPACITY: usize = 512;
const MAX_WRITE_BATCH_BYTES: usize = 64 * 1024;
const MAX_WRITE_BATCH_FRAMES: usize = 64;

#[cfg(test)]
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

fn append_tcp_frame(write_buf: &mut Vec<u8>, frame: &Bytes) -> Result<(), std::io::Error> {
    write_buf
        .extend_from_slice(&frame_length_prefix(frame.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large")
        })?);
    write_buf.extend_from_slice(frame);
    Ok(())
}

async fn write_tcp_frames<W: AsyncWrite + Unpin>(
    write_half: &mut W,
    write_buf: &mut Vec<u8>,
    first_frame: Bytes,
    outbound_rx: &mut tokio::sync::mpsc::Receiver<Bytes>,
) -> Result<usize, std::io::Error> {
    write_buf.clear();
    append_tcp_frame(write_buf, &first_frame)?;
    let mut frame_count = 1usize;
    while frame_count < MAX_WRITE_BATCH_FRAMES && write_buf.len() < MAX_WRITE_BATCH_BYTES {
        let Ok(frame) = outbound_rx.try_recv() else {
            break;
        };
        append_tcp_frame(write_buf, &frame)?;
        frame_count += 1;
    }
    write_half.write_all(write_buf).await?;
    write_half.flush().await?;
    Ok(frame_count)
}

fn spawn_tcp_writer_task(
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut outbound_rx: tokio::sync::mpsc::Receiver<Bytes>,
    session_id: u64,
    runtime: Arc<crate::boot::Runtime>,
    ingress: Arc<dyn Ingress>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        tracing::debug!(session_id, "TCP outbound writer task started");
        let mut write_buf = Vec::with_capacity(WRITE_BUF_INIT_CAPACITY);
        while let Some(frame) = outbound_rx.recv().await {
            tracing::debug!(
                session_id,
                frame_len = frame.len(),
                "TCP outbound: sending frame to wire"
            );
            let frame_count =
                write_tcp_frames(&mut write_half, &mut write_buf, frame, &mut outbound_rx)
                    .await
                    .map_err(|error| {
                        tracing::error!(session_id, %error, "TCP outbound write error");
                        format!("TCP outbound write error: {error}")
                    })?;
            for _ in 0..frame_count {
                runtime.increment_messages_sent();
                ingress.record_frame_sent(session_id);
            }
        }
        tracing::debug!(session_id, "TCP outbound writer task ended");
        Ok(())
    })
}

struct TcpFrameTaskContext {
    ingress: Arc<dyn Ingress>,
    channel_capacity: usize,
    runtime: Arc<crate::boot::Runtime>,
    outbound_sink: Arc<crate::api::outbound::SessionOutboundSink>,
    registered_inboxes: Arc<Mutex<Vec<crate::runtime::routing::RouteAddress>>>,
    finalizer: Arc<super::SessionCloseFinalizer>,
    initial_family: crate::runtime::routing::RouteFamily,
    session_id: u64,
    connect_deadline: tokio::time::Instant,
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
            context.initial_family,
        );

        let mut session = crate::session::Session::new(context.session_id, session_config);
        let mut frame_rx = frame_rx;
        let mut registered_inbox = crate::runtime::routing::session_inbox_address(
            context.initial_family,
            context.session_id,
        );

        let mut first_frame = true;
        loop {
            let next_frame = if first_frame {
                let remaining = context
                    .connect_deadline
                    .saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err("CONNECT deadline exceeded".to_string());
                }
                match tokio::time::timeout(remaining, frame_rx.recv()).await {
                    Ok(frame) => frame,
                    Err(_) => {
                        return Err("CONNECT deadline exceeded".to_string());
                    }
                }
            } else {
                frame_rx.recv().await
            };

            let Some((_sid, frame)) = next_frame else {
                break;
            };
            first_frame = false;
            context.runtime.increment_messages_received();
            context.ingress.record_frame_received(context.session_id);
            if let Err(error) = crate::api::session::process_session_frame(
                &mut session,
                frame,
                context.ingress.as_ref(),
            )
            .await
            {
                if crate::api::session::is_stale_session_frame_error(&error) {
                    tracing::debug!(
                        session_id = context.session_id,
                        error = %error,
                        "TCP frame processing encountered a stale frame for an already-closed session"
                    );
                    return Ok(());
                }
                tracing::error!(session_id = context.session_id, error = %error, "TCP frame processing error");
                return Err(error.to_string());
            }

            maybe_rebind_tcp_inbox(
                context.ingress.as_ref(),
                &context.runtime,
                context.session_id,
                &context.outbound_sink,
                &context.registered_inboxes,
                &mut registered_inbox,
                &context.finalizer,
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
    finalizer: &super::SessionCloseFinalizer,
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
            finalizer.add_route(registered_inbox.clone());
        }
    }
}

async fn resolve_tcp_run_result(
    frame_tx: tokio::sync::mpsc::Sender<(u64, bytes::Bytes)>,
    mut frame_handle: tokio::task::JoinHandle<Result<(), String>>,
    mut handler_task: tokio::task::JoinHandle<Result<(), String>>,
    mut writer_handle: tokio::task::JoinHandle<Result<(), String>>,
    session_id: u64,
    runtime: Arc<crate::boot::Runtime>,
) -> Result<(), String> {
    tokio::select! {
        res = &mut handler_task => {
            drop(frame_tx);
            let ((), ()) = tokio::join!(
                join_tcp_frame_task_or_abort(&mut frame_handle, session_id),
                abort_and_join_tcp_task(writer_handle, session_id, "writer"),
            );
            match res {
                Ok(result) => result,
                Err(e) => Err(format!("tcp handler task join error: {e}")),
            }
        }
        res = &mut frame_handle => {
            drop(frame_tx);
            tokio::join!(
                abort_and_join_tcp_task(handler_task, session_id, "handler"),
                abort_and_join_tcp_task(writer_handle, session_id, "writer"),
            );
            match res {
                Ok(Ok(())) => Ok(()),
                Ok(Err(reason)) => {
                    tracing::debug!(
                        session_id = session_id,
                        reason = %reason,
                        "TCP frame task requested transport close"
                    );
                    Err(reason)
                }
                Err(e) => {
                    Err(format!("tcp frame task join error: {e}"))
                }
            }
        }
        res = &mut writer_handle => {
            drop(frame_tx);
            tokio::join!(
                abort_and_join_tcp_task(handler_task, session_id, "handler"),
                abort_and_join_tcp_task(frame_handle, session_id, "frame"),
            );
            match res {
                Ok(Ok(())) => Err("TCP writer stopped".to_string()),
                Ok(Err(reason)) => Err(reason),
                Err(error) => Err(format!("tcp writer task join error: {error}")),
            }
        }
        () = async {
            while !runtime.is_shutting_down() {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        } => {
            drop(frame_tx);
            tokio::join!(
                abort_and_join_tcp_task(handler_task, session_id, "handler"),
                abort_and_join_tcp_task(frame_handle, session_id, "frame"),
                abort_and_join_tcp_task(writer_handle, session_id, "writer"),
            );
            Err("server shutdown".to_string())
        }
    }
}

async fn abort_and_join_tcp_task(
    handle: tokio::task::JoinHandle<Result<(), String>>,
    session_id: u64,
    task: &'static str,
) {
    handle.abort();
    if let Err(error) = handle.await {
        if !error.is_cancelled() {
            tracing::warn!(session_id, task, %error, "TCP child task failed while being joined");
        }
    }
}

async fn join_tcp_frame_task_or_abort(
    handle: &mut tokio::task::JoinHandle<Result<(), String>>,
    session_id: u64,
) {
    if let Err(error) = tokio::time::timeout(std::time::Duration::from_secs(1), &mut *handle).await
    {
        tracing::warn!(session_id, %error, "TCP frame task did not terminate in time");
        handle.abort();
        let _ = handle.await;
    }
}

/// Handle an incoming TCP connection with outbound response support.
pub(super) async fn handle_tcp_connection(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    lifecycle: super::ConnectionLifecycle,
    accepted_at: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::api::tcp::create_session;

    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let (handler, write_half) =
        create_session(ingress.clone(), config.clone(), stream, frame_tx.clone()).await?;
    let session_id = handler.session_id;

    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel::<Bytes>(config.channel_capacity);

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
    let finalizer = Arc::new(super::SessionCloseFinalizer::new(
        ingress.clone(),
        runtime.router.clone(),
        session_id,
        inbox_route.clone(),
    ));

    let writer_handle = spawn_tcp_writer_task(
        write_half,
        outbound_rx,
        session_id,
        runtime.clone(),
        ingress.clone(),
    );

    let frame_handle = spawn_tcp_frame_task(
        frame_rx,
        TcpFrameTaskContext {
            ingress: ingress.clone(),
            channel_capacity: config.channel_capacity,
            runtime: runtime.clone(),
            outbound_sink: sink.clone(),
            registered_inboxes: registered_inboxes.clone(),
            finalizer: finalizer.clone(),
            initial_family,
            session_id,
            connect_deadline: accepted_at + crate::api::ingress::CONNECT_DEADLINE,
        },
    );

    let handler_task = tokio::spawn(async move { handler.run().await });
    let run_result = resolve_tcp_run_result(
        frame_tx,
        frame_handle,
        handler_task,
        writer_handle,
        session_id,
        runtime.clone(),
    )
    .await;

    let close_reason = match &run_result {
        Ok(()) => crate::session::CloseReason::ClientClose,
        Err(reason) => crate::session::CloseReason::Error(reason.clone()),
    };
    finalizer.finish(close_reason).await;

    drop(sink);
    drop(outbound_tx);

    lifecycle.finish();

    run_result
        .map_err(std::io::Error::other)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[cfg(test)]
mod tests {
    use super::{close_tcp_session_on_frame_error, resolve_tcp_run_result, write_tcp_frames};
    use crate::api::runtime_ingress::{Ingress, IngressDecision};
    use crate::protocol::frame::ChannelId;
    use crate::session::{CloseReason, SessionError, SessionInfo};
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncReadExt;

    struct DropSignal(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

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
        let result = crate::api::session::is_stale_session_frame_error(&error);

        // Assert
        assert!(result);
    }

    #[tokio::test]
    async fn should_batch_queued_tcp_frames_in_wire_order() {
        // Arrange
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        tx.try_send(Bytes::from_static(b"bc"))
            .expect("queue second frame");
        tx.try_send(Bytes::from_static(b"def"))
            .expect("queue third frame");
        let mut write_buf = Vec::new();

        // Act
        let frame_count = write_tcp_frames(
            &mut writer,
            &mut write_buf,
            Bytes::from_static(b"a"),
            &mut rx,
        )
        .await
        .expect("write TCP frame batch");
        let mut encoded = vec![0; write_buf.len()];
        reader
            .read_exact(&mut encoded)
            .await
            .expect("read TCP frame batch");

        // Assert
        assert_eq!(frame_count, 3);
        assert_eq!(encoded, b"\0\0\0\x01a\0\0\0\x02bc\0\0\0\x03def");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn should_join_aborted_tcp_child_tasks_before_returning() {
        // Arrange
        let handler_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (handler_started_tx, handler_started_rx) = tokio::sync::oneshot::channel();
        let (writer_started_tx, writer_started_rx) = tokio::sync::oneshot::channel();
        let handler_signal = DropSignal(handler_dropped.clone());
        let writer_signal = DropSignal(writer_dropped.clone());
        let handler_task = tokio::spawn(async move {
            let _signal = handler_signal;
            let _ = handler_started_tx.send(());
            std::future::pending::<Result<(), String>>().await
        });
        let writer_handle = tokio::spawn(async move {
            let _signal = writer_signal;
            let _ = writer_started_tx.send(());
            std::future::pending::<Result<(), String>>().await
        });
        handler_started_rx.await.expect("handler task started");
        writer_started_rx.await.expect("writer task started");
        let frame_handle = tokio::spawn(async { Ok(()) });
        let (frame_tx, _frame_rx) = tokio::sync::mpsc::channel(1);
        let runtime = Arc::new(crate::boot::Runtime::new(Arc::new(
            crate::runtime::Router::new(),
        )));

        // Act
        let result = resolve_tcp_run_result(
            frame_tx,
            frame_handle,
            handler_task,
            writer_handle,
            7,
            runtime,
        )
        .await;

        // Assert
        assert_eq!(result, Ok(()));
        assert!(handler_dropped.load(std::sync::atomic::Ordering::Acquire));
        assert!(writer_dropped.load(std::sync::atomic::Ordering::Acquire));
    }
}
