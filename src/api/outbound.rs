use crate::observability as obs;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::DeliveryError;
use crate::runtime::router::MailboxSink;
use crate::runtime::EncodedClientFrame;

use bytes::{BufMut, Bytes, BytesMut};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

/// MailboxSink that forwards domain responses to a session's outbound channel
pub struct SessionOutboundSink {
    tx: mpsc::Sender<Bytes>,
}

impl SessionOutboundSink {
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self { tx }
    }
}

impl MailboxSink for SessionOutboundSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let _deliver_latency =
            crate::observability::ScopedHistogramUs::new(obs::METRIC_OUTBOUND_DELIVER_LATENCY);
        if let Some(ctx) = envelope.payload::<FrameContext>() {
            debug!(
                session_id = ctx.session_id,
                msg_type = ctx.msg_type.as_u16(),
                payload_len = ctx.payload.len(),
                "Outbound sink: encoding TLV response for session"
            );
            // TLV-encode a single record directly into an exact-sized buffer.
            let encode_start = Instant::now();
            let bytes = encode_single_tlv_frame(ctx.msg_type, &ctx.payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(ctx.session_id, bytes);
        }

        if let Some(frame) = envelope.payload::<EncodedClientFrame>() {
            debug!(
                session_id = frame.meta.session_id,
                msg_type = frame.meta.message_type,
                payload_len = frame.payload.len(),
                channel = ?frame.meta.channel,
                "Outbound sink: encoding runtime client frame"
            );
            let encode_start = Instant::now();
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(frame.meta.message_type),
                &frame.payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(frame.meta.session_id, bytes);
        }

        if let Some(response) = envelope.payload::<crate::domains::rpc::RpcClientResponse>() {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding RPC response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::rpc_codec::encode_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(delivery) = envelope.payload::<crate::domains::rpc::RpcWorkerRequestDelivery>()
        {
            debug!(
                session_id = delivery.session_id,
                route = %delivery.request.route,
                "Outbound sink: encoding RPC worker request delivery"
            );
            let encode_start = Instant::now();
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let payload = crate::protocol::rpc_codec::encode_request_into(
                &delivery.request,
                &mut payload_encoder,
            );
            let bytes =
                encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(302), &payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(delivery.session_id, bytes);
        }

        if let Some(forwarded) =
            envelope.payload::<crate::domains::rpc::RpcClientForwardedResponse>()
        {
            debug!(
                session_id = forwarded.session_id,
                "Outbound sink: encoding forwarded RPC response"
            );
            let encode_start = Instant::now();
            let payload = match &forwarded.body {
                crate::domains::rpc::RpcClientForwardedResponseBody::Response(response) => {
                    crate::protocol::rpc_codec::encode_response_message(response)
                }
                crate::domains::rpc::RpcClientForwardedResponseBody::TerminalError {
                    correlation_id,
                    code,
                    message,
                } => {
                    let error_body = crate::protocol::rpc_codec::encode_error_body(*code, message);
                    let response = crate::domains::rpc::RpcResponse::single(
                        *correlation_id,
                        bytes::Bytes::from(error_body),
                    );
                    crate::protocol::rpc_codec::encode_response_message(&response)
                }
            };
            let bytes =
                encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(303), &payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(forwarded.session_id, bytes);
        }

        if let Some(ack) = envelope.payload::<crate::domains::rpc::RpcWorkerAck>() {
            debug!(
                session_id = ack.session_id,
                correlation_id = %ack.correlation_id,
                "Outbound sink: encoding RPC worker ACK"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::rpc_codec::encode_ack(&ack.correlation_id);
            let bytes =
                encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(304), &payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(ack.session_id, bytes);
        }

        if let Some(response) = envelope.payload::<crate::domains::kv::KvClientResponse>() {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding KV response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::kv::encode_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(notification) = envelope.payload::<crate::domains::kv::KvClientNotification>() {
            debug!(
                session_id = notification.session_id,
                route = %notification.route,
                "Outbound sink: encoding KV notification"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::kv::encode_notify(
                notification.subscription_id,
                &notification.route,
                notification.notification,
            );
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::NOTIFY),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(notification.session_id, bytes);
        }

        if let Some(response) = envelope.payload::<crate::domains::lease::LeaseClientResponse>() {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding Lease response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::lease_codec::encode_domain_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::lease::LeaseClientNotification>()
        {
            debug!(
                session_id = notification.session_id,
                route = %notification.route,
                "Outbound sink: encoding Lease notification"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::lease_codec::encode_notify(
                notification.subscription_id,
                notification.route.as_str(),
                &notification.payload,
            );
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(
                    crate::protocol::lease_codec::msg_type::NOTIFY,
                ),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(notification.session_id, bytes);
        }

        if let Some(response) = envelope.payload::<crate::domains::notice::NoticeClientResponse>() {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding Notice response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::notice_codec::encode_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::notice::NoticeClientNotification>()
        {
            debug!(
                session_id = notification.session_id,
                route = %notification.route,
                "Outbound sink: encoding Notice notification"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::notice_codec::encode_notify(
                notification.subscription_id,
                &notification.route,
                &notification.payload,
            );
            let bytes =
                encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(504), &payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(notification.session_id, bytes);
        }

        if let Some(response) =
            envelope.payload::<crate::domains::schedule::ScheduleClientResponse>()
        {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding Schedule response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::schedule_codec::encode_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::schedule::ScheduleClientNotification>()
        {
            debug!(
                session_id = notification.session_id,
                "Outbound sink: encoding Schedule notification"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::schedule_codec::encode_notify(
                notification.subscription_id,
                &notification.payload,
            );
            let bytes =
                encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(705), &payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(notification.session_id, bytes);
        }

        if let Some(response) = envelope.payload::<crate::domains::stream::StreamClientResponse>() {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding Stream response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::stream_codec::encode_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::stream::StreamClientNotification>()
        {
            debug!(
                session_id = notification.session_id,
                route = %notification.route,
                "Outbound sink: encoding Stream notification"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::stream_codec::encode_notify(
                notification.subscription_id,
                &notification.route,
                &notification.payload,
            );
            let bytes =
                encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(609), &payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(notification.session_id, bytes);
        }

        if let Some(response) = envelope.payload::<crate::domains::queue::QueueClientResponse>() {
            debug!(
                session_id = response.meta.session_id,
                msg_type = response.meta.message_type,
                channel = ?response.meta.channel,
                "Outbound sink: encoding Queue response"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::queue_codec::encode_response(&response.response);
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(response.meta.message_type),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(response.meta.session_id, bytes);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::queue::QueueClientNotification>()
        {
            debug!(
                session_id = notification.session_id,
                route = %notification.route,
                "Outbound sink: encoding Queue notification"
            );
            let encode_start = Instant::now();
            let payload = crate::protocol::queue_codec::encode_notify(
                notification.subscription_id,
                &notification.route,
                notification.notification,
            );
            let bytes = encode_single_tlv_frame(
                crate::protocol::tlv::MessageType::new(
                    crate::protocol::queue_codec::msg_type::NOTIFY,
                ),
                &payload,
            );
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            return self.send_encoded_frame(notification.session_id, bytes);
        }

        warn!(
            destination = ?envelope.destination(),
            "Outbound sink: envelope payload cannot be encoded for transport"
        );
        Err(DeliveryError::ActorStopped)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl SessionOutboundSink {
    fn send_encoded_frame(&self, session_id: u64, bytes: Bytes) -> Result<(), DeliveryError> {
        const MAX_OUTBOUND_SEND_RETRIES: usize = 100;

        trace!(
            session_id = session_id,
            encoded_len = bytes.len(),
            "Outbound sink: sending TLV frame to transport channel"
        );

        let mut attempt = 0;
        loop {
            let send_start = Instant::now();
            let send_result = self.tx.try_send(bytes.clone());
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_SEND_LATENCY,
                send_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            match send_result {
                Ok(()) => {
                    crate::observability::hot_path_counter_inc(obs::METRIC_FRAMES_SENT);
                    debug!(
                        session_id = session_id,
                        "Outbound sink: frame sent to transport successfully"
                    );
                    return Ok(());
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    let capacity = self.tx.max_capacity();
                    crate::observability::hot_path_counter_inc(obs::METRIC_OUTBOUND_BACKPRESSURE);
                    attempt += 1;
                    if attempt >= MAX_OUTBOUND_SEND_RETRIES {
                        warn!(
                            session_id = session_id,
                            capacity = capacity,
                            "Outbound sink: transport channel full"
                        );
                        return Err(DeliveryError::MailboxFull {
                            capacity,
                            current_len: capacity,
                        });
                    }
                    std::thread::yield_now();
                    continue;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    warn!(
                        session_id = session_id,
                        "Outbound sink: transport channel closed"
                    );
                    return Err(DeliveryError::ActorStopped);
                }
            }
        }
    }
}

fn encode_single_tlv_frame(msg_type: crate::protocol::tlv::MessageType, payload: &[u8]) -> Bytes {
    if payload.len() > u16::MAX as usize {
        panic!(
            "TLV value too large: {} bytes (max {})",
            payload.len(),
            u16::MAX
        );
    }

    let header_len = msg_type.encoded_type_len() + 2;
    let mut out = BytesMut::with_capacity(header_len + payload.len());

    if msg_type.is_single_byte() {
        out.put_u8(msg_type.as_u16() as u8);
    } else {
        out.put_u8(crate::protocol::tlv::MessageType::ESCAPE_MARKER);
        out.extend_from_slice(&msg_type.as_u16().to_be_bytes());
    }

    let payload_len = payload.len() as u16;
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(payload);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

    fn test_envelope(payload: Bytes) -> Envelope {
        Envelope::new(
            RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/1")),
            FrameContext::new(
                1,
                ChannelId::Control,
                MessageType::new(101),
                payload,
                RouteFamily::new(1),
            ),
        )
    }

    #[tokio::test]
    async fn should_deliver_frame_without_blocking_current_thread_runtime() {
        // Arrange
        let (tx, mut rx) = mpsc::channel(1);
        let sink = SessionOutboundSink::new(tx);

        // Act
        let result = sink.deliver(test_envelope(Bytes::from_static(b"ok")));
        let frame = rx.recv().await.expect("encoded frame");

        // Assert
        assert_eq!(result, Ok(()));
        assert_eq!(frame.as_ref(), &[101, 0, 2, b'o', b'k']);
    }

    #[tokio::test]
    async fn should_return_backpressure_given_full_outbound_channel() {
        // Arrange
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"occupied")).unwrap();
        let sink = SessionOutboundSink::new(tx);

        // Act
        let result = sink.deliver(test_envelope(Bytes::from_static(b"ok")));
        let occupied = rx.recv().await.expect("occupied frame");

        // Assert
        assert_eq!(
            result,
            Err(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            })
        );
        assert_eq!(occupied, Bytes::from_static(b"occupied"));
    }

    #[tokio::test]
    async fn should_retry_outbound_send_when_channel_is_briefly_full() {
        // Arrange
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"occupied")).unwrap();

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("create runtime");
            let occupied = rt.block_on(async { rx.recv().await.expect("drained occupied frame") });
            ready_tx.send(()).expect("signal ready");
            release_rx.recv().expect("wait for release");
            occupied
        });

        let sink = SessionOutboundSink::new(tx);
        ready_rx
            .recv()
            .expect("wait until receiver drained occupied frame");

        // Act
        let result = sink.deliver(test_envelope(Bytes::from_static(b"ok")));
        release_tx.send(()).expect("release receiver thread");

        // Assert
        assert_eq!(result, Ok(()));
        let occupied = handle.join().expect("thread joined");
        assert_eq!(occupied, Bytes::from_static(b"occupied"));
    }
}
