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

/// `MailboxSink` that forwards domain responses to a session's outbound channel
pub struct SessionOutboundSink {
    tx: mpsc::Sender<Bytes>,
}

impl SessionOutboundSink {
    #[must_use]
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self { tx }
    }
}

impl MailboxSink for SessionOutboundSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let _deliver_latency =
            crate::observability::ScopedHistogramUs::new(obs::METRIC_OUTBOUND_DELIVER_LATENCY);
        if let Some(ctx) = envelope.payload::<FrameContext>() {
            return self.deliver_frame_context(ctx);
        }

        if let Some(frame) = envelope.payload::<EncodedClientFrame>() {
            return self.deliver_encoded_client_frame(frame);
        }

        if let Some(response) = envelope.payload::<crate::domains::rpc::RpcClientResponse>() {
            return self.deliver_rpc_client_response(response);
        }

        if let Some(delivery) = envelope.payload::<crate::domains::rpc::RpcWorkerRequestDelivery>()
        {
            return self.deliver_rpc_worker_request(delivery);
        }

        if let Some(forwarded) =
            envelope.payload::<crate::domains::rpc::RpcClientForwardedResponse>()
        {
            return self.deliver_rpc_forwarded_response(forwarded);
        }

        if let Some(ack) = envelope.payload::<crate::domains::rpc::RpcWorkerAck>() {
            return self.deliver_rpc_worker_ack(ack);
        }

        if let Some(response) = envelope.payload::<crate::domains::kv::KvClientResponse>() {
            return self.deliver_kv_client_response(response);
        }

        if let Some(notification) = envelope.payload::<crate::domains::kv::KvClientNotification>() {
            return self.deliver_kv_notification(notification);
        }

        if let Some(response) = envelope.payload::<crate::domains::lease::LeaseClientResponse>() {
            return self.deliver_lease_client_response(response);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::lease::LeaseClientNotification>()
        {
            return self.deliver_lease_notification(notification);
        }

        if let Some(response) = envelope.payload::<crate::domains::notice::NoticeClientResponse>() {
            return self.deliver_notice_client_response(response);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::notice::NoticeClientNotification>()
        {
            return self.deliver_notice_notification(notification);
        }

        if let Some(response) =
            envelope.payload::<crate::domains::schedule::ScheduleClientResponse>()
        {
            return self.deliver_schedule_client_response(response);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::schedule::ScheduleClientNotification>()
        {
            return self.deliver_schedule_notification(notification);
        }

        if let Some(response) = envelope.payload::<crate::domains::stream::StreamClientResponse>() {
            return self.deliver_stream_client_response(response);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::stream::StreamClientNotification>()
        {
            return self.deliver_stream_notification(notification);
        }

        if let Some(response) = envelope.payload::<crate::domains::queue::QueueClientResponse>() {
            return self.deliver_queue_client_response(response);
        }

        if let Some(notification) =
            envelope.payload::<crate::domains::queue::QueueClientNotification>()
        {
            return self.deliver_queue_notification(notification);
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
    fn elapsed_micros_u64(start: Instant) -> u64 {
        u64::try_from(start.elapsed().as_micros().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
    }

    fn observe_encode_latency(start: Instant) {
        crate::observability::hot_path_histogram_observe_us(
            obs::METRIC_OUTBOUND_ENCODE_LATENCY,
            Self::elapsed_micros_u64(start),
        );
    }

    fn deliver_frame_context(&self, ctx: &FrameContext) -> Result<(), DeliveryError> {
        debug!(
            session_id = ctx.session_id,
            msg_type = ctx.msg_type.as_u16(),
            payload_len = ctx.payload.len(),
            "Outbound sink: encoding TLV response for session"
        );
        let encode_start = Instant::now();
        let bytes = encode_single_tlv_frame(ctx.msg_type, &ctx.payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(ctx.session_id, &bytes)
    }

    fn deliver_encoded_client_frame(
        &self,
        frame: &EncodedClientFrame,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(frame.meta.session_id, &bytes)
    }

    fn deliver_rpc_client_response(
        &self,
        response: &crate::domains::rpc::RpcClientResponse,
    ) -> Result<(), DeliveryError> {
        debug!(
            session_id = response.meta.session_id,
            msg_type = response.meta.message_type,
            channel = ?response.meta.channel,
            "Outbound sink: encoding RPC response"
        );
        let encode_start = Instant::now();
        let mut payload_encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::response_body_capacity(&response.response),
        );
        let payload = crate::protocol::rpc_codec::encode_response_into(
            &response.response,
            &mut payload_encoder,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        );
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_rpc_worker_request(
        &self,
        delivery: &crate::domains::rpc::RpcWorkerRequestDelivery,
    ) -> Result<(), DeliveryError> {
        debug!(
            session_id = delivery.session_id,
            route = %delivery.request.route,
            "Outbound sink: encoding RPC worker request delivery"
        );
        let encode_start = Instant::now();
        let mut payload_encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::request_payload_capacity(&delivery.request),
        );
        let payload = crate::protocol::rpc_codec::encode_request_into(
            &delivery.request,
            &mut payload_encoder,
        );
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(302), &payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(delivery.session_id, &bytes)
    }

    fn deliver_rpc_forwarded_response(
        &self,
        forwarded: &crate::domains::rpc::RpcClientForwardedResponse,
    ) -> Result<(), DeliveryError> {
        debug!(
            session_id = forwarded.session_id,
            "Outbound sink: encoding forwarded RPC response"
        );
        let encode_start = Instant::now();
        let payload = match &forwarded.body {
            crate::domains::rpc::RpcClientForwardedResponseBody::Response(response) => {
                let mut response_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                        crate::protocol::rpc_codec::response_message_capacity(response),
                    );
                crate::protocol::rpc_codec::encode_response_message_into(
                    response,
                    &mut response_encoder,
                )
            }
            crate::domains::rpc::RpcClientForwardedResponseBody::TerminalError {
                correlation_id,
                code,
                message,
            } => {
                let mut error_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                        crate::protocol::rpc_codec::error_body_capacity(message),
                    );
                let mut response_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                        crate::protocol::rpc_codec::terminal_error_response_message_capacity(
                            message,
                        ),
                    );
                crate::protocol::rpc_codec::encode_terminal_error_response_message_into(
                    correlation_id,
                    *code,
                    message,
                    &mut response_encoder,
                    &mut error_encoder,
                )
            }
        };
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(303), &payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(forwarded.session_id, &bytes)
    }

    fn deliver_rpc_worker_ack(
        &self,
        ack: &crate::domains::rpc::RpcWorkerAck,
    ) -> Result<(), DeliveryError> {
        debug!(
            session_id = ack.session_id,
            correlation_id = %ack.correlation_id,
            "Outbound sink: encoding RPC worker ACK"
        );
        let encode_start = Instant::now();
        let mut payload_encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::ack_payload_capacity(),
        );
        let payload =
            crate::protocol::rpc_codec::encode_ack_into(&ack.correlation_id, &mut payload_encoder);
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(304), &payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(ack.session_id, &bytes)
    }

    fn deliver_kv_client_response(
        &self,
        response: &crate::domains::kv::KvClientResponse,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_kv_notification(
        &self,
        notification: &crate::domains::kv::KvClientNotification,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(notification.session_id, &bytes)
    }

    fn deliver_lease_client_response(
        &self,
        response: &crate::domains::lease::LeaseClientResponse,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_lease_notification(
        &self,
        notification: &crate::domains::lease::LeaseClientNotification,
    ) -> Result<(), DeliveryError> {
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
            crate::protocol::tlv::MessageType::new(crate::protocol::lease_codec::msg_type::NOTIFY),
            &payload,
        );
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(notification.session_id, &bytes)
    }

    fn deliver_notice_client_response(
        &self,
        response: &crate::domains::notice::NoticeClientResponse,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_notice_notification(
        &self,
        notification: &crate::domains::notice::NoticeClientNotification,
    ) -> Result<(), DeliveryError> {
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
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(504), &payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(notification.session_id, &bytes)
    }

    fn deliver_schedule_client_response(
        &self,
        response: &crate::domains::schedule::ScheduleClientResponse,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_schedule_notification(
        &self,
        notification: &crate::domains::schedule::ScheduleClientNotification,
    ) -> Result<(), DeliveryError> {
        debug!(
            session_id = notification.session_id,
            "Outbound sink: encoding Schedule notification"
        );
        let encode_start = Instant::now();
        let payload = crate::protocol::schedule_codec::encode_notify(
            notification.subscription_id,
            &notification.payload,
        );
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(705), &payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(notification.session_id, &bytes)
    }

    fn deliver_stream_client_response(
        &self,
        response: &crate::domains::stream::StreamClientResponse,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_stream_notification(
        &self,
        notification: &crate::domains::stream::StreamClientNotification,
    ) -> Result<(), DeliveryError> {
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
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(609), &payload);
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(notification.session_id, &bytes)
    }

    fn deliver_queue_client_response(
        &self,
        response: &crate::domains::queue::QueueClientResponse,
    ) -> Result<(), DeliveryError> {
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
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(response.meta.session_id, &bytes)
    }

    fn deliver_queue_notification(
        &self,
        notification: &crate::domains::queue::QueueClientNotification,
    ) -> Result<(), DeliveryError> {
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
            crate::protocol::tlv::MessageType::new(crate::protocol::queue_codec::msg_type::NOTIFY),
            &payload,
        );
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(notification.session_id, &bytes)
    }

    fn send_encoded_frame(&self, session_id: u64, bytes: &Bytes) -> Result<(), DeliveryError> {
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
                Self::elapsed_micros_u64(send_start),
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
    assert!(
        u16::try_from(payload.len()).is_ok(),
        "TLV value too large: {} bytes (max {})",
        payload.len(),
        u16::MAX
    );

    let header_len = msg_type.encoded_type_len() + 2;
    let mut out = BytesMut::with_capacity(header_len + payload.len());

    if msg_type.is_single_byte() {
        out.put_u8(u8::try_from(msg_type.as_u16()).unwrap_or(u8::MAX));
    } else {
        out.put_u8(crate::protocol::tlv::MessageType::ESCAPE_MARKER);
        out.extend_from_slice(&msg_type.as_u16().to_be_bytes());
    }

    let payload_len = u16::try_from(payload.len()).unwrap_or(u16::MAX);
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
