use crate::observability as obs;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::DeliveryError;
use crate::runtime::router::MailboxSink;
use crate::runtime::EncodedClientFrame;

use bytes::{BufMut, Bytes, BytesMut};
use std::time::{Duration, Instant};
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
        Err(DeliveryError::UnsupportedPayload)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl SessionOutboundSink {
    fn elapsed_micros_u64(start: Instant) -> u64 {
        u64::try_from(start.elapsed().as_micros().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
    }

    fn encode_latency_start() -> Option<Instant> {
        obs::hot_path_metrics_enabled().then(Instant::now)
    }

    fn observe_encode_latency(start: Option<Instant>) {
        if let Some(start) = start {
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                Self::elapsed_micros_u64(start),
            );
        }
    }

    fn deliver_frame_context(&self, ctx: &FrameContext) -> Result<(), DeliveryError> {
        debug!(
            session_id = ctx.session_id,
            msg_type = ctx.msg_type.as_u16(),
            payload_len = ctx.payload.len(),
            "Outbound sink: encoding TLV response for session"
        );
        let encode_start = Self::encode_latency_start();
        let bytes = encode_single_tlv_frame(ctx.msg_type, &ctx.payload)?;
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
        let encode_start = Self::encode_latency_start();
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(frame.meta.message_type),
            &frame.payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let bytes = crate::protocol::rpc_codec::encode_client_response_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &response.response,
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
        let encode_start = Self::encode_latency_start();
        let bytes = crate::protocol::rpc_codec::encode_worker_request_tlv_frame(&delivery.request);
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
        let encode_start = Self::encode_latency_start();
        let bytes = match &forwarded.body {
            crate::domains::rpc::RpcClientForwardedResponseBody::Response(response) => {
                crate::protocol::rpc_codec::encode_response_message_tlv_frame(response)
            }
            crate::domains::rpc::RpcClientForwardedResponseBody::TerminalError {
                correlation_id,
                code,
                message,
            } => crate::protocol::rpc_codec::encode_terminal_error_response_message_tlv_frame(
                correlation_id,
                *code,
                message,
            ),
        };
        Self::observe_encode_latency(encode_start);
        self.send_encoded_frame(forwarded.session_id, &bytes)
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::kv::encode_response(&response.response);
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::kv::encode_notify(
            notification.subscription_id,
            &notification.route,
            notification.notification,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(crate::protocol::kv::msg_type::NOTIFY),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::lease_codec::encode_domain_response(&response.response);
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::lease_codec::encode_notify(
            notification.subscription_id,
            notification.route.as_str(),
            &notification.payload,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(crate::protocol::lease_codec::msg_type::NOTIFY),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::notice_codec::encode_response(&response.response);
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::notice_codec::encode_notify(
            notification.subscription_id,
            &notification.route,
            &notification.payload,
        );
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(504), &payload)?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::schedule_codec::encode_response(
            response.meta.message_type,
            &response.response,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::schedule_codec::encode_notify(
            notification.subscription_id,
            &notification.route,
            &notification.payload,
        );
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(705), &payload)?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::stream_codec::encode_response(
            response.meta.message_type,
            &response.response,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::stream_codec::encode_notify(
            notification.subscription_id,
            &notification.route,
            &notification.payload,
        );
        let bytes = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(609), &payload)?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::queue_codec::encode_response(
            response.meta.message_type,
            &response.response,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(response.meta.message_type),
            &payload,
        )?;
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
        let encode_start = Self::encode_latency_start();
        let payload = crate::protocol::queue_codec::encode_notify(
            notification.subscription_id,
            &notification.route,
            notification.notification,
        );
        let bytes = encode_single_tlv_frame(
            crate::protocol::tlv::MessageType::new(crate::protocol::queue_codec::msg_type::NOTIFY),
            &payload,
        )?;
        Self::observe_encode_latency(encode_start);
        // Best-effort: this is delivered synchronously from the Queue domain
        // actor thread, serially per watcher, BEFORE that actor replies to the
        // client whose write just committed. The default budget can block up
        // to ~177ms per saturated consumer; a handful of saturated watchers
        // would alone exceed the actor's reply deadline for a request that
        // already succeeded. A missed ready-notification is not data loss -
        // the watcher's own next poll or RESERVE observes current state - so
        // this gives up in microseconds rather than blocking the actor.
        self.send_encoded_frame_with_budget(
            notification.session_id,
            &bytes,
            OUTBOUND_BEST_EFFORT_RETRIES,
        )
    }

    // Every `deliver_*` caller of this reaches `SessionOutboundSink::deliver`
    // synchronously from whatever domain actor thread produced the response -
    // that thread is shared by every session routed to the same actor/key.
    // A budget that can sleep (previously up to ~177ms across 100 attempts)
    // lets one session's saturated outbound channel stall every other
    // session queued behind it on that actor. Use the same yield-only budget
    // already required for the Queue ready-notification path below, for the
    // same reason: give up in microseconds rather than block the actor.
    fn send_encoded_frame(&self, session_id: u64, bytes: &Bytes) -> Result<(), DeliveryError> {
        self.send_encoded_frame_with_budget(session_id, bytes, OUTBOUND_BEST_EFFORT_RETRIES)
    }

    fn send_encoded_frame_with_budget(
        &self,
        session_id: u64,
        bytes: &Bytes,
        max_retries: usize,
    ) -> Result<(), DeliveryError> {
        let metrics_enabled = obs::hot_path_metrics_enabled();

        trace!(
            session_id = session_id,
            encoded_len = bytes.len(),
            "Outbound sink: sending TLV frame to transport channel"
        );

        let mut attempt = 0;
        loop {
            let send_start = metrics_enabled.then(Instant::now);
            let send_result = self.tx.try_send(bytes.clone());
            if let Some(send_start) = send_start {
                crate::observability::hot_path_histogram_observe_us(
                    obs::METRIC_OUTBOUND_SEND_LATENCY,
                    Self::elapsed_micros_u64(send_start),
                );
            }

            match send_result {
                Ok(()) => {
                    if metrics_enabled {
                        crate::observability::hot_path_counter_inc(obs::METRIC_FRAMES_SENT);
                    }
                    debug!(
                        session_id = session_id,
                        "Outbound sink: frame sent to transport successfully"
                    );
                    return Ok(());
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    let capacity = self.tx.max_capacity();
                    if metrics_enabled {
                        crate::observability::hot_path_counter_inc(
                            obs::METRIC_OUTBOUND_BACKPRESSURE,
                        );
                    }
                    attempt += 1;
                    if attempt >= max_retries {
                        warn!(
                            session_id = session_id,
                            capacity = capacity,
                            attempts = attempt,
                            "Outbound sink: transport channel full"
                        );
                        return Err(DeliveryError::MailboxFull {
                            capacity,
                            current_len: capacity,
                        });
                    }
                    // A best-effort budget never leaves the yield-only range,
                    // so it can never reach the sleeping tail of the backoff.
                    match outbound_retry_backoff(attempt) {
                        Some(delay) => std::thread::sleep(delay),
                        None => std::thread::yield_now(),
                    }
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

/// Sample size used only to exercise `outbound_retry_backoff`'s general
/// escalation shape in tests; no live caller requests this many attempts
/// since every `deliver_*` path now uses the yield-only best-effort budget.
#[cfg(test)]
const MAX_OUTBOUND_SEND_RETRIES: usize = 100;
/// Attempts for a best-effort delivery made synchronously from a domain
/// actor thread with its own reply deadline (e.g. Queue ready-notifications).
/// Bounded to the yield-only range so this can never sleep - see
/// `outbound_retry_backoff`.
const OUTBOUND_BEST_EFFORT_RETRIES: usize = OUTBOUND_YIELD_ATTEMPTS;
/// Attempts served by a cheap yield before real waiting begins.
const OUTBOUND_YIELD_ATTEMPTS: usize = 8;
/// Ceiling on any single wait between send attempts.
const OUTBOUND_MAX_RETRY_BACKOFF: Duration = Duration::from_millis(2);

/// How long to wait before outbound send attempt `attempt`.
///
/// `None` means yield instead of sleeping. The first few attempts stay on a
/// yield because a transport channel that is momentarily full usually drains
/// within a scheduling quantum. Past that, spinning is not waiting: a hundred
/// `yield_now` calls elapse in microseconds, so a frame would be abandoned
/// before a briefly-saturated consumer could possibly catch up. The remaining
/// attempts escalate to a bounded sleep so a real burst gets real time.
fn outbound_retry_backoff(attempt: usize) -> Option<Duration> {
    if attempt < OUTBOUND_YIELD_ATTEMPTS {
        return None;
    }
    let step = attempt - OUTBOUND_YIELD_ATTEMPTS;
    let micros = 100_u64.saturating_mul(1_u64 << step.min(5));
    Some(Duration::from_micros(micros).min(OUTBOUND_MAX_RETRY_BACKOFF))
}

/// Frame one TLV value for the wire.
///
/// # Errors
///
/// Returns `DeliveryError::InvalidPayload` when the payload exceeds the `u16`
/// length a TLV value can carry. This used to be an assertion, which turned
/// any aggregate-overflow bug in any domain - a schedule listing, a large read
/// page - into a broker panic. Framing must fail the one delivery, never the
/// process; the real fix always lives at the source, which must paginate.
fn encode_single_tlv_frame(
    msg_type: crate::protocol::tlv::MessageType,
    payload: &[u8],
) -> Result<Bytes, DeliveryError> {
    if u16::try_from(payload.len()).is_err() {
        return Err(DeliveryError::InvalidPayload {
            len: payload.len(),
            max: usize::from(u16::MAX),
        });
    }

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
    Ok(out.freeze())
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

    #[test]
    fn should_not_report_unsupported_outbound_payload_as_stopped_actor() {
        // Arrange
        let (tx, _rx) = mpsc::channel(1);
        let sink = SessionOutboundSink::new(tx);
        let envelope = Envelope::new(
            RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/1")),
            42_u64,
        );

        // Act
        let result = sink.deliver(envelope);

        // Assert
        assert_eq!(result, Err(DeliveryError::UnsupportedPayload));
    }

    #[tokio::test]
    async fn should_bound_saturated_queue_watcher_delivery_without_sleeping() {
        // Arrange
        // Queue delivers ready-notifications to every watcher SERIALLY, on the
        // actor thread, before it replies to the client whose SEND just
        // committed - see `mailbox_sink_impl.rs`'s notify loop ahead of
        // `route_queue_response`. The default retry budget blocks up to ~177ms
        // per saturated consumer (8 yields then escalating sleeps to 100
        // attempts); six saturated watchers alone would exceed
        // QUEUE_ACTOR_REPLY_TIMEOUT (1s) even though the write already
        // succeeded. A best-effort notification must give up fast instead.
        let (tx, _rx) = mpsc::channel(1);
        // Fill the channel so every attempt is met with Full. `_rx` is kept
        // alive (never read) so the channel stays Full rather than Closed.
        tx.try_send(Bytes::from_static(b"occupied"))
            .expect("prime the channel to capacity");
        let sink = SessionOutboundSink::new(tx);
        let notification = crate::domains::queue::QueueClientNotification::new(
            1,
            RouteFamily::new(1),
            7,
            Route::new("queue://acme/jobs/watched"),
            crate::domains::queue::QueueNotification {
                ready_messages: 1,
                delayed_messages: 0,
                inflight_messages: 0,
            },
        );

        // Act
        let result = sink.deliver(Envelope::new(
            RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/1")),
            notification,
        ));
        let retry_schedule = (1..OUTBOUND_BEST_EFFORT_RETRIES)
            .map(outbound_retry_backoff)
            .collect::<Vec<_>>();

        // Assert
        assert!(result.is_err(), "a permanently full channel must fail");
        assert!(
            retry_schedule.iter().all(Option::is_none),
            "best-effort delivery must never enter the sleeping backoff path: {retry_schedule:?}"
        );
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

    #[test]
    fn should_wait_meaningfully_before_giving_up_on_a_full_outbound_channel() {
        // Arrange
        // Spinning on `yield_now` for every attempt takes microseconds, so a
        // frame is abandoned long before a briefly-saturated consumer has any
        // chance to drain. The schedule must yield for the first few attempts
        // (the genuinely transient case) and then wait in escalating steps.
        let schedule = (0..MAX_OUTBOUND_SEND_RETRIES)
            .map(outbound_retry_backoff)
            .collect::<Vec<_>>();

        // Act
        let total_wait: Duration = schedule.iter().flatten().copied().sum();
        let yielded_attempts = schedule.iter().filter(|delay| delay.is_none()).count();

        // Assert
        assert!(
            yielded_attempts >= 4,
            "the first attempts should stay on a cheap yield, got {yielded_attempts}"
        );
        assert!(
            total_wait >= Duration::from_millis(50),
            "a frame must not be dropped after only {total_wait:?} of waiting"
        );
        assert!(
            total_wait <= Duration::from_millis(500),
            "the wait must stay bounded, got {total_wait:?}"
        );
        assert!(
            schedule.windows(2).all(|pair| {
                pair[0].unwrap_or(Duration::ZERO) <= pair[1].unwrap_or(Duration::ZERO)
            }),
            "the backoff must be monotonically non-decreasing"
        );
    }

    #[test]
    fn should_reject_oversized_tlv_frame_instead_of_panicking() {
        // Arrange
        // A TLV value carries a u16 length. Asserting on that turns any
        // aggregate-overflow bug in any domain into a broker panic; the
        // schedule LIST response reached 270KB this way. Framing must fail the
        // one delivery, not the process.
        let payload = vec![0x5a; usize::from(u16::MAX) + 1];

        // Act
        let result = encode_single_tlv_frame(crate::protocol::tlv::MessageType::new(701), &payload);

        // Assert
        let Err(error) = result else {
            panic!("oversized payload must not be framed");
        };
        assert!(
            matches!(error, DeliveryError::InvalidPayload { .. }),
            "unexpected error: {error:?}"
        );
    }
}
