// LAYER: API
//! Per-domain outbound frame encoding for `SessionOutboundSink`.
//!
//! Split out of `outbound.rs` purely for size: the parent owns the sink, the
//! transport channel, and the retry budget, while these methods only turn one
//! domain response or notification into bytes and hand it back to the parent's
//! single `send_encoded_frame` choke point.

use super::{
    encode_single_tlv_frame, DeliveryError, SessionOutboundSink, OUTBOUND_BEST_EFFORT_RETRIES,
};
use tracing::debug;

impl SessionOutboundSink {
    pub(super) fn deliver_rpc_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_rpc_worker_request(
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
        self.send_encoded_frame(delivery.session_id, &bytes, None)
    }

    pub(super) fn deliver_rpc_forwarded_response(
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
        self.send_encoded_frame(forwarded.session_id, &bytes, None)
    }

    pub(super) fn deliver_kv_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_kv_notification(
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
        self.send_encoded_frame(notification.session_id, &bytes, None)
    }

    pub(super) fn deliver_lease_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_lease_notification(
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
        self.send_encoded_frame(notification.session_id, &bytes, None)
    }

    pub(super) fn deliver_notice_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_notice_notification(
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
        self.send_encoded_frame(notification.session_id, &bytes, None)
    }

    pub(super) fn deliver_schedule_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_schedule_notification(
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
        self.send_encoded_frame(notification.session_id, &bytes, None)
    }

    pub(super) fn deliver_stream_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_stream_notification(
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
        self.send_encoded_frame(notification.session_id, &bytes, None)
    }

    pub(super) fn deliver_queue_client_response(
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
        self.send_encoded_frame(response.meta.session_id, &bytes, response.meta.correlation)
    }

    pub(super) fn deliver_queue_notification(
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
            None,
        )
    }
}
