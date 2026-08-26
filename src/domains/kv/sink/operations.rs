//! Actor lookup, operation dispatch, and request-envelope validation.

use super::locks::KvResourceLockKey;
use super::state::KvDomainRuntime;
use super::state::{KvAdminTransactionUpdate, KvOperationOutcome};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::domains::kv::KvActor;
#[cfg(test)]
use crate::domains::kv::KvClientFrame;
use crate::domains::kv::KvClientRequest;
use crate::domains::kv::{KvError, KvResponse};
use crate::runtime::Envelope;
use parking_lot::Mutex;
use std::sync::Arc;

impl KvDomainRuntime<'_> {
    pub(super) fn dispatch_actor_operation(
        &self,
        session_id: u64,
        meta: crate::runtime::ClientFrameMeta,
        kv_message: crate::domains::kv::KvMessage,
    ) -> KvOperationOutcome {
        use crate::domains::kv::{KvMessage, TxMode};
        let write_lock = match &kv_message {
            KvMessage::Begin { scope, mode, .. } if *mode == TxMode::ReadWrite => {
                Some(KvResourceLockKey::from_scope(scope))
            }
            _ => None,
        };
        if let Some(lock_key) = write_lock {
            return self.handle_begin_read_write(session_id, &lock_key, kv_message);
        }
        match kv_message {
            message @ KvMessage::Commit { tx_id, .. } => {
                self.handle_commit_frame(session_id, meta.route_family, tx_id, message)
            }
            message @ KvMessage::Rollback { tx_id, .. } => {
                self.handle_rollback_frame(session_id, meta.route_family, tx_id, message)
            }
            message => self.handle_regular_operation_frame(session_id, meta.message_type, message),
        }
    }

    pub(super) fn actor_for_session(&self, session_id: u64, context: &str) -> Arc<Mutex<KvActor>> {
        self.core
            .actors
            .lock()
            .entry(session_id)
            .or_insert_with(|| {
                tracing::trace!(
                    domain = "kv",
                    session_id = session_id,
                    "Creating new KvActor instance ({context})"
                );
                Arc::new(Mutex::new(KvActor::new(self.core.store.clone())))
            })
            .clone()
    }

    fn handle_regular_operation_frame(
        &self,
        session_id: u64,
        message_type: u16,
        kv_message: crate::domains::kv::KvMessage,
    ) -> KvOperationOutcome {
        let actor = self.actor_for_session(session_id, "other operation");
        let mut actor = actor.lock();
        tracing::trace!(
            domain = "kv",
            session_id = session_id,
            msg_type = message_type,
            "Calling actor.handle() for operation"
        );
        KvOperationOutcome::new(
            actor.handle(kv_message),
            KvAdminTransactionUpdate::None,
            None,
        )
    }

    pub(super) fn request_from_envelope(envelope: &Envelope) -> Option<KvClientRequest> {
        if let Some(request) = envelope.payload::<KvClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::kv::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::dispatch::protocol::kv::ParsedKvFrame::Op(message) => {
                    KvClientFrame::Op(message)
                }
                crate::dispatch::protocol::kv::ParsedKvFrame::Sub(message) => {
                    KvClientFrame::Sub(message)
                }
            });
            Some(KvClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    pub(super) fn valid_request_envelope(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    pub(super) fn valid_subscription_request(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> bool {
        family_id == meta.route_family
            && *subscriber.family() == family_id
            && session_id == meta.session_id
            && envelope.source().is_none_or(|source| source == subscriber)
    }

    pub(super) fn kv_message_family(
        message: &crate::domains::kv::KvMessage,
    ) -> crate::runtime::routing::RouteFamily {
        message.scope().route_family
    }

    pub(super) fn error_response(reason: &str) -> KvResponse {
        KvResponse::Error {
            error: KvError::InvalidRequest(reason.to_string()),
        }
    }

    pub(super) fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::dispatch::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::dispatch::protocol::frame::ChannelId::Control => {
            crate::runtime::ClientChannel::Control
        }
        crate::dispatch::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::dispatch::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::dispatch::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::dispatch::protocol::frame::ChannelId::Internal => {
            crate::runtime::ClientChannel::Internal
        }
    }
}
