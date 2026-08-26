//! KV watch registration, removal, matching, and notification routing.

use super::locks::KvResourceLockKey;
use super::state::KvDomainRuntime;
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::domains::kv::{KvError, KvResponse};
use crate::runtime::{DeliveryError, Envelope};

impl KvDomainRuntime<'_> {
    pub(super) fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: std::time::Instant,
        sub_msg: crate::domains::kv::KvSubscriptionMessage,
    ) -> Result<(), DeliveryError> {
        let response = match sub_msg {
            crate::domains::kv::KvSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self
                .handle_kv_subscribe(envelope, meta, family_id, &pattern, session_id, subscriber),
            crate::domains::kv::KvSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_kv_unsubscribe(
                envelope,
                meta,
                family_id,
                &pattern,
                session_id,
                &subscriber,
            ),
        };

        self.refresh_metrics_gauges();
        self.route_kv_response(envelope, meta, &response, request_started)
    }

    fn handle_kv_subscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> KvResponse {
        if Self::valid_subscription_request(envelope, meta, family_id, session_id, &subscriber) {
            let compiled = match Self::compile_kv_subscription_pattern(pattern) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let subscription_id = {
                let mut watch_registries = self.core.watch_registries.lock();
                let registry = watch_registries
                    .entry(family_id.as_u64())
                    .or_insert_with(|| {
                        crate::domains::kv::watch_registry::KvWatchRegistry::new(family_id)
                    });
                let subscription_id = registry.subscribe(session_id, compiled, subscriber);
                if subscription_id.is_err() && registry.is_empty() {
                    watch_registries.remove(&family_id.as_u64());
                }
                subscription_id
            };
            subscription_id.map_or_else(
                |error| KvResponse::Error { error },
                |subscription_id| KvResponse::SubscribeOk { subscription_id },
            )
        } else {
            Self::error_response("route family mismatch")
        }
    }

    fn handle_kv_unsubscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> KvResponse {
        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            if let Err(response) = Self::compile_kv_subscription_pattern(pattern) {
                return response;
            }
            let mut watch_registries = self.core.watch_registries.lock();
            let remove_family =
                if let Some(registry) = watch_registries.get_mut(&family_id.as_u64()) {
                    registry.unsubscribe(session_id, pattern.as_str());
                    registry.is_empty()
                } else {
                    false
                };
            if remove_family {
                watch_registries.remove(&family_id.as_u64());
            }
            KvResponse::UnsubscribeOk
        } else {
            Self::error_response("route family mismatch")
        }
    }

    fn compile_kv_subscription_pattern(
        pattern: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, KvResponse> {
        crate::runtime::DomainKind::Kv
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
            .map_err(|error| KvResponse::Error {
                error: KvError::InvalidSubscriptionPattern(error),
            })
    }

    fn kv_route_for_lock(resource_key: &KvResourceLockKey) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "kv://{}/{}/{}",
            resource_key.realm, resource_key.area, resource_key.resource
        ))
    }

    fn route_kv_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        mutation_count: u64,
    ) {
        #[cfg(test)]
        let notify_envelope = {
            let payload = crate::dispatch::protocol::kv::encode_notify(
                subscription_id,
                route,
                crate::domains::kv::KvNotification { mutation_count },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::dispatch::protocol::frame::ChannelId::Sub,
                crate::dispatch::protocol::tlv::MessageType::new(
                    crate::dispatch::protocol::kv::msg_type::NOTIFY,
                ),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            Envelope::new(subscriber.clone(), notify_ctx)
        };

        #[cfg(not(test))]
        let notify_envelope = {
            let notification = crate::domains::kv::KvClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.clone(),
                crate::domains::kv::KvNotification { mutation_count },
            );
            Envelope::new(subscriber.clone(), notification)
        };

        if self.core.router.route(notify_envelope).is_err() {
            self.counter_inc(crate::domains::kv::metrics::METRIC_NOTIFY_DROPS_TOTAL);
        }
    }

    pub(super) fn route_kv_notification(
        &self,
        resource_key: &KvResourceLockKey,
        mutation_count: u64,
    ) {
        let (route, watch_targets) = {
            let watch_registries = self.core.watch_registries.lock();
            let Some(registry) = watch_registries.get(&resource_key.family_id) else {
                return;
            };
            let route = Self::kv_route_for_lock(resource_key);
            let watch_targets = registry.matching_targets(&route);
            (route, watch_targets)
        };
        for target in watch_targets {
            self.route_kv_notify_to_subscription(
                target.session_id,
                target.subscription_id,
                &target.subscriber,
                &route,
                mutation_count,
            );
        }
    }
}
