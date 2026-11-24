//! KV watch registration, removal, matching, and notification routing.

use super::locks::KvResourceLockKey;
use super::state::KvFamilyRuntime;
use crate::domain::{KvError, KvResponse};
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::{DeliveryError, Envelope};

impl KvFamilyRuntime<'_> {
    pub(super) fn handle_subscription_frame(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        request_started: std::time::Instant,
        sub_msg: crate::domain::KvSubscriptionMessage,
    ) -> Result<(), DeliveryError> {
        let new_subscription = match &sub_msg {
            crate::domain::KvSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                ..
            } => {
                let exists = self
                    .core
                    .watch_registries
                    .get(&family_id.as_u64())
                    .and_then(|registry| {
                        registry.existing_subscription_id(*session_id, pattern.as_str())
                    })
                    .is_some();
                (!exists).then_some((*family_id, *session_id))
            }
            crate::domain::KvSubscriptionMessage::Unsubscribe { .. } => None,
        };
        let response = match sub_msg {
            crate::domain::KvSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self
                .handle_kv_subscribe(envelope, meta, family_id, &pattern, session_id, subscriber),
            crate::domain::KvSubscriptionMessage::Unsubscribe {
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
        let route_result = self.route_kv_response(envelope, meta, &response, request_started);
        if route_result.is_err() {
            if let (KvResponse::SubscribeOk { subscription_id }, Some((family_id, session_id))) =
                (&response, new_subscription)
            {
                self.rollback_undeliverable_subscription(family_id, session_id, *subscription_id);
            }
        }
        route_result
    }

    fn rollback_undeliverable_subscription(
        &mut self,
        family_id: fitz_core::runtime::routing::RouteFamily,
        session_id: u64,
        subscription_id: u64,
    ) {
        let registries = &mut self.core.watch_registries;
        let remove_family = registries
            .get_mut(&family_id.as_u64())
            .is_some_and(|registry| {
                registry.remove_subscription_for_session(session_id, subscription_id);
                registry.is_empty()
            });
        if remove_family {
            registries.remove(&family_id.as_u64());
        }
        self.refresh_metrics_gauges();
    }

    fn handle_kv_subscribe(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        family_id: fitz_core::runtime::routing::RouteFamily,
        pattern: &fitz_core::runtime::routing::Route,
        session_id: u64,
        subscriber: fitz_core::runtime::routing::RouteAddress,
    ) -> KvResponse {
        if Self::valid_subscription_request(envelope, meta, family_id, session_id, &subscriber) {
            let compiled = match Self::compile_kv_subscription_pattern(pattern) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let subscription_id = {
                let watch_registries = &mut self.core.watch_registries;
                let registry = watch_registries
                    .entry(family_id.as_u64())
                    .or_insert_with(|| {
                        crate::domain::watch_registry::KvWatchRegistry::new(family_id)
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
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        family_id: fitz_core::runtime::routing::RouteFamily,
        pattern: &fitz_core::runtime::routing::Route,
        session_id: u64,
        subscriber: &fitz_core::runtime::routing::RouteAddress,
    ) -> KvResponse {
        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            if let Err(response) = Self::compile_kv_subscription_pattern(pattern) {
                return response;
            }
            let watch_registries = &mut self.core.watch_registries;
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
        pattern: &fitz_core::runtime::routing::Route,
    ) -> Result<fitz_core::runtime::matcher::Pattern, KvResponse> {
        fitz_core::runtime::DomainKind::Kv
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
            .map_err(|error| KvResponse::Error {
                error: KvError::InvalidSubscriptionPattern(error),
            })
    }

    fn kv_route_for_lock(resource_key: &KvResourceLockKey) -> fitz_core::runtime::routing::Route {
        fitz_core::runtime::routing::Route::new(format!(
            "kv://{}/{}/{}",
            resource_key.realm, resource_key.area, resource_key.resource
        ))
    }

    fn route_kv_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &fitz_core::runtime::routing::RouteAddress,
        route: &fitz_core::runtime::routing::Route,
        mutation_count: u64,
    ) {
        #[cfg(test)]
        let notify_envelope = {
            let payload = crate::wire::kv::encode_notify(
                subscription_id,
                route,
                crate::domain::KvNotification { mutation_count },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::wire::frame::ChannelId::Sub,
                crate::wire::message::MessageType::new(crate::wire::kv::msg_type::NOTIFY),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            Envelope::new(subscriber.clone(), notify_ctx)
        };

        #[cfg(not(test))]
        let notify_envelope = {
            let notification = crate::domain::KvClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.clone(),
                crate::domain::KvNotification { mutation_count },
            );
            Envelope::new(subscriber.clone(), notification)
        };

        if self.core.router.route(notify_envelope).is_err() {
            self.counter_inc(crate::domain::metrics::METRIC_NOTIFY_DROPS_TOTAL);
        }
    }

    pub(super) fn route_kv_notification(
        &self,
        resource_key: &KvResourceLockKey,
        mutation_count: u64,
    ) {
        let (route, watch_targets) = {
            let watch_registries = &self.core.watch_registries;
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
