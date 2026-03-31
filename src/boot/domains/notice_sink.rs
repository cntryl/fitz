use super::parse_route_triplet;
use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

struct NoticeSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for NoticeSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

pub struct NoticeDomainSink {
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<NoticeSubscription>>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl NoticeDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn sync_admin_snapshot(&self) {
        let families = self.families.lock();
        let mut subscriptions = Vec::new();
        let mut routes: HashMap<String, usize> = HashMap::new();
        for state in families.values() {
            for subscription in state.values() {
                let pattern = subscription.pattern.route().to_string();
                if let Some((realm, _area, _resource)) = parse_route_triplet(&pattern) {
                    subscriptions.push(crate::api::admin::NoticeSubscription {
                        subscription_id: subscription.subscription_id,
                        session_id: subscription.session_id.to_string(),
                        realm,
                        pattern: pattern.clone(),
                        created_at: Utc::now().to_rfc3339(),
                        notifications_received: 0,
                    });
                    *routes.entry(pattern).or_insert(0) += 1;
                }
            }
        }
        drop(families);
        self.admin_read_model
            .replace_notice_subscriptions(subscriptions);
        self.admin_read_model.replace_notice_routes(
            routes
                .into_iter()
                .map(|(route, subscribers)| crate::api::admin::NoticeRouteInfo {
                    route,
                    subscribers,
                    publishes_total: 0,
                    publishes_per_minute: 0.0,
                })
                .collect(),
        );
    }

    fn fan_out_notice_event(
        &self,
        state: &RoutedSubscriptionSet<NoticeSubscription>,
        event: &crate::runtime::DomainPublishEvent,
        payload_encoder: &mut crate::protocol::payload_codec::PayloadEncoder,
    ) {
        state.for_each_matching(event, |subscription| {
            self.route_notice_notify(subscription, &event.route, &event.payload, payload_encoder);
        });
    }

    fn route_notice_notify(
        &self,
        subscription: &NoticeSubscription,
        route: &crate::runtime::routing::Route,
        payload: &[u8],
        payload_encoder: &mut crate::protocol::payload_codec::PayloadEncoder,
    ) {
        let notify_payload = crate::protocol::notice_codec::encode_notify_into(
            subscription.subscription_id,
            route,
            payload,
            payload_encoder,
        );
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(504),
            bytes::Bytes::from(notify_payload),
            crate::runtime::routing::RouteFamily::from_u32(subscription.subscriber.family().id()),
        );
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);
        let _ = self.router.route(notify_envelope);
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            self.fan_out_notice_event(state, event, &mut payload_encoder);
        }
        Ok(())
    }

    pub fn unsubscribe_all_for_session(&self, session_id: u64) {
        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        tracing::debug!(
            domain = "notice",
            session = session_id,
            "All notice subscriptions removed for session (disconnect cleanup)"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
    }
}

impl MailboxSink for NoticeDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all_for_session(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "notice",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Notice domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "notice", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };

        tracing::debug!(
            domain = "notice",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            payload_len = frame_ctx.payload.len(),
            "Notice: parsing request"
        );

        let notice_msg = match crate::protocol::notice_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            if let Some(src) = envelope.source() {
                src.clone()
            } else {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            },
        ) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(domain = "notice", error = %e, "Failed to parse notice message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::notice::protocol::NotificationMessage;
        use crate::protocol::notice_codec::NoticeResponse;
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let (response_opt, should_sync_admin_snapshot) = match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                let family_id = pub_msg.family_id.as_u64();
                let families = self.families.lock();
                if let Some(state) = families.get(&family_id) {
                    let event = crate::runtime::DomainPublishEvent::new(
                        pub_msg.family_id,
                        pub_msg.route.clone(),
                        pub_msg.payload.clone(),
                    );
                    self.fan_out_notice_event(state, &event, &mut payload_encoder);
                }
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    false,
                )
            }
            NotificationMessage::Subscribe(sub_msg) => {
                let family_id = sub_msg.family_id.as_u64();

                let mut families = self.families.lock();
                let state = families
                    .entry(family_id)
                    .or_insert_with(RoutedSubscriptionSet::new);

                if sub_msg.pattern.as_str().is_empty() {
                    tracing::warn!(
                        domain = "notice",
                        session = sub_msg.session_id.0,
                        "Rejected empty subscription pattern"
                    );
                    (
                        Some(NoticeResponse::Error("empty pattern".to_string())),
                        false,
                    )
                } else {
                    let existing_sub_id =
                        state.find_existing_id(sub_msg.session_id.0, sub_msg.pattern.as_str());
                    let sub_id = if let Some(id) = existing_sub_id {
                        tracing::debug!(
                            domain = "notice",
                            session = sub_msg.session_id.0,
                            subscription_id = id,
                            pattern = sub_msg.pattern.as_str(),
                            "Notice subscription already exists (idempotent)"
                        );
                        id
                    } else {
                        let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        state.insert(
                            sub_msg.family_id,
                            NoticeSubscription {
                                pattern: crate::runtime::matcher::Pattern::new(
                                    sub_msg.pattern.as_str(),
                                ),
                                session_id: sub_msg.session_id.0,
                                subscription_id: new_id,
                                subscriber: sub_msg.subscriber.clone(),
                            },
                        );

                        tracing::debug!(
                            domain = "notice",
                            session = sub_msg.session_id.0,
                            subscription_id = new_id,
                            pattern = sub_msg.pattern.as_str(),
                            "Notice subscription added"
                        );
                        new_id
                    };

                    (
                        Some(NoticeResponse::Ok {
                            subscription_id: Some(sub_id),
                        }),
                        true,
                    )
                }
            }
            NotificationMessage::Unsubscribe(unsub_msg) => {
                let family_id = unsub_msg.family_id.as_u64();
                let mut families = self.families.lock();
                if let Some(state) = families.get_mut(&family_id) {
                    state.remove_session_pattern(
                        unsub_msg.family_id,
                        unsub_msg.session_id.0,
                        unsub_msg.pattern.as_str(),
                    );
                }
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    true,
                )
            }
            NotificationMessage::UnsubscribeAll(unsub_all) => {
                let session_id = unsub_all.session_id.0;
                self.unsubscribe_all_for_session(session_id);
                tracing::debug!(
                    domain = "notice",
                    session = session_id,
                    "All subscriptions removed for session"
                );
                (
                    Some(NoticeResponse::Ok {
                        subscription_id: None,
                    }),
                    true,
                )
            }
            NotificationMessage::Deliver(_) => (
                Some(NoticeResponse::Ok {
                    subscription_id: None,
                }),
                false,
            ),
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        if let Some(response) = response_opt {
            let response_bytes = crate::protocol::notice_codec::encode_response_into(
                &response,
                &mut payload_encoder,
            );
            let response_ctx = FrameContext::new(
                frame_ctx.session_id,
                frame_ctx.channel_id,
                crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                bytes::Bytes::from(response_bytes),
                frame_ctx.route_family,
            );
            if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                let _ = self.router.route(response_envelope);
            }
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
    use crate::protocol::tlv::MessageType;
    use crate::runtime::mailbox::Mailbox;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use bytes::Bytes;
    use std::sync::Arc;

    fn encode_notice_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn encode_notice_publish(route: &str, payload: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_bytes(payload);
        Bytes::from(encoder.finish())
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    #[test]
    fn should_create_notice_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = NoticeDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_remove_notice_subscriptions_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let publisher_session_id = 11;
        let notice_route = "notice://acme/app/events";
        let notice_address = RouteAddress::new(family, Route::new(notice_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        let publisher_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(publisher_address.clone(), publisher_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(notice_route),
                family,
            ),
        ))
        .expect("subscribe notice route");
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("subscribe ack frame");
        let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
        let subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
        assert_eq!(subscribe_status, 0);
        let _subscription_id = subscribe_decoder
            .get_optional_u64()
            .expect("subscription id");
        assert!(subscribe_decoder.is_complete());

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("notice://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: subscriber_session_id,
            },
        ))
        .expect("cleanup notice subscriber");
        sink.deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"hello"),
                family,
            ),
        ))
        .expect("publish notice event");

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        let publish_envelope = publisher_mailbox
            .receiver()
            .try_recv()
            .expect("publish ack envelope");
        let publish_frame = publish_envelope
            .into_payload::<FrameContext>()
            .expect("publish ack frame");
        let mut publish_decoder = PayloadDecoder::new(&publish_frame.payload);
        let publish_status = publish_decoder.get_u8().expect("publish status");
        assert_eq!(publish_status, 0);
        let publish_subscription_id = publish_decoder
            .get_optional_u64()
            .expect("publish subscription id");
        assert!(publish_subscription_id.is_none());
        assert!(publish_decoder.is_complete());
    }

    #[test]
    fn should_retain_other_notice_subscription_given_unsubscribe_on_same_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let publisher_session_id = 11;
        let removed_route = "notice://acme/app/events";
        let retained_route = "notice://acme/app/audits";
        let notice_address = RouteAddress::new(family, Route::new(removed_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        let publisher_mailbox = Arc::new(Mailbox::new(16));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(publisher_address.clone(), publisher_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(removed_route),
                family,
            ),
        ))
        .expect("subscribe removed notice route");
        let _removed_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("removed subscribe ack envelope");

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(retained_route),
                family,
            ),
        ))
        .expect("subscribe retained notice route");
        let _retained_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained subscribe ack envelope");
        assert_eq!(sink.subscription_count(), 2);
        drain_mailbox(&subscriber_mailbox);
        drain_mailbox(&publisher_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(502),
                encode_notice_subscribe(removed_route),
                family,
            ),
        ))
        .expect("unsubscribe removed notice route");
        let unsubscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");
        let unsubscribe_frame = unsubscribe_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe ack frame");
        let mut unsubscribe_decoder = PayloadDecoder::new(&unsubscribe_frame.payload);
        assert_eq!(unsubscribe_decoder.get_u8().expect("unsubscribe status"), 0);
        let unsubscribe_subscription_id = unsubscribe_decoder
            .get_optional_u64()
            .expect("unsubscribe subscription id");
        assert!(unsubscribe_subscription_id.is_none());
        assert!(unsubscribe_decoder.is_complete());
        assert_eq!(sink.subscription_count(), 1);

        sink.deliver(Envelope::from_route(
            publisher_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(removed_route, b"removed"),
                family,
            ),
        ))
        .expect("publish removed notice event");
        let removed_publish_envelope = publisher_mailbox
            .receiver()
            .try_recv()
            .expect("removed publish ack envelope");
        let removed_publish_frame = removed_publish_envelope
            .into_payload::<FrameContext>()
            .expect("removed publish ack frame");
        let mut removed_publish_decoder = PayloadDecoder::new(&removed_publish_frame.payload);
        assert_eq!(
            removed_publish_decoder
                .get_u8()
                .expect("removed publish status"),
            0
        );
        let removed_publish_subscription_id = removed_publish_decoder
            .get_optional_u64()
            .expect("removed publish subscription id");
        assert!(removed_publish_subscription_id.is_none());
        assert!(removed_publish_decoder.is_complete());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        sink.deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(retained_route, b"retained"),
                family,
            ),
        ))
        .expect("publish retained notice event");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained notice notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("retained notice notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 504);
        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_route = notify_decoder.get_string().expect("notify route");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_route, retained_route);
        assert_eq!(notified_payload.as_ref(), b"retained");
        assert!(notify_decoder.is_complete());

        let retained_publish_envelope = publisher_mailbox
            .receiver()
            .try_recv()
            .expect("retained publish ack envelope");
        let retained_publish_frame = retained_publish_envelope
            .into_payload::<FrameContext>()
            .expect("retained publish ack frame");
        let mut retained_publish_decoder = PayloadDecoder::new(&retained_publish_frame.payload);
        assert_eq!(
            retained_publish_decoder
                .get_u8()
                .expect("retained publish status"),
            0
        );
        let retained_publish_subscription_id = retained_publish_decoder
            .get_optional_u64()
            .expect("retained publish subscription id");
        assert!(retained_publish_subscription_id.is_none());
        assert!(retained_publish_decoder.is_complete());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }
}
