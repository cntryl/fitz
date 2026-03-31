use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct ScheduleSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

struct ScheduleFamilyState {
    subscriptions: Vec<ScheduleSubscription>,
}

pub struct ScheduleDomainSink {
    store: Arc<cntryl_midge::Engine>,
    actors: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crate::domains::schedule::ScheduleActor>,
    >,
    sub_families: Mutex<HashMap<u64, ScheduleFamilyState>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl ScheduleDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Mutex::new(HashMap::new()),
            sub_families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn start_tick_loop(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("Schedule tick loop not started: no Tokio runtime available");
            return;
        };

        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let Some(sink) = weak.upgrade() else {
                    break;
                };
                if !sink.active.load(Ordering::Relaxed) {
                    break;
                }

                sink.scan_due_schedules();
            }
        });
    }

    fn scan_due_schedules(&self) {
        let mut publishes = Vec::new();
        {
            let mut actors = self.actors.lock();
            for (family, actor) in actors.iter_mut() {
                for (route, payload) in actor.scan_and_fire() {
                    let route = crate::runtime::routing::Route::new(route);
                    publishes.push(crate::runtime::DomainPublishEvent::new(
                        *family, route, payload,
                    ));
                }
            }
        }

        for event in publishes {
            let destination =
                crate::runtime::routing::RouteAddress::new(event.family_id, event.route.clone());
            let _ = self.router.route(Envelope::new(destination, event));
        }
    }

    pub(crate) fn force_due_scan_for_tests(&self, ready_count: usize) {
        {
            let mut actors = self.actors.lock();
            for actor in actors.values_mut() {
                actor.bench_prepare_scan(ready_count);
            }
        }

        self.scan_due_schedules();
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.sub_families.lock();
        if let Some(state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            for sub in &state.subscriptions {
                if sub.pattern.matches(&event.route) {
                    let notify_payload = crate::protocol::schedule_codec::encode_notify_into(
                        &mut payload_encoder,
                        sub.subscription_id,
                        &event.payload,
                    );
                    let notify_ctx = FrameContext::new(
                        sub.session_id,
                        crate::protocol::frame::ChannelId::Sub,
                        crate::protocol::tlv::MessageType::new(705),
                        bytes::Bytes::from(notify_payload),
                        crate::runtime::routing::RouteFamily::from_u32(
                            sub.subscriber.family().id(),
                        ),
                    );
                    let notify_envelope = Envelope::new(sub.subscriber.clone(), notify_ctx);
                    let _ = self.router.route(notify_envelope);
                }
            }
        }
        Ok(())
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.sub_families.lock();
        for state in families.values_mut() {
            state.subscriptions.retain(|s| s.session_id != session_id);
        }
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.sub_families.lock();
        families.values().map(|s| s.subscriptions.len()).sum()
    }

    pub fn schedule_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.schedule_count()).sum()
    }
}

impl MailboxSink for ScheduleDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "schedule",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Schedule domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "schedule", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let schedule_msg = match crate::protocol::schedule_codec::parse_request(
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
                tracing::warn!(
                    domain = "schedule",
                    error = %e,
                    "Failed to parse schedule message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};
        enum ScheduleAdminUpdate {
            Upsert {
                realm: String,
                area: String,
                resource: String,
                operation: String,
                cron: String,
            },
            Remove {
                realm: String,
                area: String,
                resource: String,
                operation: String,
            },
        }

        let mut admin_update: Option<ScheduleAdminUpdate> = None;

        let response = {
            let store = self.store.clone();
            let mut actors = self.actors.lock();
            let actor = actors.entry(route_family).or_insert_with(|| {
                crate::domains::schedule::ScheduleActor::new(
                    route_family,
                    store,
                    cntryl_midge::WriteOptions::buffered(),
                )
            });

            match schedule_msg {
                ScheduleMessage::Create {
                    route,
                    cron,
                    payload,
                } => {
                    let route_for_admin = route.clone();
                    let cron_for_admin = cron.clone();
                    match actor.create_schedule(route, cron, payload) {
                        Ok(changed) => {
                            if changed {
                                if let Ok(route_parts) =
                                    crate::domains::schedule::protocol::parse_concrete_schedule_route(
                                        &route_for_admin,
                                    )
                                {
                                    admin_update = Some(ScheduleAdminUpdate::Upsert {
                                        realm: route_parts.realm,
                                        area: route_parts.area,
                                        resource: route_parts.resource,
                                        operation: route_parts.operation,
                                        cron: cron_for_admin,
                                    });
                                }
                            }
                            ScheduleResponse::Ok
                        }
                        Err(e) => ScheduleResponse::Error(e),
                    }
                }
                ScheduleMessage::Cancel { route } => {
                    let route_for_admin = route.clone();
                    match actor.delete_schedule(route) {
                        Ok(removed) => {
                            if removed {
                                if let Ok(route_parts) =
                                    crate::domains::schedule::protocol::parse_concrete_schedule_route(
                                        &route_for_admin,
                                    )
                                {
                                    admin_update = Some(ScheduleAdminUpdate::Remove {
                                        realm: route_parts.realm,
                                        area: route_parts.area,
                                        resource: route_parts.resource,
                                        operation: route_parts.operation,
                                    });
                                }
                            }
                            ScheduleResponse::Ok
                        }
                        Err(e) => ScheduleResponse::Error(e),
                    }
                }
                ScheduleMessage::List { offset, limit } => {
                    let (entries, total_count) = actor.list_entries(offset, limit);

                    ScheduleResponse::ListDefs {
                        entries,
                        total_count,
                    }
                }
                ScheduleMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    if let Err(error) =
                        crate::domains::schedule::protocol::validate_concrete_schedule_route(
                            pattern.as_str(),
                        )
                    {
                        ScheduleResponse::Error(error)
                    } else {
                        let fam_id = family_id.as_u64();

                        let mut families = self.sub_families.lock();
                        let state = families
                            .entry(fam_id)
                            .or_insert_with(|| ScheduleFamilyState {
                                subscriptions: Vec::new(),
                            });

                        let existing_sub_id = state
                            .subscriptions
                            .iter()
                            .find(|s| {
                                s.session_id == session_id && s.pattern.route() == pattern.as_str()
                            })
                            .map(|s| s.subscription_id);

                        let sub_id = if let Some(id) = existing_sub_id {
                            tracing::debug!(
                                domain = "schedule",
                                session = session_id,
                                subscription_id = id,
                                pattern = pattern.as_str(),
                                "Schedule subscription already exists (idempotent)"
                            );
                            id
                        } else {
                            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            let pat = crate::runtime::matcher::Pattern::new(pattern.as_str());

                            state.subscriptions.push(ScheduleSubscription {
                                pattern: pat,
                                session_id,
                                subscription_id: new_id,
                                subscriber,
                            });

                            tracing::debug!(
                                domain = "schedule",
                                session = session_id,
                                subscription_id = new_id,
                                pattern = pattern.as_str(),
                                "Schedule subscription added"
                            );
                            new_id
                        };

                        ScheduleResponse::SubscribeOk {
                            subscription_id: sub_id,
                        }
                    }
                }
                ScheduleMessage::Unsubscribe {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    if let Err(error) =
                        crate::domains::schedule::protocol::validate_concrete_schedule_route(
                            pattern.as_str(),
                        )
                    {
                        ScheduleResponse::Error(error)
                    } else {
                        let fam_id = family_id.as_u64();
                        let mut families = self.sub_families.lock();
                        if let Some(state) = families.get_mut(&fam_id) {
                            state.subscriptions.retain(|s| {
                                !(s.session_id == session_id
                                    && s.pattern.route() == pattern.as_str())
                            });
                        }
                        ScheduleResponse::Ok
                    }
                }
                ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                    self.unsubscribe_all(session_id);
                    ScheduleResponse::Ok
                }
            }
        };

        if let Some(update) = admin_update {
            match update {
                ScheduleAdminUpdate::Upsert {
                    realm,
                    area,
                    resource,
                    operation,
                    cron,
                } => {
                    self.admin_read_model
                        .upsert_schedule_fields(realm, area, resource, operation, cron);
                }
                ScheduleAdminUpdate::Remove {
                    realm,
                    area,
                    resource,
                    operation,
                } => {
                    self.admin_read_model
                        .remove_schedule(&realm, &area, &resource, &operation);
                }
            }
        }

        let response_bytes =
            crate::protocol::schedule_codec::encode_response_into(&mut payload_encoder, &response);
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

    fn encode_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(cron);
        encoder.put_bytes(payload);
        Bytes::from(encoder.finish())
    }

    fn encode_schedule_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    #[test]
    fn should_create_schedule_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = ScheduleDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_publish_schedule_notify_to_subscribers_when_due() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/nightly/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", sink.clone());

        let create_ctx = FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(700),
            encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
            family,
        );
        let subscribe_ctx = FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(schedule_route),
            family,
        );

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            create_ctx,
        ))
        .expect("create schedule");
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            subscribe_ctx,
        ))
        .expect("subscribe schedule");
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("subscribe ack frame");
        let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
        let _subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
        let subscription_id = subscribe_decoder
            .get_optional_u64()
            .expect("subscription id")
            .expect("subscription id present");

        {
            let mut actors = sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
        }

        sink.scan_due_schedules();

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("schedule notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("schedule notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 705);

        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");

        assert_eq!(notified_subscription_id, subscription_id);
        assert_eq!(notified_payload.as_ref(), b"nightly");
        assert!(notify_decoder.is_complete());
    }

    #[test]
    fn should_remove_schedule_subscriptions_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/nightly/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", sink.clone());

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(700),
                encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
                family,
            ),
        ))
        .expect("create schedule");
        drain_mailbox(&subscriber_mailbox);
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(schedule_route),
                family,
            ),
        ))
        .expect("subscribe schedule");
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("schedule://cleanup")),
            crate::runtime::SessionCleanup { session_id },
        ))
        .expect("cleanup session");
        {
            let mut actors = sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
        }
        sink.scan_due_schedules();

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_retain_other_schedule_subscription_given_unsubscribe_on_same_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let removed_route = "schedule://acme/jobs/nightly/run";
        let retained_route = "schedule://acme/jobs/weekly/report";
        let removed_address = RouteAddress::new(family, Route::new(removed_route));
        let retained_address = RouteAddress::new(family, Route::new(retained_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = ScheduleDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            removed_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(removed_route),
                family,
            ),
        ))
        .expect("subscribe removed schedule route");
        let _removed_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("removed subscribe ack envelope");

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            retained_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(retained_route),
                family,
            ),
        ))
        .expect("subscribe retained schedule route");
        let _retained_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained subscribe ack envelope");
        assert_eq!(sink.subscription_count(), 2);
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            removed_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(704),
                encode_schedule_subscribe(removed_route),
                family,
            ),
        ))
        .expect("unsubscribe removed schedule route");
        let unsubscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");
        let unsubscribe_frame = unsubscribe_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe ack frame");
        let mut unsubscribe_decoder = PayloadDecoder::new(&unsubscribe_frame.payload);
        let unsubscribe_status = unsubscribe_decoder.get_u8().expect("unsubscribe status");
        assert_eq!(unsubscribe_status, 0);
        assert!(unsubscribe_decoder.is_complete());
        assert_eq!(sink.subscription_count(), 1);

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("schedule://events/removed")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(removed_route),
                Bytes::from_static(b"nightly"),
            ),
        ))
        .expect("deliver removed schedule event");
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("schedule://events/retained")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(retained_route),
                Bytes::from_static(b"weekly"),
            ),
        ))
        .expect("deliver retained schedule event");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained schedule notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("retained schedule notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 705);
        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_payload.as_ref(), b"weekly");
        assert!(notify_decoder.is_complete());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }
}
