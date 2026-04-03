use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;

fn schedule_admin_snapshot_due(
    snapshot_dirty: bool,
    force: bool,
    now_elapsed_us: u64,
    last_snapshot_elapsed_us: u64,
) -> bool {
    snapshot_dirty
        && (force
            || now_elapsed_us.saturating_sub(last_snapshot_elapsed_us)
                >= SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US)
}

struct ScheduleSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for ScheduleSubscription {
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

pub struct ScheduleDomainSink {
    store: Arc<cntryl_midge::Engine>,
    actors: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crate::domains::schedule::ScheduleActor>,
    >,
    sub_families: Mutex<HashMap<u64, RoutedSubscriptionSet<ScheduleSubscription>>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
    snapshot_dirty: AtomicBool,
    snapshot_syncing: AtomicBool,
    last_snapshot_elapsed_us: AtomicU64,
    snapshot_epoch: Instant,
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
            snapshot_dirty: AtomicBool::new(false),
            snapshot_syncing: AtomicBool::new(false),
            last_snapshot_elapsed_us: AtomicU64::new(0),
            snapshot_epoch: Instant::now(),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn preload_persisted_families(&self) -> Result<(), String> {
        let column_families = self
            .store
            .list_column_families()
            .map_err(|e| format!("list schedule column families failed: {}", e))?;

        let mut actors = self.actors.lock();
        for column_family in column_families {
            if column_family.id() == 0 {
                continue;
            }

            let family = crate::runtime::routing::RouteFamily::new(column_family.id().into());
            if actors.contains_key(&family) {
                continue;
            }

            let actor = crate::domains::schedule::ScheduleActor::try_new(
                family,
                self.store.clone(),
                cntryl_midge::WriteOptions::buffered(),
            )?;
            actors.insert(family, actor);
        }
        drop(actors);

        self.schedule_admin_snapshot(true);
        Ok(())
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
        let mut snapshot_dirty = false;
        {
            let mut actors = self.actors.lock();
            for (family, actor) in actors.iter_mut() {
                let fired = actor.scan_and_fire();
                if !fired.is_empty() {
                    snapshot_dirty = true;
                }
                for pending_fire in actor.pending_fires_for_delivery() {
                    publishes.push((
                        *family,
                        pending_fire.fire_ms,
                        pending_fire.route,
                        pending_fire.payload,
                    ));
                }
            }
        }

        let mut delivered = HashMap::<crate::runtime::routing::RouteFamily, Vec<(u64, String)>>::new();
        for (family, fire_ms, route, payload) in publishes {
            let route_value = crate::runtime::routing::Route::new(route.clone());
            let event = crate::runtime::DomainPublishEvent::new(family, route_value.clone(), payload);
            let destination = crate::runtime::routing::RouteAddress::new(family, route_value);
            if self.router.route(Envelope::new(destination, event)).is_ok() {
                delivered.entry(family).or_default().push((fire_ms, route));
            }
        }

        let had_deliveries = !delivered.is_empty();

        if !delivered.is_empty() {
            let mut actors = self.actors.lock();
            for (family, delivered_fires) in delivered {
                if let Some(actor) = actors.get_mut(&family) {
                    if let Err(error) = actor.ack_pending_fires(&delivered_fires) {
                        tracing::warn!(
                            route_family = family.as_u64(),
                            error = %error,
                            "Failed to acknowledge pending schedule fires"
                        );
                    }
                }
            }
        }

        if snapshot_dirty || had_deliveries {
            self.schedule_admin_snapshot(false);
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
        self.schedule_admin_snapshot(true);
    }

    fn get_or_create_actor<'a>(
        &'a self,
        actors: &'a mut HashMap<
            crate::runtime::routing::RouteFamily,
            crate::domains::schedule::ScheduleActor,
        >,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Result<&'a mut crate::domains::schedule::ScheduleActor, String> {
        match actors.entry(route_family) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let actor = crate::domains::schedule::ScheduleActor::try_new(
                    route_family,
                    self.store.clone(),
                    cntryl_midge::WriteOptions::buffered(),
                )?;
                Ok(entry.insert(actor))
            }
        }
    }

    fn route_schedule_notify(
        &self,
        subscription: &ScheduleSubscription,
        payload: &[u8],
        payload_encoder: &mut crate::protocol::payload_codec::PayloadEncoder,
    ) {
        let notify_payload = crate::protocol::schedule_codec::encode_notify_into(
            payload_encoder,
            subscription.subscription_id,
            payload,
        );
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(705),
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
        let families = self.sub_families.lock();
        if let Some(state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            state.for_each_matching(event, |subscription| {
                self.route_schedule_notify(subscription, &event.payload, &mut payload_encoder);
            });
        }
        Ok(())
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.sub_families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.sub_families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
    }

    pub fn schedule_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.schedule_count()).sum()
    }

    pub fn pending_fire_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.pending_fire_count()).sum()
    }

    pub fn executions_per_minute(&self) -> f64 {
        let mut actors = self.actors.lock();
        actors
            .values_mut()
            .map(|actor| actor.executions_per_minute())
            .sum()
    }

    fn sync_admin_snapshot(&self) {
        let snapshot = {
            let actors = self.actors.lock();
            let mut schedules = Vec::new();
            for actor in actors.values() {
                schedules.extend(actor.admin_snapshot());
            }
            schedules
        };

        self.admin_read_model.replace_schedules(snapshot);
    }

    fn schedule_admin_snapshot(&self, force: bool) {
        self.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

    fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        {
            let _ = force;
            return;
        }

        #[cfg(not(feature = "bench-no-snapshot"))]
        {
            let now_elapsed_us = self.snapshot_epoch.elapsed().as_micros() as u64;
            let last_snapshot_elapsed_us = self.last_snapshot_elapsed_us.load(Ordering::Relaxed);
            let snapshot_dirty = self.snapshot_dirty.load(Ordering::Relaxed);

            if !schedule_admin_snapshot_due(
                snapshot_dirty,
                force,
                now_elapsed_us,
                last_snapshot_elapsed_us,
            ) {
                return;
            }

            if self
                .snapshot_syncing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                return;
            }

            if !self.snapshot_dirty.swap(false, Ordering::AcqRel) {
                self.snapshot_syncing.store(false, Ordering::Release);
                return;
            }

            self.sync_admin_snapshot();
            self.last_snapshot_elapsed_us.store(
                self.snapshot_epoch.elapsed().as_micros() as u64,
                Ordering::Relaxed,
            );
            self.snapshot_syncing.store(false, Ordering::Release);
        }
    }

    pub(crate) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(true);
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
        let mut schedule_snapshot_dirty = false;

        let response = {
            let mut actors = self.actors.lock();
            let actor = match self.get_or_create_actor(&mut actors, route_family) {
                Ok(actor) => actor,
                Err(error) => {
                    let response = ScheduleResponse::Error(error);
                    let response_bytes = crate::protocol::schedule_codec::encode_response_into(
                        &mut payload_encoder,
                        &response,
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
                    return Ok(());
                }
            };

            match schedule_msg {
                ScheduleMessage::Create {
                    route,
                    cron,
                    payload,
                } => match actor.create_schedule(route, cron, payload) {
                    Ok(changed) => {
                        if changed {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::CreateBatch { entries } => match actor.create_schedules(entries) {
                    Ok(changed) => {
                        if changed > 0 {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::Cancel { route } => match actor.delete_schedule(route) {
                    Ok(removed) => {
                        if removed {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
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
                            .or_insert_with(RoutedSubscriptionSet::new);

                        let existing_sub_id = state.find_existing_id(session_id, pattern.as_str());

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
                            state.insert(
                                family_id,
                                ScheduleSubscription {
                                    pattern: crate::runtime::matcher::Pattern::new(
                                        pattern.as_str(),
                                    ),
                                    session_id,
                                    subscription_id: new_id,
                                    subscriber,
                                },
                            );

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
                            state.remove_session_pattern(family_id, session_id, pattern.as_str());
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

        if schedule_snapshot_dirty {
            self.schedule_admin_snapshot(false);
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
    fn should_replay_pending_schedule_notify_after_restart_given_initial_publish_failure() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/replay/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let initial_sink = Arc::new(ScheduleDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        ));

        initial_sink
            .deliver(Envelope::from_route(
                subscriber_address.clone(),
                schedule_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(700),
                    encode_schedule_create(schedule_route, "* * * * *", b"replay"),
                    family,
                ),
            ))
            .expect("create schedule");

        {
            let mut actors = initial_sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
        }

        // Act
        initial_sink.scan_due_schedules();

        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let restarted_sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", restarted_sink.clone());
        restarted_sink
            .preload_persisted_families()
            .expect("preload persisted families");

        restarted_sink
            .deliver(Envelope::from_route(
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

        restarted_sink.scan_due_schedules();

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
        assert_eq!(notified_payload.as_ref(), b"replay");
        assert!(notify_decoder.is_complete());

        restarted_sink.scan_due_schedules();
        assert!(
            subscriber_mailbox.receiver().try_recv().is_err(),
            "pending fire should be acknowledged after a successful replay"
        );
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
