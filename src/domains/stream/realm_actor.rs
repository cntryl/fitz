//! Realm actor: mints realm-level offsets and aggregates watermarks

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteFamily};

use super::constants::{NOTICE_DEBOUNCE_MS, WATERMARK_PERSIST_RETRY_MS};
use super::protocol::{LeaseGranted, StreamCoordinationMessage};
use super::store::StreamStore;

/// `RealmActor` coordinates realm-level offsets and aggregates watermarks
///
/// Responsibilities:
/// - Mint realm offset leases for `AreaActors`
/// - Track area watermarks
/// - Calculate realm watermark (minimum of all area watermarks)
#[allow(dead_code)]
pub struct RealmActor {
    #[allow(dead_code)]
    family_id: RouteFamily,

    /// Realm identity
    realm: String,

    /// Storage layer for watermark persistence
    store: Arc<StreamStore>,

    /// Next realm offset to assign
    next_realm_offset: u64,

    /// Area watermarks (for realm watermark calculation)
    /// Key: area name, Value: watermark
    area_watermarks: HashMap<String, u64>,

    /// Realm watermark (minimum of all observed area watermarks).
    ///
    /// `None` means no area has reported a committed watermark yet. Offset 0 is
    /// a valid committed watermark because realm offsets are minted from zero.
    realm_watermark: Option<u64>,

    /// Debounce timer id for realm watermark notification
    notification_timer: Option<crate::runtime::context::TimerId>,

    /// Pending realm watermark publish event (debounced)
    pending_publish: Option<DomainPublishEvent>,

    /// Retry timer for failed realm watermark persistence attempts
    watermark_retry_timer: Option<crate::runtime::context::TimerId>,
}

#[allow(dead_code)]
impl RealmActor {
    pub fn new(family_id: RouteFamily, realm: String, store: Arc<StreamStore>) -> Self {
        Self {
            family_id,
            realm,
            store,
            next_realm_offset: 0,
            area_watermarks: HashMap::new(),
            realm_watermark: None,
            notification_timer: None,
            pending_publish: None,
            watermark_retry_timer: None,
        }
    }

    /// Grant realm offset lease to `AreaActor`
    fn handle_request_realm_lease(&mut self, count: u64, _ctx: &mut Context<Self>) -> LeaseGranted {
        let start = self.next_realm_offset;
        let end_excl = start + count;
        self.next_realm_offset = end_excl;

        LeaseGranted {
            area_start: 0, // Will be filled by AreaActor
            area_end_exclusive: 0,
            realm_start: start,
            realm_end_exclusive: end_excl, // End-exclusive
        }
    }

    /// Handle `AreaWatermarkAdvanced` from `AreaActor`
    fn handle_area_watermark_advanced(
        &mut self,
        area: String,
        watermark: u64,
        ctx: &mut Context<Self>,
    ) {
        // Update area watermark
        self.area_watermarks.insert(area, watermark);

        self.flush_candidate_watermark(ctx);
    }

    /// Compute the next visible realm watermark candidate without mutating the
    /// current persisted view.
    fn candidate_watermark(&self) -> Option<u64> {
        let candidate = self.area_watermarks.values().copied().min()?;
        match self.realm_watermark {
            None => Some(candidate),
            Some(current) if candidate > current => Some(candidate),
            Some(_) => None,
        }
    }

    fn flush_candidate_watermark(&mut self, ctx: &mut Context<Self>) {
        let Some(candidate_watermark) = self.candidate_watermark() else {
            return;
        };

        match self.store.set_realm_watermark(
            self.family_id.as_u64(),
            &self.realm,
            candidate_watermark,
        ) {
            Ok(()) => self.apply_persisted_watermark(candidate_watermark, ctx),
            Err(error) => {
                tracing::warn!(
                    domain = "stream",
                    route_family = self.family_id.id(),
                    realm = self.realm.as_str(),
                    attempted_watermark = candidate_watermark,
                    error = %error,
                    "Stream realm watermark persistence failed"
                );
                self.schedule_watermark_retry(ctx);
            }
        }
    }

    fn apply_persisted_watermark(&mut self, current_watermark: u64, ctx: &mut Context<Self>) {
        let previous_watermark = self.realm_watermark.unwrap_or(0);
        self.realm_watermark = Some(current_watermark);

        if let Some(timer_id) = self.watermark_retry_timer.take() {
            let _ = ctx.timer_manager().cancel(timer_id);
        }

        let route = Route::new(format!("stream://{}/*/*/watermark", self.realm));
        let payload_json = serde_json::json!({
            "previous": previous_watermark,
            "watermark": current_watermark,
            "ts": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });
        self.pending_publish = Some(DomainPublishEvent::new(
            self.family_id,
            route,
            Bytes::from(payload_json.to_string()),
        ));
        if self.notification_timer.is_none() {
            let timer_id = ctx
                .timer_manager()
                .schedule_once(std::time::Duration::from_millis(NOTICE_DEBOUNCE_MS));
            self.notification_timer = Some(timer_id);
        }
    }

    fn schedule_watermark_retry(&mut self, ctx: &mut Context<Self>) {
        if self.watermark_retry_timer.is_some() {
            return;
        }

        let timer_id = ctx
            .timer_manager()
            .schedule_once(std::time::Duration::from_millis(WATERMARK_PERSIST_RETRY_MS));
        self.watermark_retry_timer = Some(timer_id);
    }

    /// Get current realm watermark (for testing)
    pub fn watermark(&self) -> u64 {
        self.realm_watermark.unwrap_or(0)
    }

    /// Get area watermarks (for testing)
    #[cfg(test)]
    pub fn area_watermarks(&self) -> &HashMap<String, u64> {
        &self.area_watermarks
    }
}

impl Actor for RealmActor {
    type Message = StreamCoordinationMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamCoordinationMessage::RequestRealmLease { count, .. } => {
                let _ = self.handle_request_realm_lease(count, ctx);
            }
            StreamCoordinationMessage::AreaWatermarkAdvanced(adv) => {
                self.handle_area_watermark_advanced(adv.area, adv.watermark, ctx);
            }
            StreamCoordinationMessage::RequestLease { .. }
            | StreamCoordinationMessage::LeaseGranted { .. }
            | StreamCoordinationMessage::BatchCommitted(_) => {}
        }
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
        if self.watermark_retry_timer.is_some() && Some(timer_id) == self.watermark_retry_timer {
            self.watermark_retry_timer = None;
            self.flush_candidate_watermark(ctx);
            return;
        }

        if self.notification_timer.is_some() && Some(timer_id) == self.notification_timer {
            if let Some(event) = self.pending_publish.take() {
                let _ = ctx.publish_event(event);
            }
            self.notification_timer = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteAddress};
    use crate::runtime::Mailbox;

    fn make_test_actor() -> (RealmActor, Context<RealmActor>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(0);
        let addr = RouteAddress::new(family, Route::new("stream://realm1/__realm__"));
        let db = Arc::new(
            cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
                .expect("Failed to open store"),
        );
        let store = Arc::new(StreamStore::new(db));
        let actor = RealmActor::new(family, "realm1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx)
    }

    fn make_test_actor_with_stream_mailbox() -> (RealmActor, Context<RealmActor>, Arc<Mailbox>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(0);
        let stream_mailbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("stream", stream_mailbox.clone());
        let addr = RouteAddress::new(family, Route::new("stream://realm1/__realm__"));
        let db = Arc::new(
            cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
                .expect("Failed to open store"),
        );
        let store = Arc::new(StreamStore::new(db));
        let actor = RealmActor::new(family, "realm1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx, stream_mailbox)
    }

    #[test]
    fn should_mint_realm_offsets_from_zero() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        let grant = actor.handle_request_realm_lease(100, &mut ctx);

        // Assert
        assert_eq!(grant.realm_start, 0);
        assert_eq!(grant.realm_end_exclusive, 100);
        assert_eq!(actor.next_realm_offset, 100);
    }

    #[test]
    fn should_mint_sequential_realm_offset_blocks() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        let grant1 = actor.handle_request_realm_lease(50, &mut ctx);
        let grant2 = actor.handle_request_realm_lease(30, &mut ctx);

        // Assert
        assert_eq!(grant1.realm_start, 0);
        assert_eq!(grant1.realm_end_exclusive, 50);
        assert_eq!(grant2.realm_start, 50);
        assert_eq!(grant2.realm_end_exclusive, 80);
        assert_eq!(actor.next_realm_offset, 80);
    }

    #[test]
    fn should_calculate_realm_watermark_as_minimum() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area2".to_string(), 50, &mut ctx);
        actor.handle_area_watermark_advanced("area1".to_string(), 100, &mut ctx);
        actor.handle_area_watermark_advanced("area3".to_string(), 75, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 50); // Minimum of all areas
    }

    #[test]
    fn should_update_realm_watermark_when_minimum_advances() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area2".to_string(), 50, &mut ctx);
        actor.handle_area_watermark_advanced("area1".to_string(), 100, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 50);

        // Advance area2 (the minimum)
        actor.handle_area_watermark_advanced("area2".to_string(), 75, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 75); // Now advances to 75
    }

    #[test]
    fn should_record_first_realm_watermark_at_offset_zero() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area1".to_string(), 0, &mut ctx);

        // Assert
        assert_eq!(actor.realm_watermark, Some(0));
        assert_eq!(actor.watermark(), 0);
    }

    #[test]
    fn should_not_regress_realm_watermark_given_new_lower_area() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();
        actor.handle_area_watermark_advanced("area1".to_string(), 100, &mut ctx);

        // Act
        actor.handle_area_watermark_advanced("area2".to_string(), 50, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 100);
        assert_eq!(*actor.area_watermarks().get("area2").expect("area2"), 50);
    }

    #[test]
    fn should_not_advance_realm_watermark_if_not_minimum() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area1".to_string(), 50, &mut ctx);
        actor.handle_area_watermark_advanced("area2".to_string(), 100, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 50);

        // Advance area2 (not the minimum)
        actor.handle_area_watermark_advanced("area2".to_string(), 150, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 50); // Still 50, unchanged
    }

    #[test]
    fn should_track_multiple_area_watermarks() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area1".to_string(), 10, &mut ctx);
        actor.handle_area_watermark_advanced("area2".to_string(), 20, &mut ctx);
        actor.handle_area_watermark_advanced("area3".to_string(), 15, &mut ctx);

        // Assert
        assert_eq!(actor.area_watermarks().len(), 3);
        assert_eq!(*actor.area_watermarks().get("area1").unwrap(), 10);
        assert_eq!(*actor.area_watermarks().get("area2").unwrap(), 20);
        assert_eq!(*actor.area_watermarks().get("area3").unwrap(), 15);
    }

    #[test]
    fn should_handle_single_area() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area1".to_string(), 100, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 100); // Single area, watermark equals area watermark
    }

    #[test]
    fn should_retry_failed_realm_watermark_persistence_before_publish() {
        // Arrange
        let (mut actor, mut ctx, stream_mailbox) = make_test_actor_with_stream_mailbox();
        StreamStore::fail_next_realm_watermark_persist_for_tests();

        // Act
        actor.handle_area_watermark_advanced("area1".to_string(), 3, &mut ctx);

        // Assert
        assert_eq!(actor.realm_watermark, None);
        assert!(actor.notification_timer.is_none());
        assert!(actor.pending_publish.is_none());
        assert!(actor.watermark_retry_timer.is_some());
        assert!(stream_mailbox.receiver().try_recv().is_err());

        // Act
        assert_eq!(actor.candidate_watermark(), Some(3));
        assert_eq!(
            actor
                .store
                .get_realm_watermark(actor.family_id.as_u64(), "realm1")
                .expect("read initial realm watermark"),
            0
        );
        actor.flush_candidate_watermark(&mut ctx);

        // Assert
        assert_eq!(
            actor
                .store
                .get_realm_watermark(actor.family_id.as_u64(), "realm1")
                .expect("read persisted realm watermark"),
            3
        );
        assert_eq!(actor.realm_watermark, Some(3));
        assert!(actor.watermark_retry_timer.is_none());
        assert!(stream_mailbox.receiver().try_recv().is_err());

        // Act
        let notification_timer = actor
            .notification_timer
            .expect("notification timer should be scheduled");
        actor.on_timer(notification_timer, &mut ctx);

        // Assert
        let publish = stream_mailbox
            .receiver()
            .try_recv()
            .expect("expected published realm watermark event");
        let published_event = publish
            .payload::<crate::runtime::domain_event::DomainPublishEvent>()
            .expect("expected publish event payload");
        assert_eq!(
            published_event.route.as_str(),
            "stream://realm1/*/*/watermark"
        );
    }
}
