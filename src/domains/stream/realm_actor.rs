//! Realm actor: mints realm-level offsets and tracks the realm watermark

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteFamily};

use super::constants::{NOTICE_DEBOUNCE_MS, WATERMARK_PERSIST_RETRY_MS};
use super::protocol::{LeaseGranted, StreamCoordinationMessage};
use super::store::StreamStore;

/// `RealmActor` coordinates realm-level offsets and tracks the realm watermark
///
/// Responsibilities:
/// - Mint realm offset leases for `AreaActors`
/// - Track committed realm-wide ranges from `BatchCommitted` notifications
/// - Calculate and advance the realm watermark (highest contiguous
///   *realm-wide* offset), mirroring `AreaActor`'s own contiguous-range
///   algorithm but over the realm-wide offset space rather than area-local.
///
/// Realm-wide offsets are assigned by one counter shared across every area in
/// the realm, so the realm watermark cannot be derived by aggregating each
/// area's own area-local watermark (those are a different, incomparable
/// numbering space) — it must track realm-wide contiguity directly, the same
/// way `AreaActor` tracks area-local contiguity.
pub struct RealmActor {
    family_id: RouteFamily,

    /// Realm identity
    realm: String,

    /// Storage layer for watermark persistence
    store: Arc<StreamStore>,

    /// Next realm offset to assign
    next_realm_offset: u64,

    /// Realm watermark (highest contiguous committed realm-wide offset).
    ///
    /// `None` means no offset has committed yet. Offset 0 is a valid
    /// committed watermark because realm offsets are minted from zero.
    realm_watermark: Option<u64>,

    /// Whether the durable realm watermark was loaded successfully.
    watermark_initialized: bool,

    /// Committed realm-wide ranges from `BatchCommitted` (for watermark
    /// calculation). Key: `first_realm_offset`, Value: `last_realm_offset`.
    committed_ranges: BTreeMap<u64, u64>,

    /// Debounce timer id for realm watermark notification
    notification_timer: Option<crate::runtime::context::TimerId>,

    /// Pending realm watermark publish event (debounced)
    pending_publish: Option<DomainPublishEvent>,

    /// Retry timer for failed realm watermark persistence attempts
    watermark_retry_timer: Option<crate::runtime::context::TimerId>,
}

impl RealmActor {
    pub fn new(family_id: RouteFamily, realm: String, store: Arc<StreamStore>) -> Self {
        let (realm_watermark, watermark_initialized) =
            match store.get_persisted_realm_watermark(family_id.as_u64(), &realm) {
                Ok(watermark) => (watermark, true),
                Err(error) => {
                    tracing::warn!(
                        domain = "stream",
                        route_family = family_id.id(),
                        realm = realm.as_str(),
                        error = %error,
                        "Stream realm watermark actor initialization failed"
                    );
                    (None, false)
                }
            };

        Self {
            family_id,
            realm,
            store,
            next_realm_offset: 0,
            realm_watermark,
            watermark_initialized,
            committed_ranges: BTreeMap::new(),
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

    /// Handle `BatchCommitted` from `StreamActor`
    fn handle_batch_committed(
        &mut self,
        first_realm_offset: u64,
        last_realm_offset: u64,
        ctx: &mut Context<Self>,
    ) {
        self.committed_ranges
            .insert(first_realm_offset, last_realm_offset);

        self.flush_candidate_watermark(ctx);
    }

    /// Compute the highest contiguous watermark candidate without mutating
    /// the currently visible watermark.
    fn candidate_watermark(&self) -> Option<u64> {
        let mut candidate = self.realm_watermark;
        let mut next_offset = self
            .realm_watermark
            .map_or(Some(0), |watermark| watermark.checked_add(1))?;

        while let Some(last_offset) = self.committed_ranges.get(&next_offset).copied() {
            candidate = Some(last_offset);
            let Some(following_offset) = last_offset.checked_add(1) else {
                break;
            };
            next_offset = following_offset;
        }

        match candidate {
            Some(candidate) if Some(candidate) != self.realm_watermark => Some(candidate),
            _ => None,
        }
    }

    fn flush_candidate_watermark(&mut self, ctx: &mut Context<Self>) {
        if !self.ensure_watermark_initialized(ctx) {
            return;
        }

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

    fn ensure_watermark_initialized(&mut self, ctx: &mut Context<Self>) -> bool {
        if self.watermark_initialized {
            return true;
        }

        match self
            .store
            .get_persisted_realm_watermark(self.family_id.as_u64(), &self.realm)
        {
            Ok(watermark) => {
                self.realm_watermark = watermark;
                self.watermark_initialized = true;
                true
            }
            Err(error) => {
                tracing::warn!(
                    domain = "stream",
                    route_family = self.family_id.id(),
                    realm = self.realm.as_str(),
                    error = %error,
                    "Stream realm watermark actor initialization retry failed"
                );
                self.schedule_watermark_retry(ctx);
                false
            }
        }
    }

    fn apply_persisted_watermark(&mut self, current_watermark: u64, ctx: &mut Context<Self>) {
        let previous_watermark = self.realm_watermark.unwrap_or(0);
        self.realm_watermark = Some(current_watermark);
        self.committed_ranges
            .retain(|_, last_offset| *last_offset > current_watermark);

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
    #[cfg(test)]
    pub fn watermark(&self) -> u64 {
        self.realm_watermark.unwrap_or(0)
    }

    /// Get committed ranges (for testing)
    #[cfg(test)]
    pub fn committed_ranges(&self) -> Vec<std::ops::RangeInclusive<u64>> {
        self.committed_ranges
            .iter()
            .map(|(&first, &last)| first..=last)
            .collect()
    }
}

impl Actor for RealmActor {
    type Message = StreamCoordinationMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamCoordinationMessage::RequestRealmLease { count, .. } => {
                let _ = self.handle_request_realm_lease(count, ctx);
            }
            StreamCoordinationMessage::BatchCommitted(commit) => {
                self.handle_batch_committed(
                    commit.first_realm_offset,
                    commit.last_realm_offset,
                    ctx,
                );
            }
            StreamCoordinationMessage::RequestLease { .. }
            | StreamCoordinationMessage::LeaseGranted { .. } => {}
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

    fn make_test_actor_with_watermark(
        persisted_watermark: Option<u64>,
    ) -> (RealmActor, Context<RealmActor>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(0);
        let addr = RouteAddress::new(family, Route::new("stream://realm1/__realm__"));
        let db = Arc::new(
            cntryl_midge::Engine::open(
                cntryl_midge::OpenOptions::in_memory()
                    .build()
                    .expect("build in-memory test options"),
            )
            .expect("Failed to open store"),
        );
        let store = Arc::new(StreamStore::new(db));
        if let Some(watermark) = persisted_watermark {
            store
                .set_realm_watermark(family.as_u64(), "realm1", watermark)
                .expect("persist realm watermark");
        }
        let actor = RealmActor::new(family, "realm1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx)
    }

    fn make_test_actor() -> (RealmActor, Context<RealmActor>) {
        make_test_actor_with_watermark(None)
    }

    fn make_test_actor_with_stream_mailbox() -> (RealmActor, Context<RealmActor>, Arc<Mailbox>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(0);
        let stream_mailbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("stream", stream_mailbox.clone());
        let addr = RouteAddress::new(family, Route::new("stream://realm1/__realm__"));
        let db = Arc::new(
            cntryl_midge::Engine::open(
                cntryl_midge::OpenOptions::in_memory()
                    .build()
                    .expect("build in-memory test options"),
            )
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
    fn should_advance_watermark_on_contiguous_commit() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_batch_committed(0, 3, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 3);
    }

    #[test]
    fn should_advance_from_persisted_watermark_after_restart() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor_with_watermark(Some(5));

        // Act
        actor.handle_batch_committed(6, 6, &mut ctx);

        // Assert
        assert_eq!(actor.realm_watermark, Some(6));
        assert!(actor.committed_ranges().is_empty());
    }

    #[test]
    fn should_advance_watermark_given_first_committed_offset_zero() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_batch_committed(0, 0, &mut ctx);

        // Assert
        assert_eq!(actor.realm_watermark, Some(0));
        assert_eq!(actor.watermark(), 0);
        assert!(actor.committed_ranges().is_empty());
    }

    #[test]
    fn should_not_advance_watermark_with_gap() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_batch_committed(0, 3, &mut ctx);
        actor.handle_batch_committed(6, 8, &mut ctx); // Gap at 4-5 (e.g. a
                                                      // different area's
                                                      // commit still
                                                      // in-flight)

        // Assert
        assert_eq!(actor.watermark(), 3); // Stuck at 3 due to gap
    }

    #[test]
    fn should_advance_watermark_when_gap_filled() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act - Create gap then fill it
        actor.handle_batch_committed(0, 3, &mut ctx);
        actor.handle_batch_committed(6, 8, &mut ctx);
        actor.handle_batch_committed(4, 5, &mut ctx); // Fill gap

        // Assert
        assert_eq!(actor.watermark(), 8); // Now advanced to 8
    }

    #[test]
    fn should_clean_up_consumed_ranges() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act - Commit contiguous ranges from two different areas sharing
        // this realm's offset space
        actor.handle_batch_committed(0, 3, &mut ctx);
        actor.handle_batch_committed(4, 6, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 6);
        assert_eq!(actor.committed_ranges().len(), 0); // All consumed
    }

    #[test]
    fn should_buffer_out_of_order_ranges() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act - Commit in reverse order
        actor.handle_batch_committed(11, 13, &mut ctx);
        actor.handle_batch_committed(6, 8, &mut ctx);
        actor.handle_batch_committed(0, 3, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 3);
        assert!(!actor.committed_ranges().is_empty()); // Buffered future ranges
    }

    #[test]
    fn should_retry_failed_realm_watermark_persistence_before_publish() {
        // Arrange
        let (mut actor, mut ctx, stream_mailbox) = make_test_actor_with_stream_mailbox();
        StreamStore::fail_next_realm_watermark_persist_for_tests();

        // Act
        actor.handle_batch_committed(0, 3, &mut ctx);

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
