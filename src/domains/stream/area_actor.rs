//! Area actor: mints area-level offsets and tracks watermarks

use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

use super::constants::{INTERNAL_REALM_SEGMENT, NOTICE_DEBOUNCE_MS, WATERMARK_PERSIST_RETRY_MS};
use super::protocol::{LeaseGranted, StreamCoordinationMessage, DEFAULT_REALM_LEASE_BLOCK};
use super::store::StreamStore;

/// `AreaActor` coordinates area-level offsets and watermark
///
/// Responsibilities:
/// - Mint area offset leases for `StreamActors`
/// - Track committed ranges from `BatchCommitted` notifications
/// - Calculate and advance area watermark (highest contiguous offset)
pub struct AreaActor {
    family_id: RouteFamily,

    /// Realm and area identity
    realm: String,
    area: String,

    /// Storage layer for watermark persistence
    store: Arc<StreamStore>,

    /// Next area offset to assign
    next_area_offset: u64,

    /// Area watermark (highest contiguous committed offset).
    ///
    /// `None` means no offset has committed yet. Offset 0 is a valid committed
    /// watermark because area offsets are minted from zero.
    area_watermark: Option<u64>,

    /// Whether the durable area watermark was loaded successfully.
    watermark_initialized: bool,

    /// Committed ranges from resources (for watermark calculation)
    /// Key: `first_offset`, Value: `last_offset`
    committed_ranges: BTreeMap<u64, u64>,

    /// Realm offset lease (pre-allocated from `RealmActor`)
    realm_lease_next: u64,
    realm_lease_end: u64,

    /// Pending lease requests awaiting realm lease grant
    pending_lease_requests: VecDeque<(String, String, u64, String)>,

    /// Debounce timer id for area watermark notification
    notification_timer: Option<crate::runtime::context::TimerId>,

    /// Pending area watermark publish event (debounced)
    pending_publish: Option<DomainPublishEvent>,

    /// Retry timer for failed watermark persistence attempts
    watermark_retry_timer: Option<crate::runtime::context::TimerId>,
}

impl AreaActor {
    pub fn new(
        family_id: RouteFamily,
        realm: String,
        area: String,
        store: Arc<StreamStore>,
    ) -> Self {
        let (area_watermark, watermark_initialized) = store
            .get_persisted_area_watermark(family_id.as_u64(), &realm, &area)
            .map_or_else(
                |error| {
                    tracing::warn!(
                        domain = "stream",
                        route_family = family_id.id(),
                        realm = realm.as_str(),
                        area = area.as_str(),
                        error = %error,
                        "Stream area watermark actor initialization failed"
                    );
                    (None, false)
                },
                |watermark| (watermark, true),
            );

        Self {
            family_id,
            realm,
            area,
            store,
            next_area_offset: 0,
            area_watermark,
            watermark_initialized,
            committed_ranges: BTreeMap::new(),
            realm_lease_next: 0,
            realm_lease_end: 0,
            pending_lease_requests: VecDeque::new(),
            notification_timer: None,
            pending_publish: None,
            watermark_retry_timer: None,
        }
    }

    /// Get `RouteAddress` for `RealmActor` coordination
    fn realm_actor_address(&self) -> RouteAddress {
        let route = Route::new(format!(
            "stream://{}/{}",
            self.realm, INTERNAL_REALM_SEGMENT
        ));
        RouteAddress::new(self.family_id, route)
    }

    /// Handle `RequestLease` from `StreamActor` and mint paired area+realm offsets
    fn handle_request_lease(
        &mut self,
        realm: &str,
        area: &str,
        count: u64,
        reply_to: &str,
        ctx: &mut Context<Self>,
    ) {
        // Ensure we have sufficient realm lease capacity
        let realm_remaining = self.realm_lease_end.saturating_sub(self.realm_lease_next);

        if realm_remaining < count {
            // Request realm lease from RealmActor
            let lease_size = DEFAULT_REALM_LEASE_BLOCK.max(count);
            let lease_req = StreamCoordinationMessage::RequestRealmLease { count: lease_size };
            let realm_addr = self.realm_actor_address();
            let _ = ctx.send(realm_addr, lease_req);

            // Queue this request to be processed when realm lease arrives
            self.pending_lease_requests.push_back((
                realm.to_string(),
                area.to_string(),
                count,
                reply_to.to_string(),
            ));
            return;
        }

        // Mint paired area+realm ranges with END-EXCLUSIVE semantics
        let area_start = self.next_area_offset;
        let area_end_excl = area_start + count;
        self.next_area_offset = area_end_excl;

        let realm_start = self.realm_lease_next;
        let realm_count = count.min(realm_remaining);
        let realm_end_excl = realm_start + realm_count;
        self.realm_lease_next = realm_end_excl;

        // Build paired grant
        let grant = LeaseGranted {
            area_start,
            area_end_exclusive: area_end_excl,
            realm_start,
            realm_end_exclusive: realm_end_excl,
        };

        // Reply to StreamActor
        let reply_route = Route::new(format!("stream://{realm}/{area}/{reply_to}"));
        let reply_addr = RouteAddress::new(self.family_id, reply_route);
        let _ = ctx.send(
            reply_addr,
            StreamCoordinationMessage::LeaseGranted { grant },
        );
    }

    /// Handle `BatchCommitted` from `StreamActor`
    fn handle_batch_committed(
        &mut self,
        first_area_offset: u64,
        last_area_offset: u64,
        ctx: &mut Context<Self>,
    ) {
        // Record committed range
        self.committed_ranges
            .insert(first_area_offset, last_area_offset);

        self.flush_candidate_watermark(ctx);
    }

    /// Compute the highest contiguous watermark candidate without mutating the
    /// currently visible watermark.
    fn candidate_watermark(&self) -> Option<u64> {
        let mut candidate = self.area_watermark;
        let mut next_offset = self
            .area_watermark
            .map_or(Some(0), |watermark| watermark.checked_add(1))?;

        while let Some(last_offset) = self.committed_ranges.get(&next_offset).copied() {
            candidate = Some(last_offset);
            let Some(following_offset) = last_offset.checked_add(1) else {
                break;
            };
            next_offset = following_offset;
        }

        match candidate {
            Some(candidate) if Some(candidate) != self.area_watermark => Some(candidate),
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

        match self.store.set_watermark(
            self.family_id.as_u64(),
            &self.realm,
            &self.area,
            candidate_watermark,
        ) {
            Ok(()) => self.apply_persisted_watermark(candidate_watermark, ctx),
            Err(error) => {
                tracing::warn!(
                    domain = "stream",
                    route_family = self.family_id.id(),
                    realm = self.realm.as_str(),
                    area = self.area.as_str(),
                    attempted_watermark = candidate_watermark,
                    error = %error,
                    "Stream area watermark persistence failed"
                );
                self.schedule_watermark_retry(ctx);
            }
        }
    }

    fn ensure_watermark_initialized(&mut self, ctx: &mut Context<Self>) -> bool {
        if self.watermark_initialized {
            return true;
        }

        match self.store.get_persisted_area_watermark(
            self.family_id.as_u64(),
            &self.realm,
            &self.area,
        ) {
            Ok(watermark) => {
                self.area_watermark = watermark;
                self.watermark_initialized = true;
                true
            }
            Err(error) => {
                tracing::warn!(
                    domain = "stream",
                    route_family = self.family_id.id(),
                    realm = self.realm.as_str(),
                    area = self.area.as_str(),
                    error = %error,
                    "Stream area watermark actor initialization retry failed"
                );
                self.schedule_watermark_retry(ctx);
                false
            }
        }
    }

    fn apply_persisted_watermark(&mut self, current_watermark: u64, ctx: &mut Context<Self>) {
        let previous_watermark = self.area_watermark.unwrap_or(0);
        self.area_watermark = Some(current_watermark);
        self.committed_ranges
            .retain(|_, last_offset| *last_offset > current_watermark);

        if let Some(timer_id) = self.watermark_retry_timer.take() {
            let _ = ctx.timer_manager().cancel(timer_id);
        }

        let route = Route::new(format!("stream://{}/{}/*/watermark", self.realm, self.area));
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

    /// Update realm lease from `RealmActor` grant
    pub fn update_realm_lease(&mut self, grant: LeaseGranted) {
        self.realm_lease_next = grant.realm_start;
        self.realm_lease_end = grant.realm_end_exclusive; // Already exclusive
    }

    /// Get current watermark (for testing)
    #[cfg(test)]
    pub fn watermark(&self) -> u64 {
        self.area_watermark.unwrap_or(0)
    }

    /// Process pending lease requests after realm lease grant arrives
    fn process_pending_lease_requests(&mut self, ctx: &mut Context<Self>) {
        // Process all pending requests that now have sufficient realm lease
        while let Some((realm, area, count, reply_to)) = self.pending_lease_requests.pop_front() {
            let realm_remaining = self.realm_lease_end.saturating_sub(self.realm_lease_next);

            // If still insufficient, re-queue and stop
            if realm_remaining < count {
                self.pending_lease_requests
                    .push_front((realm, area, count, reply_to));
                break;
            }

            // Process this request (will succeed now)
            self.handle_request_lease(&realm, &area, count, &reply_to, ctx);
        }
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

impl Actor for AreaActor {
    type Message = StreamCoordinationMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamCoordinationMessage::RequestLease {
                realm,
                area,
                count,
                reply_to,
            } => {
                self.handle_request_lease(&realm, &area, count, &reply_to, ctx);
            }
            StreamCoordinationMessage::BatchCommitted(commit) => {
                self.handle_batch_committed(commit.first_area_offset, commit.last_area_offset, ctx);
            }
            StreamCoordinationMessage::LeaseGranted { grant } => {
                // Update realm lease from RealmActor
                self.update_realm_lease(grant);

                // Process any pending lease requests now that we have realm lease
                self.process_pending_lease_requests(ctx);
            }
            StreamCoordinationMessage::RequestRealmLease { .. } => {}
        }
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
        if self.watermark_retry_timer.is_some() && Some(timer_id) == self.watermark_retry_timer {
            self.watermark_retry_timer = None;
            self.flush_candidate_watermark(ctx);
            return;
        }

        // If our debounce timer fired, send the pending publish
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
    use crate::runtime::routing::Route;
    use crate::runtime::Mailbox;

    fn make_test_actor_with_watermark(
        persisted_watermark: Option<u64>,
    ) -> (AreaActor, Context<AreaActor>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(0);
        let addr = RouteAddress::new(family, Route::new("stream://realm1/area1/__area__"));
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
                .set_watermark(family.as_u64(), "realm1", "area1", watermark)
                .expect("persist area watermark");
        }
        let actor = AreaActor::new(family, "realm1".to_string(), "area1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx)
    }

    fn make_test_actor() -> (AreaActor, Context<AreaActor>) {
        make_test_actor_with_watermark(None)
    }

    fn make_test_actor_with_stream_mailbox() -> (AreaActor, Context<AreaActor>, Arc<Mailbox>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(0);
        let stream_mailbox = Arc::new(Mailbox::new(8));
        router.register_domain_pattern("stream", stream_mailbox.clone());
        let addr = RouteAddress::new(family, Route::new("stream://realm1/area1/__area__"));
        let db = Arc::new(
            cntryl_midge::Engine::open(
                cntryl_midge::OpenOptions::in_memory()
                    .build()
                    .expect("build in-memory test options"),
            )
            .expect("Failed to open store"),
        );
        let store = Arc::new(StreamStore::new(db));
        let actor = AreaActor::new(family, "realm1".to_string(), "area1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx, stream_mailbox)
    }

    #[test]
    fn should_mint_area_offsets_from_zero() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Grant realm lease first
        actor.update_realm_lease(LeaseGranted {
            area_start: 0,
            area_end_exclusive: 0,
            realm_start: 0,
            realm_end_exclusive: 1000,
        });

        // Act
        actor.handle_request_lease("realm1", "area1", 10, "stream1", &mut ctx);

        // Assert
        assert_eq!(actor.next_area_offset, 10); // Moved to next block
    }

    #[test]
    fn should_mint_sequential_area_offset_blocks() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Grant realm lease first
        actor.update_realm_lease(LeaseGranted {
            area_start: 0,
            area_end_exclusive: 0,
            realm_start: 0,
            realm_end_exclusive: 1000,
        });

        // Act
        actor.handle_request_lease("realm1", "area1", 5, "stream1", &mut ctx);
        actor.handle_request_lease("realm1", "area1", 3, "stream2", &mut ctx);

        // Assert
        assert_eq!(actor.next_area_offset, 8); // 5 + 3
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
        assert_eq!(actor.area_watermark, Some(6));
        assert!(actor.committed_ranges().is_empty());
    }

    #[test]
    fn should_advance_watermark_given_first_committed_offset_zero() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_batch_committed(0, 0, &mut ctx);

        // Assert
        assert_eq!(actor.area_watermark, Some(0));
        assert_eq!(actor.watermark(), 0);
        assert!(actor.committed_ranges().is_empty());
    }

    #[test]
    fn should_not_advance_watermark_with_gap() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_batch_committed(0, 3, &mut ctx);
        actor.handle_batch_committed(6, 8, &mut ctx); // Gap at 4-5

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

        // Act - Commit contiguous ranges
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
    fn should_retry_failed_area_watermark_persistence_before_publish() {
        // Arrange
        let (mut actor, mut ctx, stream_mailbox) = make_test_actor_with_stream_mailbox();
        StreamStore::fail_next_area_watermark_persist_for_tests();

        // Act
        actor.handle_batch_committed(0, 3, &mut ctx);

        // Assert
        assert_eq!(actor.area_watermark, None);
        assert!(actor.notification_timer.is_none());
        assert!(actor.pending_publish.is_none());
        assert!(actor.watermark_retry_timer.is_some());
        assert!(stream_mailbox.receiver().try_recv().is_err());

        // Act
        assert_eq!(actor.candidate_watermark(), Some(3));
        assert_eq!(
            actor
                .store
                .get_watermark(actor.family_id.as_u64(), "realm1", "area1")
                .expect("read initial area watermark"),
            0
        );
        actor.flush_candidate_watermark(&mut ctx);

        // Assert
        assert_eq!(
            actor
                .store
                .get_watermark(actor.family_id.as_u64(), "realm1", "area1")
                .expect("read persisted area watermark"),
            3
        );
        assert_eq!(actor.area_watermark, Some(3));
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
            .expect("expected published watermark event");
        let published_event = publish
            .payload::<crate::runtime::domain_event::DomainPublishEvent>()
            .expect("expected publish event payload");
        assert_eq!(
            published_event.route.as_str(),
            "stream://realm1/area1/*/watermark"
        );
    }
}
