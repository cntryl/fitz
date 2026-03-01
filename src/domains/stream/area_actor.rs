//! Area actor: mints area-level offsets and tracks watermarks

use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

use super::protocol::{
    AreaWatermarkAdvanced, LeaseGrant, StreamMessage, DEFAULT_REALM_LEASE_BLOCK,
};
use super::store::StreamStore;

/// AreaActor coordinates area-level offsets and watermark
///
/// Responsibilities:
/// - Mint area offset leases for StreamActors
/// - Track committed ranges from BatchCommitted notifications
/// - Calculate and advance area watermark (highest contiguous offset)
/// - Notify RealmActor when watermark advances
pub struct AreaActor {
    #[allow(dead_code)]
    family_id: RouteFamily,

    /// Realm and area identity
    realm: String,
    area: String,

    /// Storage layer for watermark persistence
    store: Arc<StreamStore>,

    /// Next area offset to assign
    next_area_offset: u64,

    /// Area watermark (highest contiguous committed offset)
    area_watermark: u64,

    /// Committed ranges from resources (for watermark calculation)
    /// Key: first_offset, Value: last_offset
    committed_ranges: BTreeMap<u64, u64>,

    /// Realm offset lease (pre-allocated from RealmActor)
    realm_lease_next: u64,
    realm_lease_end: u64,

    /// Pending lease requests awaiting realm lease grant
    pending_lease_requests: VecDeque<(String, String, u64, String)>,

    /// Debounce timer id for area watermark notification
    notification_timer: Option<crate::runtime::context::TimerId>,

    /// Pending area watermark publish event (debounced)
    pending_publish: Option<DomainPublishEvent>,
}

impl AreaActor {
    const NOTICE_DEBOUNCE_MS: u64 = 25;

    pub fn new(
        family_id: RouteFamily,
        realm: String,
        area: String,
        store: Arc<StreamStore>,
    ) -> Self {
        Self {
            family_id,
            realm,
            area,
            store,
            next_area_offset: 0,
            area_watermark: 0,
            committed_ranges: BTreeMap::new(),
            realm_lease_next: 0,
            realm_lease_end: 0,
            pending_lease_requests: VecDeque::new(),
            notification_timer: None,
            pending_publish: None,
        }
    }

    /// Get RouteAddress for RealmActor coordination
    fn realm_actor_address(&self) -> RouteAddress {
        let route = Route::new(format!("stream://{}/__realm__", self.realm));
        RouteAddress::new(self.family_id, route)
    }

    /// Handle RequestLease from StreamActor and mint paired area+realm offsets
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
            let lease_req = StreamMessage::RequestRealmLease { count: lease_size };
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
        let grant = LeaseGrant {
            area_start,
            area_end_exclusive: area_end_excl,
            realm_start,
            realm_end_exclusive: realm_end_excl,
        };

        // Reply to StreamActor
        let reply_route = Route::new(format!("stream://{}/{}/{}", realm, area, reply_to));
        let reply_addr = RouteAddress::new(self.family_id, reply_route);
        let _ = ctx.send(reply_addr, StreamMessage::LeaseGranted { grant });
    }

    /// Handle BatchCommitted from StreamActor
    fn handle_batch_committed(
        &mut self,
        first_area_offset: u64,
        last_area_offset: u64,
        ctx: &mut Context<Self>,
    ) {
        // Record committed range
        self.committed_ranges
            .insert(first_area_offset, last_area_offset);

        // Try to advance watermark
        let old_watermark = self.area_watermark;
        self.advance_watermark();

        // Persist watermark and notify RealmActor if watermark advanced
        if self.area_watermark > old_watermark {
            // Persist watermark to storage
            let _ = self.store.set_watermark(
                self.family_id.as_u64(),
                &self.realm,
                &self.area,
                self.area_watermark,
            );

            // Build area watermark publish message (debounced, best-effort)
            let route_str = format!("stream://{}/{}/*/watermark", self.realm, self.area);
            let route = Route::new(route_str);
            let payload_json = serde_json::json!({
                "previous": old_watermark,
                "watermark": self.area_watermark,
                "ts": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            });
            let payload = Bytes::from(payload_json.to_string());
            let publish_event = DomainPublishEvent::new(self.family_id, route, payload);

            // Store and debounce the publish (do not send immediately)
            self.pending_publish = Some(publish_event);
            if self.notification_timer.is_none() {
                let timer_id = ctx
                    .timer_manager()
                    .schedule_once(std::time::Duration::from_millis(Self::NOTICE_DEBOUNCE_MS));
                self.notification_timer = Some(timer_id);
            }

            // Notify RealmActor
            let notification = StreamMessage::AreaWatermarkAdvanced(AreaWatermarkAdvanced {
                area: self.area.clone(),
                watermark: self.area_watermark,
            });
            let realm_addr = self.realm_actor_address();
            let _ = ctx.send(realm_addr, notification);
        }
    }

    /// Advance watermark to highest contiguous offset (NO GAP SKIPPING)
    ///
    /// **CRITICAL**: Watermark can only advance when there is a range whose
    /// start == watermark + 1 (strict contiguity).
    ///
    /// This prevents bridging gaps where offsets are missing.
    ///
    /// **OPTIMIZATION**: Uses BTreeMap::first_entry() for O(1) lookup when
    /// ranges are ordered, avoiding full iteration.
    fn advance_watermark(&mut self) {
        loop {
            let next_offset = self.area_watermark + 1;

            // Fast path: Check if first range starts at watermark+1
            // BTreeMap keeps keys sorted, so first_entry is O(log N)
            let found_range = self
                .committed_ranges
                .iter()
                .find(|(&first, _)| first == next_offset)
                .map(|(&first, &last)| (first, last));

            if let Some((first, last)) = found_range {
                // Advance watermark to end of this range
                self.area_watermark = last;

                // Remove this consumed range
                self.committed_ranges.remove(&first);
            } else {
                // No contiguous range found, stop
                break;
            }
        }

        // Clean up any ranges that are now behind the watermark
        self.committed_ranges
            .retain(|_, last| *last > self.area_watermark);
    }

    /// Update realm lease from RealmActor grant
    pub fn update_realm_lease(&mut self, grant: LeaseGrant) {
        self.realm_lease_next = grant.realm_start;
        self.realm_lease_end = grant.realm_end_exclusive; // Already exclusive
    }

    /// Get remaining realm lease capacity
    #[allow(dead_code)]
    pub fn realm_lease_remaining(&self) -> u64 {
        self.realm_lease_end.saturating_sub(self.realm_lease_next)
    }

    /// Get current watermark (for testing)
    pub fn watermark(&self) -> u64 {
        self.area_watermark
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
    #[allow(dead_code)]
    pub fn committed_ranges(&self) -> Vec<std::ops::RangeInclusive<u64>> {
        self.committed_ranges
            .iter()
            .map(|(&first, &last)| first..=last)
            .collect()
    }
}

impl Actor for AreaActor {
    type Message = StreamMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::RequestLease {
                realm,
                area,
                count,
                reply_to,
            } => {
                self.handle_request_lease(&realm, &area, count, &reply_to, ctx);
            }
            StreamMessage::BatchCommitted(commit) => {
                self.handle_batch_committed(commit.first_area_offset, commit.last_area_offset, ctx);
            }
            StreamMessage::LeaseGranted { grant } => {
                // Update realm lease from RealmActor
                self.update_realm_lease(grant);

                // Process any pending lease requests now that we have realm lease
                self.process_pending_lease_requests(ctx);
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
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

    fn make_test_actor() -> (AreaActor, Context<AreaActor>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(1);
        let addr = RouteAddress::new(family, Route::new("stream://realm1/area1/__area__"));
        let db = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open store"),
        );
        let store = Arc::new(StreamStore::new(db));
        let actor = AreaActor::new(family, "realm1".to_string(), "area1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx)
    }

    #[test]
    fn should_mint_area_offsets_from_zero() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Grant realm lease first
        actor.update_realm_lease(LeaseGrant {
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
        actor.update_realm_lease(LeaseGrant {
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

        // Act - Commit range [1,3] (watermark starts at 0, advances to 3)
        actor.handle_batch_committed(1, 3, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 3);
    }

    #[test]
    fn should_not_advance_watermark_with_gap() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act - Commit [1,3] then [6,8] (gap at 4-5)
        actor.handle_batch_committed(1, 3, &mut ctx);
        actor.handle_batch_committed(6, 8, &mut ctx); // Gap at 4-5

        // Assert
        assert_eq!(actor.watermark(), 3); // Stuck at 3 due to gap
    }

    #[test]
    fn should_advance_watermark_when_gap_filled() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act - Create gap then fill it
        actor.handle_batch_committed(1, 3, &mut ctx);
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
        actor.handle_batch_committed(1, 3, &mut ctx);
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
        actor.handle_batch_committed(1, 3, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 3);
        assert!(!actor.committed_ranges().is_empty()); // Buffered future ranges
    }
}
