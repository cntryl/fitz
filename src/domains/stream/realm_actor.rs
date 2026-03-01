//! Realm actor: mints realm-level offsets and aggregates watermarks

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteFamily};

use super::protocol::{LeaseGrant, StreamMessage};
use super::store::StreamStore;

/// RealmActor coordinates realm-level offsets and aggregates watermarks
///
/// Responsibilities:
/// - Mint realm offset leases for AreaActors
/// - Track area watermarks
/// - Calculate realm watermark (minimum of all area watermarks)
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

    /// Realm watermark (minimum of all area watermarks)
    realm_watermark: u64,

    /// Debounce timer id for realm watermark notification
    notification_timer: Option<crate::runtime::context::TimerId>,

    /// Pending realm watermark publish event (debounced)
    pending_publish: Option<DomainPublishEvent>,
}

impl RealmActor {
    const NOTICE_DEBOUNCE_MS: u64 = 25;

    pub fn new(family_id: RouteFamily, realm: String, store: Arc<StreamStore>) -> Self {
        Self {
            family_id,
            realm,
            store,
            next_realm_offset: 0,
            area_watermarks: HashMap::new(),
            realm_watermark: 0,
            notification_timer: None,
            pending_publish: None,
        }
    }

    /// Grant realm offset lease to AreaActor
    fn handle_request_realm_lease(&mut self, count: u64, _ctx: &mut Context<Self>) -> LeaseGrant {
        let start = self.next_realm_offset;
        let end_excl = start + count;
        self.next_realm_offset = end_excl;

        LeaseGrant {
            area_start: 0, // Will be filled by AreaActor
            area_end_exclusive: 0,
            realm_start: start,
            realm_end_exclusive: end_excl, // End-exclusive
        }
    }

    /// Handle AreaWatermarkAdvanced from AreaActor
    fn handle_area_watermark_advanced(
        &mut self,
        area: String,
        watermark: u64,
        ctx: &mut Context<Self>,
    ) {
        // Update area watermark
        self.area_watermarks.insert(area, watermark);

        // Recalculate realm watermark (minimum of all areas)
        self.recalculate_realm_watermark(ctx);
    }

    /// Recalculate realm watermark as minimum of all area watermarks
    fn recalculate_realm_watermark(&mut self, ctx: &mut Context<Self>) {
        if self.area_watermarks.is_empty() {
            self.realm_watermark = 0;
            return;
        }

        let old_watermark = self.realm_watermark;

        // Realm watermark is the minimum of all area watermarks
        // This ensures we only commit realm offsets when all areas have caught up
        let new_watermark = *self.area_watermarks.values().min().unwrap_or(&0);

        self.realm_watermark = new_watermark;

        // Emit realm watermark notification ONLY if watermark advanced
        if new_watermark > old_watermark {
            // Persist realm watermark to storage
            let _ =
                self.store
                    .set_realm_watermark(self.family_id.as_u64(), &self.realm, new_watermark);

            let route_str = format!("stream://{}/*/*/watermark", self.realm);
            let route = Route::new(route_str);
            let payload_json = serde_json::json!({
                "previous": old_watermark,
                "watermark": new_watermark,
                "ts": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            });
            let payload = Bytes::from(payload_json.to_string());
            let publish_event = DomainPublishEvent::new(self.family_id, route, payload);

            // Debounce realm watermark publish (do not send immediately)
            self.pending_publish = Some(publish_event);
            if self.notification_timer.is_none() {
                let timer_id = ctx
                    .timer_manager()
                    .schedule_once(std::time::Duration::from_millis(Self::NOTICE_DEBOUNCE_MS));
                self.notification_timer = Some(timer_id);
            }
        }
    }

    /// Get current realm watermark (for testing)
    pub fn watermark(&self) -> u64 {
        self.realm_watermark
    }

    /// Get area watermarks (for testing)
    #[cfg(test)]
    pub fn area_watermarks(&self) -> &HashMap<String, u64> {
        &self.area_watermarks
    }
}

impl Actor for RealmActor {
    type Message = StreamMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::RequestRealmLease { count, .. } => {
                let _ = self.handle_request_realm_lease(count, ctx);
            }
            StreamMessage::AreaWatermarkAdvanced(adv) => {
                self.handle_area_watermark_advanced(adv.area, adv.watermark, ctx);
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
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

    fn make_test_actor() -> (RealmActor, Context<RealmActor>) {
        let router = Arc::new(crate::runtime::router::Router::new());
        let family = RouteFamily::new(1);
        let addr = RouteAddress::new(family, Route::new("stream://realm1/__realm__"));
        let db = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open store"),
        );
        let store = Arc::new(StreamStore::new(db));
        let actor = RealmActor::new(family, "realm1".to_string(), store);
        let ctx = Context::new(addr, router);
        (actor, ctx)
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
        actor.handle_area_watermark_advanced("area1".to_string(), 100, &mut ctx);
        actor.handle_area_watermark_advanced("area2".to_string(), 50, &mut ctx);
        actor.handle_area_watermark_advanced("area3".to_string(), 75, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 50); // Minimum of all areas
    }

    #[test]
    fn should_update_realm_watermark_when_minimum_advances() {
        // Arrange
        let (mut actor, mut ctx) = make_test_actor();

        // Act
        actor.handle_area_watermark_advanced("area1".to_string(), 100, &mut ctx);
        actor.handle_area_watermark_advanced("area2".to_string(), 50, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 50);

        // Advance area2 (the minimum)
        actor.handle_area_watermark_advanced("area2".to_string(), 75, &mut ctx);

        // Assert
        assert_eq!(actor.watermark(), 75); // Now advances to 75
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
}
