//! Area actor: mints area-level offsets and tracks watermarks

use std::collections::BTreeMap;
use std::sync::Arc;
use bytes::Bytes;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::{RouteFamily, Route, RouteAddress};
use crate::domains::notification::protocol::{PublishMessage, NotificationMessage};

use super::protocol::{StreamMessage, LeaseGrant};
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
}

impl AreaActor {
    pub fn new(family_id: RouteFamily, realm: String, area: String, store: Arc<StreamStore>) -> Self {
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
        let realm_remaining = if self.realm_lease_end > self.realm_lease_next {
            self.realm_lease_end - self.realm_lease_next
        } else {
            0
        };
        
        if realm_remaining < count {
            // Request realm lease from RealmActor
            const DEFAULT_REALM_LEASE_BLOCK: u64 = 1000;
            let lease_size = DEFAULT_REALM_LEASE_BLOCK.max(count);
            let lease_req = StreamMessage::RequestRealmLease { count: lease_size };
            let realm_addr = self.realm_actor_address();
            let _ = ctx.send(realm_addr, lease_req);
            
            // TODO: In full async impl, would await RealmLeaseGranted
            // For now, mint with available capacity (may be partial)
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
        self.committed_ranges.insert(first_area_offset, last_area_offset);
        
        // Try to advance watermark
        let old_watermark = self.area_watermark;
        self.advance_watermark();
        
        // Persist watermark and notify RealmActor if watermark advanced
        if self.area_watermark > old_watermark {
            // Persist watermark to storage
            let _ = self.store.set_watermark(&self.realm, &self.area, self.area_watermark);
            
            // Emit area watermark notification (ephemeral, best-effort)
            let route_str = format!("notice://{}/{}/*/watermark", self.realm, self.area);
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
            let publish_msg = PublishMessage::new(self.family_id, route.clone(), payload);
            let notice_addr = RouteAddress::new(self.family_id, route);
            let _ = ctx.send(notice_addr, NotificationMessage::Publish(publish_msg));
            
            // Notify RealmActor
            let notification = StreamMessage::AreaWatermarkAdvanced {
                realm: self.realm.clone(),
                area: self.area.clone(),
                watermark: self.area_watermark,
            };
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
    fn advance_watermark(&mut self) {
        loop {
            let next_offset = self.area_watermark + 1;
            let mut found_range: Option<(u64, u64)> = None;
            
            // Look for a range that starts exactly at watermark+1
            for (&first, &last) in &self.committed_ranges {
                if first == next_offset {
                    // Found contiguous range
                    found_range = Some((first, last));
                    break;
                }
            }
            
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
        self.committed_ranges.retain(|_, last| {
            *last > self.area_watermark
        });
    }
    
    /// Update realm lease from RealmActor grant
    pub fn update_realm_lease(&mut self, grant: LeaseGrant) {
        self.realm_lease_next = grant.realm_start;
        self.realm_lease_end = grant.realm_end_exclusive;  // Already exclusive
    }
    
    /// Get remaining realm lease capacity
    #[allow(dead_code)]
    pub fn realm_lease_remaining(&self) -> u64 {
        if self.realm_lease_end > self.realm_lease_next {
            self.realm_lease_end - self.realm_lease_next
        } else {
            0
        }
    }
    
    /// Get current watermark (for testing)
    pub fn watermark(&self) -> u64 {
        self.area_watermark
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
            StreamMessage::RequestLease { realm, area, count, reply_to } => {
                self.handle_request_lease(&realm, &area, count, &reply_to, ctx);
            }
            StreamMessage::BatchCommitted { first_area_offset, last_area_offset, .. } => {
                self.handle_batch_committed(first_area_offset, last_area_offset, ctx);
            }
            StreamMessage::LeaseGranted { grant } => {
                // Update realm lease from RealmActor
                self.update_realm_lease(grant);
            }
            _ => {}
        }
    }
}

// TODO: Uncomment tests when test infrastructure is ready
/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::Route;
    use std::str::FromStr;
    
    fn create_test_actor() -> AreaActor {
        AreaActor::new(
            RouteFamily::from_str("stream").unwrap(),
            "realm1".to_string(),
            "area1".to_string(),
        )
    }
    
    // ... tests here ...
}
*/
