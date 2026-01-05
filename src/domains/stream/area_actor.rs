//! Area actor: mints area-level offsets and tracks watermarks

use std::collections::BTreeMap;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;

use super::protocol::{StreamMessage, LeaseGrant};

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
    pub fn new(family_id: RouteFamily, realm: String, area: String) -> Self {
        Self {
            family_id,
            realm,
            area,
            next_area_offset: 0,
            area_watermark: 0,
            committed_ranges: BTreeMap::new(),
            realm_lease_next: 0,
            realm_lease_end: 0,
        }
    }
    
    /// Grant area offset lease to StreamActor
    fn handle_request_area_lease(
        &mut self,
        count: u64,
        _ctx: &mut Context<Self>,
    ) -> LeaseGrant {
        let start = self.next_area_offset;
        let end = start + count;
        self.next_area_offset = end;
        
        LeaseGrant {
            area_start: start,
            area_end: end - 1,  // inclusive
            realm_start: 0,  // Will be filled by RealmActor grant
            realm_end: 0,
        }
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
        
        // Notify RealmActor if watermark advanced
        if self.area_watermark > old_watermark {
            let _notification = StreamMessage::AreaWatermarkAdvanced {
                realm: self.realm.clone(),
                area: self.area.clone(),
                watermark: self.area_watermark,
            };
            // TODO: ctx.send_to_realm_actor(notification);
            let _ = ctx; // silence warning for now
        }
    }
    
    /// Advance watermark to highest contiguous offset
    fn advance_watermark(&mut self) {
        let mut watermark = self.area_watermark;
        
        // Find all ranges that can extend the watermark
        loop {
            let mut found = false;
            
            // Look for range that starts at or before watermark+1
            for (&first, &last) in &self.committed_ranges {
                if first <= watermark + 1 && last >= watermark {
                    // This range extends or overlaps with watermark
                    watermark = watermark.max(last);
                    found = true;
                    break;
                }
            }
            
            if !found {
                break;
            }
        }
        
        self.area_watermark = watermark;
        
        // Clean up ranges that are now below watermark
        self.committed_ranges.retain(|_, &mut last| last > watermark);
    }
    
    /// Update realm lease from RealmActor grant
    pub fn update_realm_lease(&mut self, grant: LeaseGrant) {
        self.realm_lease_next = grant.realm_start;
        self.realm_lease_end = grant.realm_end;
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
    #[cfg(test)]
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
            StreamMessage::RequestAreaLease { count, .. } => {
                let _ = self.handle_request_area_lease(count, ctx);
            }
            StreamMessage::BatchCommitted { first_area_offset, last_area_offset, .. } => {
                self.handle_batch_committed(first_area_offset, last_area_offset, ctx);
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
