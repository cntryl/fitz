//! Realm actor: mints realm-level offsets and aggregates watermarks

use std::collections::HashMap;
use bytes::Bytes;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::{RouteFamily, Route, RouteAddress};
use crate::domains::notification::protocol::{PublishMessage, NotificationMessage};

use super::protocol::{StreamMessage, LeaseGrant};

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
    #[allow(dead_code)]
    realm: String,
    
    /// Next realm offset to assign
    next_realm_offset: u64,
    
    /// Area watermarks (for realm watermark calculation)
    /// Key: area name, Value: watermark
    area_watermarks: HashMap<String, u64>,
    
    /// Realm watermark (minimum of all area watermarks)
    realm_watermark: u64,
}

impl RealmActor {
    pub fn new(family_id: RouteFamily, realm: String) -> Self {
        Self {
            family_id,
            realm,
            next_realm_offset: 0,
            area_watermarks: HashMap::new(),
            realm_watermark: 0,
        }
    }
    
    /// Grant realm offset lease to AreaActor
    fn handle_request_realm_lease(
        &mut self,
        count: u64,
        ctx: &mut Context<Self>,
    ) -> LeaseGrant {
        let start = self.next_realm_offset;
        let end = start + count;
        self.next_realm_offset = end;
        
        let grant = LeaseGrant {
            area_start: 0,  // Will be filled by AreaActor
            area_end: 0,
            realm_start: start,
            realm_end: end - 1,  // inclusive (protocol uses inclusive)
        };
        
        // Send grant back to requesting AreaActor
        // Note: In full impl, would extract reply_to from request
        // For now, this is fire-and-forget
        let _ = ctx;
        
        grant
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
        let new_watermark = *self.area_watermarks
            .values()
            .min()
            .unwrap_or(&0);
        
        self.realm_watermark = new_watermark;
        
        // Emit realm watermark notification only if watermark advanced
        if new_watermark > old_watermark {
            let route_str = format!("notice://{}/*/*/watermark", self.realm);
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
            let publish_msg = PublishMessage::new(self.family_id, route.clone(), payload);
            let notice_addr = RouteAddress::new(self.family_id, route);
            let _ = ctx.send(notice_addr, NotificationMessage::Publish(publish_msg));
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
            StreamMessage::AreaWatermarkAdvanced { area, watermark, .. } => {
                self.handle_area_watermark_advanced(area, watermark, ctx);
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
    
    // ... tests here ...
}
*/
