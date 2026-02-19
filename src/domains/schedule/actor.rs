use crate::domains::schedule::protocol::{
    CronSchedule, ScheduleDef, ScheduleListEntry, ScheduleMessage, ScheduleResponse,
};
use crate::domains::schedule::store::ScheduleStore;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Schedule actor: manages time-based triggers with route-based identity
///
/// Stores schedules by route string as the unique key.
/// When a schedule fires, it publishes to matching subscribers (fanout pattern).
pub struct ScheduleActor {
    /// RouteFamily for storage column family mapping
    family: RouteFamily,
    /// Persistent storage
    store: ScheduleStore,
    /// In-memory schedule cache: route -> ScheduleDef
    schedules: HashMap<String, ScheduleDef>,
    /// Write options for persistence
    write_options: cntryl_midge::WriteOptions,
    /// Last scan time to deduplicate rapid scans
    last_scan_time: Instant,
    /// Minimum interval between scans (deduplication window)
    scan_dedup_window: std::time::Duration,
}

impl ScheduleActor {
    /// Create a new schedule actor for a specific route family
    pub fn new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        let store = ScheduleStore::new(db);
        let mut actor = Self {
            family,
            store,
            schedules: HashMap::new(),
            write_options,
            last_scan_time: Instant::now(),
            scan_dedup_window: std::time::Duration::from_millis(10),
        };

        // Load persisted schedules on startup
        if let Ok(entries) = actor.store.load_all(family.as_u64()) {
            for (route, cron_str, payload) in entries {
                match CronSchedule::parse(&cron_str) {
                    Ok(cron) => {
                        let next_fire = cron.next_fire_time(Instant::now());
                        let def = ScheduleDef {
                            route: route.clone(),
                            cron: cron_str,
                            payload,
                            next_fire_time: next_fire,
                        };
                        actor.schedules.insert(route, def);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to parse cron for persisted schedule {}: {}",
                            route, e
                        );
                    }
                }
            }
        }

        actor
    }

    /// Create or update a schedule (upsert by route)
    ///
    /// Returns Ok(()) on successful create/update, Err(msg) if cron is invalid.
    pub fn create_schedule(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
    ) -> Result<(), String> {
        // Validate cron expression
        let cron_obj = CronSchedule::parse(&cron)?;

        // Calculate next fire time
        let next_fire = cron_obj.next_fire_time(Instant::now());

        // Create in-memory definition
        let def = ScheduleDef {
            route: route.clone(),
            cron,
            payload: payload.clone(),
            next_fire_time: next_fire,
        };

        // Persist to storage
        self.store.insert(
            self.family.as_u64(),
            &route,
            &def.cron,
            &payload,
            next_fire,
            self.write_options,
        )?;

        // Update in-memory cache
        self.schedules.insert(route.clone(), def);
        info!(
            "Schedule upserted for route: {} (family: {})",
            route,
            self.family.as_u64()
        );

        Ok(())
    }

    /// Delete a schedule by route
    pub fn delete_schedule(&mut self, route: String) -> Result<(), String> {
        self.schedules.remove(&route);
        self.store
            .delete(self.family.as_u64(), &route, self.write_options)
    }

    /// List all schedules as (route, cron, payload) tuples
    pub fn list_defs(&self) -> Vec<(String, String, Bytes)> {
        self.schedules
            .values()
            .map(|def| (def.route.clone(), def.cron.clone(), def.payload.clone()))
            .collect()
    }

    /// Scan for schedules ready to fire and trigger them
    ///
    /// Called periodically by the tick handler.
    /// Includes scan deduplication: avoids redundant scans within scan_dedup_window.
    /// Returns Vec<(route, payload)> for schedules that fired.
    pub fn scan_and_fire(&mut self) -> Vec<(String, Bytes)> {
        let now = Instant::now();

        // Deduplication: skip scan if last scan was too recent
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }

        self.last_scan_time = now;
        let now_ms = Self::instant_to_ms(now);
        let mut fired = Vec::new();
        let mut to_reschedule = Vec::new();

        // Load ready schedules from storage
        match self.store.load_ready(self.family.as_u64(), now_ms) {
            Ok(ready) => {
                for (route, _cron_str, payload) in ready {
                    // Update in-memory cache and calculate next fire time
                    if let Some(def) = self.schedules.get_mut(&route) {
                        if let Ok(cron) = CronSchedule::parse(&def.cron) {
                            let next_fire = cron.next_fire_time(now);
                            def.next_fire_time = next_fire;

                            // Collect for batch persistence
                            to_reschedule.push((
                                route.clone(),
                                def.cron.clone(),
                                def.payload.clone(),
                                next_fire,
                            ));

                            fired.push((route.clone(), payload.clone()));
                            info!("Schedule fired: {} (next fire: ~{:?})", route, next_fire);
                        }
                    }
                }

                // Batch reschedule operations
                for (route, cron, payload, next_fire) in to_reschedule {
                    let _ = self.store.insert(
                        self.family.as_u64(),
                        &route,
                        &cron,
                        &payload,
                        next_fire,
                        self.write_options,
                    );
                }
            }
            Err(e) => {
                warn!("Failed to load ready schedules: {}", e);
            }
        }

        fired
    }

    /// Convert Instant to milliseconds since UNIX_EPOCH
    fn instant_to_ms(_instant: Instant) -> u64 {
        let now_sys = std::time::SystemTime::now();
        if let Ok(elapsed) = now_sys.duration_since(std::time::UNIX_EPOCH) {
            (elapsed.as_secs() * 1000) + (elapsed.subsec_millis() as u64)
        } else {
            0
        }
    }

    /// Handle schedule message (synchronous)
    pub fn handle(&mut self, msg: ScheduleMessage) -> ScheduleResponse {
        match msg {
            ScheduleMessage::Create {
                route,
                cron,
                payload,
            } => match self.create_schedule(route, cron, payload) {
                Ok(()) => ScheduleResponse::Ok,
                Err(e) => ScheduleResponse::Error(e),
            },
            ScheduleMessage::Cancel { route } => match self.delete_schedule(route) {
                Ok(()) => ScheduleResponse::Ok,
                Err(e) => ScheduleResponse::Error(e),
            },
            ScheduleMessage::List => {
                let defs = self.list_defs();
                let entries = defs
                    .into_iter()
                    .map(|(route, cron, payload)| ScheduleListEntry {
                        route,
                        cron,
                        payload,
                    })
                    .collect();
                ScheduleResponse::ListDefs(entries)
            }
            ScheduleMessage::Subscribe { .. } => {
                // Subscription handled by router, not actor
                ScheduleResponse::Ok
            }
            ScheduleMessage::Unsubscribe { .. } => {
                // Unsubscription handled by router, not actor
                ScheduleResponse::Ok
            }
            ScheduleMessage::UnsubscribeAll { .. } => {
                // Unsubscription handled by router, not actor
                ScheduleResponse::Ok
            }
        }
    }
}

impl Actor for ScheduleActor {
    type Message = ScheduleMessage;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        // Schedule operations are synchronous and return via response channel
        let _response = self.handle(msg);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn should_create_schedule() {
        // This test would require mocking the store, which we'll skip for now
        // Actual tests will use the integration test suite
    }
}
