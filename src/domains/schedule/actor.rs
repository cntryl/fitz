use crate::domains::schedule::protocol::{
    validate_concrete_schedule_route, CronSchedule, ScheduleDef, ScheduleListEntry,
    ScheduleMessage, ScheduleResponse,
};
use crate::domains::schedule::store::{ScheduleInsert, ScheduleStore};
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;
use bytes::Bytes;
use fxhash::FxBuildHasher;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;

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
    schedules: FastMap<String, ScheduleDef>,
    /// Parsed cron expressions reused across repeated creates/upserts.
    cron_cache: FastMap<String, CronSchedule>,
    /// Canonical mutable LIST backing store.
    list_entries: Vec<Arc<ScheduleListEntry>>,
    /// Cached full LIST snapshot reused by the common `offset=0, limit=0` path.
    /// Mutations invalidate this cache instead of cloning it via `Arc::make_mut`.
    list_cache: Option<Arc<Vec<Arc<ScheduleListEntry>>>>,
    /// Write options for persistence
    write_options: cntryl_midge::WriteOptions,
    /// Last scan time to deduplicate rapid scans
    last_scan_time: Instant,
    /// Minimum interval between scans (deduplication window)
    scan_dedup_window: std::time::Duration,
    /// Priority queue of schedules ordered by next fire time (min-heap with Reverse)
    /// Stores (next_fire_ms, route_clone) for efficient ready detection
    ready_heap: BinaryHeap<(Reverse<u64>, String)>,
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
            schedules: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            cron_cache: HashMap::with_capacity_and_hasher(32, FxBuildHasher::default()),
            list_entries: Vec::new(),
            list_cache: None,
            write_options,
            last_scan_time: Instant::now(),
            scan_dedup_window: std::time::Duration::from_millis(10),
            ready_heap: BinaryHeap::new(),
        };

        // Load persisted schedules on startup
        if let Ok(entries) = actor.store.load_all(family.as_u64()) {
            for (route, cron_str, payload, next_fire_ms) in entries {
                match CronSchedule::parse(&cron_str) {
                    Ok(cron) => {
                        actor.cron_cache.insert(cron_str.clone(), cron.clone());
                        let next_fire = Self::ms_to_instant(next_fire_ms);
                        let list_index =
                            actor.push_list_entry(route.as_str(), cron_str.as_str(), &payload);
                        let def = ScheduleDef {
                            route: route.clone(),
                            cron: cron_str,
                            parsed_cron: cron,
                            payload,
                            next_fire_time: next_fire,
                            next_fire_ms,
                            storage_key: ScheduleStore::encode_key(next_fire_ms, &route),
                            index_key: ScheduleStore::encode_index_key(&route),
                            list_index,
                        };
                        actor.schedules.insert(route.clone(), def);
                        // Add to ready heap for efficient scanning
                        actor.ready_heap.push((Reverse(next_fire_ms), route));
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

    fn parsed_cron_for(&mut self, cron: &str) -> Result<CronSchedule, String> {
        if let Some(parsed) = self.cron_cache.get(cron) {
            return Ok(parsed.clone());
        }

        let parsed = CronSchedule::parse(cron)?;
        self.cron_cache.insert(cron.to_string(), parsed.clone());
        Ok(parsed)
    }

    /// Create or update a schedule (upsert by route)
    ///
    /// Returns Ok(true) if the schedule was created or updated.
    /// Returns Ok(false) for an idempotent no-op upsert.
    pub fn create_schedule(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
    ) -> Result<bool, String> {
        validate_concrete_schedule_route(&route)?;

        let now = Instant::now();
        let (previous_next_fire_ms, previous_list_index) = self
            .schedules
            .get(&route)
            .map(|existing| (Some(existing.next_fire_ms), Some(existing.list_index)))
            .unwrap_or((None, None));

        if let Some(existing) = self.schedules.get(&route) {
            if existing.cron == cron && existing.payload == payload && existing.next_fire_time > now
            {
                return Ok(false);
            }
        }

        // Reuse parsed schedules for repeated cron strings on the hot create path.
        let cron_obj = self.parsed_cron_for(&cron)?;

        // Calculate next fire time
        let next_fire = cron_obj.next_fire_time(now);
        let next_fire_ms = Self::instant_to_ms(next_fire);

        // Persist first, then move payload into the in-memory definition.
        let storage_key = self.store.insert(
            self.family.as_u64(),
            ScheduleInsert {
                route: &route,
                cron: &cron,
                payload: &payload,
                next_fire_time: next_fire,
                next_fire_ms,
                previous_fire_ms: previous_next_fire_ms,
                previous_storage_key: self
                    .schedules
                    .get(&route)
                    .map(|existing| existing.storage_key.clone()),
                index_key: self
                    .schedules
                    .get(&route)
                    .map(|existing| existing.index_key.clone())
                    .or_else(|| Some(ScheduleStore::encode_index_key(&route))),
            },
            self.write_options,
        )?;

        let list_index = self.upsert_list_entry(previous_list_index, &route, &cron, &payload);

        // Create in-memory definition with cached parsed cron
        let def = ScheduleDef {
            route: route.clone(),
            cron,
            parsed_cron: cron_obj,
            payload,
            next_fire_time: next_fire,
            next_fire_ms,
            storage_key,
            index_key: ScheduleStore::encode_index_key(&route),
            list_index,
        };

        self.schedules.insert(route.clone(), def);

        // Add to ready heap for efficient scanning
        self.ready_heap.push((Reverse(next_fire_ms), route.clone()));

        Ok(true)
    }

    /// Delete a schedule by route
    pub fn delete_schedule(&mut self, route: String) -> Result<bool, String> {
        validate_concrete_schedule_route(&route)?;

        let Some(removed_def) = self.schedules.remove(&route) else {
            return Ok(false);
        };
        self.remove_list_entry(removed_def.list_index);

        self.store
            .delete_prepared(
                self.family.as_u64(),
                removed_def.index_key,
                removed_def.storage_key,
                self.write_options,
            )
            .map(|()| true)
    }

    pub fn schedule_count(&self) -> usize {
        self.schedules.len()
    }

    /// List all schedules as (route, cron, payload) tuples
    pub fn list_defs(&self) -> Vec<(String, String, Bytes)> {
        self.list_entries
            .iter()
            .map(|entry| {
                (
                    entry.route.clone(),
                    entry.cron.clone(),
                    entry.payload.clone(),
                )
            })
            .collect()
    }

    fn push_list_entry(&mut self, route: &str, cron: &str, payload: &Bytes) -> usize {
        let entry = Arc::new(ScheduleListEntry {
            route: route.to_string(),
            cron: cron.to_string(),
            payload: payload.clone(),
        });
        let index = self.list_entries.len();
        self.list_entries.push(entry);
        index
    }

    fn sync_cached_upsert(&mut self, current_index: Option<usize>, entry: Arc<ScheduleListEntry>) {
        let invalidate_cache = self
            .list_cache
            .as_ref()
            .is_some_and(|cache| Arc::strong_count(cache) > 1);
        if invalidate_cache {
            self.list_cache = None;
            return;
        }

        let Some(cache) = self.list_cache.as_mut() else {
            return;
        };
        let cache_entries = Arc::get_mut(cache).expect("schedule list cache must be exclusive");
        if let Some(index) = current_index {
            cache_entries[index] = entry;
        } else {
            cache_entries.push(entry);
        }
    }

    fn sync_cached_remove(&mut self, index: usize) {
        let invalidate_cache = self
            .list_cache
            .as_ref()
            .is_some_and(|cache| Arc::strong_count(cache) > 1);
        if invalidate_cache {
            self.list_cache = None;
            return;
        }

        let Some(cache) = self.list_cache.as_mut() else {
            return;
        };
        let cache_entries = Arc::get_mut(cache).expect("schedule list cache must be exclusive");
        if index < cache_entries.len() {
            cache_entries.swap_remove(index);
        }
    }

    fn upsert_list_entry(
        &mut self,
        current_index: Option<usize>,
        route: &str,
        cron: &str,
        payload: &Bytes,
    ) -> usize {
        let entry = Arc::new(ScheduleListEntry {
            route: route.to_string(),
            cron: cron.to_string(),
            payload: payload.clone(),
        });
        if let Some(index) = current_index {
            self.list_entries[index] = entry.clone();
            self.sync_cached_upsert(Some(index), entry);
            index
        } else {
            let index = self.list_entries.len();
            self.list_entries.push(entry.clone());
            self.sync_cached_upsert(None, entry);
            index
        }
    }

    fn remove_list_entry(&mut self, index: usize) {
        if index >= self.list_entries.len() {
            return;
        }

        self.list_entries.swap_remove(index);
        self.sync_cached_remove(index);
        if let Some(swapped_entry) = self.list_entries.get(index) {
            if let Some(swapped_def) = self.schedules.get_mut(&swapped_entry.route) {
                swapped_def.list_index = index;
            }
        }
    }

    pub fn list_entries(
        &mut self,
        offset: u64,
        limit: u64,
    ) -> (Arc<Vec<Arc<ScheduleListEntry>>>, u64) {
        let total_count = self.schedules.len() as u64;
        let start = offset as usize;
        if start >= self.list_entries.len() {
            return (Arc::new(Vec::new()), total_count);
        }

        let remaining = self.list_entries.len() - start;
        let take = if limit == 0 {
            remaining
        } else {
            remaining.min(limit as usize)
        };

        if start == 0 && take == self.list_entries.len() {
            if let Some(cache) = &self.list_cache {
                return (cache.clone(), total_count);
            }
            let cache = Arc::new(self.list_entries.clone());
            self.list_cache = Some(cache.clone());
            return (cache, total_count);
        }

        (
            Arc::new(self.list_entries[start..start + take].to_vec()),
            total_count,
        )
    }

    /// Scan for schedules ready to fire and trigger them
    ///
    /// Called periodically by the tick handler.
    /// Includes scan deduplication: avoids redundant scans within scan_dedup_window.
    /// Uses a min-heap (ready_heap) to efficiently find schedules ready to fire O(k log n)
    /// instead of scanning all schedules O(n).
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
        // Reuse single vec for persistence: (route, cron, payload, next_fire, next_fire_ms, previous_fire_ms)
        let mut to_reschedule: Vec<(String, String, Bytes, Instant, u64, u64)> = Vec::new();
        let mut heap_popped = Vec::new();

        // Peek the heap for schedules ready to fire (fire_ms <= now_ms)
        // Pop all ready items and temporarily store them
        while let Some(&(Reverse(fire_ms), _)) = self.ready_heap.peek() {
            if fire_ms <= now_ms {
                if let Some((Reverse(fire_ms), route)) = self.ready_heap.pop() {
                    heap_popped.push((fire_ms, route));
                }
            } else {
                break; // Remaining items in heap are not ready yet
            }
        }

        // Process fired schedules: build to_reschedule in store API shape (no extra batch clone)
        for (fire_ms, route) in heap_popped {
            if let Some(def) = self.schedules.get_mut(&route) {
                // Use cached parsed cron instead of reparsing
                let next_fire = def.parsed_cron.next_fire_time(now);
                let next_fire_ms = Self::instant_to_ms(next_fire);
                def.next_fire_time = next_fire;
                def.next_fire_ms = next_fire_ms;
                def.storage_key = ScheduleStore::encode_key(next_fire_ms, &route);
                def.index_key = ScheduleStore::encode_index_key(&route);

                to_reschedule.push((
                    route.clone(),
                    def.cron.clone(),
                    def.payload.clone(),
                    next_fire,
                    next_fire_ms,
                    fire_ms,
                ));
                fired.push((route.clone(), def.payload.clone()));
                info!("Schedule fired: {} (next fire: ~{:?})", route, next_fire);
            }
        }

        // Batch reschedule: one transaction, pass to_reschedule directly (no extra clone)
        let _ = self
            .store
            .insert_batch(self.family.as_u64(), &to_reschedule, self.write_options);
        for (route, _cron, _payload, _next_fire, next_fire_ms, _previous_fire_ms) in to_reschedule {
            self.ready_heap.push((Reverse(next_fire_ms), route));
        }

        fired
    }

    /// Scan and fire without persisting or re-adding to heap (for benchmarking CPU cost only).
    #[doc(hidden)]
    pub fn scan_and_fire_cpu_only(&mut self) -> Vec<(String, Bytes)> {
        let now = Instant::now();
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }
        self.last_scan_time = now;
        let now_ms = Self::instant_to_ms(now);
        let mut fired = Vec::new();
        let mut heap_popped = Vec::new();

        while let Some(&(Reverse(fire_ms), _)) = self.ready_heap.peek() {
            if fire_ms <= now_ms {
                if let Some((Reverse(_fire_ms), route)) = self.ready_heap.pop() {
                    heap_popped.push(route);
                }
            } else {
                break;
            }
        }

        for route in heap_popped {
            if let Some(def) = self.schedules.get_mut(&route) {
                let _next_fire = def.parsed_cron.next_fire_time(now);
                fired.push((route.clone(), def.payload.clone()));
            }
        }

        fired
    }

    /// Configure benchmark-ready schedule state without touching persistence.
    #[doc(hidden)]
    pub fn bench_prepare_scan(&mut self, ready_count: usize) {
        let now = Instant::now();
        let ready_limit = ready_count.min(self.list_entries.len());
        let ready_ms = Self::instant_to_ms(now).saturating_sub(1);
        let not_ready_time = now.checked_add(Duration::from_secs(60)).unwrap_or(now);
        let not_ready_ms = Self::instant_to_ms(not_ready_time);
        let routes: Vec<_> = self
            .list_entries
            .iter()
            .map(|entry| entry.route.clone())
            .collect();

        self.ready_heap.clear();

        for (idx, route) in routes.into_iter().enumerate() {
            if let Some(def) = self.schedules.get_mut(&route) {
                let (next_fire_time, next_fire_ms) = if idx < ready_limit {
                    (now, ready_ms)
                } else {
                    (not_ready_time, not_ready_ms)
                };

                def.next_fire_time = next_fire_time;
                def.next_fire_ms = next_fire_ms;
                def.storage_key = ScheduleStore::encode_key(next_fire_ms, &route);
                def.index_key = ScheduleStore::encode_index_key(&route);
                self.ready_heap.push((Reverse(next_fire_ms), route));
            }
        }

        self.last_scan_time = now
            .checked_sub(self.scan_dedup_window + Duration::from_millis(1))
            .unwrap_or(now);
    }

    /// Convert a target `Instant` into an approximate UNIX epoch timestamp (ms).
    ///
    /// `Instant` is monotonic and not directly epoch-based, so we anchor to "now"
    /// and add/subtract the monotonic delta.
    fn instant_to_ms(instant: Instant) -> u64 {
        let now_instant = Instant::now();
        let now_sys = std::time::SystemTime::now();

        let now_ms = if let Ok(elapsed) = now_sys.duration_since(std::time::UNIX_EPOCH) {
            (elapsed.as_secs() * 1000) + (elapsed.subsec_millis() as u64)
        } else {
            return 0;
        };

        if instant >= now_instant {
            now_ms.saturating_add(instant.duration_since(now_instant).as_millis() as u64)
        } else {
            now_ms.saturating_sub(now_instant.duration_since(instant).as_millis() as u64)
        }
    }

    fn ms_to_instant(timestamp_ms: u64) -> Instant {
        let now_instant = Instant::now();
        let now_ms = if let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) {
            (elapsed.as_secs() * 1000) + (elapsed.subsec_millis() as u64)
        } else {
            return now_instant;
        };

        if timestamp_ms >= now_ms {
            now_instant
                .checked_add(Duration::from_millis(timestamp_ms - now_ms))
                .unwrap_or(now_instant)
        } else {
            now_instant
                .checked_sub(Duration::from_millis(now_ms - timestamp_ms))
                .unwrap_or(now_instant)
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
                Ok(_) => ScheduleResponse::Ok,
                Err(e) => ScheduleResponse::Error(e),
            },
            ScheduleMessage::Cancel { route } => match self.delete_schedule(route) {
                Ok(_) => ScheduleResponse::Ok,
                Err(e) => ScheduleResponse::Error(e),
            },
            ScheduleMessage::List { offset, limit } => {
                let (entries, total_count) = self.list_entries(offset, limit);
                ScheduleResponse::ListDefs {
                    entries,
                    total_count,
                }
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

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = self.handle(msg);
        let _ = ctx.reply(response).ok();
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
