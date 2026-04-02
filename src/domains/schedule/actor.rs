use crate::domains::schedule::protocol::{
    parse_concrete_schedule_route, validate_concrete_schedule_route, CronSchedule,
    ScheduleCreateEntry, ScheduleDef, ScheduleListEntry, ScheduleMessage, ScheduleResponse,
};
use crate::domains::schedule::store::{PersistedSchedule, ScheduleInsert, ScheduleStore};
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;
use bytes::Bytes;
use fxhash::FxBuildHasher;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;
type FastSet<K> = HashSet<K, FxBuildHasher>;

struct PendingScheduleCreate {
    route: String,
    cron: String,
    parsed_cron: CronSchedule,
    payload: Bytes,
    next_fire_time: Instant,
    next_fire_ms: u64,
    previous_fire_ms: Option<u64>,
    previous_list_index: Option<usize>,
}

struct PendingScheduleFire {
    route: String,
    cron: String,
    payload: Bytes,
    next_fire_time: Instant,
    next_fire_ms: u64,
    previous_fire_ms: u64,
}

/// Durable schedule coordinator for one route family.
///
/// Persisted schedule definitions survive broker restart and downtime through
/// Midge. Live subscriptions and fanout delivery remain session-scoped and
/// ephemeral. The in-memory heap is only a derived accelerator: authoritative
/// state lives in the persisted definition row, and the due index can always be
/// rebuilt from those durable definitions.
pub struct ScheduleActor {
    /// RouteFamily for storage column family mapping.
    family: RouteFamily,
    /// Durable schedule storage plus legacy migration helpers.
    store: ScheduleStore,
    /// In-memory schedule cache: route -> ScheduleDef.
    schedules: FastMap<String, ScheduleDef>,
    /// Parsed cron expressions reused across repeated creates/upserts.
    cron_cache: FastMap<String, CronSchedule>,
    /// Canonical mutable LIST backing store.
    list_entries: Vec<Arc<ScheduleListEntry>>,
    /// Cached full LIST snapshot reused by the common `offset=0, limit=0` path.
    list_cache: Option<Arc<Vec<Arc<ScheduleListEntry>>>>,
    /// Write options for persistence.
    write_options: cntryl_midge::WriteOptions,
    /// Last scan time to deduplicate rapid scans.
    last_scan_time: Instant,
    /// Minimum interval between scans (deduplication window).
    scan_dedup_window: std::time::Duration,
    /// Derived min-heap of due timestamps keyed by route. Stale entries are
    /// tolerated and ignored by comparing each popped timestamp against the
    /// current definition's `next_fire_ms`.
    ready_heap: BinaryHeap<(Reverse<u64>, String)>,
}

impl ScheduleActor {
    pub fn try_new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        Self::try_new_at(family, db, write_options, Instant::now())
    }

    pub fn new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_new(family, db, write_options).expect("schedule actor startup should succeed")
    }

    fn try_new_at(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        now: Instant,
    ) -> Result<Self, String> {
        let store = ScheduleStore::new(db);
        let mut actor = Self {
            family,
            store,
            schedules: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            cron_cache: HashMap::with_capacity_and_hasher(32, FxBuildHasher::default()),
            list_entries: Vec::new(),
            list_cache: None,
            write_options,
            last_scan_time: now,
            scan_dedup_window: std::time::Duration::from_millis(10),
            ready_heap: BinaryHeap::new(),
        };

        actor.preload_from_store_at(now)?;
        Ok(actor)
    }

    fn preload_from_store_at(&mut self, now: Instant) -> Result<(), String> {
        let now_ms = Self::instant_to_ms_at(now, now);
        let entries = self
            .store
            .load_all(self.family.as_u64(), self.write_options)?;
        let mut normalization_batch = Vec::new();

        for PersistedSchedule {
            route,
            cron,
            payload,
            next_fire_ms,
        } in entries
        {
            match CronSchedule::parse(&cron) {
                Ok(parsed_cron) => {
                    self.cron_cache.insert(cron.clone(), parsed_cron.clone());

                    let (effective_next_fire_time, effective_next_fire_ms, previous_fire_ms) =
                        if next_fire_ms <= now_ms {
                            let normalized_next_fire_time = parsed_cron.next_fire_time(now);
                            let normalized_next_fire_ms =
                                Self::instant_to_ms_at(normalized_next_fire_time, now);
                            normalization_batch.push((
                                route.clone(),
                                cron.clone(),
                                payload.clone(),
                                normalized_next_fire_ms,
                                Some(next_fire_ms),
                            ));
                            (
                                normalized_next_fire_time,
                                normalized_next_fire_ms,
                                Some(next_fire_ms),
                            )
                        } else {
                            (
                                Self::ms_to_instant_at(next_fire_ms, now),
                                next_fire_ms,
                                None,
                            )
                        };

                    let _ = previous_fire_ms;
                    let list_index = self.push_list_entry(route.as_str(), cron.as_str(), &payload);
                    let def = ScheduleDef {
                        route: route.clone(),
                        cron,
                        parsed_cron,
                        payload,
                        next_fire_time: effective_next_fire_time,
                        next_fire_ms: effective_next_fire_ms,
                        list_index,
                    };

                    self.schedules.insert(route.clone(), def);
                    self.ready_heap
                        .push((Reverse(effective_next_fire_ms), route.clone()));
                }
                Err(error) => {
                    warn!(
                        "Failed to parse cron for persisted schedule {}: {}",
                        route, error
                    );
                }
            }
        }

        if !normalization_batch.is_empty() {
            self.store.insert_batch(
                self.family.as_u64(),
                &normalization_batch,
                self.write_options,
            )?;
        }

        Ok(())
    }

    pub fn admin_snapshot(&self) -> Vec<crate::api::admin::ScheduleInfo> {
        let mut snapshot: Vec<_> = self
            .schedules
            .values()
            .filter_map(|schedule| {
                parse_concrete_schedule_route(&schedule.route)
                    .ok()
                    .map(|route| crate::api::admin::ScheduleInfo {
                        realm: route.realm,
                        area: route.area,
                        resource: route.resource,
                        operation: route.operation,
                        cron: schedule.cron.clone(),
                        next_run: chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                            schedule.next_fire_ms as i64,
                        )
                        .map(|timestamp| timestamp.to_rfc3339())
                        .unwrap_or_default(),
                        last_run: None,
                        executions_total: 0,
                        enabled: true,
                    })
            })
            .collect();

        snapshot.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource, &left.operation).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
                &right.operation,
            ))
        });

        snapshot
    }

    fn parsed_cron_for(&mut self, cron: &str) -> Result<CronSchedule, String> {
        if let Some(parsed) = self.cron_cache.get(cron) {
            return Ok(parsed.clone());
        }

        let parsed = CronSchedule::parse(cron)?;
        self.cron_cache.insert(cron.to_string(), parsed.clone());
        Ok(parsed)
    }

    fn create_schedule_at(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
        now: Instant,
    ) -> Result<bool, String> {
        validate_concrete_schedule_route(&route)?;

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

        let cron_obj = self.parsed_cron_for(&cron)?;
        let next_fire_time = cron_obj.next_fire_time(now);
        let next_fire_ms = Self::instant_to_ms_at(next_fire_time, now);

        self.store.insert(
            self.family.as_u64(),
            ScheduleInsert {
                route: &route,
                cron: &cron,
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: previous_next_fire_ms,
            },
            self.write_options,
        )?;

        let list_index = self.upsert_list_entry(previous_list_index, &route, &cron, &payload);
        let def = ScheduleDef {
            route: route.clone(),
            cron,
            parsed_cron: cron_obj,
            payload,
            next_fire_time,
            next_fire_ms,
            list_index,
        };

        self.schedules.insert(route.clone(), def);
        self.ready_heap.push((Reverse(next_fire_ms), route));
        Ok(true)
    }

    pub fn create_schedule(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
    ) -> Result<bool, String> {
        self.create_schedule_at(route, cron, payload, Instant::now())
    }

    fn create_schedules_at(
        &mut self,
        entries: Vec<ScheduleCreateEntry>,
        now: Instant,
    ) -> Result<usize, String> {
        if entries.is_empty() {
            return Err("schedule batch must not be empty".to_string());
        }

        let mut seen_routes =
            FastSet::with_capacity_and_hasher(entries.len(), FxBuildHasher::default());
        let mut pending = Vec::with_capacity(entries.len());

        for entry in entries {
            if !seen_routes.insert(entry.route.clone()) {
                return Err(format!(
                    "duplicate schedule route in batch: {}",
                    entry.route
                ));
            }

            validate_concrete_schedule_route(&entry.route)?;

            let (previous_fire_ms, previous_list_index, should_skip) =
                match self.schedules.get(&entry.route) {
                    Some(existing)
                        if existing.cron == entry.cron
                            && existing.payload == entry.payload
                            && existing.next_fire_time > now =>
                    {
                        (Some(existing.next_fire_ms), Some(existing.list_index), true)
                    }
                    Some(existing) => (
                        Some(existing.next_fire_ms),
                        Some(existing.list_index),
                        false,
                    ),
                    None => (None, None, false),
                };

            if should_skip {
                continue;
            }

            let parsed_cron = self.parsed_cron_for(&entry.cron)?;
            let next_fire_time = parsed_cron.next_fire_time(now);
            let next_fire_ms = Self::instant_to_ms_at(next_fire_time, now);

            pending.push(PendingScheduleCreate {
                route: entry.route,
                cron: entry.cron,
                parsed_cron,
                payload: entry.payload,
                next_fire_time,
                next_fire_ms,
                previous_fire_ms,
                previous_list_index,
            });
        }

        if pending.is_empty() {
            return Ok(0);
        }

        let store_items: Vec<_> = pending
            .iter()
            .map(|entry| {
                (
                    entry.route.clone(),
                    entry.cron.clone(),
                    entry.payload.clone(),
                    entry.next_fire_ms,
                    entry.previous_fire_ms,
                )
            })
            .collect();

        self.store
            .insert_batch(self.family.as_u64(), &store_items, self.write_options)?;

        let changed = pending.len();
        for entry in pending {
            let list_index = self.upsert_list_entry(
                entry.previous_list_index,
                &entry.route,
                &entry.cron,
                &entry.payload,
            );
            let def = ScheduleDef {
                route: entry.route.clone(),
                cron: entry.cron,
                parsed_cron: entry.parsed_cron,
                payload: entry.payload,
                next_fire_time: entry.next_fire_time,
                next_fire_ms: entry.next_fire_ms,
                list_index,
            };

            self.ready_heap
                .push((Reverse(entry.next_fire_ms), entry.route.clone()));
            self.schedules.insert(entry.route, def);
        }

        Ok(changed)
    }

    pub fn create_schedules(&mut self, entries: Vec<ScheduleCreateEntry>) -> Result<usize, String> {
        self.create_schedules_at(entries, Instant::now())
    }

    pub fn delete_schedule(&mut self, route: String) -> Result<bool, String> {
        validate_concrete_schedule_route(&route)?;

        let Some(existing) = self.schedules.get(&route) else {
            return Ok(false);
        };

        self.store.delete_current(
            self.family.as_u64(),
            &route,
            existing.next_fire_ms,
            self.write_options,
        )?;

        if let Some(removed_def) = self.schedules.remove(&route) {
            self.remove_list_entry(removed_def.list_index);
            return Ok(true);
        }

        Ok(false)
    }

    pub fn schedule_count(&self) -> usize {
        self.schedules.len()
    }

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

    fn scan_and_fire_at(&mut self, now: Instant) -> Vec<(String, Bytes)> {
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }
        self.last_scan_time = now;

        let now_ms = Self::instant_to_ms_at(now, now);
        let mut heap_popped = Vec::new();
        while let Some(&(Reverse(fire_ms), _)) = self.ready_heap.peek() {
            if fire_ms > now_ms {
                break;
            }
            if let Some((Reverse(fire_ms), route)) = self.ready_heap.pop() {
                heap_popped.push((fire_ms, route));
            }
        }

        let mut to_reschedule = Vec::new();
        for (fire_ms, route) in heap_popped {
            let Some(def) = self.schedules.get(&route) else {
                continue;
            };
            if def.next_fire_ms != fire_ms {
                continue;
            }

            let next_fire_time = def.parsed_cron.next_fire_time(now);
            let next_fire_ms = Self::instant_to_ms_at(next_fire_time, now);
            to_reschedule.push(PendingScheduleFire {
                route: route.clone(),
                cron: def.cron.clone(),
                payload: def.payload.clone(),
                next_fire_time,
                next_fire_ms,
                previous_fire_ms: fire_ms,
            });
        }

        if to_reschedule.is_empty() {
            return Vec::new();
        }

        let store_items: Vec<_> = to_reschedule
            .iter()
            .map(|item| {
                (
                    item.route.clone(),
                    item.cron.clone(),
                    item.payload.clone(),
                    item.next_fire_ms,
                    Some(item.previous_fire_ms),
                )
            })
            .collect();

        if let Err(error) =
            self.store
                .insert_batch(self.family.as_u64(), &store_items, self.write_options)
        {
            warn!("Failed to persist schedule reschedule batch: {}", error);
            for item in to_reschedule {
                self.ready_heap
                    .push((Reverse(item.previous_fire_ms), item.route.clone()));
            }
            return Vec::new();
        }

        let mut fired = Vec::with_capacity(store_items.len());
        for item in to_reschedule {
            let Some(def) = self.schedules.get_mut(&item.route) else {
                continue;
            };
            if def.next_fire_ms != item.previous_fire_ms {
                continue;
            }

            def.next_fire_time = item.next_fire_time;
            def.next_fire_ms = item.next_fire_ms;
            self.ready_heap
                .push((Reverse(item.next_fire_ms), item.route.clone()));
            fired.push((item.route.clone(), item.payload.clone()));
            info!(
                "Schedule fired: {} (next fire: ~{:?})",
                item.route, item.next_fire_time
            );
        }

        fired
    }

    pub fn scan_and_fire(&mut self) -> Vec<(String, Bytes)> {
        self.scan_and_fire_at(Instant::now())
    }

    #[doc(hidden)]
    pub fn scan_and_fire_cpu_only(&mut self) -> Vec<(String, Bytes)> {
        let now = Instant::now();
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }
        self.last_scan_time = now;

        let now_ms = Self::instant_to_ms_at(now, now);
        let mut heap_popped = Vec::new();

        while let Some(&(Reverse(fire_ms), _)) = self.ready_heap.peek() {
            if fire_ms > now_ms {
                break;
            }
            if let Some((Reverse(fire_ms), route)) = self.ready_heap.pop() {
                heap_popped.push((fire_ms, route));
            }
        }

        let mut fired = Vec::new();
        for (fire_ms, route) in heap_popped {
            if let Some(def) = self.schedules.get(&route) {
                if def.next_fire_ms == fire_ms {
                    fired.push((route.clone(), def.payload.clone()));
                }
            }
        }

        fired
    }

    #[doc(hidden)]
    pub fn bench_prepare_scan(&mut self, ready_count: usize) {
        let now = Instant::now();
        let ready_limit = ready_count.min(self.list_entries.len());
        let ready_ms = Self::instant_to_ms_at(now, now).saturating_sub(1);
        let not_ready_time = now.checked_add(Duration::from_secs(60)).unwrap_or(now);
        let not_ready_ms = Self::instant_to_ms_at(not_ready_time, now);
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
                self.ready_heap.push((Reverse(next_fire_ms), route));
            }
        }

        self.last_scan_time = now
            .checked_sub(self.scan_dedup_window + Duration::from_millis(1))
            .unwrap_or(now);
    }

    fn current_epoch_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis() as u64
    }

    fn instant_to_ms_at(instant: Instant, anchor: Instant) -> u64 {
        let anchor_ms = Self::current_epoch_ms();
        if instant >= anchor {
            anchor_ms.saturating_add(instant.duration_since(anchor).as_millis() as u64)
        } else {
            anchor_ms.saturating_sub(anchor.duration_since(instant).as_millis() as u64)
        }
    }

    fn ms_to_instant_at(timestamp_ms: u64, anchor: Instant) -> Instant {
        let anchor_ms = Self::current_epoch_ms();
        if timestamp_ms >= anchor_ms {
            anchor
                .checked_add(Duration::from_millis(timestamp_ms - anchor_ms))
                .unwrap_or(anchor)
        } else {
            anchor
                .checked_sub(Duration::from_millis(anchor_ms - timestamp_ms))
                .unwrap_or(anchor)
        }
    }

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
            ScheduleMessage::CreateBatch { entries } => match self.create_schedules(entries) {
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
            ScheduleMessage::Subscribe { .. } => ScheduleResponse::Ok,
            ScheduleMessage::Unsubscribe { .. } => ScheduleResponse::Ok,
            ScheduleMessage::UnsubscribeAll { .. } => ScheduleResponse::Ok,
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
    use super::*;
    use crate::testkit::create_test_engine_with_cfs;

    fn make_actor() -> ScheduleActor {
        let store = create_test_engine_with_cfs(vec![1]);
        ScheduleActor::new(
            RouteFamily::new(1),
            store,
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    #[test]
    fn should_normalize_overdue_persisted_schedule_forward_on_preload() {
        let db = create_test_engine_with_cfs(vec![1]);
        let store = ScheduleStore::new(db.clone());
        let route = "schedule://acme/jobs/normalize/run";
        let payload = Bytes::from_static(b"payload");
        let overdue_ms = ScheduleActor::current_epoch_ms().saturating_sub(4 * 60 * 60 * 1000);

        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms: overdue_ms,
                    previous_fire_ms: None,
                },
                cntryl_midge::WriteOptions::buffered(),
            )
            .expect("insert overdue schedule");

        let actor = ScheduleActor::try_new(
            RouteFamily::new(1),
            db,
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("load actor");

        let schedule = actor
            .schedules
            .get(route)
            .expect("persisted schedule should be loaded");
        assert!(schedule.next_fire_ms > overdue_ms);
    }

    #[test]
    fn should_not_advance_in_memory_or_fire_when_reschedule_persist_fails() {
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/failure/run";
        actor
            .create_schedule(
                route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);

        let before = actor.schedules.get(route).expect("schedule").next_fire_ms;

        actor.store.fail_next_commit_for_tests();
        let fired = actor.scan_and_fire();

        assert!(fired.is_empty(), "scan should not emit on persist failure");
        assert_eq!(
            actor.schedules.get(route).expect("schedule").next_fire_ms,
            before,
            "in-memory schedule should not advance when persistence fails"
        );
    }

    #[test]
    fn should_ignore_stale_heap_entries_left_by_upsert() {
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/stale/run";
        actor
            .create_schedule(
                route.to_string(),
                "0 2 * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");

        let now = Instant::now();
        let stale_fire_ms = ScheduleActor::instant_to_ms_at(now, now).saturating_sub(1);
        let current_fire_ms =
            ScheduleActor::instant_to_ms_at(now.checked_add(Duration::from_secs(60)).unwrap(), now);

        {
            let schedule = actor.schedules.get_mut(route).expect("schedule");
            schedule.next_fire_time = now.checked_add(Duration::from_secs(60)).unwrap();
            schedule.next_fire_ms = current_fire_ms;
        }
        actor.ready_heap.clear();
        actor
            .ready_heap
            .push((Reverse(stale_fire_ms), route.to_string()));
        actor.last_scan_time = now
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        let fired = actor.scan_and_fire_at(now);

        assert!(
            fired.is_empty(),
            "stale heap entry should not fire when definition has moved forward"
        );
        assert_eq!(
            actor.schedules.get(route).expect("schedule").next_fire_ms,
            current_fire_ms
        );
    }
}
