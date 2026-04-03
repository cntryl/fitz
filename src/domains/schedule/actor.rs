use crate::domains::schedule::protocol::{
    epoch_ms_to_instant_with_reference, instant_to_epoch_ms_with_reference,
    parse_concrete_schedule_route, validate_concrete_schedule_route, Clock, CronSchedule,
    ScheduleCreateEntry, ScheduleDef, ScheduleListEntry, ScheduleMessage, ScheduleResponse,
    SystemClock,
};
use crate::domains::schedule::store::{
    PersistedPendingFire, PersistedSchedule, ScheduleFireClaim, ScheduleInsert, ScheduleStore,
};
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;
use bytes::Bytes;
use fxhash::FxBuildHasher;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
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
    last_fire_ms: Option<u64>,
    executions_total: u64,
    previous_list_index: Option<usize>,
}

struct PendingScheduleFire {
    route: String,
    cron: String,
    payload: Bytes,
    next_fire_time: Instant,
    next_fire_ms: u64,
    previous_fire_ms: u64,
    last_fire_ms: Option<u64>,
    executions_total: u64,
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
    /// Durably claimed schedule fires awaiting successful publish.
    pending_fires: BTreeMap<(u64, String), Bytes>,
    /// Recent successful schedule delivery timestamps for live per-minute stats.
    recent_execution_ms: VecDeque<u64>,
    /// Injected wall-clock and monotonic time source.
    clock: Arc<dyn Clock>,
}

impl ScheduleActor {
    const READY_HEAP_REBUILD_SLACK: usize = 32;

    pub fn try_new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        Self::try_new_with_clock(family, db, write_options, Arc::new(SystemClock))
    }

    pub fn try_new_with_clock(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, String> {
        let now = clock.now_instant();
        Self::try_new_at_with_clock(family, db, write_options, clock, now)
    }

    pub fn new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_new(family, db, write_options).expect("schedule actor startup should succeed")
    }

    pub fn new_with_clock(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::try_new_with_clock(family, db, write_options, clock)
            .expect("schedule actor startup should succeed")
    }

    #[cfg(test)]
    fn try_new_at(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        now: Instant,
    ) -> Result<Self, String> {
        Self::try_new_at_with_clock(family, db, write_options, Arc::new(SystemClock), now)
    }

    fn try_new_at_with_clock(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
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
            pending_fires: BTreeMap::new(),
            recent_execution_ms: VecDeque::new(),
            clock,
        };

        actor.preload_from_store_at(now)?;
        Ok(actor)
    }

    fn preload_from_store_at(&mut self, now: Instant) -> Result<(), String> {
        let now_ms = Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref());
        let entries = self
            .store
            .load_all(self.family.as_u64(), self.write_options)?;
        let mut normalization_batch = Vec::new();

        for PersistedSchedule {
            route,
            cron,
            payload,
            next_fire_ms,
            last_fire_ms,
            executions_total,
        } in entries
        {
            match CronSchedule::parse(&cron) {
                Ok(parsed_cron) => {
                    self.cron_cache.insert(cron.clone(), parsed_cron.clone());

                    let (effective_next_fire_time, effective_next_fire_ms, previous_fire_ms) =
                        if next_fire_ms <= now_ms {
                            let normalized_next_fire_time =
                                parsed_cron.next_fire_time_with_clock(now, self.clock.as_ref());
                            let normalized_next_fire_ms = Self::instant_to_ms_at_with_clock(
                                normalized_next_fire_time,
                                now,
                                self.clock.as_ref(),
                            );
                            normalization_batch.push(
                                crate::domains::schedule::store::ScheduleBatchInsert {
                                    route: route.clone(),
                                    cron: cron.clone(),
                                    payload: payload.clone(),
                                    next_fire_ms: normalized_next_fire_ms,
                                    previous_fire_ms: Some(next_fire_ms),
                                    last_fire_ms,
                                    executions_total,
                                },
                            );
                            (
                                normalized_next_fire_time,
                                normalized_next_fire_ms,
                                Some(next_fire_ms),
                            )
                        } else {
                            (
                                Self::ms_to_instant_at_with_clock(
                                    next_fire_ms,
                                    now,
                                    self.clock.as_ref(),
                                ),
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
                        last_fire_ms,
                        executions_total,
                        list_index,
                    };

                    if let Some(last_fire_ms) = last_fire_ms {
                        if now_ms.saturating_sub(last_fire_ms) <= 60_000 {
                            self.recent_execution_ms.push_back(last_fire_ms);
                        }
                    }

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

        for pending_fire in self.store.load_pending_fires(self.family.as_u64())? {
            self.pending_fires.insert(
                (pending_fire.fire_ms, pending_fire.route),
                pending_fire.payload,
            );
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
                        last_run: schedule.last_fire_ms.and_then(|timestamp_ms| {
                            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                                timestamp_ms as i64,
                            )
                            .map(|timestamp| timestamp.to_rfc3339())
                        }),
                        executions_total: schedule.executions_total,
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

        let (previous_next_fire_ms, previous_list_index, previous_last_fire_ms, previous_executions_total) = self
            .schedules
            .get(&route)
            .map(|existing| {
                (
                    Some(existing.next_fire_ms),
                    Some(existing.list_index),
                    existing.last_fire_ms,
                    existing.executions_total,
                )
            })
            .unwrap_or((None, None, None, 0));

        if let Some(existing) = self.schedules.get(&route) {
            if existing.cron == cron && existing.payload == payload {
                return Ok(false);
            }
        }

        let cron_obj = self.parsed_cron_for(&cron)?;
        let next_fire_time = cron_obj.next_fire_time_with_clock(now, self.clock.as_ref());
        let next_fire_ms =
            Self::instant_to_ms_at_with_clock(next_fire_time, now, self.clock.as_ref());

        self.store.insert(
            self.family.as_u64(),
            ScheduleInsert {
                route: &route,
                cron: &cron,
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: previous_next_fire_ms,
                last_fire_ms: previous_last_fire_ms,
                executions_total: previous_executions_total,
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
            last_fire_ms: previous_last_fire_ms,
            executions_total: previous_executions_total,
            list_index,
        };

        self.schedules.insert(route.clone(), def);
        self.ready_heap.push((Reverse(next_fire_ms), route));
        self.compact_ready_heap_if_needed();
        Ok(true)
    }

    pub fn create_schedule(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
    ) -> Result<bool, String> {
        self.create_schedule_at(route, cron, payload, self.clock.now_instant())
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

            let (previous_fire_ms, previous_list_index, last_fire_ms, executions_total, should_skip) =
                match self.schedules.get(&entry.route) {
                    Some(existing)
                        if existing.cron == entry.cron && existing.payload == entry.payload =>
                    {
                        (
                            Some(existing.next_fire_ms),
                            Some(existing.list_index),
                            existing.last_fire_ms,
                            existing.executions_total,
                            true,
                        )
                    }
                    Some(existing) => (
                        Some(existing.next_fire_ms),
                        Some(existing.list_index),
                        existing.last_fire_ms,
                        existing.executions_total,
                        false,
                    ),
                    None => (None, None, None, 0, false),
                };

            if should_skip {
                continue;
            }

            let parsed_cron = self.parsed_cron_for(&entry.cron)?;
            let next_fire_time = parsed_cron.next_fire_time_with_clock(now, self.clock.as_ref());
            let next_fire_ms =
                Self::instant_to_ms_at_with_clock(next_fire_time, now, self.clock.as_ref());

            pending.push(PendingScheduleCreate {
                route: entry.route,
                cron: entry.cron,
                parsed_cron,
                payload: entry.payload,
                next_fire_time,
                next_fire_ms,
                previous_fire_ms,
                last_fire_ms,
                executions_total,
                previous_list_index,
            });
        }

        if pending.is_empty() {
            return Ok(0);
        }

        let store_items: Vec<_> = pending
            .iter()
            .map(|entry| crate::domains::schedule::store::ScheduleBatchInsert {
                route: entry.route.clone(),
                cron: entry.cron.clone(),
                payload: entry.payload.clone(),
                next_fire_ms: entry.next_fire_ms,
                previous_fire_ms: entry.previous_fire_ms,
                last_fire_ms: entry.last_fire_ms,
                executions_total: entry.executions_total,
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
                last_fire_ms: entry.last_fire_ms,
                executions_total: entry.executions_total,
                list_index,
            };

            self.ready_heap
                .push((Reverse(entry.next_fire_ms), entry.route.clone()));
            self.schedules.insert(entry.route, def);
        }

        self.compact_ready_heap_if_needed();

        Ok(changed)
    }

    pub fn create_schedules(&mut self, entries: Vec<ScheduleCreateEntry>) -> Result<usize, String> {
        self.create_schedules_at(entries, self.clock.now_instant())
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
            self.compact_ready_heap_if_needed();
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

    fn compact_ready_heap_if_needed(&mut self) {
        if self.schedules.is_empty() {
            self.ready_heap.clear();
            return;
        }

        let live_entries = self.schedules.len();
        let heap_entries = self.ready_heap.len();
        let stale_entries = heap_entries.saturating_sub(live_entries);

        if stale_entries >= Self::READY_HEAP_REBUILD_SLACK
            || heap_entries > live_entries.saturating_mul(2)
        {
            self.rebuild_ready_heap();
        }
    }

    fn rebuild_ready_heap(&mut self) {
        let mut rebuilt = BinaryHeap::with_capacity(self.schedules.len());
        for schedule in self.schedules.values() {
            rebuilt.push((Reverse(schedule.next_fire_ms), schedule.route.clone()));
        }
        self.ready_heap = rebuilt;
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

    fn claim_due_fires_at(&mut self, now: Instant) -> Vec<PersistedPendingFire> {
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }
        self.last_scan_time = now;

        let now_ms = Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref());
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

            let next_fire_time =
                def.parsed_cron.next_fire_time_with_clock(now, self.clock.as_ref());
            let next_fire_ms =
                Self::instant_to_ms_at_with_clock(next_fire_time, now, self.clock.as_ref());
            to_reschedule.push(PendingScheduleFire {
                route: route.clone(),
                cron: def.cron.clone(),
                payload: def.payload.clone(),
                next_fire_time,
                next_fire_ms,
                previous_fire_ms: fire_ms,
                last_fire_ms: def.last_fire_ms,
                executions_total: def.executions_total,
            });
        }

        if to_reschedule.is_empty() {
            return Vec::new();
        }

        let store_items: Vec<_> = to_reschedule
            .iter()
            .map(|item| ScheduleFireClaim {
                route: &item.route,
                cron: &item.cron,
                payload: &item.payload,
                next_fire_ms: item.next_fire_ms,
                previous_fire_ms: item.previous_fire_ms,
                last_fire_ms: item.last_fire_ms,
                executions_total: item.executions_total,
            })
            .collect();

        if let Err(error) = self
            .store
            .claim_due_batch(self.family.as_u64(), &store_items, self.write_options)
        {
            warn!("Failed to persist schedule reschedule batch: {}", error);
            for item in to_reschedule {
                self.ready_heap
                    .push((Reverse(item.previous_fire_ms), item.route.clone()));
            }
            return Vec::new();
        }

        let mut claimed = Vec::with_capacity(store_items.len());
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
            self.pending_fires.insert(
                (item.previous_fire_ms, item.route.clone()),
                item.payload.clone(),
            );
            claimed.push(PersistedPendingFire {
                route: item.route.clone(),
                payload: item.payload.clone(),
                fire_ms: item.previous_fire_ms,
            });
            info!(
                "Schedule fired: {} (next fire: ~{:?})",
                item.route, item.next_fire_time
            );
        }

        claimed
    }

    fn scan_and_fire_at(&mut self, now: Instant) -> Vec<(String, Bytes)> {
        self.claim_due_fires_at(now)
            .into_iter()
            .map(|item| (item.route, item.payload))
            .collect()
    }

    pub fn scan_and_fire(&mut self) -> Vec<(String, Bytes)> {
        self.scan_and_fire_at(self.clock.now_instant())
    }

    pub(crate) fn pending_fires_for_delivery(&self) -> Vec<PersistedPendingFire> {
        self.pending_fires
            .iter()
            .map(|((fire_ms, route), payload)| PersistedPendingFire {
                route: route.clone(),
                payload: payload.clone(),
                fire_ms: *fire_ms,
            })
            .collect()
    }

    pub(crate) fn ack_pending_fires(
        &mut self,
        delivered: &[(u64, String)],
    ) -> Result<usize, String> {
        if delivered.is_empty() {
            return Ok(0);
        }

        let acknowledged_at_ms = self.clock.now_epoch_ms();
        let store_items: Vec<_> = delivered
            .iter()
            .map(|(fire_ms, route)| crate::domains::schedule::store::SchedulePendingFireAck {
                route,
                fire_ms: *fire_ms,
                executed_at_ms: acknowledged_at_ms,
            })
            .collect();

        self.store
            .ack_pending_fires(self.family.as_u64(), &store_items, self.write_options)?;

        let mut acked = 0;
        for (fire_ms, route) in delivered {
            if self.pending_fires.remove(&(*fire_ms, route.clone())).is_some() {
                if let Some(schedule) = self.schedules.get_mut(route) {
                    schedule.last_fire_ms = Some(acknowledged_at_ms);
                    schedule.executions_total = schedule.executions_total.saturating_add(1);
                }
                self.record_recent_execution(acknowledged_at_ms);
                acked += 1;
            }
        }

        Ok(acked)
    }

    pub(crate) fn pending_fire_count(&self) -> usize {
        self.pending_fires.len()
    }

    pub(crate) fn executions_per_minute(&mut self) -> f64 {
        let now_ms = self.clock.now_epoch_ms();
        self.prune_recent_executions(now_ms);
        self.recent_execution_ms.len() as f64
    }

    fn record_recent_execution(&mut self, executed_at_ms: u64) {
        self.prune_recent_executions(executed_at_ms);
        self.recent_execution_ms.push_back(executed_at_ms);
    }

    fn prune_recent_executions(&mut self, now_ms: u64) {
        while let Some(oldest) = self.recent_execution_ms.front().copied() {
            if now_ms.saturating_sub(oldest) <= 60_000 {
                break;
            }
            self.recent_execution_ms.pop_front();
        }
    }

    #[doc(hidden)]
    pub fn scan_and_fire_cpu_only(&mut self) -> Vec<(String, Bytes)> {
        let now = self.clock.now_instant();
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }
        self.last_scan_time = now;

        let now_ms = Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref());
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
        let now = self.clock.now_instant();
        let ready_limit = ready_count.min(self.list_entries.len());
        let ready_ms = Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref())
            .saturating_sub(1);
        let not_ready_time = now.checked_add(Duration::from_secs(60)).unwrap_or(now);
        let not_ready_ms =
            Self::instant_to_ms_at_with_clock(not_ready_time, now, self.clock.as_ref());
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

    #[cfg(test)]
    fn current_epoch_ms() -> u64 {
        Self::current_epoch_ms_with_clock(&SystemClock)
    }

    #[cfg(test)]
    fn current_epoch_ms_with_clock(clock: &dyn Clock) -> u64 {
        clock.now_epoch_ms()
    }

    fn anchor_epoch_ms_with_clock(anchor: Instant, clock: &dyn Clock) -> u64 {
        instant_to_epoch_ms_with_reference(anchor, clock.now_instant(), clock.now_epoch_ms())
    }

    #[cfg(test)]
    fn instant_to_ms_at(instant: Instant, anchor: Instant) -> u64 {
        Self::instant_to_ms_at_with_clock(instant, anchor, &SystemClock)
    }

    fn instant_to_ms_at_with_clock(instant: Instant, anchor: Instant, clock: &dyn Clock) -> u64 {
        let anchor_ms = Self::anchor_epoch_ms_with_clock(anchor, clock);
        instant_to_epoch_ms_with_reference(instant, anchor, anchor_ms)
    }

    fn ms_to_instant_at_with_clock(timestamp_ms: u64, anchor: Instant, clock: &dyn Clock) -> Instant {
        let anchor_ms = Self::anchor_epoch_ms_with_clock(anchor, clock);
        epoch_ms_to_instant_with_reference(timestamp_ms, anchor, anchor_ms)
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
    use chrono::TimeZone;
    use std::sync::Mutex;

    #[derive(Clone)]
    struct MockClock {
        state: Arc<Mutex<MockClockState>>,
    }

    #[derive(Clone, Copy)]
    struct MockClockState {
        instant: Instant,
        epoch_ms: u64,
    }

    impl MockClock {
        fn new(epoch_ms: u64) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockClockState {
                    instant: Instant::now(),
                    epoch_ms,
                })),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut state = self.state.lock().expect("lock mock clock");
            state.instant += duration;
            state.epoch_ms = state.epoch_ms.saturating_add(duration.as_millis() as u64);
        }
    }

    impl Clock for MockClock {
        fn now_instant(&self) -> Instant {
            self.state.lock().expect("lock mock clock").instant
        }

        fn now_epoch_ms(&self) -> u64 {
            self.state.lock().expect("lock mock clock").epoch_ms
        }
    }

    fn epoch_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u64 {
        chrono::Utc
            .with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .expect("valid datetime")
            .timestamp_millis() as u64
    }

    fn make_actor() -> ScheduleActor {
        let store = create_test_engine_with_cfs(vec![1]);
        ScheduleActor::new(
            RouteFamily::new(1),
            store,
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    fn make_actor_with_clock(clock: Arc<dyn Clock>) -> ScheduleActor {
        let store = create_test_engine_with_cfs(vec![1]);
        ScheduleActor::new_with_clock(
            RouteFamily::new(1),
            store,
            cntryl_midge::WriteOptions::buffered(),
            clock,
        )
    }

    #[test]
    fn should_normalize_overdue_persisted_schedule_forward_on_preload() {
        // Arrange
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
                    last_fire_ms: None,
                    executions_total: 0,
                },
                cntryl_midge::WriteOptions::buffered(),
            )
            .expect("insert overdue schedule");

        // Act
        let actor = ScheduleActor::try_new(
            RouteFamily::new(1),
            db,
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("load actor");

        // Assert
        let schedule = actor
            .schedules
            .get(route)
            .expect("persisted schedule should be loaded");
        assert!(schedule.next_fire_ms > overdue_ms);
    }

    #[test]
    fn should_skip_missed_execution_given_overdue_schedule_on_preload() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let store = ScheduleStore::new(db.clone());
        let route = "schedule://acme/jobs/skip/run";
        let payload = Bytes::from_static(b"payload");
        let now = Instant::now();
        let overdue_at = now.checked_sub(Duration::from_secs(120)).unwrap();
        let overdue_ms = ScheduleActor::instant_to_ms_at(overdue_at, now);

        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms: overdue_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                cntryl_midge::WriteOptions::buffered(),
            )
            .expect("insert overdue schedule");

        let mut actor = ScheduleActor::try_new_at(
            RouteFamily::new(1),
            db,
            cntryl_midge::WriteOptions::buffered(),
            now,
        )
        .expect("load actor");
        actor.last_scan_time = now
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        // Act
        let fired = actor.scan_and_fire_at(now);

        // Assert
        assert!(
            fired.is_empty(),
            "overdue schedules should normalize forward instead of replaying missed executions"
        );
        assert!(
            actor
                .schedules
                .get(route)
                .expect("schedule")
                .next_fire_time
                > now
        );
    }

    #[test]
    fn should_preserve_pending_due_occurrence_given_identical_create_retry_at_due_boundary() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/retry/run".to_string();
        let cron = "* * * * *".to_string();
        let payload = Bytes::from_static(b"payload");
        let created_at = Instant::now();

        actor
            .create_schedule_at(route.clone(), cron.clone(), payload.clone(), created_at)
            .expect("create schedule");

        let original = actor.schedules.get(&route).expect("schedule").clone();

        // Act
        let changed = actor
            .create_schedule_at(
                route.clone(),
                cron,
                payload,
                original.next_fire_time,
            )
            .expect("retry identical create");

        // Assert
        assert!(!changed, "identical retry should remain idempotent at due time");
        let schedule = actor.schedules.get(&route).expect("schedule");
        assert_eq!(schedule.next_fire_ms, original.next_fire_ms);
        assert_eq!(schedule.next_fire_time, original.next_fire_time);
    }

    #[test]
    fn should_preserve_pending_due_occurrence_given_identical_batch_retry_at_due_boundary() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/batch-retry/run".to_string();
        let cron = "* * * * *".to_string();
        let payload = Bytes::from_static(b"payload");
        let created_at = Instant::now();

        actor
            .create_schedule_at(route.clone(), cron.clone(), payload.clone(), created_at)
            .expect("create schedule");

        let original = actor.schedules.get(&route).expect("schedule").clone();
        let entries = vec![ScheduleCreateEntry {
            route: route.clone(),
            cron,
            payload,
        }];

        // Act
        let changed = actor
            .create_schedules_at(entries, original.next_fire_time)
            .expect("retry identical batch create");

        // Assert
        assert_eq!(changed, 0, "identical batch retry should not rewrite the schedule");
        let schedule = actor.schedules.get(&route).expect("schedule");
        assert_eq!(schedule.next_fire_ms, original.next_fire_ms);
        assert_eq!(schedule.next_fire_time, original.next_fire_time);
    }

    #[test]
    fn should_not_advance_in_memory_or_fire_when_reschedule_persist_fails() {
        // Arrange
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

        // Act
        actor.store.fail_next_commit_for_tests();
        let fired = actor.scan_and_fire();

        // Assert
        assert!(fired.is_empty(), "scan should not emit on persist failure");
        assert_eq!(
            actor.schedules.get(route).expect("schedule").next_fire_ms,
            before,
            "in-memory schedule should not advance when persistence fails"
        );
    }

    #[test]
    fn should_report_execution_state_given_acknowledged_pending_fire() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/execution-state/run";
        actor
            .create_schedule(
                route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);
        let claimed_fire_ms = actor.schedules.get(route).expect("schedule").next_fire_ms;
        let now = Instant::now();
        actor.last_scan_time = now
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        // Act
        let fired = actor.scan_and_fire_at(now);
        actor
            .ack_pending_fires(&[(claimed_fire_ms, route.to_string())])
            .expect("ack pending fire");
        let snapshot = actor.admin_snapshot();

        // Assert
        assert_eq!(fired.len(), 1, "scan should claim one due occurrence");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].executions_total, 1);
        assert!(snapshot[0].last_run.is_some());
        assert!(actor.executions_per_minute() >= 1.0);
    }

    #[test]
    fn should_compact_ready_heap_given_repeated_schedule_upserts() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/compact/run".to_string();
        let now = Instant::now();
        actor
            .create_schedule_at(
                route.clone(),
                "0 2 * * *".to_string(),
                Bytes::from_static(b"one"),
                now,
            )
            .expect("create schedule");
        actor
            .create_schedule_at(
                route.clone(),
                "0 3 * * *".to_string(),
                Bytes::from_static(b"two"),
                now,
            )
            .expect("first upsert");

        // Act
        actor
            .create_schedule_at(
                route.clone(),
                "0 4 * * *".to_string(),
                Bytes::from_static(b"three"),
                now,
            )
            .expect("second upsert");

        // Assert
        assert_eq!(actor.schedule_count(), 1);
        assert_eq!(actor.ready_heap.len(), 1);
        assert_eq!(actor.schedules.get(&route).expect("schedule").cron, "0 4 * * *");
    }

    #[test]
    fn should_clear_ready_heap_given_last_schedule_delete() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/delete-compact/run";
        actor
            .create_schedule(
                route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");

        // Act
        let deleted = actor
            .delete_schedule(route.to_string())
            .expect("delete schedule");

        // Assert
        assert!(deleted);
        assert!(actor.ready_heap.is_empty());
        assert_eq!(actor.schedule_count(), 0);
    }

    #[test]
    fn should_invalidate_shared_full_list_cache_given_schedule_delete() {
        // Arrange
        let mut actor = make_actor();
        let first_route = "schedule://acme/jobs/cache-first/run";
        let second_route = "schedule://acme/jobs/cache-second/run";
        actor
            .create_schedule(
                first_route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"first"),
            )
            .expect("create first schedule");
        actor
            .create_schedule(
                second_route.to_string(),
                "0 * * * *".to_string(),
                Bytes::from_static(b"second"),
            )
            .expect("create second schedule");
        let (cached, _) = actor.list_entries(0, 0);

        // Act
        actor
            .delete_schedule(first_route.to_string())
            .expect("delete schedule");
        let (refreshed, total_count) = actor.list_entries(0, 0);

        // Assert
        assert_eq!(cached.len(), 2);
        assert_eq!(refreshed.len(), 1);
        assert_eq!(total_count, 1);
        assert!(refreshed.iter().all(|entry| entry.route != first_route));
        assert!(refreshed.iter().any(|entry| entry.route == second_route));
    }

    #[test]
    fn should_ignore_stale_heap_entries_left_by_upsert() {
        // Arrange
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

        // Act
        let fired = actor.scan_and_fire_at(now);

        // Assert
        assert!(
            fired.is_empty(),
            "stale heap entry should not fire when definition has moved forward"
        );
        assert_eq!(
            actor.schedules.get(route).expect("schedule").next_fire_ms,
            current_fire_ms
        );
    }

    #[test]
    fn should_not_fire_deleted_schedule_given_stale_ready_heap_entry() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/cancel/run";
        actor
            .create_schedule(
                route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);
        actor
            .delete_schedule(route.to_string())
            .expect("delete schedule");

        let now = Instant::now();
        actor.last_scan_time = now
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        // Act
        let fired = actor.scan_and_fire_at(now);

        // Assert
        assert!(
            fired.is_empty(),
            "deleted schedules should not ghost-fire from stale heap entries"
        );
        assert_eq!(actor.schedule_count(), 0);
    }

    #[test]
    fn should_not_fire_future_occurrence_given_cancel_after_due_scan() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/cancel-after-scan/run";
        actor
            .create_schedule(
                route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);

        let first_scan_at = Instant::now();
        actor.last_scan_time = first_scan_at
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        // Act
        let first_fired = actor.scan_and_fire_at(first_scan_at);
        let next_fire_time = actor
            .schedules
            .get(route)
            .expect("schedule")
            .next_fire_time;
        actor
            .delete_schedule(route.to_string())
            .expect("delete schedule");
        actor.last_scan_time = next_fire_time
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();
        let second_fired = actor.scan_and_fire_at(next_fire_time);

        // Assert
        assert_eq!(first_fired.len(), 1, "scan should claim the due occurrence once");
        assert!(
            second_fired.is_empty(),
            "cancel after the due scan should suppress all future occurrences"
        );
        assert_eq!(actor.schedule_count(), 0);
    }

    #[test]
    fn should_not_fire_original_due_occurrence_given_batch_reschedule_before_due_scan() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/batch-reschedule/run".to_string();
        let original_payload = Bytes::from_static(b"original");
        let replacement_payload = Bytes::from_static(b"replacement");
        let created_at = Instant::now();

        actor
            .create_schedule_at(
                route.clone(),
                "* * * * *".to_string(),
                original_payload,
                created_at,
            )
            .expect("create schedule");

        let original = actor.schedules.get(&route).expect("schedule").clone();
        let entries = vec![ScheduleCreateEntry {
            route: route.clone(),
            cron: "0 2 * * *".to_string(),
            payload: replacement_payload.clone(),
        }];

        // Act
        let changed = actor
            .create_schedules_at(entries, original.next_fire_time)
            .expect("batch reschedule");
        actor.last_scan_time = original
            .next_fire_time
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();
        let fired = actor.scan_and_fire_at(original.next_fire_time);

        // Assert
        assert_eq!(changed, 1, "batch reschedule should rewrite the schedule");
        assert!(
            fired.is_empty(),
            "reschedule before the due scan should suppress the original occurrence"
        );
        let schedule = actor.schedules.get(&route).expect("schedule");
        assert_eq!(schedule.cron, "0 2 * * *");
        assert_eq!(schedule.payload, replacement_payload);
        assert!(schedule.next_fire_time > original.next_fire_time);
    }

    #[test]
    fn should_not_fire_original_due_occurrence_given_single_reschedule_before_due_scan() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/reschedule/run".to_string();
        let original_payload = Bytes::from_static(b"original");
        let replacement_payload = Bytes::from_static(b"replacement");
        let created_at = Instant::now();

        actor
            .create_schedule_at(
                route.clone(),
                "* * * * *".to_string(),
                original_payload,
                created_at,
            )
            .expect("create schedule");

        let original = actor.schedules.get(&route).expect("schedule").clone();

        // Act
        let changed = actor
            .create_schedule_at(
                route.clone(),
                "0 2 * * *".to_string(),
                replacement_payload.clone(),
                original.next_fire_time,
            )
            .expect("reschedule before due scan");
        actor.last_scan_time = original
            .next_fire_time
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();
        let fired = actor.scan_and_fire_at(original.next_fire_time);

        // Assert
        assert!(changed, "single reschedule should rewrite the schedule");
        assert!(
            fired.is_empty(),
            "reschedule before the due scan should suppress the original occurrence"
        );
        let schedule = actor.schedules.get(&route).expect("schedule");
        assert_eq!(schedule.cron, "0 2 * * *");
        assert_eq!(schedule.payload, replacement_payload);
        assert!(schedule.next_fire_time > original.next_fire_time);
    }

    #[test]
    fn should_allow_claimed_due_occurrence_given_single_reschedule_after_due_scan() {
        // Arrange
        let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 59, 30)));
        let mut actor = make_actor_with_clock(clock.clone());
        let route = "schedule://acme/jobs/reschedule-after-scan/run".to_string();
        let original_payload = Bytes::from_static(b"original");
        let replacement_payload = Bytes::from_static(b"replacement");

        actor
            .create_schedule_at(
                route.clone(),
                "* * * * *".to_string(),
                original_payload.clone(),
                clock.now_instant(),
            )
            .expect("create schedule");

        let original = actor.schedules.get(&route).expect("schedule").clone();
        clock.advance(original.next_fire_time.duration_since(clock.now_instant()));
        actor.last_scan_time = clock
            .now_instant()
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        // Act
        let first_fired = actor.scan_and_fire();
        let changed = actor
            .create_schedule_at(
                route.clone(),
                "0 2 * * *".to_string(),
                replacement_payload.clone(),
                clock.now_instant(),
            )
            .expect("reschedule after due scan");
        clock.advance(Duration::from_millis(1));
        actor.last_scan_time = clock
            .now_instant()
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();
        let second_fired = actor.scan_and_fire();

        // Assert
        assert_eq!(first_fired.len(), 1, "scan should claim the original due occurrence");
        assert_eq!(first_fired[0].0, route);
        assert_eq!(first_fired[0].1, original_payload);
        assert!(changed, "reschedule after the due scan should update future occurrences");
        assert!(
            second_fired.is_empty(),
            "rescheduling after the due scan should not create a duplicate fire at the old due boundary"
        );
        let schedule = actor
            .schedules
            .get("schedule://acme/jobs/reschedule-after-scan/run")
            .expect("schedule");
        assert_eq!(schedule.cron, "0 2 * * *");
        assert_eq!(schedule.payload, replacement_payload);
        assert!(schedule.next_fire_time > original.next_fire_time);
    }

    #[test]
    fn should_not_fire_twice_given_repeated_scan_within_dedup_window() {
        // Arrange
        let mut actor = make_actor();
        let route = "schedule://acme/jobs/dedup/run";
        actor
            .create_schedule(
                route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);

        let now = Instant::now();
        actor.last_scan_time = now
            .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
            .unwrap();

        // Act
        let first = actor.scan_and_fire_at(now);
        let second = actor.scan_and_fire_at(now.checked_add(Duration::from_millis(1)).unwrap());

        // Assert
        assert_eq!(first.len(), 1, "first due scan should emit one fire");
        assert!(
            second.is_empty(),
            "repeated scans inside the dedup window should not emit a duplicate fire"
        );
    }
}
