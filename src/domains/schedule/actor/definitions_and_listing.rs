use super::model::{
    parse_concrete_schedule_route, Arc, BinaryHeap, Bytes, CronSchedule, FastSet, FxBuildHasher,
    Instant, PendingScheduleCreate, Reverse, ScheduleActor, ScheduleCreateEntry, ScheduleDef,
    ScheduleDeliveryMode, ScheduleInsert, ScheduleListEntry,
    METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL, METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL,
    METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL,
};

impl ScheduleActor {
    /// # Errors
    ///
    /// Returns an error when the cron expression is invalid.
    pub(super) fn parsed_cron_for(&mut self, cron: &str) -> Result<CronSchedule, String> {
        if let Some(parsed) = self.cron_cache.get(cron) {
            return Ok(parsed.clone());
        }

        let parsed = CronSchedule::parse(cron)?;
        self.cron_cache.insert(cron.to_string(), parsed.clone());
        Ok(parsed)
    }

    /// # Errors
    ///
    /// Returns an error when cron parsing fails or the updated definition
    /// cannot be persisted.
    pub(super) fn create_schedule_with_mode_at(
        &mut self,
        route: String,
        cron: String,
        delivery_mode: ScheduleDeliveryMode,
        payload: Bytes,
        now: Instant,
    ) -> Result<bool, String> {
        let route_parts = parse_concrete_schedule_route(&route)?;
        crate::domains::schedule::list_wire_budget::validate_listable_definition(
            &route,
            &cron,
            payload.len(),
        )?;
        let (
            previous_next_fire_ms,
            previous_list_index,
            previous_last_fire_ms,
            previous_executions_total,
        ) = self
            .schedules
            .get(&route)
            .map_or((None, None, None, 0), |existing| {
                (
                    Some(existing.next_fire_ms),
                    Some(existing.list_index),
                    existing.last_fire_ms,
                    existing.executions_total,
                )
            });

        if let Some(existing) = self.schedules.get(&route) {
            if existing.cron == cron
                && existing.delivery_mode == delivery_mode
                && existing.payload == payload
            {
                return Ok(false);
            }
        }

        let cron_obj = self.parsed_cron_for(&cron)?;
        let next_fire_time = cron_obj.try_next_fire_time_with_clock(now, self.clock.as_ref())?;
        let next_fire_ms =
            Self::instant_to_ms_at_with_clock(next_fire_time, now, self.clock.as_ref());

        if let Err(error) = self.store.insert(
            self.family.as_u64(),
            ScheduleInsert {
                route: &route,
                cron: &cron,
                delivery_mode,
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: previous_next_fire_ms,
                last_fire_ms: previous_last_fire_ms,
                executions_total: previous_executions_total,
            },
            self.write_options,
        ) {
            if previous_next_fire_ms.is_some() {
                crate::observability::counter_inc(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL);
            } else {
                crate::observability::counter_inc(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL);
            }
            return Err(error);
        }

        let list_index =
            self.upsert_list_entry(previous_list_index, &route, &cron, delivery_mode, &payload);
        let def = ScheduleDef {
            route: route.clone(),
            route_parts,
            cron,
            delivery_mode,
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

    /// # Errors
    ///
    /// Returns an error when cron parsing fails or the definition cannot be
    /// persisted.
    #[cfg(test)]
    pub(super) fn create_schedule_at(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
        now: Instant,
    ) -> Result<bool, String> {
        self.create_schedule_with_mode_at(
            route,
            cron,
            ScheduleDeliveryMode::Broadcast,
            payload,
            now,
        )
    }

    /// # Errors
    ///
    /// Returns an error when validation or durable persistence fails.
    pub fn create_schedule_with_mode(
        &mut self,
        route: String,
        cron: String,
        delivery_mode: ScheduleDeliveryMode,
        payload: Bytes,
    ) -> Result<bool, String> {
        self.create_schedule_with_mode_at(
            route,
            cron,
            delivery_mode,
            payload,
            self.clock.now_instant(),
        )
    }

    /// # Errors
    ///
    /// Returns an error when validation or durable persistence fails.
    pub fn create_schedule(
        &mut self,
        route: String,
        cron: String,
        payload: Bytes,
    ) -> Result<bool, String> {
        self.create_schedule_with_mode(route, cron, ScheduleDeliveryMode::Broadcast, payload)
    }

    /// # Errors
    ///
    /// Returns an error when the batch is empty, contains duplicate routes,
    /// includes an invalid cron expression, or cannot be persisted.
    pub(super) fn create_schedules_at(
        &mut self,
        entries: Vec<ScheduleCreateEntry>,
        now: Instant,
    ) -> Result<usize, String> {
        if entries.is_empty() {
            return Err("schedule batch must not be empty".to_string());
        }

        let mut seen_routes = FastSet::with_capacity_and_hasher(entries.len(), FxBuildHasher);
        let mut pending = Vec::with_capacity(entries.len());

        for entry in entries {
            if !seen_routes.insert(entry.route.clone()) {
                return Err(format!(
                    "duplicate schedule route in batch: {}",
                    entry.route
                ));
            }
            crate::domains::schedule::list_wire_budget::validate_listable_definition(
                &entry.route,
                &entry.cron,
                entry.payload.len(),
            )?;

            let (
                previous_fire_ms,
                previous_list_index,
                last_fire_ms,
                executions_total,
                should_skip,
            ) = match self.schedules.get(&entry.route) {
                Some(existing)
                    if existing.cron == entry.cron
                        && existing.delivery_mode == entry.delivery_mode
                        && existing.payload == entry.payload =>
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

            pending.push(self.build_pending_schedule_create(
                entry,
                now,
                previous_fire_ms,
                previous_list_index,
                last_fire_ms,
                executions_total,
            )?);
        }

        if pending.is_empty() {
            return Ok(0);
        }

        let store_items = Self::build_schedule_batch_insert_items(&pending);

        if let Err(error) =
            self.store
                .insert_batch(self.family.as_u64(), &store_items, self.write_options)
        {
            Self::record_batch_persistence_failures(&pending);
            return Err(error);
        }

        let changed = pending.len();
        for entry in pending {
            let list_index = self.upsert_list_entry(
                entry.previous_list_index,
                &entry.route,
                &entry.cron,
                entry.delivery_mode,
                &entry.payload,
            );
            let def = ScheduleDef {
                route: entry.route.clone(),
                route_parts: entry.route_parts,
                cron: entry.cron,
                delivery_mode: entry.delivery_mode,
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

    /// # Errors
    ///
    /// Returns an error when the batch is empty, contains duplicate routes,
    /// includes an invalid cron expression, or cannot be persisted.
    pub fn create_schedules(&mut self, entries: Vec<ScheduleCreateEntry>) -> Result<usize, String> {
        self.create_schedules_at(entries, self.clock.now_instant())
    }

    /// # Errors
    ///
    /// Returns an error when the route cannot be parsed or the deletion cannot
    /// be persisted.
    pub fn delete_schedule(&mut self, route: &str) -> Result<bool, String> {
        let parsed_route = parse_concrete_schedule_route(route)?;

        let Some(existing) = self.schedules.get(route) else {
            return Ok(false);
        };

        if let Err(error) = self.store.delete_current_with_realm(
            self.family.as_u64(),
            &parsed_route.realm,
            route,
            existing.next_fire_ms,
            self.write_options,
        ) {
            crate::observability::counter_inc(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL);
            return Err(error);
        }

        if let Some(removed_def) = self.schedules.remove(route) {
            self.remove_list_entry(removed_def.list_index);
            self.compact_ready_heap_if_needed();
            return Ok(true);
        }

        Ok(false)
    }

    fn build_pending_schedule_create(
        &mut self,
        entry: ScheduleCreateEntry,
        now: Instant,
        previous_fire_ms: Option<u64>,
        previous_list_index: Option<usize>,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    ) -> Result<PendingScheduleCreate, String> {
        let route_parts = parse_concrete_schedule_route(&entry.route)?;
        let parsed_cron = self.parsed_cron_for(&entry.cron)?;
        let next_fire_time = parsed_cron.try_next_fire_time_with_clock(now, self.clock.as_ref())?;
        let next_fire_ms =
            Self::instant_to_ms_at_with_clock(next_fire_time, now, self.clock.as_ref());

        Ok(PendingScheduleCreate {
            route: entry.route,
            route_parts,
            cron: entry.cron,
            delivery_mode: entry.delivery_mode,
            parsed_cron,
            payload: entry.payload,
            next_fire_time,
            next_fire_ms,
            previous_fire_ms,
            last_fire_ms,
            executions_total,
            previous_list_index,
        })
    }

    fn build_schedule_batch_insert_items(
        pending: &[PendingScheduleCreate],
    ) -> Vec<crate::domains::schedule::store::ScheduleBatchInsert> {
        pending
            .iter()
            .map(
                |entry| crate::domains::schedule::store::ScheduleBatchInsert {
                    route: entry.route.clone(),
                    cron: entry.cron.clone(),
                    delivery_mode: entry.delivery_mode,
                    payload: entry.payload.clone(),
                    next_fire_ms: entry.next_fire_ms,
                    previous_fire_ms: entry.previous_fire_ms,
                    last_fire_ms: entry.last_fire_ms,
                    executions_total: entry.executions_total,
                },
            )
            .collect()
    }

    fn record_batch_persistence_failures(pending: &[PendingScheduleCreate]) {
        let upsert_failures = pending
            .iter()
            .filter(|entry| entry.previous_list_index.is_some())
            .count() as u64;
        let create_failures = pending.len() as u64 - upsert_failures;

        if create_failures > 0 {
            crate::observability::counter_add(
                METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL,
                create_failures,
            );
        }
        if upsert_failures > 0 {
            crate::observability::counter_add(
                METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL,
                upsert_failures,
            );
        }
    }

    #[must_use]
    pub fn schedule_count(&self) -> usize {
        self.schedules.len()
    }

    #[must_use]
    pub fn list_defs(&self) -> Vec<(String, String, ScheduleDeliveryMode, Bytes)> {
        self.list_entries
            .iter()
            .map(|entry| {
                (
                    entry.route.clone(),
                    entry.cron.clone(),
                    entry.delivery_mode,
                    entry.payload.clone(),
                )
            })
            .collect()
    }

    pub(in crate::domains::schedule::actor) fn push_list_entry(
        &mut self,
        route: &str,
        cron: &str,
        delivery_mode: ScheduleDeliveryMode,
        payload: &Bytes,
    ) -> usize {
        let entry = Arc::new(ScheduleListEntry {
            route: route.to_string(),
            cron: cron.to_string(),
            delivery_mode,
            payload: payload.clone(),
        });
        let index = self.list_entries.len();
        self.list_entries.push(entry);
        index
    }

    pub(super) fn sync_cached_upsert(
        &mut self,
        current_index: Option<usize>,
        entry: Arc<ScheduleListEntry>,
    ) {
        let Some(cache) = self.list_cache.as_mut() else {
            return;
        };
        let cache_entries = Arc::make_mut(cache);
        if let Some(index) = current_index {
            cache_entries[index] = entry;
        } else {
            cache_entries.push(entry);
        }
    }

    pub(super) fn sync_cached_remove(&mut self, index: usize) {
        let Some(cache) = self.list_cache.as_mut() else {
            return;
        };
        let cache_entries = Arc::make_mut(cache);
        if index < cache_entries.len() {
            cache_entries.swap_remove(index);
        }
    }

    pub(super) fn upsert_list_entry(
        &mut self,
        current_index: Option<usize>,
        route: &str,
        cron: &str,
        delivery_mode: ScheduleDeliveryMode,
        payload: &Bytes,
    ) -> usize {
        let entry = Arc::new(ScheduleListEntry {
            route: route.to_string(),
            cron: cron.to_string(),
            delivery_mode,
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

    pub(super) fn remove_list_entry(&mut self, index: usize) {
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

    pub(super) fn compact_ready_heap_if_needed(&mut self) {
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

    pub(super) fn rebuild_ready_heap(&mut self) {
        let mut rebuilt = BinaryHeap::with_capacity(self.schedules.len());
        for schedule in self.schedules.values() {
            rebuilt.push((Reverse(schedule.next_fire_ms), schedule.route.clone()));
        }
        self.ready_heap = rebuilt;
    }
}
