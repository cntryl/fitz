use super::model::{
    parse_concrete_schedule_route, Arc, BTreeMap, BinaryHeap, Clock, CronSchedule, FxBuildHasher,
    HashMap, Instant, PendingClaim, PersistedSchedule, Reverse, RouteFamily, ScheduleActor,
    ScheduleDef, ScheduleStore, SystemClock,
};

impl ScheduleActor {
    pub(super) const READY_HEAP_REBUILD_SLACK: usize = 32;

    /// # Errors
    ///
    /// Returns an error when schedule storage initialization or preload fails.
    pub fn try_new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        Self::try_new_with_storage(
            family,
            crate::storage::FitzStorageEngine::new(db),
            write_options,
        )
    }

    pub(crate) fn try_new_with_storage(
        family: RouteFamily,
        db: crate::storage::FitzStorageEngine,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        Self::try_new_with_storage_clock(family, db, write_options, Arc::new(SystemClock))
    }

    /// # Errors
    ///
    /// Returns an error when schedule storage initialization or preload fails.
    pub fn try_new_with_clock(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, String> {
        Self::try_new_with_storage_clock(
            family,
            crate::storage::FitzStorageEngine::new(db),
            write_options,
            clock,
        )
    }

    pub(crate) fn try_new_with_storage_clock(
        family: RouteFamily,
        db: crate::storage::FitzStorageEngine,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, String> {
        let now = clock.now_instant();
        Self::try_new_at_with_storage_clock(family, db, write_options, clock, now)
    }

    /// # Panics
    ///
    /// Panics when schedule actor startup fails.
    pub fn new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_new(family, db, write_options).expect("schedule actor startup should succeed")
    }

    /// # Panics
    ///
    /// Panics when schedule actor startup fails.
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
    pub(super) fn try_new_at(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        now: Instant,
    ) -> Result<Self, String> {
        Self::try_new_at_with_clock(family, db, write_options, Arc::new(SystemClock), now)
    }

    #[cfg(test)]
    pub(super) fn try_new_at_with_clock(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
        now: Instant,
    ) -> Result<Self, String> {
        Self::try_new_at_with_storage_clock(
            family,
            crate::storage::FitzStorageEngine::new(db),
            write_options,
            clock,
            now,
        )
    }

    pub(super) fn try_new_at_with_storage_clock(
        family: RouteFamily,
        db: crate::storage::FitzStorageEngine,
        write_options: cntryl_midge::WriteOptions,
        clock: Arc<dyn Clock>,
        now: Instant,
    ) -> Result<Self, String> {
        let store = ScheduleStore::new_with_storage(db);
        let mut actor = Self {
            family,
            store,
            schedules: HashMap::with_capacity_and_hasher(128, FxBuildHasher),
            cron_cache: HashMap::with_capacity_and_hasher(32, FxBuildHasher),
            list_entries: Vec::new(),
            list_cache: None,
            write_options,
            last_scan_time: now,
            scan_dedup_window: std::time::Duration::from_millis(10),
            ready_heap: BinaryHeap::new(),
            pending_claimed_occurrences: BTreeMap::new(),
            clock,
            overdue_normalizations: 0,
        };

        actor.preload_from_store_at(now)?;
        Ok(actor)
    }

    /// # Errors
    ///
    /// Returns an error when persisted schedules cannot be loaded, parsed, or
    /// normalized, or when pending claimed occurrences cannot be loaded.
    pub(super) fn preload_from_store_at(&mut self, now: Instant) -> Result<(), String> {
        let now_ms = Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref());
        let entries = self
            .store
            .load_all(self.family.as_u64(), self.write_options)?;
        let normalization_batch = self.preload_persisted_schedules(entries, now, now_ms)?;
        self.persist_normalization_batch(&normalization_batch)?;
        self.preload_pending_fire_claims()?;

        Ok(())
    }

    #[must_use]
    pub fn admin_snapshot(&self) -> Vec<crate::control::admin::ScheduleInfo> {
        let mut snapshot: Vec<_> = self
            .schedules
            .values()
            .map(|schedule| {
                let route = &schedule.route_parts;
                crate::control::admin::ScheduleInfo {
                    route_family: self.family.as_u64(),
                    realm: route.realm.clone(),
                    area: route.area.clone(),
                    resource: route.resource.clone(),
                    operation: route.operation.clone(),
                    cron: schedule.cron.clone(),
                    delivery_mode: schedule.delivery_mode,
                    next_run: chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                        Self::u64_to_i64_saturating(schedule.next_fire_ms),
                    )
                    .map(|timestamp| timestamp.to_rfc3339())
                    .unwrap_or_default(),
                    last_run: schedule.last_fire_ms.and_then(|timestamp_ms| {
                        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
                            Self::u64_to_i64_saturating(timestamp_ms),
                        )
                        .map(|timestamp| timestamp.to_rfc3339())
                    }),
                    executions_total: schedule.executions_total,
                    enabled: true,
                }
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

    fn u64_to_i64_saturating(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    fn preload_persisted_schedules(
        &mut self,
        entries: Vec<PersistedSchedule>,
        now: Instant,
        now_ms: u64,
    ) -> Result<Vec<crate::domains::schedule::store::ScheduleBatchInsert>, String> {
        let mut normalization_batch = Vec::new();

        for entry in entries {
            self.preload_schedule_entry(entry, now, now_ms, &mut normalization_batch)?;
        }

        Ok(normalization_batch)
    }

    fn preload_schedule_entry(
        &mut self,
        entry: PersistedSchedule,
        now: Instant,
        now_ms: u64,
        normalization_batch: &mut Vec<crate::domains::schedule::store::ScheduleBatchInsert>,
    ) -> Result<(), String> {
        let PersistedSchedule {
            route,
            cron,
            delivery_mode,
            payload,
            next_fire_ms,
            last_fire_ms,
            executions_total,
        } = entry;
        let route_parts = parse_concrete_schedule_route(&route)?;
        let parsed_cron = CronSchedule::parse(&cron).map_err(|error| {
            format!("parse persisted schedule cron failed for {route} with cron {cron:?}: {error}")
        })?;
        self.cron_cache.insert(cron.clone(), parsed_cron.clone());

        let (effective_next_fire_time, effective_next_fire_ms) = self.resolve_preloaded_fire_time(
            &route,
            &cron,
            delivery_mode,
            &payload,
            next_fire_ms,
            last_fire_ms,
            executions_total,
            &parsed_cron,
            now,
            now_ms,
            normalization_batch,
        )?;
        let list_index =
            self.push_list_entry(route.as_str(), cron.as_str(), delivery_mode, &payload);
        let def = ScheduleDef {
            route: route.clone(),
            route_parts,
            cron,
            delivery_mode,
            parsed_cron,
            payload,
            next_fire_time: effective_next_fire_time,
            next_fire_ms: effective_next_fire_ms,
            last_fire_ms,
            executions_total,
            list_index,
        };

        self.schedules.insert(route.clone(), def);
        self.ready_heap
            .push((Reverse(effective_next_fire_ms), route));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_preloaded_fire_time(
        &self,
        route: &str,
        cron: &str,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: &bytes::Bytes,
        next_fire_ms: u64,
        last_fire_ms: Option<u64>,
        executions_total: u64,
        parsed_cron: &CronSchedule,
        now: Instant,
        now_ms: u64,
        normalization_batch: &mut Vec<crate::domains::schedule::store::ScheduleBatchInsert>,
    ) -> Result<(Instant, u64), String> {
        if next_fire_ms <= now_ms {
            let normalized_next_fire_time = parsed_cron
                .try_next_fire_time_with_clock(now, self.clock.as_ref())
                .map_err(|error| {
                    format!(
                        "normalize persisted schedule failed for route {route} with cron {cron:?}: {error}"
                    )
                })?;
            let normalized_next_fire_ms = Self::instant_to_ms_at_with_clock(
                normalized_next_fire_time,
                now,
                self.clock.as_ref(),
            );
            normalization_batch.push(crate::domains::schedule::store::ScheduleBatchInsert {
                route: route.to_string(),
                cron: cron.to_string(),
                delivery_mode,
                payload: payload.clone(),
                next_fire_ms: normalized_next_fire_ms,
                previous_fire_ms: Some(next_fire_ms),
                last_fire_ms,
                executions_total,
            });
            Ok((normalized_next_fire_time, normalized_next_fire_ms))
        } else {
            Ok((
                Self::ms_to_instant_at_with_clock(next_fire_ms, now, self.clock.as_ref()),
                next_fire_ms,
            ))
        }
    }

    fn persist_normalization_batch(
        &mut self,
        normalization_batch: &[crate::domains::schedule::store::ScheduleBatchInsert],
    ) -> Result<(), String> {
        if normalization_batch.is_empty() {
            return Ok(());
        }

        self.overdue_normalizations = normalization_batch.len() as u64;
        self.store.insert_batch(
            self.family.as_u64(),
            normalization_batch,
            self.write_options,
        )?;
        Ok(())
    }

    fn preload_pending_fire_claims(&mut self) -> Result<(), String> {
        for pending_claimed_occurrence in
            self.store.load_pending_fire_claims(self.family.as_u64())?
        {
            let claimed_at_ms = if pending_claimed_occurrence.claimed_at_ms == 0 {
                pending_claimed_occurrence.fire_ms
            } else {
                pending_claimed_occurrence.claimed_at_ms
            };
            self.pending_claimed_occurrences.insert(
                (
                    pending_claimed_occurrence.fire_ms,
                    pending_claimed_occurrence.route,
                ),
                PendingClaim {
                    payload: pending_claimed_occurrence.payload,
                    delivery_mode: pending_claimed_occurrence.delivery_mode,
                    claimed_at_ms,
                },
            );
        }

        Ok(())
    }
}
