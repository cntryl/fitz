use super::model::*;

impl ScheduleActor {
    pub(super) const READY_HEAP_REBUILD_SLACK: usize = 32;

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
            schedules: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            cron_cache: HashMap::with_capacity_and_hasher(32, FxBuildHasher::default()),
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

    pub(super) fn preload_from_store_at(&mut self, now: Instant) -> Result<(), String> {
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

                    self.schedules.insert(route.clone(), def);
                    self.ready_heap
                        .push((Reverse(effective_next_fire_ms), route.clone()));
                }
                Err(error) => {
                    return Err(format!(
                        "parse persisted schedule cron failed for {}: {}",
                        route, error
                    ));
                }
            }
        }

        if !normalization_batch.is_empty() {
            self.overdue_normalizations = normalization_batch.len() as u64;
            self.store.insert_batch(
                self.family.as_u64(),
                &normalization_batch,
                self.write_options,
            )?;
        }

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
                    claimed_at_ms,
                },
            );
        }

        Ok(())
    }

    pub fn admin_snapshot(&self) -> Vec<crate::control::admin::ScheduleInfo> {
        let mut snapshot: Vec<_> = self
            .schedules
            .values()
            .filter_map(|schedule| {
                parse_concrete_schedule_route(&schedule.route)
                    .ok()
                    .map(|route| crate::control::admin::ScheduleInfo {
                        route_family: self.family.as_u64(),
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
}
