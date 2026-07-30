#[cfg(test)]
use super::model::SystemClock;
use super::model::{
    epoch_ms_to_instant_with_reference, instant_to_epoch_ms_with_reference,
    parse_concrete_schedule_route, Clock, CronSchedule, Duration, Instant, Reverse, ScheduleActor,
    ScheduleFailure, ScheduleFailureCategory, ScheduleMessage, ScheduleResponse,
};

impl ScheduleActor {
    #[doc(hidden)]
    pub fn bench_prepare_scan(&mut self, ready_count: usize) {
        let now = self.clock.now_instant();
        let ready_limit = ready_count.min(self.list_entries.len());
        let ready_ms =
            Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref()).saturating_sub(1);
        let not_ready_time = now.checked_add(Duration::from_mins(1)).unwrap_or(now);
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

    /// # Errors
    ///
    /// Returns an error when the backing schedule family cannot be flushed.
    #[doc(hidden)]
    pub fn bench_drain_storage(&self) -> Result<(), String> {
        self.store
            .sync_family(self.family.as_u64(), self.write_options)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_store_commit_for_tests(&self) {
        self.store.fail_next_commit_for_tests();
    }

    #[cfg(test)]
    pub(super) fn current_epoch_ms() -> u64 {
        Self::current_epoch_ms_with_clock(&SystemClock)
    }

    #[cfg(test)]
    pub(super) fn current_epoch_ms_with_clock(clock: &dyn Clock) -> u64 {
        clock.now_epoch_ms()
    }

    pub(super) fn anchor_epoch_ms_with_clock(anchor: Instant, clock: &dyn Clock) -> u64 {
        instant_to_epoch_ms_with_reference(anchor, clock.now_instant(), clock.now_epoch_ms())
    }

    #[cfg(test)]
    pub(super) fn instant_to_ms_at(instant: Instant, anchor: Instant) -> u64 {
        Self::instant_to_ms_at_with_clock(instant, anchor, &SystemClock)
    }

    pub(in crate::domains::schedule::actor) fn instant_to_ms_at_with_clock(
        instant: Instant,
        anchor: Instant,
        clock: &dyn Clock,
    ) -> u64 {
        let anchor_ms = Self::anchor_epoch_ms_with_clock(anchor, clock);
        instant_to_epoch_ms_with_reference(instant, anchor, anchor_ms)
    }

    pub(in crate::domains::schedule::actor) fn ms_to_instant_at_with_clock(
        timestamp_ms: u64,
        anchor: Instant,
        clock: &dyn Clock,
    ) -> Instant {
        let anchor_ms = Self::anchor_epoch_ms_with_clock(anchor, clock);
        epoch_ms_to_instant_with_reference(timestamp_ms, anchor, anchor_ms)
    }

    pub fn handle(&mut self, msg: ScheduleMessage) -> ScheduleResponse {
        match msg {
            ScheduleMessage::Create {
                route,
                cron,
                delivery_mode,
                payload,
            } => {
                if let Err(error) = parse_concrete_schedule_route(&route) {
                    return ScheduleResponse::Error(ScheduleFailure::new(
                        ScheduleFailureCategory::InvalidTarget,
                        error,
                    ));
                }
                if let Err(error) = CronSchedule::parse(&cron) {
                    return ScheduleResponse::Error(ScheduleFailure::new(
                        ScheduleFailureCategory::InvalidCron,
                        error,
                    ));
                }
                match self.create_schedule_with_mode(route, cron, delivery_mode, payload) {
                    Ok(_) => ScheduleResponse::Ok,
                    Err(e) => ScheduleResponse::Error(ScheduleFailure::parse(e)),
                }
            }
            ScheduleMessage::CreateBatch { entries } => {
                for entry in &entries {
                    if let Err(error) = parse_concrete_schedule_route(&entry.route) {
                        return ScheduleResponse::Error(ScheduleFailure::new(
                            ScheduleFailureCategory::InvalidTarget,
                            error,
                        ));
                    }
                    if let Err(error) = CronSchedule::parse(&entry.cron) {
                        return ScheduleResponse::Error(ScheduleFailure::new(
                            ScheduleFailureCategory::InvalidCron,
                            error,
                        ));
                    }
                }
                match self.create_schedules(entries) {
                    Ok(_) => ScheduleResponse::Ok,
                    Err(e) => ScheduleResponse::Error(ScheduleFailure::parse(e)),
                }
            }
            ScheduleMessage::Cancel { route } => {
                if let Err(error) = parse_concrete_schedule_route(&route) {
                    return ScheduleResponse::Error(ScheduleFailure::new(
                        ScheduleFailureCategory::InvalidTarget,
                        error,
                    ));
                }
                match self.delete_schedule(&route) {
                    Ok(_) => ScheduleResponse::Ok,
                    Err(e) => ScheduleResponse::Error(ScheduleFailure::parse(e)),
                }
            }
            ScheduleMessage::List { offset, limit } => {
                let (entries, total_count) = self.list_entries(offset, limit);
                ScheduleResponse::ListDefs {
                    entries,
                    total_count,
                }
            }
            ScheduleMessage::Subscribe { .. }
            | ScheduleMessage::Unsubscribe { .. }
            | ScheduleMessage::UnsubscribeAll { .. } => {
                ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::InvalidTarget,
                    "schedule subscription state is owned by the schedule domain sink",
                ))
            }
        }
    }
}
