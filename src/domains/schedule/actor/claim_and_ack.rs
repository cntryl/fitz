use super::model::{
    info, warn, Arc, Bytes, FastMap, FxBuildHasher, HashMap, Instant, PendingClaim,
    PendingScheduleFire, PersistedPendingFireClaim, Reverse, ScheduleAckDefinition, ScheduleActor,
    ScheduleFireClaim, ScheduleListEntry, SchedulePendingFireClaimAck,
};

impl ScheduleActor {
    pub fn list_entries_v2(
        &mut self,
        cursor: Option<&str>,
        limit: u64,
    ) -> (Arc<Vec<Arc<ScheduleListEntry>>>, bool, Option<String>) {
        const MAX_LIMIT: usize = 1_000;
        let take = usize::try_from(limit)
            .unwrap_or(MAX_LIMIT)
            .clamp(1, MAX_LIMIT);
        let mut ordered = self.list_entries.clone();
        ordered.sort_unstable_by(|left, right| left.route.cmp(&right.route));
        let family_prefix = format!("schedule-list-v1:{}:", self.family.as_u64());
        let start = cursor
            .and_then(|value| value.strip_prefix(&family_prefix))
            .and_then(|value| ordered.iter().position(|entry| entry.route == value))
            .map_or(0, |index| index.saturating_add(1));
        let end = start.saturating_add(take).min(ordered.len());
        let entries = Arc::new(ordered[start.min(ordered.len())..end].to_vec());
        let has_more = end < ordered.len();
        let continuation = has_more.then(|| format!("{family_prefix}{}", ordered[end - 1].route));
        (entries, has_more, continuation)
    }

    fn u64_to_usize_saturating(value: u64) -> usize {
        usize::try_from(value).unwrap_or(usize::MAX)
    }

    pub fn list_entries(
        &mut self,
        offset: u64,
        limit: u64,
    ) -> (Arc<Vec<Arc<ScheduleListEntry>>>, u64) {
        let total_count = self.schedules.len() as u64;
        let start = Self::u64_to_usize_saturating(offset);
        if start >= self.list_entries.len() {
            return (Arc::new(Vec::new()), total_count);
        }

        let remaining = self.list_entries.len() - start;
        let take = if limit == 0 {
            remaining
        } else {
            remaining.min(Self::u64_to_usize_saturating(limit))
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

    fn store_claims_for<'a>(
        &'a self,
        to_reschedule: &'a [PendingScheduleFire],
        claimed_at_ms: u64,
    ) -> Vec<ScheduleFireClaim<'a>> {
        to_reschedule
            .iter()
            .filter_map(|item| {
                let schedule = self.schedules.get(&item.route)?;

                Some(ScheduleFireClaim {
                    route: &item.route,
                    route_parts: &schedule.route_parts,
                    cron: &schedule.cron,
                    delivery_mode: schedule.delivery_mode,
                    payload: &schedule.payload,
                    claimed_at_ms,
                    next_fire_ms: item.next_fire_ms,
                    previous_fire_ms: item.previous_fire_ms,
                    last_fire_ms: schedule.last_fire_ms,
                    executions_total: schedule.executions_total,
                })
            })
            .collect()
    }

    pub(super) fn claim_due_fires_at(&mut self, now: Instant) -> Vec<PersistedPendingFireClaim> {
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

            let next_fire_time = match def
                .parsed_cron
                .try_next_fire_time_with_clock(now, self.clock.as_ref())
            {
                Ok(next_fire_time) => next_fire_time,
                Err(error) => {
                    warn!(
                        route = %route,
                        cron = %def.cron,
                        error = %error,
                        "Schedule next-fire calculation failed closed"
                    );
                    self.ready_heap.push((Reverse(fire_ms), route));
                    continue;
                }
            };
            let next_fire_ms =
                Self::instant_to_ms_at_with_clock(next_fire_time, now, self.clock.as_ref());
            to_reschedule.push(PendingScheduleFire {
                route,
                next_fire_time,
                next_fire_ms,
                previous_fire_ms: fire_ms,
            });
        }

        if to_reschedule.is_empty() {
            return Vec::new();
        }

        let store_items = self.store_claims_for(&to_reschedule, now_ms);

        if let Err(error) =
            self.store
                .claim_due_batch(self.family.as_u64(), &store_items, self.write_options)
        {
            warn!("Failed to persist schedule reschedule batch: {}", error);
            for item in to_reschedule {
                self.ready_heap
                    .push((Reverse(item.previous_fire_ms), item.route));
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
            let payload = def.payload.clone();
            self.ready_heap
                .push((Reverse(item.next_fire_ms), item.route.clone()));
            self.pending_claimed_occurrences.insert(
                (item.previous_fire_ms, item.route.clone()),
                PendingClaim {
                    payload: payload.clone(),
                    delivery_mode: def.delivery_mode,
                    claimed_at_ms: now_ms,
                },
            );
            claimed.push(PersistedPendingFireClaim {
                route: item.route,
                payload,
                delivery_mode: def.delivery_mode,
                claimed_at_ms: now_ms,
                fire_ms: item.previous_fire_ms,
            });
            info!(
                "Schedule occurrence claimed: {} (next fire: ~{:?})",
                claimed.last().expect("claimed fire present").route,
                item.next_fire_time
            );
        }

        claimed
    }

    pub(super) fn collect_due_occurrences_for_publish_at(
        &mut self,
        now: Instant,
    ) -> Vec<(String, Bytes)> {
        self.claim_due_fires_at(now)
            .into_iter()
            .map(|item| (item.route, item.payload))
            .collect()
    }

    pub(crate) fn claim_due_fires(&mut self) -> Vec<PersistedPendingFireClaim> {
        self.claim_due_fires_at(self.clock.now_instant())
    }

    pub fn collect_due_occurrences_for_publish(&mut self) -> Vec<(String, Bytes)> {
        self.collect_due_occurrences_for_publish_at(self.clock.now_instant())
    }

    #[doc(hidden)]
    pub fn bench_claim_due_fires(&mut self) -> Vec<PersistedPendingFireClaim> {
        self.claim_due_fires()
    }

    pub(crate) fn pending_claimed_occurrences_for_publish(&self) -> Vec<PersistedPendingFireClaim> {
        self.pending_claimed_occurrences
            .iter()
            .map(|((fire_ms, route), claim)| PersistedPendingFireClaim {
                route: route.clone(),
                payload: claim.payload.clone(),
                delivery_mode: claim.delivery_mode,
                claimed_at_ms: claim.claimed_at_ms,
                fire_ms: *fire_ms,
            })
            .collect()
    }

    pub(crate) fn admin_pending_claims(
        &self,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        self.pending_claimed_occurrences
            .iter()
            .map(
                |((fire_ms, route), claim)| crate::control::admin::SchedulePendingClaimInfo {
                    route_family: self.family.as_u64(),
                    route: route.clone(),
                    delivery_mode: claim.delivery_mode,
                    fire_ms: *fire_ms,
                    claimed_at_ms: claim.claimed_at_ms,
                },
            )
            .collect()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn bench_pending_claimed_occurrences_for_publish(&self) -> Vec<PersistedPendingFireClaim> {
        self.pending_claimed_occurrences_for_publish()
    }

    /// Acknowledges occurrences handed off to the live publish path.
    /// Returns `(acked_count, acknowledged_at_ms)`.
    /// `acknowledged_at_ms` is only meaningful when `acked_count > 0`.
    ///
    /// # Errors
    ///
    /// Returns an error when the pending-claim acknowledgement batch cannot be
    /// persisted.
    pub(crate) fn ack_pending_fire_claims(
        &mut self,
        handed_off_occurrences: &[(u64, String)],
    ) -> Result<(usize, u64), String> {
        if handed_off_occurrences.is_empty() {
            return Ok((0, 0));
        }

        let acknowledged_at_ms = self.clock.now_epoch_ms();
        let mut acknowledgement_counts: FastMap<&str, u64> =
            HashMap::with_capacity_and_hasher(handed_off_occurrences.len(), FxBuildHasher);
        // `handed_off_occurrences` comes from the pending-claim BTreeMap in ascending
        // `(fire_ms, route)` order. Preserve that order: each per-route running count below must
        // describe the number of earlier occurrences included in this same persisted batch.
        let store_items: Vec<_> = handed_off_occurrences
            .iter()
            .map(|(fire_ms, route)| {
                let route_str = route.as_str();
                let definition = self.schedules.get(route_str).map(|schedule| {
                    let acknowledgement_count =
                        acknowledgement_counts.entry(route_str).or_insert(0);
                    *acknowledgement_count = acknowledgement_count.saturating_add(1);

                    ScheduleAckDefinition {
                        next_fire_ms: schedule.next_fire_ms,
                        cron: &schedule.cron,
                        payload: &schedule.payload,
                        executions_total: schedule
                            .executions_total
                            .saturating_add(*acknowledgement_count),
                    }
                });

                SchedulePendingFireClaimAck {
                    route: route_str,
                    fire_ms: *fire_ms,
                    acknowledged_at_ms,
                    definition,
                }
            })
            .collect();

        self.store.ack_pending_fire_claims(
            self.family.as_u64(),
            &store_items,
            self.write_options,
        )?;

        let mut acked = 0;
        for (fire_ms, route) in handed_off_occurrences {
            if self
                .pending_claimed_occurrences
                .remove(&(*fire_ms, route.clone()))
                .is_some()
            {
                if let Some(schedule) = self.schedules.get_mut(route) {
                    schedule.last_fire_ms = Some(acknowledged_at_ms);
                    schedule.executions_total = schedule.executions_total.saturating_add(1);
                }
                acked += 1;
            }
        }

        Ok((acked, acknowledged_at_ms))
    }

    #[doc(hidden)]
    pub fn bench_ack_pending_fire_claims(
        &mut self,
        delivered: &[(u64, String)],
    ) -> Result<(usize, u64), String> {
        self.ack_pending_fire_claims(delivered)
    }

    pub(crate) fn pending_fire_count(&self) -> usize {
        self.pending_claimed_occurrences.len()
    }

    pub(crate) fn oldest_pending_claim_age_seconds(&self, now_epoch_ms: u64) -> u64 {
        self.pending_claimed_occurrences
            .values()
            .map(|claim| now_epoch_ms.saturating_sub(claim.claimed_at_ms) / 1_000)
            .max()
            .unwrap_or(0)
    }

    /// Returns all acknowledged live-handoff timestamps after `cutoff_ms`.
    /// Used by the sink to seed its rolling-window acknowledgement counter on startup.
    pub(crate) fn last_fire_timestamps_since(&self, cutoff_ms: u64) -> Vec<u64> {
        self.schedules
            .values()
            .filter_map(|def| def.last_fire_ms)
            .filter(|&ts| ts > cutoff_ms)
            .collect()
    }

    pub(crate) fn overdue_normalization_count(&self) -> u64 {
        self.overdue_normalizations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;

    #[test]
    fn should_skip_claim_persistence_item_when_schedule_disappears() {
        // Arrange
        let actor = ScheduleActor::new(
            RouteFamily::new(1),
            crate::testkit::create_test_engine_with_cfs(vec![1]),
            cntryl_midge::WriteOptions::buffered(),
        );
        let pending = [PendingScheduleFire {
            route: "schedule://acme/jobs/missing/run".to_string(),
            next_fire_time: Instant::now(),
            next_fire_ms: 2,
            previous_fire_ms: 1,
        }];

        // Act
        let claims = actor.store_claims_for(&pending, 1);

        // Assert
        assert!(claims.is_empty());
    }
}
