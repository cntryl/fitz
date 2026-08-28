use super::model::{
    info, warn, Arc, Bytes, FastMap, FxBuildHasher, HashMap, Instant, PendingClaim,
    PendingScheduleFire, PersistedPendingFireClaim, Reverse, ScheduleAckDefinition, ScheduleActor,
    ScheduleFireClaim, ScheduleListEntry, SchedulePendingFireClaimAck, SchedulePersistence,
};

fn persist_claim_batch<P: SchedulePersistence>(
    persistence: &P,
    family_id: u64,
    claims: &[ScheduleFireClaim<'_>],
    write_options: cntryl_midge::WriteOptions,
) -> Result<(), String> {
    persistence.persist_claims(family_id, claims, write_options)
}

fn acknowledge_claim_batch<P: SchedulePersistence>(
    persistence: &P,
    family_id: u64,
    claims: &[SchedulePendingFireClaimAck<'_>],
    write_options: cntryl_midge::WriteOptions,
) -> Result<(), String> {
    persistence.acknowledge_claims(family_id, claims, write_options)
}

/// One page of schedule definitions: entries, whether more remain, and the
/// continuation cursor.
pub(crate) type ScheduleListPage = (Arc<Vec<Arc<ScheduleListEntry>>>, bool, Option<String>);

/// One page of schedule definitions plus the total definition count.
pub(crate) type ScheduleListDefs = (Arc<Vec<Arc<ScheduleListEntry>>>, u64);

impl ScheduleActor {
    /// Longest run of entries starting at `start` that still fits one wire
    /// frame, always at least one so a page makes forward progress.
    ///
    /// Every entry may be individually small and legal while the aggregate is
    /// unencodable; without this the response is built, handed to the outbound
    /// sink, and fails the TLV length assertion.
    /// # Errors
    ///
    /// Returns the offending route when a single entry exceeds the ceiling on
    /// its own. Creation now refuses such definitions, so this only covers
    /// entries stored before that check existed. Forcing one into a page would
    /// produce a response that cannot be framed and is silently dropped, so it
    /// surfaces as an explicit, classifiable error naming the route instead.
    ///
    /// `continuation_reserve` charges each entry as if it might end up being
    /// the last one on the page, so a caller whose page format re-encodes
    /// that boundary entry's route a second time (e.g. `list_entries_v2`'s
    /// continuation cursor, which duplicates `family_prefix + route`) does
    /// not silently exceed the ceiling it already budgeted against. Callers
    /// with no such duplication pass a reserve of zero.
    fn bounded_page_len(
        entries: &[Arc<ScheduleListEntry>],
        start: usize,
        take: usize,
        continuation_reserve: impl Fn(&ScheduleListEntry) -> usize,
    ) -> Result<usize, String> {
        let ceiling =
            crate::domains::schedule::list_wire_budget::schedule_list_response_byte_ceiling();
        let mut used = 0usize;
        let mut fitted = 0usize;
        for entry in entries.iter().skip(start).take(take) {
            let cost =
                crate::domains::schedule::list_wire_budget::schedule_list_entry_wire_bytes(entry)
                    .saturating_add(continuation_reserve(entry));
            if cost > ceiling {
                if fitted > 0 {
                    // End the page here; the next page starts at the offending
                    // entry and reports it.
                    break;
                }
                return Err(format!(
                    "schedule {} is {cost} wire bytes, exceeding the {ceiling}-byte limit a \
                     list response can return",
                    entry.route
                ));
            }
            let next = used.saturating_add(cost);
            if next > ceiling && fitted > 0 {
                break;
            }
            used = next;
            fitted = fitted.saturating_add(1);
            if used > ceiling {
                break;
            }
        }
        Ok(fitted.max(1).min(take))
    }

    /// # Errors
    ///
    /// Returns the offending route when a stored definition is too large to
    /// appear in any list response.
    pub fn list_entries_v2(
        &mut self,
        cursor: Option<&str>,
        limit: u64,
    ) -> Result<ScheduleListPage, String> {
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
        let start = start.min(ordered.len());
        let requested = start.saturating_add(take).min(ordered.len()) - start;
        // Reserve room for the continuation field, which re-encodes
        // `family_prefix + route` for whichever entry ends up last on the
        // page - on top of that route already being counted once inside the
        // entry itself.
        let family_prefix_len = family_prefix.len();
        let fitted = if requested == 0 {
            0
        } else {
            Self::bounded_page_len(&ordered, start, requested, |entry| {
                family_prefix_len.saturating_add(entry.route.len())
            })?
        };
        let end = start.saturating_add(fitted);
        let entries = Arc::new(ordered[start..end].to_vec());
        let has_more = end < ordered.len();
        let continuation = has_more.then(|| format!("{family_prefix}{}", ordered[end - 1].route));
        Ok((entries, has_more, continuation))
    }

    fn u64_to_usize_saturating(value: u64) -> usize {
        usize::try_from(value).unwrap_or(usize::MAX)
    }

    /// # Errors
    ///
    /// Returns the offending route when a stored definition is too large to
    /// appear in any list response.
    pub fn list_entries(&mut self, offset: u64, limit: u64) -> Result<ScheduleListDefs, String> {
        let total_count = self.schedules.len() as u64;
        let start = Self::u64_to_usize_saturating(offset);
        if start >= self.list_entries.len() {
            return Ok((Arc::new(Vec::new()), total_count));
        }

        let remaining = self.list_entries.len() - start;
        let requested = if limit == 0 {
            remaining
        } else {
            remaining.min(Self::u64_to_usize_saturating(limit))
        };
        // `limit = 0` means "all remaining", so the caller's count cannot bound
        // the response - only the wire budget can. Clients detect the short
        // page by comparing entry count against `total_count` and continue
        // from `offset`, which the V1 contract already supports.
        let take = Self::bounded_page_len(&self.list_entries, start, requested, |_| 0)?;

        if start == 0 && take == self.list_entries.len() {
            if let Some(cache) = &self.list_cache {
                return Ok((cache.clone(), total_count));
            }
            let cache = Arc::new(self.list_entries.clone());
            self.list_cache = Some(cache.clone());
            return Ok((cache, total_count));
        }

        Ok((
            Arc::new(self.list_entries[start..start + take].to_vec()),
            total_count,
        ))
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

    fn pop_due_from_heap(&mut self, now_ms: u64) -> Vec<(u64, String)> {
        let mut popped = Vec::with_capacity(super::MAX_DUE_CLAIMS_PER_SCAN);
        while popped.len() < super::MAX_DUE_CLAIMS_PER_SCAN {
            let Some(&(Reverse(fire_ms), _)) = self.ready_heap.peek() else {
                break;
            };
            if fire_ms > now_ms {
                break;
            }
            if let Some((Reverse(fire_ms), route)) = self.ready_heap.pop() {
                popped.push((fire_ms, route));
            }
        }
        popped
    }

    fn recompute_next_fires(
        &mut self,
        popped: Vec<(u64, String)>,
        now: Instant,
    ) -> Vec<PendingScheduleFire> {
        let mut rescheduled = Vec::new();
        for (fire_ms, route) in popped {
            let Some(def) = self.schedules.get(&route) else {
                continue;
            };
            if def.next_fire_ms != fire_ms {
                continue;
            }
            let Ok(next_fire_time) = def
                .parsed_cron
                .try_next_fire_time_with_clock(now, self.clock.as_ref())
                .inspect_err(|error| {
                    warn!(route = %route, cron = %def.cron, error = %error, "Schedule next-fire calculation failed closed");
                })
            else {
                self.ready_heap.push((Reverse(fire_ms), route));
                continue;
            };
            rescheduled.push(PendingScheduleFire {
                next_fire_ms: Self::instant_to_ms_at_with_clock(
                    next_fire_time,
                    now,
                    self.clock.as_ref(),
                ),
                route,
                next_fire_time,
                previous_fire_ms: fire_ms,
            });
        }
        rescheduled
    }

    fn persist_claims(&mut self, claims: &[PendingScheduleFire], claimed_at_ms: u64) -> bool {
        let store_items = self.store_claims_for(claims, claimed_at_ms);
        if let Err(error) = persist_claim_batch(
            &self.store,
            self.family.as_u64(),
            &store_items,
            self.write_options,
        ) {
            warn!("Failed to persist schedule reschedule batch: {error}");
            for item in claims {
                self.ready_heap
                    .push((Reverse(item.previous_fire_ms), item.route.clone()));
            }
            return false;
        }
        true
    }

    fn apply_claims_to_state(
        &mut self,
        claims: Vec<PendingScheduleFire>,
        claimed_at_ms: u64,
    ) -> Vec<PersistedPendingFireClaim> {
        let mut claimed = Vec::with_capacity(claims.len());
        for item in claims {
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
                    claimed_at_ms,
                },
            );
            claimed.push(PersistedPendingFireClaim {
                route: item.route,
                payload,
                delivery_mode: def.delivery_mode,
                claimed_at_ms,
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

    pub(super) fn claim_due_fires_at(&mut self, now: Instant) -> Vec<PersistedPendingFireClaim> {
        if now.duration_since(self.last_scan_time) < self.scan_dedup_window {
            return Vec::new();
        }
        self.last_scan_time = now;

        let now_ms = Self::instant_to_ms_at_with_clock(now, now, self.clock.as_ref());
        let heap_popped = self.pop_due_from_heap(now_ms);
        let to_reschedule = self.recompute_next_fires(heap_popped, now);

        if to_reschedule.is_empty() {
            return Vec::new();
        }

        if !self.persist_claims(&to_reschedule, now_ms) {
            return Vec::new();
        }
        self.apply_claims_to_state(to_reschedule, now_ms)
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

        acknowledge_claim_batch(
            &self.store,
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

    struct FailingPersistence;

    impl SchedulePersistence for FailingPersistence {
        fn persist_claims(
            &self,
            _family_id: u64,
            _claims: &[ScheduleFireClaim<'_>],
            _write_options: cntryl_midge::WriteOptions,
        ) -> Result<(), String> {
            Err("claim failed".to_string())
        }

        fn acknowledge_claims(
            &self,
            _family_id: u64,
            _claims: &[SchedulePendingFireClaimAck<'_>],
            _write_options: cntryl_midge::WriteOptions,
        ) -> Result<(), String> {
            Err("ack failed".to_string())
        }
    }

    #[test]
    fn should_surface_claim_error_from_fake_persistence() {
        // Arrange
        let persistence = FailingPersistence;
        let claims = [];

        // Act
        let result = persist_claim_batch(
            &persistence,
            1,
            &claims,
            cntryl_midge::WriteOptions::buffered(),
        );

        // Assert
        assert_eq!(result, Err("claim failed".to_string()));
    }

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
