use super::super::budget::{Budget, Reservation};
use super::super::subscriptions::compact;
use super::notification_gating::{CommitEvent, VisibilityScope};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

const MAX_STAGE_JOBS: usize = 5_000;
const MAX_STAGE_TARGETS: usize = 65_536;
const MAX_STAGE_BYTES: usize = 32 * 1024 * 1024;
const MAX_PROMOTIONS_PER_TURN: usize = 64;

#[derive(Clone)]
pub(in crate::domains::stream::sink) struct SharedDeliveryBudgets {
    ready: Budget,
    parked: Budget,
}

#[cfg(test)]
impl SharedDeliveryBudgets {
    pub(in crate::domains::stream::sink) fn parked_bytes(&self) -> usize {
        self.parked.used()
    }
}

impl Default for SharedDeliveryBudgets {
    fn default() -> Self {
        Self {
            ready: Budget::new(MAX_STAGE_BYTES),
            parked: Budget::new(MAX_STAGE_BYTES),
        }
    }
}

struct StageBudget {
    jobs: Budget,
    targets: Budget,
    bytes: Budget,
}

impl StageBudget {
    fn new(bytes: Budget) -> Self {
        Self {
            jobs: Budget::new(MAX_STAGE_JOBS),
            targets: Budget::new(MAX_STAGE_TARGETS),
            bytes,
        }
    }

    fn reserve(&self, targets: usize, bytes: usize) -> Option<StageReservation> {
        Some(StageReservation {
            _job: self.jobs.reserve(1)?,
            targets: self.targets.reserve(targets)?,
            bytes: self.bytes.reserve(bytes)?,
        })
    }
}

struct StageReservation {
    _job: Reservation,
    targets: Reservation,
    bytes: Reservation,
}

pub(super) struct DeliveryJob {
    pub(super) event: Arc<CommitEvent>,
    pub(super) selector: Arc<str>,
    pub(super) cutoff: u64,
    pub(super) cursor: u64,
    parked_at: Option<(VisibilityScope, u64)>,
    immediate: bool,
    reservation: StageReservation,
}

impl DeliveryJob {
    pub(super) fn finish_target(&mut self, subscription: u64) {
        self.cursor = subscription;
        self.reservation.targets.release(1);
    }

    pub(super) fn remaining(&self) -> usize {
        self.reservation.targets.amount()
    }
}

pub(in crate::domains::stream::sink) struct DeliveryQueue {
    ready_budget: StageBudget,
    parked_budget: StageBudget,
    jobs: HashMap<u64, DeliveryJob>,
    selector_jobs: HashMap<Arc<str>, BTreeSet<u64>>,
    ready: BTreeMap<Arc<str>, BTreeSet<u64>>,
    ready_cursor: Option<Arc<str>>,
    parked: BTreeMap<VisibilityScope, BTreeSet<(u64, u64)>>,
    parked_cursor: Option<VisibilityScope>,
    promotions_left: usize,
    next_job: u64,
    last_job_before_turn: u64,
    dispatched_this_turn: bool,
}

impl DeliveryQueue {
    pub(in crate::domains::stream::sink) fn new(shared: &SharedDeliveryBudgets) -> Self {
        Self {
            ready_budget: StageBudget::new(shared.ready.clone()),
            parked_budget: StageBudget::new(shared.parked.clone()),
            jobs: HashMap::new(),
            selector_jobs: HashMap::new(),
            ready: BTreeMap::new(),
            ready_cursor: None,
            parked: BTreeMap::new(),
            parked_cursor: None,
            promotions_left: MAX_PROMOTIONS_PER_TURN,
            next_job: 0,
            last_job_before_turn: 0,
            dispatched_this_turn: false,
        }
    }

    pub(super) fn enqueue(
        &mut self,
        event: Arc<CommitEvent>,
        selector: Arc<str>,
        cutoff: u64,
        targets: usize,
        parked_at: Option<(VisibilityScope, u64)>,
    ) -> bool {
        let budget = if parked_at.is_some() {
            &self.parked_budget
        } else {
            &self.ready_budget
        };
        let bytes =
            512 + event.event.route.as_str().len() + selector.len() + event.event.payload.len();
        let Some(reservation) = budget.reserve(targets, bytes) else {
            return false;
        };
        let Some(id) = self.next_job.checked_add(1) else {
            return false;
        };
        self.next_job = id;
        if let Some((scope, offset)) = &parked_at {
            self.parked
                .entry(scope.clone())
                .or_default()
                .insert((*offset, id));
        } else {
            self.ready.entry(selector.clone()).or_default().insert(id);
        }
        self.selector_jobs
            .entry(selector.clone())
            .or_default()
            .insert(id);
        let immediate = event.local_commit && parked_at.is_none();
        self.jobs.insert(
            id,
            DeliveryJob {
                event,
                selector,
                cutoff,
                cursor: 0,
                parked_at,
                immediate,
                reservation,
            },
        );
        true
    }

    pub(super) fn promote(&mut self, scope: &VisibilityScope, visible_through: u64) {
        while self.promotions_left > 0 {
            let Some(&(offset, id)) = self.parked.get(scope).and_then(BTreeSet::first) else {
                break;
            };
            if offset > visible_through {
                break;
            }
            let job = self
                .jobs
                .get_mut(&id)
                .expect("parked index references a live job");
            let Some(reservation) = self
                .ready_budget
                .reserve(job.remaining(), job.reservation.bytes.amount())
            else {
                break;
            };
            job.reservation = reservation;
            job.parked_at = None;
            self.ready
                .entry(job.selector.clone())
                .or_default()
                .insert(id);
            remove_index_entry(&mut self.parked, scope, &(offset, id));
            self.promotions_left -= 1;
        }
    }

    pub(in crate::domains::stream::sink) fn begin_turn(&mut self) {
        self.promotions_left = MAX_PROMOTIONS_PER_TURN;
        self.last_job_before_turn = self.next_job;
        self.dispatched_this_turn = false;
    }

    pub(super) fn begin_dispatch(&mut self) -> bool {
        if self.dispatched_this_turn || !self.has_ready() {
            return false;
        }
        self.dispatched_this_turn = true;
        true
    }

    pub(super) fn has_parked_work(&self) -> bool {
        !self.parked.is_empty()
    }

    pub(super) fn has_parked(&self, scope: &VisibilityScope) -> bool {
        self.parked.contains_key(scope)
    }

    pub(super) fn parked_scopes(&mut self, limit: usize) -> Vec<VisibilityScope> {
        let scopes = match &self.parked_cursor {
            Some(cursor) => self
                .parked
                .range((Excluded(cursor), Unbounded))
                .chain(self.parked.range(..=cursor))
                .take(limit)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
            None => self.parked.keys().take(limit).cloned().collect(),
        };
        self.parked_cursor = scopes.last().cloned();
        scopes
    }

    pub(in crate::domains::stream::sink) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    pub(super) fn next_ready(&mut self) -> Option<(u64, bool, &mut DeliveryJob)> {
        let next = self
            .ready_cursor
            .as_ref()
            .and_then(|last| {
                self.ready
                    .range::<Arc<str>, _>((Excluded(last), Unbounded))
                    .next()
            })
            .or_else(|| self.ready.first_key_value());
        let (selector, ids) = next?;
        self.ready_cursor = Some(selector.clone());
        let id = *ids.first().expect("empty ready buckets are removed");
        let job = self
            .jobs
            .get_mut(&id)
            .expect("ready index references a live job");
        // A commit that is ready in its own actor turn has just passed the
        // storage commit fence. Retained work rechecks the persisted cutoff.
        let deferred = !job.immediate || id <= self.last_job_before_turn || job.cursor != 0;
        Some((id, deferred, job))
    }

    pub(super) fn remove(&mut self, id: u64) {
        let job = self.jobs.remove(&id).expect("remove a live job");
        if let Some((scope, offset)) = job.parked_at {
            remove_index_entry(&mut self.parked, &scope, &(offset, id));
        } else {
            remove_index_entry(&mut self.ready, &job.selector, &id);
        }
        if let Some(ids) = self.selector_jobs.get_mut(&job.selector) {
            ids.remove(&id);
            if ids.is_empty() {
                self.selector_jobs.remove(&job.selector);
            }
        }
        if self.ready.is_empty() {
            self.ready_cursor = None;
        }
        if self.parked.is_empty() {
            self.parked_cursor = None;
        }
        compact(&mut self.jobs);
        compact(&mut self.selector_jobs);
    }

    pub(in crate::domains::stream::sink) fn retire(&mut self, selector: &str, subscription: u64) {
        let Some(ids) = self.selector_jobs.get(selector) else {
            return;
        };
        let mut drained = Vec::new();
        for &id in ids {
            let job = self
                .jobs
                .get_mut(&id)
                .expect("selector index references a live job");
            if subscription > job.cursor && subscription <= job.cutoff {
                job.reservation.targets.release(1);
                if job.remaining() == 0 {
                    drained.push(id);
                }
            }
        }
        for id in drained {
            self.remove(id);
        }
    }

    pub(super) fn discard_where(&mut self, mut matches: impl FnMut(&CommitEvent) -> bool) -> usize {
        let ids: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(&id, job)| matches(&job.event).then_some(id))
            .collect();
        let mut discarded = 0;
        for id in ids {
            discarded += self.jobs[&id].remaining();
            self.remove(id);
        }
        discarded
    }

    #[cfg(test)]
    pub(in crate::domains::stream::sink) fn parked_counts(&self) -> (usize, usize) {
        (
            self.parked_budget.jobs.used(),
            self.parked_budget.targets.used(),
        )
    }

    #[cfg(test)]
    pub(in crate::domains::stream::sink) fn parked_is_empty(&self) -> bool {
        self.parked.is_empty()
    }
}

fn remove_index_entry<K: Ord, V: Ord>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K, value: &V) {
    let entries = index.get_mut(key).expect("job is indexed");
    entries.remove(value);
    if entries.is_empty() {
        index.remove(key);
    }
}
