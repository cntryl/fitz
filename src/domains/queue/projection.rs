use crate::control::admin::read_model::AdminReadModel;
use crate::control::admin::{
    QueueAgeBuckets, QueueDeadLetter, QueueDeadLetterSnapshot as AdminQueueDeadLetterSnapshot,
    QueueInflight, QueueInflightSnapshot as AdminQueueInflightSnapshot, QueueInfo,
    QueueInfoSnapshot as AdminQueueInfoSnapshot,
};
use crate::domains::queue::core::QueueKey;
use chrono::{TimeZone, Utc};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

/// Point-in-time warm-actor queue counts for admin diagnostics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueueAdminSnapshot {
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
    pub oldest_backlog_age_seconds: u64,
    pub backlog_age_buckets: QueueAgeBuckets,
    pub delay_age_buckets: QueueAgeBuckets,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
}

/// Point-in-time live inflight snapshot for admin diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueInflightSnapshot {
    pub message_id: u64,
    pub inflight_token: u64,
    pub session_id: Option<u64>,
    pub expires_at_epoch_ms: u64,
    pub attempts: usize,
}

/// Point-in-time dead-letter snapshot for admin diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeadLetterSnapshot {
    pub message_id: u64,
    pub dead_lettered_at_epoch_ms: u64,
    pub attempts: usize,
    pub reason: &'static str,
}

pub(crate) struct QueueProjectionEntry {
    pub key: QueueKey,
    pub snapshot: QueueAdminSnapshot,
    pub subscriptions_active: usize,
    pub inflight: Vec<QueueInflightSnapshot>,
    pub dead_letters: Vec<QueueDeadLetterSnapshot>,
}

#[derive(Clone)]
pub(crate) struct QueueProjectionState {
    queues: Vec<QueueInfo>,
    inflight: Vec<QueueInflight>,
    dead_letters: Vec<QueueDeadLetter>,
}

impl QueueProjectionState {
    pub(crate) fn from_entries(entries: Vec<QueueProjectionEntry>) -> Self {
        let mut queues = Vec::with_capacity(entries.len());
        let mut inflight = Vec::new();
        let mut dead_letters = Vec::new();

        for entry in entries {
            queues.push(Self::project_queue_info(&entry));
            inflight.extend(Self::project_inflight_entries(&entry));
            dead_letters.extend(Self::project_dead_letter_entries(&entry));
        }

        queues.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
            ))
        });
        inflight.sort_by(|left, right| {
            (
                &left.realm,
                &left.area,
                &left.resource,
                left.message_id,
                &left.session_id,
            )
                .cmp(&(
                    &right.realm,
                    &right.area,
                    &right.resource,
                    right.message_id,
                    &right.session_id,
                ))
        });
        dead_letters.sort_by(|left, right| {
            (
                &left.realm,
                &left.area,
                &left.resource,
                &left.dead_lettered_at,
                left.message_id,
            )
                .cmp(&(
                    &right.realm,
                    &right.area,
                    &right.resource,
                    &right.dead_lettered_at,
                    right.message_id,
                ))
        });

        Self {
            queues,
            inflight,
            dead_letters,
        }
    }

    fn combine(states: impl Iterator<Item = Self>) -> Self {
        let mut combined = Self {
            queues: Vec::new(),
            inflight: Vec::new(),
            dead_letters: Vec::new(),
        };
        for state in states {
            combined.queues.extend(state.queues);
            combined.inflight.extend(state.inflight);
            combined.dead_letters.extend(state.dead_letters);
        }
        combined.queues.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
            ))
        });
        combined.inflight.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource, left.message_id).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
                right.message_id,
            ))
        });
        combined.dead_letters.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource, left.message_id).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
                right.message_id,
            ))
        });
        combined
    }

    fn project_queue_info(entry: &QueueProjectionEntry) -> QueueInfo {
        QueueInfo::snapshot(&AdminQueueInfoSnapshot {
            family: entry.key.family.as_u64(),
            realm: &entry.key.realm,
            area: &entry.key.area,
            resource: &entry.key.resource,
            messages_ready: entry.snapshot.messages_ready,
            messages_delayed: entry.snapshot.messages_delayed,
            messages_inflight: entry.snapshot.messages_inflight,
            messages_dead_lettered: entry.snapshot.messages_dead_lettered,
            messages_total: entry.snapshot.messages_total,
            oldest_message_age_seconds: entry.snapshot.oldest_message_age_seconds,
            oldest_backlog_age_seconds: entry.snapshot.oldest_backlog_age_seconds,
            backlog_age_buckets: entry.snapshot.backlog_age_buckets,
            delay_age_buckets: entry.snapshot.delay_age_buckets,
            subscriptions_active: entry.subscriptions_active,
            enqueue_success_total: entry.snapshot.enqueue_success_total,
            complete_success_total: entry.snapshot.complete_success_total,
            in_rate_per_second: entry.snapshot.in_rate_per_second,
            out_rate_per_second: entry.snapshot.out_rate_per_second,
        })
    }

    fn project_inflight_entries(entry: &QueueProjectionEntry) -> Vec<QueueInflight> {
        entry
            .inflight
            .iter()
            .map(|inflight_entry| {
                let expires_at = Self::rfc3339_from_epoch_ms(inflight_entry.expires_at_epoch_ms);
                QueueInflight::snapshot(&AdminQueueInflightSnapshot {
                    message_id: inflight_entry.message_id,
                    family: entry.key.family.as_u64(),
                    realm: &entry.key.realm,
                    area: &entry.key.area,
                    resource: &entry.key.resource,
                    inflight_token: inflight_entry.inflight_token,
                    session_id: inflight_entry.session_id,
                    expires_at: &expires_at,
                    attempts: inflight_entry.attempts,
                })
            })
            .collect()
    }

    fn project_dead_letter_entries(entry: &QueueProjectionEntry) -> Vec<QueueDeadLetter> {
        entry
            .dead_letters
            .iter()
            .map(|dead_letter| {
                let dead_lettered_at =
                    Self::rfc3339_from_epoch_ms(dead_letter.dead_lettered_at_epoch_ms);
                QueueDeadLetter::snapshot(&AdminQueueDeadLetterSnapshot {
                    message_id: dead_letter.message_id,
                    family: entry.key.family.as_u64(),
                    realm: &entry.key.realm,
                    area: &entry.key.area,
                    resource: &entry.key.resource,
                    dead_lettered_at: &dead_lettered_at,
                    attempts: dead_letter.attempts,
                    reason: dead_letter.reason,
                })
            })
            .collect()
    }

    fn rfc3339_from_epoch_ms(epoch_ms: u64) -> String {
        Utc.timestamp_millis_opt(epoch_ms.cast_signed())
            .single()
            .map(|timestamp| timestamp.to_rfc3339())
            .unwrap_or_default()
    }
}

pub(crate) struct QueueAdminProjection {
    read_model: Arc<AdminReadModel>,
    family_states: Mutex<BTreeMap<u32, QueueProjectionState>>,
    dirty_families: Mutex<HashSet<u32>>,
}

impl QueueAdminProjection {
    pub(crate) fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            family_states: Mutex::new(BTreeMap::new()),
            dirty_families: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn mark_dirty(&self, family: crate::runtime::routing::RouteFamily) {
        self.dirty_families.lock().insert(family.id());
    }

    pub(crate) fn refresh_if_dirty<F>(
        &self,
        family: crate::runtime::routing::RouteFamily,
        build_state: F,
    ) where
        F: FnOnce() -> QueueProjectionState,
    {
        if self.dirty_families.lock().remove(&family.id()) {
            let combined = {
                let mut states = self.family_states.lock();
                states.insert(family.id(), build_state());
                QueueProjectionState::combine(states.values().cloned())
            };
            self.apply(combined);
        }
    }

    fn apply(&self, state: QueueProjectionState) {
        self.read_model.replace_queues(state.queues);
        self.read_model.replace_queue_inflight(state.inflight);
        self.read_model
            .replace_queue_dead_letters(state.dead_letters);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;

    fn assert_f64_eq(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < f64::EPSILON);
    }

    fn queue_key(realm: &str, area: &str, resource: &str) -> QueueKey {
        QueueKey {
            family: RouteFamily::new(7),
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }

    fn projection_entry(realm: &str, area: &str, resource: &str) -> QueueProjectionEntry {
        QueueProjectionEntry {
            key: queue_key(realm, area, resource),
            snapshot: QueueAdminSnapshot {
                messages_ready: 1,
                messages_delayed: 2,
                messages_inflight: 3,
                messages_dead_lettered: 4,
                messages_total: 10,
                oldest_message_age_seconds: 5,
                oldest_backlog_age_seconds: 6,
                backlog_age_buckets: QueueAgeBuckets::default(),
                delay_age_buckets: QueueAgeBuckets::default(),
                enqueue_success_total: 7,
                complete_success_total: 8,
                in_rate_per_second: 1.5,
                out_rate_per_second: 0.75,
            },
            subscriptions_active: 9,
            inflight: vec![QueueInflightSnapshot {
                message_id: 22,
                inflight_token: 33,
                session_id: Some(44),
                expires_at_epoch_ms: 1_700_000_000_000,
                attempts: 2,
            }],
            dead_letters: vec![QueueDeadLetterSnapshot {
                message_id: 55,
                dead_lettered_at_epoch_ms: 1_700_000_001_000,
                attempts: 6,
                reason: "dlq",
            }],
        }
    }

    #[test]
    fn should_sort_projection_rows_given_unsorted_entries() {
        // Arrange
        let entries = vec![
            projection_entry("zeta", "ops", "emails"),
            projection_entry("alpha", "jobs", "billing"),
        ];

        // Act
        let state = QueueProjectionState::from_entries(entries);

        // Assert
        assert_eq!(state.queues[0].realm, "alpha");
        assert_eq!(state.queues[0].area, "jobs");
        assert_eq!(state.queues[0].resource, "billing");
        assert_eq!(state.queues[1].realm, "zeta");
        assert_eq!(state.inflight[0].realm, "alpha");
        assert_eq!(state.dead_letters[0].realm, "alpha");
    }

    #[test]
    fn should_refresh_admin_read_model_when_projection_marked_dirty() {
        // Arrange
        let read_model = AdminReadModel::new();
        let projection = QueueAdminProjection::new(read_model.clone());
        let family = RouteFamily::new(7);
        projection.mark_dirty(family);

        // Act
        projection.refresh_if_dirty(family, || {
            QueueProjectionState::from_entries(vec![projection_entry("acme", "jobs", "emails")])
        });

        // Assert
        let queues = read_model.queues(None);
        let inflight = read_model.queue_inflight(None);
        let dead_letters = read_model.queue_dead_letters(None);
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].realm, "acme");
        assert_eq!(queues[0].subscriptions_active, 9);
        assert_f64_eq(queues[0].in_rate_per_second, 1.5);
        assert_f64_eq(queues[0].out_rate_per_second, 0.75);
        assert_eq!(inflight.len(), 1);
        assert_eq!(inflight[0].resource, "emails");
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].reason, "dlq");
    }

    #[test]
    fn should_leave_admin_read_model_unchanged_when_projection_is_not_dirty() {
        // Arrange
        let read_model = AdminReadModel::new();
        let projection = QueueAdminProjection::new(read_model.clone());
        let family = RouteFamily::new(7);

        // Act
        projection.refresh_if_dirty(family, || {
            QueueProjectionState::from_entries(vec![projection_entry("acme", "jobs", "emails")])
        });

        // Assert
        assert!(read_model.queues(None).is_empty());
        assert!(read_model.queue_inflight(None).is_empty());
        assert!(read_model.queue_dead_letters(None).is_empty());
    }

    #[test]
    fn should_preserve_sibling_family_rows_when_one_family_refreshes() {
        // Arrange
        let read_model = AdminReadModel::new();
        let projection = QueueAdminProjection::new(read_model.clone());
        let first_family = RouteFamily::new(7);
        let second_family = RouteFamily::new(8);
        projection.mark_dirty(first_family);
        projection.refresh_if_dirty(first_family, || {
            QueueProjectionState::from_entries(vec![projection_entry("alpha", "jobs", "first")])
        });
        let mut second_entry = projection_entry("beta", "jobs", "second");
        second_entry.key.family = second_family;
        projection.mark_dirty(second_family);

        // Act
        projection.refresh_if_dirty(second_family, || {
            QueueProjectionState::from_entries(vec![second_entry])
        });

        // Assert
        let queues = read_model.queues(None);
        assert_eq!(queues.len(), 2);
        assert_eq!(queues[0].resource, "first");
        assert_eq!(queues[1].resource, "second");
    }
}
