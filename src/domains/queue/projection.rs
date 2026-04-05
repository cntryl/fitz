use crate::api::admin::read_model::AdminReadModel;
use crate::api::admin::{
    QueueDeadLetter, QueueDeadLetterSnapshot as AdminQueueDeadLetterSnapshot, QueueInfo,
    QueueInfoSnapshot as AdminQueueInfoSnapshot, QueueInflight,
    QueueInflightSnapshot as AdminQueueInflightSnapshot,
};
use crate::domains::queue::core::QueueKey;
use chrono::{TimeZone, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Point-in-time warm-actor queue counts for admin diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueAdminSnapshot {
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
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
    pub inflight: Vec<QueueInflightSnapshot>,
    pub dead_letters: Vec<QueueDeadLetterSnapshot>,
}

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
            queues.push(QueueInfo::snapshot(AdminQueueInfoSnapshot {
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
            }));

            for inflight_entry in entry.inflight {
                let expires_at = Utc
                    .timestamp_millis_opt(inflight_entry.expires_at_epoch_ms as i64)
                    .single()
                    .map(|timestamp| timestamp.to_rfc3339())
                    .unwrap_or_default();
                inflight.push(QueueInflight::snapshot(AdminQueueInflightSnapshot {
                    message_id: inflight_entry.message_id,
                    family: entry.key.family.as_u64(),
                    realm: &entry.key.realm,
                    area: &entry.key.area,
                    resource: &entry.key.resource,
                    inflight_token: inflight_entry.inflight_token,
                    session_id: inflight_entry.session_id,
                    expires_at: &expires_at,
                    attempts: inflight_entry.attempts,
                }));
            }

            for dead_letter in entry.dead_letters {
                let dead_lettered_at = Utc
                    .timestamp_millis_opt(dead_letter.dead_lettered_at_epoch_ms as i64)
                    .single()
                    .map(|timestamp| timestamp.to_rfc3339())
                    .unwrap_or_default();
                dead_letters.push(QueueDeadLetter::snapshot(AdminQueueDeadLetterSnapshot {
                    message_id: dead_letter.message_id,
                    family: entry.key.family.as_u64(),
                    realm: &entry.key.realm,
                    area: &entry.key.area,
                    resource: &entry.key.resource,
                    dead_lettered_at: &dead_lettered_at,
                    attempts: dead_letter.attempts,
                    reason: dead_letter.reason,
                }));
            }
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
}

pub(crate) struct QueueAdminProjection {
    read_model: Arc<AdminReadModel>,
    dirty: AtomicBool,
}

impl QueueAdminProjection {
    pub(crate) fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            dirty: AtomicBool::new(false),
        }
    }

    pub(crate) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub(crate) fn refresh_if_dirty<F>(&self, build_state: F)
    where
        F: FnOnce() -> QueueProjectionState,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.apply(build_state());
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
            },
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
        projection.mark_dirty();

        // Act
        projection.refresh_if_dirty(|| {
            QueueProjectionState::from_entries(vec![projection_entry("acme", "jobs", "emails")])
        });

        // Assert
        let queues = read_model.queues(None);
        let inflight = read_model.queue_inflight(None);
        let dead_letters = read_model.queue_dead_letters(None);
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].realm, "acme");
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

        // Act
        projection.refresh_if_dirty(|| {
            QueueProjectionState::from_entries(vec![projection_entry("acme", "jobs", "emails")])
        });

        // Assert
        assert!(read_model.queues(None).is_empty());
        assert!(read_model.queue_inflight(None).is_empty());
        assert!(read_model.queue_dead_letters(None).is_empty());
    }
}
