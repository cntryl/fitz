use crate::api::admin::{
    QueueDeadLetter, QueueDeadLetterSnapshot as AdminQueueDeadLetterSnapshot, QueueInfo,
    QueueInfoSnapshot as AdminQueueInfoSnapshot, QueueLease,
    QueueLeaseSnapshot as AdminQueueLeaseSnapshot,
};
use crate::domains::queue::{
    QueueAdminSnapshot, QueueDeadLetterSnapshot as ActorQueueDeadLetterSnapshot,
    QueueKey, QueueLeaseSnapshot as ActorQueueLeaseSnapshot,
};
use chrono::{TimeZone, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(super) struct QueueProjectionEntry {
    pub key: QueueKey,
    pub snapshot: QueueAdminSnapshot,
    pub leases: Vec<ActorQueueLeaseSnapshot>,
    pub dead_letters: Vec<ActorQueueDeadLetterSnapshot>,
}

pub(super) struct QueueProjectionState {
    queues: Vec<QueueInfo>,
    leases: Vec<QueueLease>,
    dead_letters: Vec<QueueDeadLetter>,
}

impl QueueProjectionState {
    pub fn from_entries(entries: Vec<QueueProjectionEntry>) -> Self {
        let mut queues = Vec::with_capacity(entries.len());
        let mut leases = Vec::new();
        let mut dead_letters = Vec::new();

        for entry in entries {
            queues.push(QueueInfo::snapshot(AdminQueueInfoSnapshot {
                family: entry.key.family.as_u64(),
                realm: &entry.key.realm,
                area: &entry.key.area,
                resource: &entry.key.resource,
                messages_ready: entry.snapshot.messages_ready,
                messages_delayed: entry.snapshot.messages_delayed,
                messages_leased: entry.snapshot.messages_leased,
                messages_dead_lettered: entry.snapshot.messages_dead_lettered,
                messages_total: entry.snapshot.messages_total,
                oldest_message_age_seconds: entry.snapshot.oldest_message_age_seconds,
            }));

            for lease in entry.leases {
                let expires_at = Utc
                    .timestamp_millis_opt(lease.expires_at_epoch_ms as i64)
                    .single()
                    .map(|timestamp| timestamp.to_rfc3339())
                    .unwrap_or_default();
                leases.push(QueueLease::snapshot(AdminQueueLeaseSnapshot {
                    message_id: lease.message_id,
                    family: entry.key.family.as_u64(),
                    realm: &entry.key.realm,
                    area: &entry.key.area,
                    resource: &entry.key.resource,
                    lease_token: lease.lease_token,
                    session_id: lease.session_id,
                    expires_at: &expires_at,
                    attempts: lease.attempts,
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
        leases.sort_by(|left, right| {
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
            leases,
            dead_letters,
        }
    }
}

pub(super) struct QueueAdminProjection {
    read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    dirty: AtomicBool,
}

impl QueueAdminProjection {
    pub fn new(read_model: Arc<crate::api::admin::read_model::AdminReadModel>) -> Self {
        Self {
            read_model,
            dirty: AtomicBool::new(false),
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn refresh_if_dirty<F>(&self, build_state: F)
    where
        F: FnOnce() -> QueueProjectionState,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.apply(build_state());
        }
    }

    fn apply(&self, state: QueueProjectionState) {
        self.read_model.replace_queues(state.queues);
        self.read_model.replace_queue_leases(state.leases);
        self.read_model.replace_queue_dead_letters(state.dead_letters);
    }
}