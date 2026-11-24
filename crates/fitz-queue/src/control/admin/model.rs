use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct QueueAgeBuckets {
    pub under_1m: usize,
    pub under_5m: usize,
    pub under_15m: usize,
    pub over_15m: usize,
}

impl QueueAgeBuckets {
    pub fn record_age_seconds(&mut self, age_seconds: u64) {
        if age_seconds < 60 {
            self.under_1m += 1;
        } else if age_seconds < 300 {
            self.under_5m += 1;
        } else if age_seconds < 900 {
            self.under_15m += 1;
        } else {
            self.over_15m += 1;
        }
    }

    pub fn merge(&mut self, other: QueueAgeBuckets) {
        self.under_1m += other.under_1m;
        self.under_5m += other.under_5m;
        self.under_15m += other.under_15m;
        self.over_15m += other.over_15m;
    }
}

#[must_use]
pub fn queue_status_label(
    messages_ready: usize,
    messages_delayed: usize,
    messages_inflight: usize,
    messages_dead_lettered: usize,
    in_rate_per_second: f64,
    out_rate_per_second: f64,
) -> String {
    let backlog = messages_ready
        .saturating_add(messages_delayed)
        .saturating_add(messages_dead_lettered);

    if messages_ready.saturating_add(messages_delayed) > 0
        && in_rate_per_second > out_rate_per_second
    {
        "falling_behind".to_string()
    } else if backlog > 0 {
        "backlogged".to_string()
    } else if messages_inflight > 0 {
        "draining".to_string()
    } else {
        "idle".to_string()
    }
}

fn queue_status_rank(status: &str) -> u8 {
    match status {
        "falling_behind" => 3,
        "backlogged" => 2,
        "draining" => 1,
        _ => 0,
    }
}

#[must_use]
pub fn worse_queue_status(left: &str, right: &str) -> String {
    if queue_status_rank(right) > queue_status_rank(left) {
        right.to_string()
    } else {
        left.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    pub family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub subscriptions_active: usize,
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
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInflight {
    pub message_id: u64,
    pub family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub inflight_token: String,
    pub session_id: String,
    pub expires_at: String,
    pub attempts: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDeadLetter {
    pub message_id: u64,
    pub family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub dead_lettered_at: String,
    pub attempts: usize,
    pub reason: String,
}

#[derive(Clone, Copy)]
pub struct QueueInflightSnapshot<'a> {
    pub message_id: u64,
    pub family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub inflight_token: u64,
    pub session_id: Option<u64>,
    pub expires_at: &'a str,
    pub attempts: usize,
}

#[derive(Clone, Copy)]
pub struct QueueInfoSnapshot<'a> {
    pub family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub subscriptions_active: usize,
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

#[derive(Clone, Copy)]
pub struct QueueDeadLetterSnapshot<'a> {
    pub message_id: u64,
    pub family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub dead_lettered_at: &'a str,
    pub attempts: usize,
    pub reason: &'a str,
}

impl QueueInfo {
    #[must_use]
    pub fn snapshot(snapshot: &QueueInfoSnapshot<'_>) -> Self {
        let QueueInfoSnapshot {
            family,
            realm,
            area,
            resource,
            subscriptions_active,
            messages_ready,
            messages_delayed,
            messages_inflight,
            messages_dead_lettered,
            messages_total,
            oldest_message_age_seconds,
            oldest_backlog_age_seconds,
            backlog_age_buckets,
            delay_age_buckets,
            enqueue_success_total,
            complete_success_total,
            in_rate_per_second,
            out_rate_per_second,
        } = *snapshot;
        let status = queue_status_label(
            messages_ready,
            messages_delayed,
            messages_inflight,
            messages_dead_lettered,
            in_rate_per_second,
            out_rate_per_second,
        );

        Self {
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            subscriptions_active,
            messages_ready,
            messages_delayed,
            messages_inflight,
            messages_dead_lettered,
            messages_total,
            oldest_message_age_seconds,
            oldest_backlog_age_seconds,
            backlog_age_buckets,
            delay_age_buckets,
            enqueue_success_total,
            complete_success_total,
            in_rate_per_second,
            out_rate_per_second,
            status,
        }
    }
}

impl QueueInflight {
    #[must_use]
    pub fn snapshot(snapshot: &QueueInflightSnapshot<'_>) -> Self {
        let QueueInflightSnapshot {
            message_id,
            family,
            realm,
            area,
            resource,
            inflight_token,
            session_id,
            expires_at,
            attempts,
        } = *snapshot;

        Self {
            message_id,
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            inflight_token: inflight_token.to_string(),
            session_id: session_id.map(|id| id.to_string()).unwrap_or_default(),
            expires_at: expires_at.to_string(),
            attempts,
        }
    }
}

impl QueueDeadLetter {
    #[must_use]
    pub fn snapshot(snapshot: &QueueDeadLetterSnapshot<'_>) -> Self {
        let QueueDeadLetterSnapshot {
            message_id,
            family,
            realm,
            area,
            resource,
            dead_lettered_at,
            attempts,
            reason,
        } = *snapshot;

        Self {
            message_id,
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            dead_lettered_at: dead_lettered_at.to_string(),
            attempts,
            reason: reason.to_string(),
        }
    }
}

use fitz_core::projection::{FamilyRow, ResourceIdentity};

macro_rules! resource_row {
    ($field:ident; $($ty:ty),+) => { $(impl FamilyRow for $ty {
        fn family(&self) -> u64 { self.$field }
        fn inventory_identity(&self) -> Option<ResourceIdentity> { Some((self.$field, self.realm.clone(), self.area.clone(), self.resource.clone())) }
    })+ };
}
resource_row!(family; QueueInfo, QueueInflight, QueueDeadLetter);
