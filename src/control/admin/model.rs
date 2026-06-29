use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAgeBuckets {
    pub under_1m: usize,
    pub under_5m: usize,
    pub under_15m: usize,
    pub over_15m: usize,
}

impl QueueAgeBuckets {
    pub(crate) fn record_age_seconds(&mut self, age_seconds: u64) {
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

    pub(crate) fn merge(&mut self, other: QueueAgeBuckets) {
        self.under_1m += other.under_1m;
        self.under_5m += other.under_5m;
        self.under_15m += other.under_15m;
        self.over_15m += other.over_15m;
    }
}

pub(crate) fn queue_status_label(
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

pub(crate) fn worse_queue_status(left: &str, right: &str) -> String {
    if queue_status_rank(right) > queue_status_rank(left) {
        right.to_string()
    } else {
        left.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTransaction {
    pub route_family: u64,
    pub tx_id: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub mode: String,
    pub started_at: String,
    pub operations_count: usize,
    pub idle_seconds: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct KvLatencySnapshot {
    pub avg_ms: f64,
    pub p95_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResourceInventoryEntry {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub estimated_record_count: u64,
    pub estimated_storage_bytes: u64,
    pub estimate_complete: bool,
    pub read_latency_avg_ms: f64,
    pub read_latency_p95_ms: f64,
    pub write_latency_avg_ms: f64,
    pub write_latency_p95_ms: f64,
    pub transactions_active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLagBuckets {
    pub caught_up: usize,
    pub under_10: usize,
    pub under_100: usize,
    pub over_100: usize,
}

impl StreamLagBuckets {
    pub(crate) fn record_lag_events(&mut self, lag_events: u64) {
        if lag_events == 0 {
            self.caught_up += 1;
        } else if lag_events < 10 {
            self.under_10 += 1;
        } else if lag_events < 100 {
            self.under_100 += 1;
        } else {
            self.over_100 += 1;
        }
    }

    pub(crate) fn merge(&mut self, other: StreamLagBuckets) {
        self.caught_up += other.caught_up;
        self.under_10 += other.under_10;
        self.under_100 += other.under_100;
        self.over_100 += other.over_100;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLatencyBuckets {
    pub under_1ms: usize,
    pub under_5ms: usize,
    pub under_10ms: usize,
    pub under_50ms: usize,
    pub under_100ms: usize,
    pub under_500ms: usize,
    pub under_1s: usize,
    pub under_5s: usize,
    pub over_5s: usize,
}

impl StreamLatencyBuckets {
    pub(crate) fn from_histogram(buckets: [u64; 9]) -> Self {
        Self {
            under_1ms: buckets[0] as usize,
            under_5ms: buckets[1] as usize,
            under_10ms: buckets[2] as usize,
            under_50ms: buckets[3] as usize,
            under_100ms: buckets[4] as usize,
            under_500ms: buckets[5] as usize,
            under_1s: buckets[6] as usize,
            under_5s: buckets[7] as usize,
            over_5s: buckets[8] as usize,
        }
    }

    pub(crate) fn total(&self) -> usize {
        self.under_1ms
            + self.under_5ms
            + self.under_10ms
            + self.under_50ms
            + self.under_100ms
            + self.under_500ms
            + self.under_1s
            + self.under_5s
            + self.over_5s
    }

    pub(crate) fn slow_tail_count(&self) -> usize {
        self.under_500ms + self.under_1s + self.under_5s + self.over_5s
    }

    pub(crate) fn slow_tail_ratio(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.slow_tail_count() as f64 / total as f64
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleLatencyBuckets {
    pub under_1ms: usize,
    pub under_5ms: usize,
    pub under_10ms: usize,
    pub under_50ms: usize,
    pub under_100ms: usize,
    pub under_500ms: usize,
    pub under_1s: usize,
    pub under_5s: usize,
    pub over_5s: usize,
}

impl ScheduleLatencyBuckets {
    pub(crate) fn from_histogram(buckets: [u64; 9]) -> Self {
        Self {
            under_1ms: buckets[0] as usize,
            under_5ms: buckets[1] as usize,
            under_10ms: buckets[2] as usize,
            under_50ms: buckets[3] as usize,
            under_100ms: buckets[4] as usize,
            under_500ms: buckets[5] as usize,
            under_1s: buckets[6] as usize,
            under_5s: buckets[7] as usize,
            over_5s: buckets[8] as usize,
        }
    }

    pub(crate) fn total(&self) -> usize {
        self.under_1ms
            + self.under_5ms
            + self.under_10ms
            + self.under_50ms
            + self.under_100ms
            + self.under_500ms
            + self.under_1s
            + self.under_5s
            + self.over_5s
    }

    pub(crate) fn slow_tail_count(&self) -> usize {
        self.under_500ms + self.under_1s + self.under_5s + self.over_5s
    }

    pub(crate) fn slow_tail_ratio(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.slow_tail_count() as f64 / total as f64
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscription {
    pub route_family: u64,
    pub subscription_id: u64,
    pub session_id: String,
    pub realm: String,
    pub pattern: String,
    pub created_at: String,
    pub notifications_received: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRouteInfo {
    pub route_family: u64,
    pub route: String,
    pub subscribers: usize,
    pub publishes_total: u64,
    pub publishes_per_minute: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRealmWatermarkDetail {
    pub realm: String,
    pub area_count: usize,
    pub resource_count: usize,
    pub family_watermarks: Vec<StreamRealmWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRealmWatermark {
    pub family: u64,
    pub watermark: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAreaWatermarkDetail {
    pub realm: String,
    pub area: String,
    pub resource_count: usize,
    pub family_watermarks: Vec<StreamAreaWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAreaWatermark {
    pub family: u64,
    pub watermark: u64,
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

pub(crate) struct QueueInflightSnapshot<'a> {
    pub(crate) message_id: u64,
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) inflight_token: u64,
    pub(crate) session_id: Option<u64>,
    pub(crate) expires_at: &'a str,
    pub(crate) attempts: usize,
}

pub(crate) struct QueueInfoSnapshot<'a> {
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) subscriptions_active: usize,
    pub(crate) messages_ready: usize,
    pub(crate) messages_delayed: usize,
    pub(crate) messages_inflight: usize,
    pub(crate) messages_dead_lettered: usize,
    pub(crate) messages_total: usize,
    pub(crate) oldest_message_age_seconds: u64,
    pub(crate) oldest_backlog_age_seconds: u64,
    pub(crate) backlog_age_buckets: QueueAgeBuckets,
    pub(crate) delay_age_buckets: QueueAgeBuckets,
    pub(crate) enqueue_success_total: u64,
    pub(crate) complete_success_total: u64,
    pub(crate) in_rate_per_second: f64,
    pub(crate) out_rate_per_second: f64,
}

pub(crate) struct QueueDeadLetterSnapshot<'a> {
    pub(crate) message_id: u64,
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) dead_lettered_at: &'a str,
    pub(crate) attempts: usize,
    pub(crate) reason: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcWorker {
    pub route_family: u64,
    pub session_id: String,
    pub realm: String,
    pub route: String,
    pub registered_at: String,
    pub requests_handled: u64,
    pub average_latency_ms: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcLatencyBuckets {
    pub under_5ms: usize,
    pub under_25ms: usize,
    pub under_100ms: usize,
    pub over_100ms: usize,
}

impl RpcLatencyBuckets {
    pub(crate) fn record_latency_ms(&mut self, latency_ms: f64) {
        if latency_ms < 5.0 {
            self.under_5ms += 1;
        } else if latency_ms < 25.0 {
            self.under_25ms += 1;
        } else if latency_ms < 100.0 {
            self.under_100ms += 1;
        } else {
            self.over_100ms += 1;
        }
    }

    pub(crate) fn merge(&mut self, other: RpcLatencyBuckets) {
        self.under_5ms += other.under_5ms;
        self.under_25ms += other.under_25ms;
        self.under_100ms += other.under_100ms;
        self.over_100ms += other.over_100ms;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingRequest {
    pub route_family: u64,
    pub correlation_id: String,
    pub route: String,
    pub submitted_at: String,
    pub age_seconds: u64,
    pub worker_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub owner_session_id: String,
    pub acquired_at: String,
    pub expires_at: String,
    pub renewals: usize,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseWaiterInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub owner_id: String,
    pub session_id: String,
    pub queued_token: u64,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub cron: String,
    pub next_run: String,
    pub last_run: Option<String>,
    pub executions_total: u64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulePendingClaimInfo {
    pub route_family: u64,
    pub route: String,
    pub fire_ms: u64,
    pub claimed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub route_family: u64,
    pub subject: String,
    pub identity_claim: String,
    pub identity_value: String,
    pub connected_at: String,
    pub idle_seconds: u64,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub transport: String,
    pub remote_addr: String,
}

pub(crate) struct StreamInfoSnapshot<'a> {
    pub route_family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
}

impl KvTransaction {
    pub(crate) fn snapshot(
        route_family: u64,
        tx_id: u64,
        session_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        started_at: &str,
    ) -> Self {
        Self {
            route_family,
            tx_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            mode: format!("session:{session_id}:readwrite"),
            started_at: started_at.to_string(),
            operations_count: 0,
            idle_seconds: 0,
        }
    }
}

impl StreamInfo {
    pub(crate) fn snapshot(snapshot: StreamInfoSnapshot<'_>) -> Self {
        Self {
            route_family: snapshot.route_family,
            realm: snapshot.realm.to_string(),
            area: snapshot.area.to_string(),
            resource: snapshot.resource.to_string(),
            offset: snapshot.offset,
            watermark: snapshot.watermark,
            size_bytes: snapshot.size_bytes,
            sessions_active: snapshot.sessions_active,
        }
    }
}

impl NoticeSubscription {
    pub(crate) fn snapshot(
        route_family: u64,
        subscription_id: u64,
        session_id: u64,
        realm: &str,
        pattern: String,
        created_at: &str,
    ) -> Self {
        Self {
            route_family,
            subscription_id,
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            pattern,
            created_at: created_at.to_string(),
            notifications_received: 0,
        }
    }
}

impl NoticeRouteInfo {
    pub(crate) fn snapshot(route_family: u64, route: String, subscribers: usize) -> Self {
        Self {
            route_family,
            route,
            subscribers,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        }
    }
}

impl StreamRealmWatermarkDetail {
    pub(crate) fn snapshot(
        realm: &str,
        area_count: usize,
        resource_count: usize,
        family_watermarks: Vec<StreamRealmWatermark>,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area_count,
            resource_count,
            family_watermarks,
        }
    }
}

impl StreamRealmWatermark {
    pub(crate) fn snapshot(family: u64, watermark: u64) -> Self {
        Self { family, watermark }
    }
}

impl StreamAreaWatermarkDetail {
    pub(crate) fn snapshot(
        realm: &str,
        area: &str,
        resource_count: usize,
        family_watermarks: Vec<StreamAreaWatermark>,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area: area.to_string(),
            resource_count,
            family_watermarks,
        }
    }
}

impl StreamAreaWatermark {
    pub(crate) fn snapshot(family: u64, watermark: u64) -> Self {
        Self { family, watermark }
    }
}

impl QueueInfo {
    pub(crate) fn snapshot(snapshot: QueueInfoSnapshot<'_>) -> Self {
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
        } = snapshot;
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
    pub(crate) fn snapshot(snapshot: QueueInflightSnapshot<'_>) -> Self {
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
        } = snapshot;

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
    pub(crate) fn snapshot(snapshot: QueueDeadLetterSnapshot<'_>) -> Self {
        let QueueDeadLetterSnapshot {
            message_id,
            family,
            realm,
            area,
            resource,
            dead_lettered_at,
            attempts,
            reason,
        } = snapshot;

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

impl RpcWorker {
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(crate) fn snapshot(
        route_family: u64,
        session_id: u64,
        realm: &str,
        route: &str,
        registered_at: &str,
        requests_handled: u64,
        average_latency_ms: f64,
    ) -> Self {
        Self {
            route_family,
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            route: route.to_string(),
            registered_at: registered_at.to_string(),
            requests_handled,
            average_latency_ms,
        }
    }
}

impl RpcPendingRequest {
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(crate) fn snapshot(
        route_family: u64,
        correlation_id: String,
        route: &str,
        submitted_at: &str,
        age_seconds: u64,
        worker_session_id: Option<String>,
    ) -> Self {
        Self {
            route_family,
            correlation_id,
            route: route.to_string(),
            submitted_at: submitted_at.to_string(),
            age_seconds,
            worker_session_id,
        }
    }
}

impl LeaseInfo {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn snapshot(
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        owner_session_id: &str,
        acquired_at: &str,
        expires_at: String,
        renewals: usize,
        fencing_token: u64,
    ) -> Self {
        Self {
            route_family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            owner_session_id: owner_session_id.to_string(),
            acquired_at: acquired_at.to_string(),
            expires_at,
            renewals,
            fencing_token,
        }
    }
}

impl ScheduleInfo {
    pub(crate) fn enabled_snapshot(
        route_family: u64,
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
        next_run: &str,
    ) -> Self {
        Self {
            route_family,
            realm,
            area,
            resource,
            operation,
            cron,
            next_run: next_run.to_string(),
            last_run: None,
            executions_total: 0,
            enabled: true,
        }
    }
}
