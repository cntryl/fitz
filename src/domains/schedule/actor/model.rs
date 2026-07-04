pub(super) use crate::domains::schedule::metrics::{
    METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL, METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL,
    METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL,
};
pub(super) use crate::domains::schedule::protocol::{
    epoch_ms_to_instant_with_reference, instant_to_epoch_ms_with_reference,
    parse_concrete_schedule_route, Clock, ConcreteScheduleRoute, CronSchedule, ScheduleCreateEntry,
    ScheduleDef, ScheduleListEntry, ScheduleMessage, ScheduleResponse, SystemClock,
};
pub(super) use crate::domains::schedule::store::{
    PersistedPendingFireClaim, PersistedSchedule, ScheduleAckDefinition, ScheduleFireClaim,
    ScheduleInsert, SchedulePendingFireClaimAck, ScheduleStore,
};
pub(super) use crate::prelude::Actor;
pub(super) use crate::runtime::actor::Context;
pub(super) use crate::runtime::routing::RouteFamily;
pub(super) use bytes::Bytes;
pub(super) use fxhash::FxBuildHasher;
pub(super) use std::cmp::Reverse;
pub(super) use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
pub(super) use std::sync::Arc;
pub(super) use std::time::{Duration, Instant};
pub(super) use tracing::{info, warn};

pub(super) type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;
pub(super) type FastSet<K> = HashSet<K, FxBuildHasher>;

pub(super) struct PendingScheduleCreate {
    pub(super) route: String,
    pub(super) route_parts: ConcreteScheduleRoute,
    pub(super) cron: String,
    pub(super) parsed_cron: CronSchedule,
    pub(super) payload: Bytes,
    pub(super) next_fire_time: Instant,
    pub(super) next_fire_ms: u64,
    pub(super) previous_fire_ms: Option<u64>,
    pub(super) last_fire_ms: Option<u64>,
    pub(super) executions_total: u64,
    pub(super) previous_list_index: Option<usize>,
}

pub(super) struct PendingScheduleFire {
    pub(super) route: String,
    pub(super) next_fire_time: Instant,
    pub(super) next_fire_ms: u64,
    pub(super) previous_fire_ms: u64,
}

pub(super) struct PendingClaim {
    pub(super) payload: Bytes,
    pub(super) claimed_at_ms: u64,
}

/// Durable schedule coordinator for one route family.
///
/// Persisted schedule definitions and pending claimed occurrences survive broker
/// restart and downtime through Midge. Live subscriptions and live notify
/// routing stay session-scoped and ephemeral. The in-memory heap is only a
/// derived accelerator: authoritative state lives in the persisted definition
/// row, and the due index can always be rebuilt from those durable definitions.
pub struct ScheduleActor {
    /// `RouteFamily` for storage column family mapping.
    pub(super) family: RouteFamily,
    /// Durable schedule storage plus legacy migration helpers.
    pub(super) store: ScheduleStore,
    /// In-memory schedule cache: route -> `ScheduleDef`.
    pub(super) schedules: FastMap<String, ScheduleDef>,
    /// Parsed cron expressions reused across repeated creates/upserts.
    pub(super) cron_cache: FastMap<String, CronSchedule>,
    /// Canonical mutable LIST backing store.
    pub(super) list_entries: Vec<Arc<ScheduleListEntry>>,
    /// Cached full LIST snapshot reused by the common `offset=0, limit=0` path.
    pub(super) list_cache: Option<Arc<Vec<Arc<ScheduleListEntry>>>>,
    /// Write options for persistence.
    pub(super) write_options: cntryl_midge::WriteOptions,
    /// Last scan time to deduplicate rapid scans.
    pub(super) last_scan_time: Instant,
    /// Minimum interval between scans (deduplication window).
    pub(super) scan_dedup_window: std::time::Duration,
    /// Derived min-heap of due timestamps keyed by route. Stale entries are
    /// tolerated and ignored by comparing each popped timestamp against the
    /// current definition's `next_fire_ms`.
    pub(super) ready_heap: BinaryHeap<(Reverse<u64>, String)>,
    /// Durably claimed occurrences awaiting acknowledged handoff into the live
    /// publish path.
    pub(super) pending_claimed_occurrences: BTreeMap<(u64, String), PendingClaim>,
    /// Injected wall-clock and monotonic time source.
    pub(super) clock: Arc<dyn Clock>,
    /// Number of schedules normalized forward during the last preload.
    pub(super) overdue_normalizations: u64,
}
