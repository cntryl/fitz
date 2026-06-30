pub(super) use bytes::Bytes;
pub(super) use lexkey::{Encoder, LexKey};
pub(super) use std::collections::BTreeMap;
pub(super) use std::sync::Arc;

pub(super) use cntryl_midge::WriteOptions;

pub(super) use crate::domains::schedule::protocol::parse_concrete_schedule_route;
pub(super) use crate::utils::storage_key::{self, DomainKeyspace};

pub(super) const DEFINITION_VALUE_VERSION_V1: u8 = 1;
pub(super) const DEFINITION_VALUE_VERSION_V2: u8 = 2;
pub(super) const DEFINITION_VALUE_VERSION_V3: u8 = 3;
pub(super) const BODY_VALUE_VERSION_V1: u8 = 1;
pub(super) const PENDING_FIRE_VALUE_VERSION_V1: u8 = 1;
pub(super) const PENDING_FIRE_VALUE_VERSION_V2: u8 = 2;
pub(super) const DEFINITION_PREFIX: &[u8] = &[0x01];
pub(super) const BODY_PREFIX: &[u8] = &[0x02];
pub(super) const DUE_PREFIX: &[u8] = &[0x03];
pub(super) const PENDING_FIRE_PREFIX: &[u8] = &[0x04];
pub(super) const DUE_INDEX_VALUE: &[u8] = &[1];
pub(super) const LEGACY_PREFIX: &[u8] = b"sched:m";
pub(super) const LEGACY_INDEX_PREFIX: &[u8] = b"sched:idx:";

pub struct ScheduleBatchInsert {
    pub route: String,
    pub cron: String,
    pub payload: Bytes,
    pub next_fire_ms: u64,
    pub previous_fire_ms: Option<u64>,
    pub last_fire_ms: Option<u64>,
    pub executions_total: u64,
}

#[derive(Clone, Copy)]
pub struct ScheduleInsert<'a> {
    pub route: &'a str,
    pub cron: &'a str,
    pub payload: &'a Bytes,
    pub next_fire_ms: u64,
    pub previous_fire_ms: Option<u64>,
    pub last_fire_ms: Option<u64>,
    pub executions_total: u64,
}

pub(super) struct ScheduleDefinitionData<'a> {
    pub(super) next_fire_ms: u64,
    pub(super) last_fire_ms: Option<u64>,
    pub(super) executions_total: u64,
    pub(super) cron: &'a str,
    pub(super) payload: &'a Bytes,
}

pub struct ScheduleFireClaim<'a> {
    pub route: &'a str,
    pub cron: &'a str,
    pub payload: &'a Bytes,
    pub claimed_at_ms: u64,
    pub next_fire_ms: u64,
    pub previous_fire_ms: u64,
    pub last_fire_ms: Option<u64>,
    pub executions_total: u64,
}

pub struct SchedulePendingFireClaimAck<'a> {
    pub route: &'a str,
    pub fire_ms: u64,
    pub acknowledged_at_ms: u64,
    pub definition: Option<ScheduleAckDefinition<'a>>,
}

pub struct ScheduleAckDefinition<'a> {
    pub next_fire_ms: u64,
    pub cron: &'a str,
    pub payload: &'a Bytes,
    pub executions_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedSchedule {
    pub route: String,
    pub cron: String,
    pub payload: Bytes,
    pub next_fire_ms: u64,
    pub last_fire_ms: Option<u64>,
    pub executions_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedPendingFireClaim {
    pub route: String,
    pub payload: Bytes,
    pub claimed_at_ms: u64,
    pub fire_ms: u64,
}

pub struct ScheduleStore {
    pub(super) db: crate::storage::FitzStorageEngine,
    #[cfg(test)]
    pub(super) fail_next_commit: Arc<std::sync::atomic::AtomicBool>,
}

pub(super) type ScheduleRows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum DecodedDefinitionRow {
    Inline {
        next_fire_ms: u64,
        cron: String,
        payload: Bytes,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    },
    Metadata {
        next_fire_ms: u64,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    },
}
