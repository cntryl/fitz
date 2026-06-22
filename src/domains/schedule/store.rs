use bytes::Bytes;
use lexkey::{Encoder, LexKey};
use std::collections::BTreeMap;
use std::sync::Arc;

use cntryl_midge::WriteOptions;

use crate::domains::schedule::protocol::parse_concrete_schedule_route;
use crate::utils::storage_key::{self, DomainKeyspace};

const DEFINITION_VALUE_VERSION_V1: u8 = 1;
const DEFINITION_VALUE_VERSION_V2: u8 = 2;
const DEFINITION_VALUE_VERSION_V3: u8 = 3;
const BODY_VALUE_VERSION_V1: u8 = 1;
const PENDING_FIRE_VALUE_VERSION_V1: u8 = 1;
const PENDING_FIRE_VALUE_VERSION_V2: u8 = 2;
const DEFINITION_PREFIX: &[u8] = &[0x01];
const BODY_PREFIX: &[u8] = &[0x02];
const DUE_PREFIX: &[u8] = &[0x03];
const PENDING_FIRE_PREFIX: &[u8] = &[0x04];
const DUE_INDEX_VALUE: &[u8] = &[1];
const LEGACY_PREFIX: &[u8] = b"sched:m";
const LEGACY_INDEX_PREFIX: &[u8] = b"sched:idx:";

pub struct ScheduleBatchInsert {
    pub route: String,
    pub cron: String,
    pub payload: Bytes,
    pub next_fire_ms: u64,
    pub previous_fire_ms: Option<u64>,
    pub last_fire_ms: Option<u64>,
    pub executions_total: u64,
}

pub struct ScheduleInsert<'a> {
    pub route: &'a str,
    pub cron: &'a str,
    pub payload: &'a Bytes,
    pub next_fire_ms: u64,
    pub previous_fire_ms: Option<u64>,
    pub last_fire_ms: Option<u64>,
    pub executions_total: u64,
}

struct ScheduleDefinitionData<'a> {
    next_fire_ms: u64,
    last_fire_ms: Option<u64>,
    executions_total: u64,
    cron: &'a str,
    payload: &'a Bytes,
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
    db: Arc<cntryl_midge::Engine>,
    #[cfg(test)]
    fail_next_commit: Arc<std::sync::atomic::AtomicBool>,
}

type ScheduleRows = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, PartialEq, Eq)]
enum DecodedDefinitionRow {
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

impl ScheduleStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self {
            db,
            #[cfg(test)]
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn schedule_key_suffix(key: &[u8]) -> &[u8] {
        storage_key::strip_domain_prefix(key, DomainKeyspace::Schedule).unwrap_or(key)
    }

    fn encode_route_identity_into(
        encoder: &mut Encoder,
        area: &str,
        resource: &str,
        operation: &str,
    ) {
        storage_key::encode_segment_into(encoder, area);
        storage_key::encode_segment_into(encoder, resource);
        encoder.encode_string_into(operation);
    }

    fn decode_route_identity_from_suffix(realm: &str, suffix: &[u8]) -> Result<String, String> {
        let mut parts = suffix.splitn(3, |byte| *byte == LexKey::SEPARATOR);
        let area = parts
            .next()
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| "Invalid schedule route identity".to_string())?;
        let resource = parts
            .next()
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| "Invalid schedule route identity".to_string())?;
        let operation = parts
            .next()
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| "Invalid schedule route identity".to_string())?;

        if operation.contains(&LexKey::SEPARATOR) {
            return Err("Invalid schedule route identity".to_string());
        }

        let area = std::str::from_utf8(area)
            .map_err(|e| format!("Invalid schedule area encoding: {}", e))?;
        let resource = std::str::from_utf8(resource)
            .map_err(|e| format!("Invalid schedule resource encoding: {}", e))?;
        let operation = std::str::from_utf8(operation)
            .map_err(|e| format!("Invalid schedule operation encoding: {}", e))?;

        Ok(format!("schedule://{realm}/{area}/{resource}/{operation}"))
    }

    fn encode_prefixed_route_key_from_realm(
        realm: &str,
        route: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        let mut key = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Schedule,
            suffix_prefix[0],
            parsed.area.len() + parsed.resource.len() + parsed.operation.len() + 2,
        );
        Self::encode_route_identity_into(
            &mut key,
            &parsed.area,
            &parsed.resource,
            &parsed.operation,
        );
        key.into_vec()
    }

    fn encode_prefixed_route_key(route: &str, suffix_prefix: &[u8]) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        Self::encode_prefixed_route_key_from_realm(&parsed.realm, route, suffix_prefix)
    }

    fn encode_prefixed_timed_route_key_from_realm(
        realm: &str,
        timestamp_ms: u64,
        route: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        let mut key = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Schedule,
            suffix_prefix[0],
            9 + parsed.area.len() + parsed.resource.len() + parsed.operation.len() + 2,
        );
        key.encode_bytes_into(&timestamp_ms.to_be_bytes());
        key.push_separator();
        Self::encode_route_identity_into(
            &mut key,
            &parsed.area,
            &parsed.resource,
            &parsed.operation,
        );
        key.into_vec()
    }

    fn encode_prefixed_timed_route_key(
        timestamp_ms: u64,
        route: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        Self::encode_prefixed_timed_route_key_from_realm(
            &parsed.realm,
            timestamp_ms,
            route,
            suffix_prefix,
        )
    }

    fn scan_schedule_rows(
        tx: &cntryl_midge::Transaction,
        suffix_prefix: &[u8],
    ) -> Result<ScheduleRows, String> {
        let rows = tx
            .scan(&cntryl_midge::Query::new())
            .map_err(|e| format!("scan schedule rows failed: {:?}", e))?
            .collect_all();

        Ok(rows
            .into_iter()
            .filter(|(key, _)| Self::schedule_key_suffix(key).starts_with(suffix_prefix))
            .collect())
    }

    pub(crate) fn encode_definition_key(route: &str) -> Vec<u8> {
        Self::encode_prefixed_route_key(route, DEFINITION_PREFIX)
    }

    pub(crate) fn encode_body_key(route: &str) -> Vec<u8> {
        Self::encode_prefixed_route_key(route, BODY_PREFIX)
    }

    #[allow(dead_code)]
    pub(crate) fn encode_due_key(next_fire_ms: u64, route: &str) -> Vec<u8> {
        Self::encode_prefixed_timed_route_key(next_fire_ms, route, DUE_PREFIX)
    }

    pub(crate) fn encode_pending_fire_key(fire_ms: u64, route: &str) -> Vec<u8> {
        Self::encode_prefixed_timed_route_key(fire_ms, route, PENDING_FIRE_PREFIX)
    }

    fn decode_definition_key(key: &[u8]) -> Result<String, String> {
        let Some((realm, suffix)) = storage_key::split_domain_key(key, DomainKeyspace::Schedule)
        else {
            return Err("Invalid schedule definition key prefix".to_string());
        };
        if !suffix.starts_with(DEFINITION_PREFIX) {
            return Err("Invalid schedule definition key prefix".to_string());
        }

        Self::decode_route_identity_from_suffix(realm, &suffix[DEFINITION_PREFIX.len()..])
    }

    fn decode_body_key(key: &[u8]) -> Result<String, String> {
        let Some((realm, suffix)) = storage_key::split_domain_key(key, DomainKeyspace::Schedule)
        else {
            return Err("Invalid schedule body key prefix".to_string());
        };
        if !suffix.starts_with(BODY_PREFIX) {
            return Err("Invalid schedule body key prefix".to_string());
        }

        Self::decode_route_identity_from_suffix(realm, &suffix[BODY_PREFIX.len()..])
    }

    fn decode_due_key_with_prefix(key: &[u8], prefix: &[u8]) -> Result<(u64, String), String> {
        if prefix == LEGACY_PREFIX {
            let suffix = Self::schedule_key_suffix(key);
            if !suffix.starts_with(prefix) {
                return Err("Invalid schedule due key prefix".to_string());
            }

            return Self::decode_legacy_timed_suffix(&suffix[prefix.len()..]);
        }

        let Some((realm, suffix)) = storage_key::split_domain_key(key, DomainKeyspace::Schedule)
        else {
            return Err("Invalid schedule due key prefix".to_string());
        };
        if !suffix.starts_with(prefix) {
            return Err("Invalid schedule due key prefix".to_string());
        }

        let remaining = &suffix[prefix.len()..];
        if remaining.len() < 9 {
            return Err("Schedule due key too short".to_string());
        }
        if remaining[8] != LexKey::SEPARATOR {
            return Err("Missing time/route separator".to_string());
        }

        let fire_ms = u64::from_be_bytes(remaining[0..8].try_into().unwrap());
        let route = Self::decode_route_identity_from_suffix(realm, &remaining[9..])?;

        Ok((fire_ms, route))
    }

    fn decode_legacy_timed_suffix(remaining: &[u8]) -> Result<(u64, String), String> {
        if remaining.len() < 18 {
            return Err("Schedule due key too short".to_string());
        }

        let minute_bytes = &remaining[0..8];
        let minute_epoch = u64::from_be_bytes([
            minute_bytes[0],
            minute_bytes[1],
            minute_bytes[2],
            minute_bytes[3],
            minute_bytes[4],
            minute_bytes[5],
            minute_bytes[6],
            minute_bytes[7],
        ]);

        if remaining[8] != b'/' {
            return Err("Missing minute/offset separator".to_string());
        }

        let ms_offset = if remaining.len() > 17 && remaining[17] == b':' {
            let offset_bytes = &remaining[9..17];
            u64::from_be_bytes(offset_bytes.try_into().unwrap())
        } else if remaining.len() > 15 && remaining[15] == b':' {
            let offset_bytes = &remaining[9..15];
            u64::from_be_bytes([
                0,
                0,
                offset_bytes[0],
                offset_bytes[1],
                offset_bytes[2],
                offset_bytes[3],
                offset_bytes[4],
                offset_bytes[5],
            ])
        } else {
            return Err("Missing offset/route separator".to_string());
        };

        let route_start = if remaining.len() > 17 && remaining[17] == b':' {
            18
        } else {
            16
        };

        let route = String::from_utf8(remaining[route_start..].to_vec())
            .map_err(|e| format!("Invalid route encoding: {}", e))?;

        Ok(((minute_epoch * 60_000) + ms_offset, route))
    }

    #[allow(dead_code)]
    pub(crate) fn decode_due_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, DUE_PREFIX)
    }

    fn decode_legacy_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, LEGACY_PREFIX)
    }

    fn decode_pending_fire_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, PENDING_FIRE_PREFIX)
    }

    fn encode_definition_metadata_value(
        next_fire_ms: u64,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    ) -> Vec<u8> {
        let mut value = Vec::with_capacity(1 + 8 + 8 + 8);
        value.push(DEFINITION_VALUE_VERSION_V3);
        value.extend_from_slice(&next_fire_ms.to_be_bytes());
        value.extend_from_slice(&last_fire_ms.unwrap_or(0).to_be_bytes());
        value.extend_from_slice(&executions_total.to_be_bytes());
        value
    }

    fn encode_definition_body_value(cron: &str, payload: &Bytes) -> Vec<u8> {
        let mut value = Vec::with_capacity(1 + 4 + cron.len() + 4 + payload.len());
        value.push(BODY_VALUE_VERSION_V1);
        value.extend_from_slice(&(cron.len() as u32).to_be_bytes());
        value.extend_from_slice(cron.as_bytes());
        value.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        value.extend_from_slice(payload);
        value
    }

    fn decode_definition_value(value: &[u8]) -> Result<DecodedDefinitionRow, String> {
        if value.is_empty() {
            return Err("Schedule definition value too short".to_string());
        }

        match value[0] {
            DEFINITION_VALUE_VERSION_V1 => {
                if value.len() < 17 {
                    return Err("Schedule definition value too short".to_string());
                }

                let next_fire_ms = u64::from_be_bytes(value[1..9].try_into().unwrap());
                let cron_len = u32::from_be_bytes(value[9..13].try_into().unwrap()) as usize;
                let cron_start = 13;
                let cron_end = cron_start + cron_len;
                if value.len() < cron_end + 4 {
                    return Err("Schedule definition value truncated before cron".to_string());
                }

                let cron = String::from_utf8(value[cron_start..cron_end].to_vec())
                    .map_err(|e| format!("Invalid cron encoding: {}", e))?;
                let payload_len =
                    u32::from_be_bytes(value[cron_end..cron_end + 4].try_into().unwrap()) as usize;
                let payload_start = cron_end + 4;
                let payload_end = payload_start + payload_len;
                if value.len() != payload_end {
                    return Err("Schedule definition value has invalid payload length".to_string());
                }

                Ok(DecodedDefinitionRow::Inline {
                    next_fire_ms,
                    cron,
                    payload: Bytes::copy_from_slice(&value[payload_start..payload_end]),
                    last_fire_ms: None,
                    executions_total: 0,
                })
            }
            DEFINITION_VALUE_VERSION_V2 => {
                if value.len() < 33 {
                    return Err("Schedule definition value too short".to_string());
                }

                let next_fire_ms = u64::from_be_bytes(value[1..9].try_into().unwrap());
                let last_fire_ms = u64::from_be_bytes(value[9..17].try_into().unwrap());
                let executions_total = u64::from_be_bytes(value[17..25].try_into().unwrap());
                let cron_len = u32::from_be_bytes(value[25..29].try_into().unwrap()) as usize;
                let cron_start = 29;
                let cron_end = cron_start + cron_len;
                if value.len() < cron_end + 4 {
                    return Err("Schedule definition value truncated before cron".to_string());
                }

                let cron = String::from_utf8(value[cron_start..cron_end].to_vec())
                    .map_err(|e| format!("Invalid cron encoding: {}", e))?;
                let payload_len =
                    u32::from_be_bytes(value[cron_end..cron_end + 4].try_into().unwrap()) as usize;
                let payload_start = cron_end + 4;
                let payload_end = payload_start + payload_len;
                if value.len() != payload_end {
                    return Err("Schedule definition value has invalid payload length".to_string());
                }

                Ok(DecodedDefinitionRow::Inline {
                    next_fire_ms,
                    cron,
                    payload: Bytes::copy_from_slice(&value[payload_start..payload_end]),
                    last_fire_ms: (last_fire_ms != 0).then_some(last_fire_ms),
                    executions_total,
                })
            }
            DEFINITION_VALUE_VERSION_V3 => {
                if value.len() != 25 {
                    return Err("Schedule definition metadata value has invalid length".to_string());
                }

                let next_fire_ms = u64::from_be_bytes(value[1..9].try_into().unwrap());
                let last_fire_ms = u64::from_be_bytes(value[9..17].try_into().unwrap());
                let executions_total = u64::from_be_bytes(value[17..25].try_into().unwrap());
                Ok(DecodedDefinitionRow::Metadata {
                    next_fire_ms,
                    last_fire_ms: (last_fire_ms != 0).then_some(last_fire_ms),
                    executions_total,
                })
            }
            other => Err(format!(
                "Unsupported schedule definition value version: {}",
                other
            )),
        }
    }

    fn decode_definition_body_value(value: &[u8]) -> Result<(String, Bytes), String> {
        if value.is_empty() {
            return Err("Schedule definition body value too short".to_string());
        }

        match value[0] {
            BODY_VALUE_VERSION_V1 => {
                if value.len() < 9 {
                    return Err("Schedule definition body value too short".to_string());
                }

                let cron_len = u32::from_be_bytes(value[1..5].try_into().unwrap()) as usize;
                let cron_start = 5;
                let cron_end = cron_start + cron_len;
                if value.len() < cron_end + 4 {
                    return Err("Schedule definition body value truncated before cron".to_string());
                }

                let cron = String::from_utf8(value[cron_start..cron_end].to_vec())
                    .map_err(|e| format!("Invalid cron encoding: {}", e))?;
                let payload_len =
                    u32::from_be_bytes(value[cron_end..cron_end + 4].try_into().unwrap()) as usize;
                let payload_start = cron_end + 4;
                let payload_end = payload_start + payload_len;
                if value.len() != payload_end {
                    return Err(
                        "Schedule definition body value has invalid payload length".to_string()
                    );
                }

                Ok((
                    cron,
                    Bytes::copy_from_slice(&value[payload_start..payload_end]),
                ))
            }
            other => Err(format!(
                "Unsupported schedule definition body value version: {}",
                other
            )),
        }
    }

    fn decode_legacy_value(value: &[u8]) -> Result<(String, Bytes), String> {
        let sep_pos = value
            .iter()
            .position(|&b| b == b'|')
            .ok_or_else(|| "Invalid legacy schedule value format".to_string())?;

        let cron = String::from_utf8(value[..sep_pos].to_vec())
            .map_err(|e| format!("Invalid cron encoding: {}", e))?;
        let payload = Bytes::copy_from_slice(&value[sep_pos + 1..]);
        Ok((cron, payload))
    }

    fn encode_pending_fire_value(payload: &Bytes, claimed_at_ms: u64) -> Vec<u8> {
        let mut value = Vec::with_capacity(1 + std::mem::size_of::<u64>() + payload.len());
        value.push(PENDING_FIRE_VALUE_VERSION_V2);
        value.extend_from_slice(&claimed_at_ms.to_le_bytes());
        value.extend_from_slice(payload);
        value
    }

    fn decode_pending_fire_value(value: &[u8]) -> Result<(u64, Bytes), String> {
        if value.is_empty() {
            return Err("Schedule pending fire value too short".to_string());
        }

        match value[0] {
            PENDING_FIRE_VALUE_VERSION_V1 => Ok((0, Bytes::copy_from_slice(&value[1..]))),
            PENDING_FIRE_VALUE_VERSION_V2 => {
                if value.len() < 1 + std::mem::size_of::<u64>() {
                    return Err(
                        "Schedule pending fire value missing claimed-at timestamp".to_string()
                    );
                }

                let claimed_at_ms = u64::from_le_bytes(value[1..9].try_into().unwrap());
                Ok((claimed_at_ms, Bytes::copy_from_slice(&value[9..])))
            }
            other => Err(format!(
                "Unsupported schedule pending fire value version: {}",
                other
            )),
        }
    }

    fn put_definition_metadata(
        txn: &mut cntryl_midge::Transaction,
        route: &str,
        next_fire_ms: u64,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    ) -> Result<(), String> {
        txn.put(
            Self::encode_definition_key(route),
            Self::encode_definition_metadata_value(next_fire_ms, last_fire_ms, executions_total),
            None,
        )
        .map_err(|e| format!("put schedule definition failed: {:?}", e))
    }

    fn put_definition_body(
        txn: &mut cntryl_midge::Transaction,
        route: &str,
        cron: &str,
        payload: &Bytes,
    ) -> Result<(), String> {
        txn.put(
            Self::encode_body_key(route),
            Self::encode_definition_body_value(cron, payload),
            None,
        )
        .map_err(|e| format!("put schedule body failed: {:?}", e))
    }

    fn put_schedule_definition(
        txn: &mut cntryl_midge::Transaction,
        route: &str,
        definition: &ScheduleDefinitionData<'_>,
    ) -> Result<(), String> {
        Self::put_definition_metadata(
            txn,
            route,
            definition.next_fire_ms,
            definition.last_fire_ms,
            definition.executions_total,
        )?;
        Self::put_definition_body(txn, route, definition.cron, definition.payload)
    }

    pub fn insert(
        &self,
        cf_id: u64,
        schedule: ScheduleInsert<'_>,
        write_options: WriteOptions,
    ) -> Result<Vec<u8>, String> {
        let parsed = parse_concrete_schedule_route(schedule.route)?;
        let due_key = Self::encode_prefixed_timed_route_key_from_realm(
            &parsed.realm,
            schedule.next_fire_ms,
            schedule.route,
            DUE_PREFIX,
        );

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        Self::put_schedule_definition(
            &mut txn,
            schedule.route,
            &ScheduleDefinitionData {
                next_fire_ms: schedule.next_fire_ms,
                last_fire_ms: schedule.last_fire_ms,
                executions_total: schedule.executions_total,
                cron: schedule.cron,
                payload: schedule.payload,
            },
        )?;

        if let Some(previous_fire_ms) = schedule.previous_fire_ms {
            if previous_fire_ms != schedule.next_fire_ms {
                txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                    &parsed.realm,
                    previous_fire_ms,
                    schedule.route,
                    DUE_PREFIX,
                ))
                .map_err(|e| format!("delete previous due key failed: {:?}", e))?;
            }
        }

        // The durable definition row is authoritative. Live actors use their in-memory
        // ready heap and `load_all()` rebuilds the due index on restart.

        self.commit_or_inject(txn, write_options)?;
        Ok(due_key)
    }

    pub fn insert_batch(
        &self,
        cf_id: u64,
        items: &[ScheduleBatchInsert],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for item in items {
            let parsed = parse_concrete_schedule_route(&item.route)?;
            Self::put_schedule_definition(
                &mut txn,
                &item.route,
                &ScheduleDefinitionData {
                    next_fire_ms: item.next_fire_ms,
                    last_fire_ms: item.last_fire_ms,
                    executions_total: item.executions_total,
                    cron: &item.cron,
                    payload: &item.payload,
                },
            )?;

            if let Some(previous_fire_ms) = item.previous_fire_ms {
                if previous_fire_ms != item.next_fire_ms {
                    txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                        &parsed.realm,
                        previous_fire_ms,
                        &item.route,
                        DUE_PREFIX,
                    ))
                    .map_err(|e| format!("delete previous due key failed: {:?}", e))?;
                }
            }
        }

        self.commit_or_inject(txn, write_options)
    }

    pub fn claim_due_batch(
        &self,
        cf_id: u64,
        items: &[ScheduleFireClaim<'_>],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for item in items {
            let parsed = parse_concrete_schedule_route(item.route)?;
            Self::put_definition_metadata(
                &mut txn,
                item.route,
                item.next_fire_ms,
                item.last_fire_ms,
                item.executions_total,
            )?;
            let pending_fire_key = Self::encode_prefixed_timed_route_key_from_realm(
                &parsed.realm,
                item.previous_fire_ms,
                item.route,
                PENDING_FIRE_PREFIX,
            );

            if item.previous_fire_ms != item.next_fire_ms {
                txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                    &parsed.realm,
                    item.previous_fire_ms,
                    item.route,
                    DUE_PREFIX,
                ))
                .map_err(|e| format!("delete previous due key failed: {:?}", e))?;
            }
            txn.put(
                pending_fire_key,
                Self::encode_pending_fire_value(item.payload, item.claimed_at_ms),
                None,
            )
            .map_err(|e| format!("put pending fire failed: {:?}", e))?;
        }

        self.commit_or_inject(txn, write_options)
    }

    pub fn ack_pending_fire_claims(
        &self,
        cf_id: u64,
        items: &[SchedulePendingFireClaimAck<'_>],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for item in items {
            let parsed = parse_concrete_schedule_route(item.route)?;
            txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                &parsed.realm,
                item.fire_ms,
                item.route,
                PENDING_FIRE_PREFIX,
            ))
            .map_err(|e| format!("delete pending fire failed: {:?}", e))?;

            if let Some(definition) = &item.definition {
                Self::put_definition_metadata(
                    &mut txn,
                    item.route,
                    definition.next_fire_ms,
                    Some(item.acknowledged_at_ms),
                    definition.executions_total,
                )
                .map_err(|error| {
                    format!("update schedule acknowledgement state failed: {}", error)
                })?;
            }
        }

        self.commit_or_inject(txn, write_options)
    }

    pub fn delete_current(
        &self,
        cf_id: u64,
        route: &str,
        next_fire_ms: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let parsed = parse_concrete_schedule_route(route)?;
        self.delete_current_with_realm(cf_id, &parsed.realm, route, next_fire_ms, write_options)
    }

    pub(crate) fn delete_current_with_realm(
        &self,
        cf_id: u64,
        realm: &str,
        route: &str,
        next_fire_ms: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        txn.delete(Self::encode_prefixed_route_key_from_realm(
            realm,
            route,
            DEFINITION_PREFIX,
        ))
        .map_err(|e| format!("delete schedule definition failed: {:?}", e))?;
        txn.delete(Self::encode_prefixed_route_key_from_realm(
            realm,
            route,
            BODY_PREFIX,
        ))
        .map_err(|e| format!("delete schedule body failed: {:?}", e))?;
        txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
            realm,
            next_fire_ms,
            route,
            DUE_PREFIX,
        ))
        .map_err(|e| format!("delete schedule due index failed: {:?}", e))?;

        self.commit_or_inject(txn, write_options)
    }

    /// Load authoritative durable schedule definitions, migrate any legacy TTL-backed
    /// rows that are still present, rebuild the full due index, and delete stale
    /// legacy rows and indexes for the current family.
    pub fn load_all(
        &self,
        cf_id: u64,
        write_options: WriteOptions,
    ) -> Result<Vec<PersistedSchedule>, String> {
        let read_tx = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let mut schedules = BTreeMap::<String, PersistedSchedule>::new();
        let mut normalized_definitions = Vec::<PersistedSchedule>::new();
        let mut metadata_rows = BTreeMap::<String, (u64, Option<u64>, u64)>::new();

        let definition_rows = Self::scan_schedule_rows(&read_tx, DEFINITION_PREFIX)?;
        let body_rows = Self::scan_schedule_rows(&read_tx, BODY_PREFIX)?;
        let legacy_rows = Self::scan_schedule_rows(&read_tx, LEGACY_PREFIX)?;

        for (key, value) in definition_rows {
            match (
                Self::decode_definition_key(&key),
                Self::decode_definition_value(&value),
            ) {
                (
                    Ok(route),
                    Ok(DecodedDefinitionRow::Inline {
                        next_fire_ms,
                        cron,
                        payload,
                        last_fire_ms,
                        executions_total,
                    }),
                ) => {
                    let persisted = PersistedSchedule {
                        route: route.clone(),
                        cron,
                        payload,
                        next_fire_ms,
                        last_fire_ms,
                        executions_total,
                    };
                    normalized_definitions.push(persisted.clone());
                    schedules.insert(route, persisted);
                }
                (
                    Ok(route),
                    Ok(DecodedDefinitionRow::Metadata {
                        next_fire_ms,
                        last_fire_ms,
                        executions_total,
                    }),
                ) => {
                    metadata_rows.insert(route, (next_fire_ms, last_fire_ms, executions_total));
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!(
                        "decode persisted schedule definition failed: {}",
                        error
                    ));
                }
            }
        }

        let mut body_definitions = BTreeMap::<String, (String, Bytes)>::new();
        for (key, value) in body_rows {
            match (
                Self::decode_body_key(&key),
                Self::decode_definition_body_value(&value),
            ) {
                (Ok(route), Ok((cron, payload))) => {
                    body_definitions.insert(route, (cron, payload));
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!("decode persisted schedule body failed: {}", error));
                }
            }
        }

        for (route, (next_fire_ms, last_fire_ms, executions_total)) in metadata_rows {
            match body_definitions.get(&route) {
                Some((cron, payload)) => {
                    schedules.insert(
                        route.clone(),
                        PersistedSchedule {
                            route,
                            cron: cron.clone(),
                            payload: payload.clone(),
                            next_fire_ms,
                            last_fire_ms,
                            executions_total,
                        },
                    );
                }
                None => {
                    return Err(format!(
                        "missing schedule body row for persisted definition: {}",
                        route
                    ));
                }
            }
        }

        for (key, value) in &legacy_rows {
            match (
                Self::decode_legacy_key(key),
                Self::decode_legacy_value(value),
            ) {
                (Ok((next_fire_ms, route)), Ok((cron, payload))) => {
                    if schedules.contains_key(&route) {
                        continue;
                    }

                    let persisted = PersistedSchedule {
                        route: route.clone(),
                        cron,
                        payload,
                        next_fire_ms,
                        last_fire_ms: None,
                        executions_total: 0,
                    };
                    normalized_definitions.push(persisted.clone());
                    schedules.insert(route.clone(), persisted);
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!("decode legacy schedule row failed: {}", error));
                }
            }
        }

        let due_rows = Self::scan_schedule_rows(&read_tx, DUE_PREFIX)?;
        let legacy_index_rows = Self::scan_schedule_rows(&read_tx, LEGACY_INDEX_PREFIX)?;
        drop(read_tx);

        let mut write_tx = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin migration tx failed: {:?}", e))?;

        for schedule in &normalized_definitions {
            Self::put_schedule_definition(
                &mut write_tx,
                &schedule.route,
                &ScheduleDefinitionData {
                    next_fire_ms: schedule.next_fire_ms,
                    last_fire_ms: schedule.last_fire_ms,
                    executions_total: schedule.executions_total,
                    cron: &schedule.cron,
                    payload: &schedule.payload,
                },
            )
            .map_err(|error| format!("import schedule definition failed: {}", error))?;
        }

        for (key, _) in due_rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("delete stale due index failed: {:?}", e))?;
        }

        for schedule in schedules.values() {
            let parsed = parse_concrete_schedule_route(&schedule.route)?;
            write_tx
                .put(
                    Self::encode_prefixed_timed_route_key_from_realm(
                        &parsed.realm,
                        schedule.next_fire_ms,
                        &schedule.route,
                        DUE_PREFIX,
                    ),
                    DUE_INDEX_VALUE.to_vec(),
                    None,
                )
                .map_err(|e| format!("rebuild due index failed: {:?}", e))?;
        }

        for (key, _) in legacy_index_rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("delete legacy schedule index failed: {:?}", e))?;
        }

        for (key, _) in legacy_rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("delete legacy schedule row failed: {:?}", e))?;
        }

        self.commit_or_inject(write_tx, write_options)?;
        Ok(schedules.into_values().collect())
    }

    pub fn load_pending_fire_claims(
        &self,
        cf_id: u64,
    ) -> Result<Vec<PersistedPendingFireClaim>, String> {
        let read_tx = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let rows = Self::scan_schedule_rows(&read_tx, PENDING_FIRE_PREFIX)?;

        let mut pending = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            match (
                Self::decode_pending_fire_key(&key),
                Self::decode_pending_fire_value(&value),
            ) {
                (Ok((fire_ms, route)), Ok((claimed_at_ms, payload))) => {
                    pending.push(PersistedPendingFireClaim {
                        route,
                        payload,
                        claimed_at_ms,
                        fire_ms,
                    });
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!("decode pending schedule fire failed: {}", error));
                }
            }
        }

        pending.sort_by(|left, right| {
            (left.fire_ms, left.route.as_str()).cmp(&(right.fire_ms, right.route.as_str()))
        });
        Ok(pending)
    }

    pub fn delete_pending_fire_claims(
        &self,
        cf_id: u64,
        items: &[(u64, String)],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for (fire_ms, route) in items {
            txn.delete(Self::encode_pending_fire_key(*fire_ms, route))
                .map_err(|e| format!("delete pending fire failed: {:?}", e))?;
        }

        self.commit_or_inject(txn, write_options)
    }

    #[cfg(test)]
    pub fn fail_next_commit_for_tests(&self) {
        self.fail_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected schedule store commit failure".to_string());
        }

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))
    }

    #[cfg(not(test))]
    fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::create_test_engine_with_cfs;

    fn make_store() -> (ScheduleStore, Arc<cntryl_midge::Engine>) {
        let db = create_test_engine_with_cfs(vec![1]);
        (ScheduleStore::new(db.clone()), db)
    }

    fn put_raw(
        db: &Arc<cntryl_midge::Engine>,
        cf_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), String> {
        let mut txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;
        txn.put(key, value, None)
            .map_err(|e| format!("put failed: {:?}", e))?;
        txn.commit(WriteOptions::buffered())
            .map_err(|e| format!("commit failed: {:?}", e))
    }

    fn read_raw_value(db: &Arc<cntryl_midge::Engine>, cf_id: u64, key: &[u8]) -> Option<Vec<u8>> {
        let txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");

        txn.get(key)
            .expect("read raw key")
            .map(|value| value.to_vec())
    }

    fn count_prefix(db: &Arc<cntryl_midge::Engine>, cf_id: u64, prefix: &'static [u8]) -> usize {
        let txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        ScheduleStore::scan_schedule_rows(&txn, prefix)
            .expect("scan prefix")
            .len()
    }

    #[test]
    fn should_encode_schedule_definition_key_with_typed_segments() {
        // Arrange
        let mut expected = storage_key::domain_marker_encoder(
            "acme",
            DomainKeyspace::Schedule,
            DEFINITION_PREFIX[0],
            13,
        );
        storage_key::encode_segment_into(&mut expected, "jobs");
        storage_key::encode_segment_into(&mut expected, "backup");
        expected.encode_string_into("run");

        // Act
        let key = ScheduleStore::encode_definition_key("schedule://acme/jobs/backup/run");

        // Assert
        assert_eq!(key, expected.into_vec());
    }

    #[test]
    fn should_order_schedule_due_keys_by_fire_time_before_route_segments() {
        // Arrange
        let first = ScheduleStore::encode_due_key(1_700_000_000_001, "schedule://acme/jobs/a/run");
        let second = ScheduleStore::encode_due_key(1_700_000_000_010, "schedule://acme/jobs/a/run");

        // Act
        let ordered = first < second;

        // Assert
        assert!(ordered);
    }

    fn encode_inline_definition_value_v2(
        next_fire_ms: u64,
        cron: &str,
        payload: &Bytes,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    ) -> Vec<u8> {
        let mut value = Vec::with_capacity(1 + 8 + 8 + 8 + 4 + cron.len() + 4 + payload.len());
        value.push(DEFINITION_VALUE_VERSION_V2);
        value.extend_from_slice(&next_fire_ms.to_be_bytes());
        value.extend_from_slice(&last_fire_ms.unwrap_or(0).to_be_bytes());
        value.extend_from_slice(&executions_total.to_be_bytes());
        value.extend_from_slice(&(cron.len() as u32).to_be_bytes());
        value.extend_from_slice(cron.as_bytes());
        value.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        value.extend_from_slice(payload);
        value
    }

    #[test]
    fn should_persist_definition_without_due_index_for_inserted_schedule() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/backup/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_ms = 1_700_000_001_000_u64;
        let expected_due_key = ScheduleStore::encode_due_key(next_fire_ms, route);

        // Act
        let stored_due_key = store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");

        let definition_key = ScheduleStore::encode_definition_key(route);
        let definition_value = read_raw_value(&db, 1, &definition_key).expect("definition row");
        let body_key = ScheduleStore::encode_body_key(route);
        let body_value = read_raw_value(&db, 1, &body_key).expect("body row");

        // Assert
        assert_eq!(stored_due_key, expected_due_key);
        assert_eq!(
            ScheduleStore::decode_definition_value(&definition_value).unwrap(),
            DecodedDefinitionRow::Metadata {
                next_fire_ms,
                last_fire_ms: None,
                executions_total: 0,
            }
        );
        assert_eq!(
            ScheduleStore::decode_definition_body_value(&body_value).unwrap(),
            ("* * * * *".to_string(), Bytes::from_static(b"payload"),)
        );
        assert_eq!(
            count_prefix(&db, 1, BODY_PREFIX),
            1,
            "body row should be persisted alongside metadata"
        );
        assert_eq!(
            count_prefix(&db, 1, DEFINITION_PREFIX),
            1,
            "definition metadata row should be persisted"
        );
        assert!(
            read_raw_value(&db, 1, &expected_due_key).is_none(),
            "live insert should not persist the derived due index"
        );
    }

    #[test]
    fn should_rebuild_due_index_from_inserted_schedule_definitions_on_load() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/rebuild/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_ms = 1_700_000_001_500_u64;
        let expected_due_key = ScheduleStore::encode_due_key(next_fire_ms, route);

        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "*/5 * * * *",
                    payload: &payload,
                    next_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");
        assert!(
            read_raw_value(&db, 1, &expected_due_key).is_none(),
            "live insert should not persist the derived due index"
        );

        // Act
        let loaded = store
            .load_all(1, WriteOptions::buffered())
            .expect("load schedules");

        // Assert
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].route, route);
        assert_eq!(loaded[0].cron, "*/5 * * * *");
        assert_eq!(loaded[0].payload, payload);
        assert_eq!(loaded[0].next_fire_ms, next_fire_ms);
        assert_eq!(
            read_raw_value(&db, 1, &expected_due_key),
            Some(DUE_INDEX_VALUE.to_vec())
        );
    }

    #[test]
    fn should_rebuild_due_index_from_definitions_after_legacy_import() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/legacy/run";
        let legacy_due_key = {
            let minute_epoch = 1_700_000_002_000_u64 / 60_000;
            let ms_offset = 1_700_000_002_000_u64 % 60_000;
            let mut key = Vec::new();
            key.extend_from_slice(LEGACY_PREFIX);
            key.extend_from_slice(minute_epoch.to_be_bytes().as_slice());
            key.push(b'/');
            key.extend_from_slice(ms_offset.to_be_bytes().as_slice());
            key.push(b':');
            key.extend_from_slice(route.as_bytes());
            key
        };
        put_raw(&db, 1, legacy_due_key, b"*/5 * * * *|legacy".to_vec()).expect("write legacy row");
        put_raw(
            &db,
            1,
            ScheduleStore::encode_due_key(9_999_999_999_999, "schedule://stale/index/only/run"),
            DUE_INDEX_VALUE.to_vec(),
        )
        .expect("write stale due row");

        // Act
        let loaded = store
            .load_all(1, WriteOptions::buffered())
            .expect("load schedules");

        // Assert
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].route, route);
        assert_eq!(loaded[0].cron, "*/5 * * * *");
        assert_eq!(loaded[0].payload, Bytes::from_static(b"legacy"));
        assert_eq!(count_prefix(&db, 1, LEGACY_PREFIX), 0);
        assert_eq!(count_prefix(&db, 1, LEGACY_INDEX_PREFIX), 0);
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_due_key(loaded[0].next_fire_ms, route),
            )
            .is_some(),
            "rebuilt due index should exist for the imported schedule"
        );
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_due_key(
                    9_999_999_999_999,
                    "schedule://stale/index/only/run",
                ),
            )
            .is_none(),
            "stale due index rows should be removed during rebuild"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_some(),
            "definition row should exist after legacy import"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route)).is_some(),
            "body row should exist after legacy import"
        );
    }

    #[test]
    fn should_split_inline_definition_rows_on_load() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/migrate/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_ms = 1_700_000_003_000_u64;
        let last_fire_ms = Some(1_700_000_002_500_u64);
        let executions_total = 7;

        put_raw(
            &db,
            1,
            ScheduleStore::encode_definition_key(route),
            encode_inline_definition_value_v2(
                next_fire_ms,
                "*/10 * * * *",
                &payload,
                last_fire_ms,
                executions_total,
            ),
        )
        .expect("write inline definition row");
        put_raw(
            &db,
            1,
            ScheduleStore::encode_due_key(next_fire_ms, route),
            DUE_INDEX_VALUE.to_vec(),
        )
        .expect("write due row");

        // Act
        let loaded = store
            .load_all(1, WriteOptions::buffered())
            .expect("load schedules");
        let metadata = read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route))
            .expect("definition metadata row");
        let body =
            read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route)).expect("body row");

        // Assert
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].route, route);
        assert_eq!(loaded[0].cron, "*/10 * * * *");
        assert_eq!(loaded[0].payload, payload);
        assert_eq!(loaded[0].next_fire_ms, next_fire_ms);
        assert_eq!(loaded[0].last_fire_ms, last_fire_ms);
        assert_eq!(loaded[0].executions_total, executions_total);
        assert_eq!(
            ScheduleStore::decode_definition_value(&metadata).unwrap(),
            DecodedDefinitionRow::Metadata {
                next_fire_ms,
                last_fire_ms,
                executions_total,
            }
        );
        assert_eq!(
            ScheduleStore::decode_definition_body_value(&body).unwrap(),
            ("*/10 * * * *".to_string(), Bytes::from_static(b"payload"))
        );
    }

    #[test]
    fn should_remove_definition_with_due_index_when_canceling_schedule() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/delete/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_ms = 1_700_000_010_000_u64;
        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "0 * * * *",
                    payload: &payload,
                    next_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");

        // Act
        store
            .delete_current(1, route, next_fire_ms, WriteOptions::buffered())
            .expect("delete schedule");

        // Assert
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_none(),
            "definition row should be deleted"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route)).is_none(),
            "body row should be deleted"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_due_key(next_fire_ms, route)).is_none(),
            "due index should be deleted"
        );
    }

    #[test]
    fn should_persist_pending_fire_given_claimed_due_schedule() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/claim/run";
        let payload = Bytes::from_static(b"payload");
        let original_fire_ms = 1_700_000_020_000_u64;
        let next_fire_ms = 1_700_000_080_000_u64;

        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms: original_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");

        // Act
        store
            .claim_due_batch(
                1,
                &[ScheduleFireClaim {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    claimed_at_ms: 1_700_000_020_500_u64,
                    next_fire_ms,
                    previous_fire_ms: original_fire_ms,
                    last_fire_ms: None,
                    executions_total: 0,
                }],
                WriteOptions::buffered(),
            )
            .expect("claim due schedule");
        let pending = store
            .load_pending_fire_claims(1)
            .expect("load pending fire claims");

        // Assert
        assert_eq!(
            pending,
            vec![PersistedPendingFireClaim {
                route: route.to_string(),
                payload: payload.clone(),
                claimed_at_ms: 1_700_000_020_500_u64,
                fire_ms: original_fire_ms,
            }]
        );
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_due_key(original_fire_ms, route),
            )
            .is_none(),
            "original due key should be removed after claim"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_due_key(next_fire_ms, route)).is_none(),
            "live claim should not persist the next due key"
        );
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_pending_fire_key(original_fire_ms, route),
            )
            .is_some(),
            "pending fire should be persisted after claim"
        );
        assert_eq!(
            ScheduleStore::decode_definition_body_value(
                &read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route))
                    .expect("body row should remain after claim"),
            )
            .unwrap(),
            ("* * * * *".to_string(), Bytes::from_static(b"payload"))
        );
    }

    #[test]
    fn should_record_acknowledgement_state_given_acknowledged_claimed_due_schedule() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/claim/run";
        let payload = Bytes::from_static(b"payload");
        let original_fire_ms = 1_700_000_020_000_u64;
        let next_fire_ms = 1_700_000_080_000_u64;
        let acknowledged_at_ms = 1_700_000_021_500_u64;

        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms: original_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");
        store
            .claim_due_batch(
                1,
                &[ScheduleFireClaim {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    claimed_at_ms: 1_700_000_020_500_u64,
                    next_fire_ms,
                    previous_fire_ms: original_fire_ms,
                    last_fire_ms: None,
                    executions_total: 0,
                }],
                WriteOptions::buffered(),
            )
            .expect("claim due schedule");

        // Act
        store
            .ack_pending_fire_claims(
                1,
                &[SchedulePendingFireClaimAck {
                    route,
                    fire_ms: original_fire_ms,
                    acknowledged_at_ms,
                    definition: Some(ScheduleAckDefinition {
                        next_fire_ms,
                        cron: "* * * * *",
                        payload: &payload,
                        executions_total: 1,
                    }),
                }],
                WriteOptions::buffered(),
            )
            .expect("ack pending fire claim");
        let pending = store
            .load_pending_fire_claims(1)
            .expect("load pending fire claims");
        let schedules = store
            .load_all(1, WriteOptions::buffered())
            .expect("load schedules");

        // Assert
        assert!(
            pending.is_empty(),
            "pending fire should be removed after ack"
        );
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].last_fire_ms, Some(acknowledged_at_ms));
        assert_eq!(schedules[0].executions_total, 1);
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_pending_fire_key(original_fire_ms, route),
            )
            .is_none(),
            "pending fire row should be deleted after ack"
        );
    }

    #[test]
    fn should_remove_pending_fire_without_recreating_definition_given_missing_schedule_state() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/claim/run";
        let payload = Bytes::from_static(b"payload");
        let original_fire_ms = 1_700_000_020_000_u64;
        let next_fire_ms = 1_700_000_080_000_u64;
        let acknowledged_at_ms = 1_700_000_021_500_u64;

        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms: original_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");
        store
            .claim_due_batch(
                1,
                &[ScheduleFireClaim {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    claimed_at_ms: 1_700_000_020_500_u64,
                    next_fire_ms,
                    previous_fire_ms: original_fire_ms,
                    last_fire_ms: None,
                    executions_total: 0,
                }],
                WriteOptions::buffered(),
            )
            .expect("claim due schedule");
        store
            .delete_current(1, route, next_fire_ms, WriteOptions::buffered())
            .expect("delete schedule definition");

        // Act
        store
            .ack_pending_fire_claims(
                1,
                &[SchedulePendingFireClaimAck {
                    route,
                    fire_ms: original_fire_ms,
                    acknowledged_at_ms,
                    definition: None,
                }],
                WriteOptions::buffered(),
            )
            .expect("ack pending fire claim");
        let pending = store
            .load_pending_fire_claims(1)
            .expect("load pending fire claims");
        let schedules = store
            .load_all(1, WriteOptions::buffered())
            .expect("load schedules");

        // Assert
        assert!(
            pending.is_empty(),
            "pending fire should be removed after ack"
        );
        assert!(
            schedules.is_empty(),
            "schedule definition should stay deleted"
        );
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_pending_fire_key(original_fire_ms, route),
            )
            .is_none(),
            "pending fire row should be deleted after ack"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_none(),
            "schedule definition should not be recreated"
        );
    }

    #[test]
    fn should_reject_malformed_persisted_schedule_definition_on_load() {
        // Arrange
        let (store, db) = make_store();
        put_raw(
            &db,
            1,
            ScheduleStore::encode_definition_key("schedule://acme/jobs/bad/run"),
            b"broken".to_vec(),
        )
        .expect("write malformed definition");

        // Act
        let result = store.load_all(1, WriteOptions::buffered());

        // Assert
        assert!(result
            .expect_err("malformed definition should fail recovery")
            .contains("decode persisted schedule definition failed"));
    }

    #[test]
    fn should_reject_missing_schedule_body_on_load() {
        // Arrange
        let (store, db) = make_store();
        put_raw(
            &db,
            1,
            ScheduleStore::encode_definition_key("schedule://acme/jobs/missing/run"),
            ScheduleStore::encode_definition_metadata_value(1_700_000_000_000, None, 0),
        )
        .expect("write metadata without body");

        // Act
        let result = store.load_all(1, WriteOptions::buffered());

        // Assert
        assert!(result
            .expect_err("missing body should fail recovery")
            .contains("missing schedule body row"));
    }

    #[test]
    fn should_reject_malformed_pending_fire_claim_on_load() {
        // Arrange
        let (store, db) = make_store();
        put_raw(
            &db,
            1,
            ScheduleStore::encode_pending_fire_key(
                1_700_000_000_000,
                "schedule://acme/jobs/bad/run",
            ),
            b"broken".to_vec(),
        )
        .expect("write malformed pending fire claim");

        // Act
        let result = store.load_pending_fire_claims(1);

        // Assert
        assert!(result
            .expect_err("malformed pending claim should fail recovery")
            .contains("decode pending schedule fire failed"));
    }
}
