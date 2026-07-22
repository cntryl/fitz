use super::model::{
    parse_concrete_schedule_route, storage_key, Arc, Bytes, ConcreteScheduleRoute,
    DecodedDefinitionRow, DomainKeyspace, Encoder, LexKey, ScheduleDefinitionData,
    ScheduleDeliveryMode, ScheduleRows, ScheduleStore, BODY_PREFIX, BODY_VALUE_VERSION_V1,
    BODY_VALUE_VERSION_V2, DEFINITION_PREFIX, DEFINITION_VALUE_VERSION_V1,
    DEFINITION_VALUE_VERSION_V2, DEFINITION_VALUE_VERSION_V3, DUE_PREFIX, PENDING_FIRE_PREFIX,
    PENDING_FIRE_VALUE_VERSION_V1, PENDING_FIRE_VALUE_VERSION_V2, PENDING_FIRE_VALUE_VERSION_V3,
};

impl ScheduleStore {
    fn usize_to_u32_saturating(value: usize) -> u32 {
        u32::try_from(value).unwrap_or(u32::MAX)
    }

    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self::new_with_storage(crate::storage::FitzStorageEngine::new(db))
    }

    pub(crate) fn new_with_storage(db: crate::storage::FitzStorageEngine) -> Self {
        Self {
            db,
            #[cfg(test)]
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(super) fn schedule_key_suffix(key: &[u8]) -> &[u8] {
        storage_key::strip_domain_prefix(key, DomainKeyspace::Schedule).unwrap_or(key)
    }

    pub(super) fn encode_route_identity_into(
        encoder: &mut Encoder,
        area: &str,
        resource: &str,
        operation: &str,
    ) {
        storage_key::encode_segment_into(encoder, area);
        storage_key::encode_segment_into(encoder, resource);
        encoder.encode_string_into(operation);
    }

    pub(super) fn decode_route_identity_from_suffix(
        realm: &str,
        suffix: &[u8],
    ) -> Result<String, String> {
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
            .map_err(|e| format!("Invalid schedule area encoding: {e}"))?;
        let resource = std::str::from_utf8(resource)
            .map_err(|e| format!("Invalid schedule resource encoding: {e}"))?;
        let operation = std::str::from_utf8(operation)
            .map_err(|e| format!("Invalid schedule operation encoding: {e}"))?;

        Ok(format!("schedule://{realm}/{area}/{resource}/{operation}"))
    }

    pub(super) fn encode_prefixed_route_key_from_realm(
        realm: &str,
        route: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        Self::encode_prefixed_route_key_from_parts(
            realm,
            &parsed.area,
            &parsed.resource,
            &parsed.operation,
            suffix_prefix,
        )
    }

    pub(super) fn encode_prefixed_route_key_from_parts(
        realm: &str,
        area: &str,
        resource: &str,
        operation: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let mut key = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Schedule,
            suffix_prefix[0],
            area.len() + resource.len() + operation.len() + 2,
        );
        Self::encode_route_identity_into(&mut key, area, resource, operation);
        key.into_vec()
    }

    pub(super) fn encode_prefixed_route_key(route: &str, suffix_prefix: &[u8]) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        Self::encode_prefixed_route_key_from_realm(&parsed.realm, route, suffix_prefix)
    }

    pub(super) fn encode_prefixed_timed_route_key_from_realm(
        realm: &str,
        timestamp_ms: u64,
        route: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let parsed = parse_concrete_schedule_route(route)
            .expect("schedule storage key requires a concrete route");
        Self::encode_prefixed_timed_route_key_from_parts(
            realm,
            timestamp_ms,
            &parsed.area,
            &parsed.resource,
            &parsed.operation,
            suffix_prefix,
        )
    }

    pub(super) fn encode_prefixed_timed_route_key_from_parts(
        realm: &str,
        timestamp_ms: u64,
        area: &str,
        resource: &str,
        operation: &str,
        suffix_prefix: &[u8],
    ) -> Vec<u8> {
        let mut key = storage_key::domain_marker_encoder(
            realm,
            DomainKeyspace::Schedule,
            suffix_prefix[0],
            9 + area.len() + resource.len() + operation.len() + 2,
        );
        key.encode_bytes_into(&timestamp_ms.to_be_bytes());
        key.push_separator();
        Self::encode_route_identity_into(&mut key, area, resource, operation);
        key.into_vec()
    }

    pub(super) fn encode_prefixed_timed_route_key(
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

    pub(super) fn scan_schedule_rows(
        tx: &cntryl_midge::Transaction,
        suffix_prefix: &[u8],
    ) -> Result<ScheduleRows, String> {
        let rows = tx
            .scan(&cntryl_midge::Query::new())
            .map_err(|e| format!("scan schedule rows failed: {e:?}"))?
            .try_collect()
            .map_err(|e| format!("scan schedule rows failed: {e:?}"))?;

        Ok(rows
            .into_iter()
            .filter(|(key, _)| Self::schedule_key_suffix(key).starts_with(suffix_prefix))
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
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

    pub(super) fn decode_definition_key(key: &[u8]) -> Result<String, String> {
        let Some((realm, suffix)) = storage_key::split_domain_key(key, DomainKeyspace::Schedule)
        else {
            return Err("Invalid schedule definition key prefix".to_string());
        };
        if !suffix.starts_with(DEFINITION_PREFIX) {
            return Err("Invalid schedule definition key prefix".to_string());
        }

        Self::decode_route_identity_from_suffix(realm, &suffix[DEFINITION_PREFIX.len()..])
    }

    pub(super) fn decode_body_key(key: &[u8]) -> Result<String, String> {
        let Some((realm, suffix)) = storage_key::split_domain_key(key, DomainKeyspace::Schedule)
        else {
            return Err("Invalid schedule body key prefix".to_string());
        };
        if !suffix.starts_with(BODY_PREFIX) {
            return Err("Invalid schedule body key prefix".to_string());
        }

        Self::decode_route_identity_from_suffix(realm, &suffix[BODY_PREFIX.len()..])
    }

    pub(super) fn decode_due_key_with_prefix(
        key: &[u8],
        prefix: &[u8],
    ) -> Result<(u64, String), String> {
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

    #[allow(dead_code)]
    pub(crate) fn decode_due_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, DUE_PREFIX)
    }

    pub(super) fn decode_pending_fire_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, PENDING_FIRE_PREFIX)
    }

    pub(super) fn encode_definition_metadata_value(
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

    pub(super) fn encode_definition_body_value(
        cron: &str,
        delivery_mode: ScheduleDeliveryMode,
        payload: &Bytes,
    ) -> Vec<u8> {
        let mut value = Vec::with_capacity(2 + 4 + cron.len() + 4 + payload.len());
        value.push(BODY_VALUE_VERSION_V2);
        value.push(delivery_mode as u8);
        value.extend_from_slice(&Self::usize_to_u32_saturating(cron.len()).to_be_bytes());
        value.extend_from_slice(cron.as_bytes());
        value.extend_from_slice(&Self::usize_to_u32_saturating(payload.len()).to_be_bytes());
        value.extend_from_slice(payload);
        value
    }

    pub(super) fn decode_definition_value(value: &[u8]) -> Result<DecodedDefinitionRow, String> {
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
                    .map_err(|e| format!("Invalid cron encoding: {e}"))?;
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
                    .map_err(|e| format!("Invalid cron encoding: {e}"))?;
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
                "Unsupported schedule definition value version: {other}"
            )),
        }
    }

    pub(super) fn decode_definition_body_value(
        value: &[u8],
    ) -> Result<(String, ScheduleDeliveryMode, Bytes), String> {
        if value.is_empty() {
            return Err("Schedule definition body value too short".to_string());
        }

        match value[0] {
            BODY_VALUE_VERSION_V1 | BODY_VALUE_VERSION_V2 => {
                if value.len() < 9 {
                    return Err("Schedule definition body value too short".to_string());
                }

                let (delivery_mode, length_start) = if value[0] == BODY_VALUE_VERSION_V1 {
                    (ScheduleDeliveryMode::Broadcast, 1)
                } else {
                    if value.len() < 10 {
                        return Err("Schedule definition body value too short".to_string());
                    }
                    (ScheduleDeliveryMode::try_from(value[1])?, 2)
                };
                let cron_len =
                    u32::from_be_bytes(value[length_start..length_start + 4].try_into().unwrap())
                        as usize;
                let cron_start = length_start + 4;
                let cron_end = cron_start + cron_len;
                if value.len() < cron_end + 4 {
                    return Err("Schedule definition body value truncated before cron".to_string());
                }

                let cron = String::from_utf8(value[cron_start..cron_end].to_vec())
                    .map_err(|e| format!("Invalid cron encoding: {e}"))?;
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
                    delivery_mode,
                    Bytes::copy_from_slice(&value[payload_start..payload_end]),
                ))
            }
            other => Err(format!(
                "Unsupported schedule definition body value version: {other}"
            )),
        }
    }

    pub(super) fn encode_pending_fire_value(
        payload: &Bytes,
        claimed_at_ms: u64,
        delivery_mode: ScheduleDeliveryMode,
    ) -> Vec<u8> {
        let mut value = Vec::with_capacity(2 + std::mem::size_of::<u64>() + payload.len());
        value.push(PENDING_FIRE_VALUE_VERSION_V3);
        value.extend_from_slice(&claimed_at_ms.to_le_bytes());
        value.push(delivery_mode as u8);
        value.extend_from_slice(payload);
        value
    }

    pub(super) fn decode_pending_fire_value(
        value: &[u8],
    ) -> Result<(u64, ScheduleDeliveryMode, Bytes), String> {
        if value.is_empty() {
            return Err("Schedule pending fire value too short".to_string());
        }

        match value[0] {
            PENDING_FIRE_VALUE_VERSION_V1 => Ok((
                0,
                ScheduleDeliveryMode::Broadcast,
                Bytes::copy_from_slice(&value[1..]),
            )),
            PENDING_FIRE_VALUE_VERSION_V2 => {
                if value.len() < 1 + std::mem::size_of::<u64>() {
                    return Err(
                        "Schedule pending fire value missing claimed-at timestamp".to_string()
                    );
                }

                let claimed_at_ms = u64::from_le_bytes(value[1..9].try_into().unwrap());
                Ok((
                    claimed_at_ms,
                    ScheduleDeliveryMode::Broadcast,
                    Bytes::copy_from_slice(&value[9..]),
                ))
            }
            PENDING_FIRE_VALUE_VERSION_V3 => {
                if value.len() < 10 {
                    return Err("Schedule pending fire value missing delivery mode".to_string());
                }
                let claimed_at_ms = u64::from_le_bytes(value[1..9].try_into().unwrap());
                let delivery_mode = ScheduleDeliveryMode::try_from(value[9])?;
                Ok((
                    claimed_at_ms,
                    delivery_mode,
                    Bytes::copy_from_slice(&value[10..]),
                ))
            }
            other => Err(format!(
                "Unsupported schedule pending fire value version: {other}"
            )),
        }
    }

    pub(super) fn put_definition_metadata(
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
        .map_err(|e| format!("put schedule definition failed: {e:?}"))
    }

    pub(super) fn put_definition_metadata_from_parts(
        txn: &mut cntryl_midge::Transaction,
        route_parts: &ConcreteScheduleRoute,
        next_fire_ms: u64,
        last_fire_ms: Option<u64>,
        executions_total: u64,
    ) -> Result<(), String> {
        txn.put(
            Self::encode_prefixed_route_key_from_parts(
                &route_parts.realm,
                &route_parts.area,
                &route_parts.resource,
                &route_parts.operation,
                DEFINITION_PREFIX,
            ),
            Self::encode_definition_metadata_value(next_fire_ms, last_fire_ms, executions_total),
            None,
        )
        .map_err(|e| format!("put schedule definition failed: {e:?}"))
    }

    pub(super) fn put_definition_body(
        txn: &mut cntryl_midge::Transaction,
        route: &str,
        cron: &str,
        delivery_mode: ScheduleDeliveryMode,
        payload: &Bytes,
    ) -> Result<(), String> {
        txn.put(
            Self::encode_body_key(route),
            Self::encode_definition_body_value(cron, delivery_mode, payload),
            None,
        )
        .map_err(|e| format!("put schedule body failed: {e:?}"))
    }

    pub(super) fn put_schedule_definition(
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
        Self::put_definition_body(
            txn,
            route,
            definition.cron,
            definition.delivery_mode,
            definition.payload,
        )
    }
}
