use super::super::*;

impl QueueActor {
    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn encode_meta(meta: QueueMetaSnapshot) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + (8 * 7));
        out.push(Self::META_VERSION_V2);
        out.extend_from_slice(&meta.next_id.to_le_bytes());
        out.extend_from_slice(&meta.next_ready_seq.to_le_bytes());
        out.extend_from_slice(&meta.ready_count.to_le_bytes());
        out.extend_from_slice(&meta.delayed_count.to_le_bytes());
        out.extend_from_slice(&meta.inflight_count.to_le_bytes());
        out.extend_from_slice(&meta.dlq_count.to_le_bytes());
        out.extend_from_slice(&meta.oldest_ready_enqueued_at_ms.unwrap_or(0).to_le_bytes());
        out
    }

    pub(in crate::domains::queue::actor) fn decode_meta(bytes: &[u8]) -> Option<QueueMetaSnapshot> {
        if bytes.len() == 8 {
            let next_id = u64::from_le_bytes(bytes.try_into().ok()?);
            return Some(QueueMetaSnapshot {
                next_id,
                next_ready_seq: next_id,
                ready_count: 0,
                delayed_count: 0,
                inflight_count: 0,
                dlq_count: 0,
                oldest_ready_enqueued_at_ms: None,
            });
        }

        if bytes.first().copied()? != Self::META_VERSION_V2 || bytes.len() != 57 {
            return None;
        }

        let next_id = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
        let next_ready_seq = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let ready_count = u64::from_le_bytes(bytes[17..25].try_into().ok()?);
        let delayed_count = u64::from_le_bytes(bytes[25..33].try_into().ok()?);
        let inflight_count = u64::from_le_bytes(bytes[33..41].try_into().ok()?);
        let dlq_count = u64::from_le_bytes(bytes[41..49].try_into().ok()?);
        let oldest_ready = u64::from_le_bytes(bytes[49..57].try_into().ok()?);
        Some(QueueMetaSnapshot {
            next_id,
            next_ready_seq,
            ready_count,
            delayed_count,
            inflight_count,
            dlq_count,
            oldest_ready_enqueued_at_ms: (oldest_ready != 0).then_some(oldest_ready),
        })
    }

    pub(in crate::domains::queue::actor) fn encode_index_meta(
        next_id: u64,
        ready_count: u64,
        delayed_count: u64,
        next_delayed_visibility_ms: Option<u64>,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(34);
        out.push(Self::INDEX_VERSION_V2);
        out.push(Self::INDEX_META_VALID_MARKER);
        out.extend_from_slice(&next_id.to_le_bytes());
        out.extend_from_slice(&ready_count.to_le_bytes());
        out.extend_from_slice(&delayed_count.to_le_bytes());
        out.extend_from_slice(
            &next_delayed_visibility_ms
                .unwrap_or(Self::INDEX_META_NEXT_DELAY_NONE)
                .to_le_bytes(),
        );
        out
    }

    pub(in crate::domains::queue::actor) fn index_meta_is_valid(bytes: &[u8]) -> bool {
        Self::decode_index_meta(bytes).is_ok()
    }

    pub(in crate::domains::queue::actor) fn decode_index_meta(
        bytes: &[u8],
    ) -> Result<DecodedIndexMeta, String> {
        if bytes.len() < 2 {
            return Err("Queue index meta too short".to_string());
        }
        if bytes[1] != Self::INDEX_META_VALID_MARKER {
            return Err("Queue index meta missing validity marker".to_string());
        }

        match bytes[0] {
            Self::INDEX_VERSION_V1 => Ok(DecodedIndexMeta::LegacyV1),
            Self::INDEX_VERSION_V2 => {
                if bytes.len() < 34 {
                    return Err("Queue index meta v2 payload too short".to_string());
                }

                let next_id = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
                let ready_count = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
                let delayed_count = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
                let raw_next_delayed = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
                let next_delayed_visibility_ms =
                    if raw_next_delayed == Self::INDEX_META_NEXT_DELAY_NONE {
                        None
                    } else {
                        Some(raw_next_delayed)
                    };

                if next_id == 0 {
                    return Err("Queue index meta v2 has invalid next_id=0".to_string());
                }
                if delayed_count == 0 && next_delayed_visibility_ms.is_some() {
                    return Err(
                        "Queue index meta v2 delayed_count=0 with non-empty next delayed"
                            .to_string(),
                    );
                }
                if delayed_count > 0 && next_delayed_visibility_ms.is_none() {
                    return Err(
                        "Queue index meta v2 delayed_count>0 without next delayed".to_string()
                    );
                }

                Ok(DecodedIndexMeta::V2(IndexMetaSnapshot {
                    next_id,
                    ready_count,
                    delayed_count,
                    next_delayed_visibility_ms,
                }))
            }
            other => Err(format!("Unsupported queue index meta version {}", other)),
        }
    }

    pub(in crate::domains::queue::actor) fn decode_next_id(bytes: Option<&[u8]>) -> u64 {
        bytes
            .and_then(Self::decode_meta)
            .map(|meta| meta.next_id)
            .unwrap_or(1)
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn load_meta_from_store(
        &self,
    ) -> Option<QueueMetaSnapshot> {
        let cf_id = self.queue_key.family.id();
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .ok()?;
        let bytes = txn.get(&self.meta_key).ok()??;
        Self::decode_meta(bytes.as_ref())
    }

    pub(in crate::domains::queue::actor) fn load_next_id_from_meta_key(&self) -> u64 {
        let cf_id = self.queue_key.family.id();
        let txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        {
            Ok(txn) => txn,
            Err(e) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    error = ?e,
                    "Failed to begin queue meta recovery transaction; starting from 1"
                );
                return 1;
            }
        };

        match txn.get(&self.meta_key) {
            Ok(Some(bytes)) => Self::decode_next_id(Some(bytes.as_ref())),
            Ok(None) => 1,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => 1,
            Err(e) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    error = ?e,
                    "Failed to recover queue next_id; starting from 1"
                );
                1
            }
        }
    }
}
