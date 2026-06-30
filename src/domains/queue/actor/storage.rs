use super::{
    Bytes, DlqReason, Instant, MessageId, QueueActor, QueueMetaSnapshot, QueueRecord, QueueState,
    StoredRecordLayout,
};
#[cfg(test)]
use super::{FAIL_NEXT_ACK_COMMIT, FAIL_NEXT_REDELIVERY_COMMIT};
use crate::observability as obs;

impl QueueActor {
    pub(super) fn decode_legacy_record<B: Into<Bytes>>(bytes: B) -> Result<QueueRecord, String> {
        let bytes = bytes.into();

        if bytes.len() < 16 {
            return Err("Invalid record format".to_string());
        }

        let attempts = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let visible_at_ms = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let body_len = usize::try_from(u32::from_le_bytes(bytes[12..16].try_into().unwrap()))
            .unwrap_or(usize::MAX);

        if bytes.len().saturating_sub(16) < body_len {
            return Err("Truncated record body".to_string());
        }
        if bytes.len() != 16 + body_len {
            return Err("Invalid record trailing bytes".to_string());
        }

        Ok(QueueRecord::loaded_legacy(
            bytes.slice(16..16 + body_len),
            attempts,
            visible_at_ms,
        ))
    }

    pub(super) fn decode_record_header(bytes: &[u8]) -> Result<QueueRecord, String> {
        if bytes.len() == 12 {
            let attempts = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let visible_at_ms = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
            return Ok(QueueRecord::metadata_only(attempts, visible_at_ms));
        }

        if bytes.len() != 79 || bytes[0] != Self::HEADER_VERSION_V2 {
            return Err("Invalid record format".to_string());
        }

        let state = match bytes[1] {
            0 => QueueState::Ready,
            1 => QueueState::Delayed,
            2 => QueueState::Inflight,
            3 => QueueState::Dlq,
            other => return Err(format!("Unknown queue state {other}")),
        };
        let enqueue_seq = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let ready_seq = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let attempts = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
        let visible_at_ms = u64::from_le_bytes(bytes[22..30].try_into().unwrap());
        let first_enqueued_at_ms = u64::from_le_bytes(bytes[30..38].try_into().unwrap());
        let last_inflight_at_ms = u64::from_le_bytes(bytes[38..46].try_into().unwrap());
        let inflight_epoch = u64::from_le_bytes(bytes[46..54].try_into().unwrap());
        let inflight_token = u64::from_le_bytes(bytes[54..62].try_into().unwrap());
        let inflight_expires_at_ms = u64::from_le_bytes(bytes[62..70].try_into().unwrap());
        let dead_lettered_at_ms = u64::from_le_bytes(bytes[70..78].try_into().unwrap());
        let dlq_reason = match bytes[78] {
            0 => None,
            1 => Some(DlqReason::MaxAttemptsExceeded),
            other => return Err(format!("Unknown DLQ reason {other}")),
        };

        Ok(QueueRecord {
            body: None,
            state,
            enqueue_seq,
            ready_seq: (ready_seq != 0).then_some(ready_seq),
            attempts,
            visible_at_ms,
            first_enqueued_at_ms,
            last_inflight_at_ms: (last_inflight_at_ms != 0).then_some(last_inflight_at_ms),
            inflight_epoch,
            inflight_token: (inflight_token != 0).then_some(inflight_token),
            inflight_expires_at_ms: (inflight_expires_at_ms != 0).then_some(inflight_expires_at_ms),
            dead_lettered_at_ms: (dead_lettered_at_ms != 0).then_some(dead_lettered_at_ms),
            dlq_reason,
        })
    }

    pub(super) fn load_record_metadata_from_store(
        &self,
        id: MessageId,
    ) -> Result<(QueueRecord, StoredRecordLayout), String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let legacy_key = self.cached_legacy_message_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message {id}: {e:?}"))?;

        match txn.get(&header_key) {
            Ok(Some(bytes)) => {
                let layout = if Self::is_versioned_header(&bytes) || bytes.len() == 12 {
                    StoredRecordLayout::SplitHeaderBody
                } else {
                    StoredRecordLayout::EmbeddedHeader
                };
                match layout {
                    StoredRecordLayout::SplitHeaderBody => {
                        Self::decode_record_header(&bytes).map(|record| (record, layout))
                    }
                    StoredRecordLayout::EmbeddedHeader => Self::decode_legacy_record(bytes)
                        .map(|record| (record.metadata_only_from(), layout)),
                    StoredRecordLayout::LegacyKey => unreachable!(),
                }
            }
            Ok(None) => match txn.get(&legacy_key) {
                Ok(Some(bytes)) => {
                    let record = Self::decode_legacy_record(bytes)?;
                    Ok((record.metadata_only_from(), StoredRecordLayout::LegacyKey))
                }
                Ok(None) => Err(format!("Message {id} disappeared from storage")),
                Err(e) => Err(format!("Failed to read legacy message {id}: {e:?}")),
            },
            Err(e) => Err(format!("Failed to read message header {id}: {e:?}")),
        }
    }

    pub(super) fn load_body_from_store(&self, id: MessageId) -> Result<Bytes, String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message body {id}: {e:?}"))?;

        match txn.get(&body_key) {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => match txn.get(&header_key) {
                Ok(Some(bytes)) if !Self::is_versioned_header(&bytes) && bytes.len() >= 16 => {
                    let record = Self::decode_legacy_record(bytes)?;
                    record
                        .body
                        .ok_or_else(|| format!("Embedded message {id} body missing"))
                }
                Ok(Some(_) | None) => match txn.get(&legacy_key) {
                    Ok(Some(bytes)) => {
                        let record = Self::decode_legacy_record(bytes)?;
                        record
                            .body
                            .ok_or_else(|| format!("Legacy message {id} body missing"))
                    }
                    Ok(None) => Err(format!("Message body {id} disappeared from storage")),
                    Err(e) => Err(format!("Failed to read legacy message body {id}: {e:?}")),
                },
                Err(e) => Err(format!("Failed to read message header {id}: {e:?}")),
            },
            Err(e) => Err(format!("Failed to read message body {id}: {e:?}")),
        }
    }

    pub(super) fn load_record_for_receive_from_store(
        &self,
        id: MessageId,
    ) -> Result<(QueueRecord, StoredRecordLayout), String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message {id}: {e:?}"))?;

        match txn.get(&header_key) {
            Ok(Some(header_bytes)) => {
                if !Self::is_versioned_header(&header_bytes) && header_bytes.len() >= 16 {
                    if let Ok(record) = Self::decode_legacy_record(header_bytes.clone()) {
                        return Ok((record, StoredRecordLayout::EmbeddedHeader));
                    }
                }
                let header = Self::decode_record_header(&header_bytes)?;
                match txn.get(&body_key) {
                    Ok(Some(body_bytes)) => Ok((
                        QueueRecord {
                            body: Some(body_bytes),
                            ..header
                        },
                        StoredRecordLayout::SplitHeaderBody,
                    )),
                    Ok(None) => Err(format!("Message body {id} disappeared from storage")),
                    Err(e) => Err(format!("Failed to read message body {id}: {e:?}")),
                }
            }
            Err(e) => Err(format!("Failed to read message {id}: {e:?}")),
            Ok(None) => match txn.get(&legacy_key) {
                Ok(Some(bytes)) => Self::decode_legacy_record(bytes)
                    .map(|record| (record, StoredRecordLayout::LegacyKey)),
                Ok(None) => Err(format!("Message {id} disappeared from storage")),
                Err(e) => Err(format!("Failed to read legacy message {id}: {e:?}")),
            },
        }
    }

    pub(super) fn load_full_record_for_admin_mutation(
        &mut self,
        id: MessageId,
    ) -> Result<(QueueRecord, StoredRecordLayout), String> {
        let (record, layout) = if let Some(record) = self.records.get(&id).cloned() {
            (
                record,
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            )
        } else {
            self.load_record_metadata_from_store(id)?
        };

        let body = if let Some(body) = record.body.clone() {
            body
        } else if let Some(body) = self.body_cache.get(&id).cloned() {
            body
        } else {
            self.load_body_from_store(id)?
        };

        Ok((
            QueueRecord {
                body: Some(body),
                ..record
            },
            layout,
        ))
    }

    pub(super) fn observe_histogram_us(metric_name: &str, value_us: u64) {
        crate::observability::histogram_observe_us(metric_name, value_us);
    }

    pub(super) fn observe_elapsed_us(metric_name: &str, start: Instant) {
        let elapsed_us = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        Self::observe_histogram_us(metric_name, elapsed_us);
    }

    pub(super) fn increment_counter(metric_name: &str) {
        crate::observability::counter_inc(metric_name);
    }

    pub(super) fn is_missing_read_snapshot_error(error: &impl std::fmt::Debug) -> bool {
        format!("{error:?}").contains("read snapshot not available")
    }

    pub(super) fn hydrate_record_for_receive(
        &mut self,
        id: MessageId,
    ) -> Result<(Bytes, u32), String> {
        if let Some(record) = self.records.get(&id) {
            let attempts = record.attempts;
            if let Some(body) = self.body_cache.get(&id) {
                return Ok((body.clone(), attempts));
            }

            let start = Instant::now();
            let body = self.load_body_from_store(id)?;
            Self::observe_elapsed_us(obs::METRIC_QUEUE_RECEIVE_HYDRATE_LATENCY, start);
            return Ok((body, attempts));
        }

        let start = Instant::now();
        let (record, layout) = self.load_record_for_receive_from_store(id)?;
        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECEIVE_HYDRATE_LATENCY, start);
        let body = record
            .body
            .clone()
            .ok_or_else(|| format!("Message {id} body missing after hydration"))?;
        self.cache_record(id, record.metadata_only_from(), layout);
        Ok((body, record.attempts))
    }

    pub(super) fn is_versioned_header(bytes: &[u8]) -> bool {
        bytes.len() >= 79 && bytes.first().copied() == Some(Self::HEADER_VERSION_V2)
    }

    #[allow(dead_code)]
    pub(super) fn encode_ack_dedup_value(expires_at_ms: u64) -> Vec<u8> {
        expires_at_ms.to_le_bytes().to_vec()
    }

    #[allow(dead_code)]
    pub(super) fn decode_ack_dedup_value(bytes: &[u8]) -> Option<u64> {
        Some(u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?))
    }

    #[allow(dead_code)]
    pub(super) fn durable_meta_snapshot(&self) -> QueueMetaSnapshot {
        QueueMetaSnapshot {
            next_id: self.next_id,
            next_ready_seq: self.next_ready_seq,
            ready_count: Self::usize_to_u64(self.ready.len()),
            delayed_count: Self::usize_to_u64(self.persisted_delayed.len()),
            inflight_count: Self::usize_to_u64(self.inflight.len()),
            dlq_count: Self::usize_to_u64(self.persisted_dlq.len()),
            oldest_ready_enqueued_at_ms: self.oldest_ready_enqueued_at_ms,
        }
    }

    #[allow(dead_code)]
    pub(super) fn write_meta_snapshot(
        &self,
        txn: &mut cntryl_midge::Transaction,
        meta: QueueMetaSnapshot,
    ) -> Result<(), String> {
        txn.put(self.meta_key.clone(), Self::encode_meta(meta), None)
            .map_err(|e| format!("Failed to write queue meta: {e:?}"))
    }

    #[allow(dead_code)]
    pub(super) fn write_record_as_split(
        &self,
        txn: &mut cntryl_midge::Transaction,
        id: MessageId,
        record: &QueueRecord,
        prior_layout: Option<StoredRecordLayout>,
    ) -> Result<(), String> {
        if matches!(prior_layout, Some(StoredRecordLayout::LegacyKey)) {
            txn.delete(self.cached_legacy_message_key(id))
                .map_err(|e| format!("Failed to delete legacy queue record: {e:?}"))?;
        }

        txn.put(
            self.cached_header_key(id),
            Self::encode_record_header(record),
            None,
        )
        .map_err(|e| format!("Failed to write queue header: {e:?}"))?;

        let body = record
            .body
            .as_ref()
            .ok_or_else(|| format!("Queue record {id} missing body for write"))?;
        txn.put(self.cached_body_key(id), body.to_vec(), None)
            .map_err(|e| format!("Failed to write queue body: {e:?}"))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn read_durable_ack_dedup(
        &self,
        id: MessageId,
        token: u64,
        now_epoch_ms: u64,
    ) -> Option<bool> {
        let cf_id = self.queue_key.family.id();
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .ok()?;
        let bytes = txn.get(&self.ack_dedup_key(id, token)).ok()??;
        Some(
            Self::decode_ack_dedup_value(bytes.as_ref())
                .is_some_and(|expires_at_ms| expires_at_ms > now_epoch_ms),
        )
    }

    #[allow(dead_code)]
    pub(super) fn cache_record_state(
        &mut self,
        id: MessageId,
        record: &QueueRecord,
        layout: StoredRecordLayout,
    ) {
        self.cache_record(id, record.metadata_only_from(), layout);
        if let Some(body) = record.body.as_ref() {
            self.cache_body(id, body.clone());
        }
    }

    pub(super) fn update_cached_inflight_metadata(
        &mut self,
        id: MessageId,
        inflight_epoch: u64,
        inflight_token: Option<u64>,
        inflight_expires_at_ms: Option<u64>,
        last_inflight_at_ms: Option<u64>,
    ) {
        if let Some(record) = self.records.get_mut(&id) {
            record.inflight_epoch = inflight_epoch;
            record.inflight_token = inflight_token;
            record.inflight_expires_at_ms = inflight_expires_at_ms;
            record.last_inflight_at_ms = last_inflight_at_ms;
        }
    }

    pub(super) fn commit_ack_transaction(
        txn: cntryl_midge::Transaction,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_ACK_COMMIT.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected queue ack commit failure".to_string());
            }
        }

        txn.commit(write_options).map_err(|e| format!("{e:?}"))
    }

    pub(super) fn commit_redelivery_transaction(
        txn: cntryl_midge::Transaction,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_REDELIVERY_COMMIT.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected queue redelivery commit failure".to_string());
            }
        }

        txn.commit(write_options).map_err(|e| format!("{e:?}"))
    }

    #[cfg(test)]
    pub(super) fn fail_next_ack_commit_for_tests() {
        FAIL_NEXT_ACK_COMMIT.with(|cell| cell.set(true));
    }

    #[cfg(test)]
    pub(super) fn fail_next_redelivery_commit_for_tests() {
        FAIL_NEXT_REDELIVERY_COMMIT.with(|cell| cell.set(true));
    }
}
