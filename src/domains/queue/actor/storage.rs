use super::{
    Bytes, DlqReason, Instant, MessageId, QueueActor, QueueCommit, QueueRecord, QueueState,
};
#[cfg(test)]
use super::{FAIL_NEXT_ACK_COMMIT, FAIL_NEXT_REDELIVERY_COMMIT};
use crate::observability as obs;

impl QueueActor {
    pub(super) fn decode_record_header(bytes: &[u8]) -> Result<QueueRecord, String> {
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
        let dlq_reason = DlqReason::from_wire_code(bytes[78])?;

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
    ) -> Result<QueueRecord, String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message {id}: {e:?}"))?;

        match txn.get(&header_key) {
            Ok(Some(bytes)) => Self::decode_record_header(&bytes),
            Ok(None) => Err(format!("Message {id} disappeared from storage")),
            Err(e) => Err(format!("Failed to read message header {id}: {e:?}")),
        }
    }

    pub(super) fn load_body_from_store(&self, id: MessageId) -> Result<Bytes, String> {
        let cf_id = self.queue_key.family.id();
        let body_key = self.cached_body_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message body {id}: {e:?}"))?;

        match txn.get(&body_key) {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => Err(format!("Message body {id} disappeared from storage")),
            Err(e) => Err(format!("Failed to read message body {id}: {e:?}")),
        }
    }

    pub(super) fn load_record_for_receive_from_store(
        &self,
        id: MessageId,
    ) -> Result<QueueRecord, String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message {id}: {e:?}"))?;

        match txn.get(&header_key) {
            Ok(Some(header_bytes)) => {
                let header = Self::decode_record_header(&header_bytes)?;
                match txn.get(&body_key) {
                    Ok(Some(body_bytes)) => Ok(QueueRecord {
                        body: Some(body_bytes),
                        ..header
                    }),
                    Ok(None) => Err(format!("Message body {id} disappeared from storage")),
                    Err(e) => Err(format!("Failed to read message body {id}: {e:?}")),
                }
            }
            Ok(None) => Err(format!("Message {id} disappeared from storage")),
            Err(e) => Err(format!("Failed to read message {id}: {e:?}")),
        }
    }

    pub(super) fn load_full_record_for_admin_mutation(
        &mut self,
        id: MessageId,
    ) -> Result<QueueRecord, String> {
        let record = if let Some(record) = self.records.get(&id).cloned() {
            record
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

        Ok(QueueRecord {
            body: Some(body),
            ..record
        })
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
        let record = self.load_record_for_receive_from_store(id)?;
        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECEIVE_HYDRATE_LATENCY, start);
        let body = record
            .body
            .clone()
            .ok_or_else(|| format!("Message {id} body missing after hydration"))?;
        self.cache_record(id, record.metadata_only_from());
        Ok((body, record.attempts))
    }

    pub(super) fn write_record_as_split(
        &self,
        txn: &mut cntryl_midge::Transaction,
        id: MessageId,
        record: &QueueRecord,
    ) -> Result<(), String> {
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

    pub(super) fn cache_record_state(&mut self, id: MessageId, record: &QueueRecord) {
        self.cache_record(id, record.metadata_only_from());
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

    pub(super) fn commit_transaction(
        txn: cntryl_midge::Transaction,
        write_options: cntryl_midge::WriteOptions,
        commit: QueueCommit,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let failpoint = match commit {
                QueueCommit::Ack => &FAIL_NEXT_ACK_COMMIT,
                QueueCommit::Redelivery => &FAIL_NEXT_REDELIVERY_COMMIT,
            };
            let should_fail = failpoint.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                let operation = match commit {
                    QueueCommit::Ack => "ack",
                    QueueCommit::Redelivery => "redelivery",
                };
                return Err(format!("Injected queue {operation} commit failure"));
            }
        }

        #[cfg(not(test))]
        let _ = commit;

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
