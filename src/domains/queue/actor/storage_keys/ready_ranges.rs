use super::super::{MessageId, QueueActor, ReadyRange};

impl QueueActor {
    pub(in crate::domains::queue::actor) fn ready_range_key_with_prefix(
        prefix: &[u8],
        shard: usize,
        start: u64,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 9);
        key.extend_from_slice(prefix);
        key.push(u8::try_from(shard).unwrap_or(u8::MAX));
        key.extend_from_slice(&start.to_be_bytes());
        key
    }

    pub(in crate::domains::queue::actor) fn ready_range_key(
        &self,
        shard: usize,
        start: u64,
    ) -> Vec<u8> {
        Self::ready_range_key_with_prefix(&self.ready_index_prefix, shard, start)
    }

    pub(in crate::domains::queue::actor) fn parse_ready_range_key(
        key: &[u8],
        prefix: &[u8],
    ) -> Option<(usize, u64)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != 9 {
            return None;
        }

        let shard = rest[0] as usize;
        if shard >= Self::READY_SHARDS {
            return None;
        }

        let start = u64::from_be_bytes(rest[1..9].try_into().ok()?);
        Some((shard, start))
    }

    pub(in crate::domains::queue::actor) fn encode_ready_range_value(range: ReadyRange) -> Vec<u8> {
        range.end.to_le_bytes().to_vec()
    }

    pub(in crate::domains::queue::actor) fn decode_ready_range(
        start: u64,
        value: &[u8],
    ) -> Option<ReadyRange> {
        let end = u64::from_le_bytes(value.get(0..8)?.try_into().ok()?);
        let step = Self::ready_shards_u64();
        if end < start || !(end - start).is_multiple_of(step) {
            return None;
        }
        Some(ReadyRange { next: start, end })
    }

    pub(in crate::domains::queue::actor) fn delayed_index_key_with_prefix(
        prefix: &[u8],
        visible_at_ms: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 16);
        key.extend_from_slice(prefix);
        key.extend_from_slice(&visible_at_ms.to_be_bytes());
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key
    }

    pub(in crate::domains::queue::actor) fn delayed_index_key(
        &self,
        visible_at_ms: u64,
        id: MessageId,
    ) -> Vec<u8> {
        Self::delayed_index_key_with_prefix(&self.delayed_index_prefix, visible_at_ms, id)
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn delayed_entry_index_key(
        &self,
        visible_at_ms: u64,
        enqueue_seq: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.delayed_index_prefix.len() + 24);
        key.extend_from_slice(&self.delayed_index_prefix);
        key.extend_from_slice(&visible_at_ms.to_be_bytes());
        key.extend_from_slice(&enqueue_seq.to_be_bytes());
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn parse_delayed_entry_index_key(
        key: &[u8],
        prefix: &[u8],
    ) -> Option<(u64, u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != 24 {
            return None;
        }
        let visible_at_ms = u64::from_be_bytes(rest[0..8].try_into().ok()?);
        let enqueue_seq = u64::from_be_bytes(rest[8..16].try_into().ok()?);
        let id = u64::from_be_bytes(rest[16..24].try_into().ok()?);
        Some((visible_at_ms, enqueue_seq, MessageId::new(id)))
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn inflight_index_key(
        &self,
        expires_at_ms: u64,
        inflight_epoch: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.inflight_index_prefix.len() + 24);
        key.extend_from_slice(&self.inflight_index_prefix);
        key.extend_from_slice(&expires_at_ms.to_be_bytes());
        key.extend_from_slice(&inflight_epoch.to_be_bytes());
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn dlq_index_key(
        &self,
        dead_lettered_at_ms: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.dlq_index_prefix.len() + 16);
        key.extend_from_slice(&self.dlq_index_prefix);
        key.extend_from_slice(&dead_lettered_at_ms.to_be_bytes());
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn ack_dedup_key(
        &self,
        id: MessageId,
        token: u64,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.ack_dedup_prefix.len() + 16);
        key.extend_from_slice(&self.ack_dedup_prefix);
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key.extend_from_slice(&token.to_be_bytes());
        key
    }

    pub(in crate::domains::queue::actor) fn parse_delayed_index_key(
        key: &[u8],
        prefix: &[u8],
    ) -> Option<(u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != 16 {
            return None;
        }

        let visible_at_ms = u64::from_be_bytes(rest[0..8].try_into().ok()?);
        let id = u64::from_be_bytes(rest[8..16].try_into().ok()?);
        Some((visible_at_ms, MessageId::new(id)))
    }

    pub(in crate::domains::queue::actor) fn parse_dlq_index_key(
        key: &[u8],
        prefix: &[u8],
    ) -> Option<(u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != 16 {
            return None;
        }

        let dead_lettered_at_ms = u64::from_be_bytes(rest[0..8].try_into().ok()?);
        let id = u64::from_be_bytes(rest[8..16].try_into().ok()?);
        Some((dead_lettered_at_ms, MessageId::new(id)))
    }

    #[inline]
    pub(in crate::domains::queue::actor) fn parse_message_id_from_key(
        key: &[u8],
        prefix: &[u8],
    ) -> Option<MessageId> {
        if !key.starts_with(prefix) || key.len() != prefix.len() + 8 {
            return None;
        }

        Some(MessageId::new(u64::from_be_bytes(
            key[prefix.len()..].try_into().ok()?,
        )))
    }
}
