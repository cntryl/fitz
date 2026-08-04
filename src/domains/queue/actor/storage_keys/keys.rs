use super::super::{
    storage_key, DomainKeyspace, LexKey, MessageId, QueueActor, QueueKey,
    QUEUE_KEY_FAMILY_ACK_DEDUP, QUEUE_KEY_FAMILY_BODY, QUEUE_KEY_FAMILY_DELAYED_INDEX,
    QUEUE_KEY_FAMILY_DLQ_INDEX, QUEUE_KEY_FAMILY_HEADER, QUEUE_KEY_FAMILY_INDEX_META,
    QUEUE_KEY_FAMILY_INFLIGHT_INDEX, QUEUE_KEY_FAMILY_LEGACY_MESSAGE, QUEUE_KEY_FAMILY_META,
    QUEUE_KEY_FAMILY_READY_INDEX,
};

impl QueueActor {
    pub(crate) fn queue_key_from_authoritative_storage_key(
        family: crate::runtime::routing::RouteFamily,
        key: &[u8],
    ) -> Option<QueueKey> {
        let (_, suffix) = storage_key::split_domain_key(key, DomainKeyspace::Queue)?;
        let (_, family_marker, message_id) = Self::split_authoritative_key(suffix)?;
        if !matches!(
            family_marker,
            QUEUE_KEY_FAMILY_HEADER | QUEUE_KEY_FAMILY_LEGACY_MESSAGE
        ) || message_id.len() != 8
            || u64::from_be_bytes(message_id.try_into().ok()?) == 0
        {
            return None;
        }

        QueueKey::from_storage_key(family, key)
    }

    pub(in crate::domains::queue::actor) fn split_authoritative_key(
        suffix: &[u8],
    ) -> Option<(&[u8], u8, &[u8])> {
        let area_end = suffix.iter().position(|byte| *byte == LexKey::SEPARATOR)?;
        let resource_start = area_end + 1;
        let resource_len = suffix[resource_start..]
            .iter()
            .position(|byte| *byte == LexKey::SEPARATOR)?;
        let resource_end = resource_start + resource_len;
        let family_index = resource_end + 1;
        let family_marker = *suffix.get(family_index)?;

        Some((
            &suffix[..=resource_end],
            family_marker,
            &suffix[(family_index + 1)..],
        ))
    }

    pub(in crate::domains::queue::actor) fn validate_authoritative_message_id(
        family: u32,
        category: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        if bytes.len() != 8 {
            return Err(format!(
                "queue validation failed: family={family} key_category={category} error=invalid message id"
            ));
        }
        let id = u64::from_be_bytes(bytes.try_into().unwrap());
        if id == 0 {
            return Err(format!(
                "queue validation failed: family={family} key_category={category} error=invalid message id"
            ));
        }
        Ok(())
    }

    #[inline]
    pub(in crate::domains::queue::actor) fn reserved_id_limit_for(
        &self,
        additional_ids: u64,
    ) -> Option<u64> {
        let required_limit = self.next_id.saturating_add(additional_ids);
        if required_limit <= self.next_id_limit {
            return None;
        }

        let deficit = required_limit.saturating_sub(self.next_id_limit);
        let blocks = deficit.div_ceil(Self::ID_RESERVATION_BLOCK);
        Some(
            self.next_id_limit
                .saturating_add(blocks.saturating_mul(Self::ID_RESERVATION_BLOCK)),
        )
    }

    pub(in crate::domains::queue::actor) fn has_message_id_capacity(
        &self,
        additional_ids: u64,
    ) -> bool {
        self.next_id.checked_add(additional_ids).is_some()
    }

    /// Generate a random inflight token
    pub(in crate::domains::queue::actor) fn generate_token() -> u64 {
        let uuid = uuid::Uuid::new_v4();
        let mut token = [0_u8; 8];
        token.copy_from_slice(&uuid.as_bytes()[..8]);
        u64::from_be_bytes(token)
    }

    pub(in crate::domains::queue::actor) fn queue_scope_suffix(
        queue_key: &QueueKey,
        family_marker: u8,
    ) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(queue_key.area.len() + queue_key.resource.len() + 3);
        storage_key::push_segment(&mut suffix, &queue_key.area);
        storage_key::push_segment(&mut suffix, &queue_key.resource);
        suffix.push(family_marker);
        suffix
    }

    pub(in crate::domains::queue::actor) fn prefixed_queue_key(
        queue_key: &QueueKey,
        family_marker: u8,
    ) -> Vec<u8> {
        let suffix = Self::queue_scope_suffix(queue_key, family_marker);
        storage_key::prefixed_key(&queue_key.realm, DomainKeyspace::Queue, &suffix)
    }

    /// Midge key for queue metadata
    pub(in crate::domains::queue::actor) fn meta_key(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_META)
    }

    pub(in crate::domains::queue::actor) fn index_meta_key(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_INDEX_META)
    }

    /// Midge key for legacy combined message record
    #[cfg(test)]
    pub(in crate::domains::queue::actor) fn legacy_message_key(
        queue_key: &QueueKey,
        id: MessageId,
    ) -> Vec<u8> {
        Self::cached_id_key(&Self::legacy_message_key_prefix(queue_key), id)
    }

    /// Midge key for persisted message header
    #[cfg(test)]
    pub(in crate::domains::queue::actor) fn header_key(
        queue_key: &QueueKey,
        id: MessageId,
    ) -> Vec<u8> {
        Self::cached_id_key(&Self::header_key_prefix(queue_key), id)
    }

    /// Midge key for persisted message body
    #[cfg(test)]
    pub(in crate::domains::queue::actor) fn body_key(
        queue_key: &QueueKey,
        id: MessageId,
    ) -> Vec<u8> {
        Self::cached_id_key(&Self::body_key_prefix(queue_key), id)
    }

    pub(in crate::domains::queue::actor) fn header_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_HEADER)
    }

    pub(in crate::domains::queue::actor) fn body_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_BODY)
    }

    pub(in crate::domains::queue::actor) fn ready_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_READY_INDEX)
    }

    pub(in crate::domains::queue::actor) fn delayed_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_DELAYED_INDEX)
    }

    pub(in crate::domains::queue::actor) fn inflight_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_INFLIGHT_INDEX)
    }

    pub(in crate::domains::queue::actor) fn dlq_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_DLQ_INDEX)
    }

    pub(in crate::domains::queue::actor) fn ack_dedup_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_ACK_DEDUP)
    }

    pub(in crate::domains::queue::actor) fn legacy_message_key_prefix(
        queue_key: &QueueKey,
    ) -> Vec<u8> {
        Self::prefixed_queue_key(queue_key, QUEUE_KEY_FAMILY_LEGACY_MESSAGE)
    }

    pub(in crate::domains::queue::actor) fn cached_id_key(prefix: &[u8], id: MessageId) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 8);
        key.extend_from_slice(prefix);
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key
    }

    #[inline]
    pub(in crate::domains::queue::actor) fn cached_header_key(&self, id: MessageId) -> Vec<u8> {
        Self::cached_id_key(&self.header_key_prefix, id)
    }

    #[inline]
    pub(in crate::domains::queue::actor) fn cached_body_key(&self, id: MessageId) -> Vec<u8> {
        Self::cached_id_key(&self.body_key_prefix, id)
    }

    #[inline]
    pub(in crate::domains::queue::actor) fn cached_legacy_message_key(
        &self,
        id: MessageId,
    ) -> Vec<u8> {
        Self::cached_id_key(&self.legacy_message_key_prefix, id)
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn ready_entry_index_key_with_prefix(
        prefix: &[u8],
        ready_seq: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + 16);
        key.extend_from_slice(prefix);
        key.extend_from_slice(&ready_seq.to_be_bytes());
        key.extend_from_slice(&id.as_u64().to_be_bytes());
        key
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn ready_entry_index_key(
        &self,
        ready_seq: u64,
        id: MessageId,
    ) -> Vec<u8> {
        Self::ready_entry_index_key_with_prefix(&self.ready_index_prefix, ready_seq, id)
    }

    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) fn parse_ready_entry_index_key(
        key: &[u8],
        prefix: &[u8],
    ) -> Option<(u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != 16 {
            return None;
        }
        let ready_seq = u64::from_be_bytes(rest[0..8].try_into().ok()?);
        let id = u64::from_be_bytes(rest[8..16].try_into().ok()?);
        Some((ready_seq, MessageId::new(id)))
    }
}
