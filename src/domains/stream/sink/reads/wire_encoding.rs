use super::super::model::{
    usize_to_u32_saturating, usize_to_u64_saturating, PayloadEncoder, StreamClientResponseBody,
    StreamDomainCore, StreamFilteredReason, StreamMetadata, StreamReadItem, StreamRecord,
};

impl StreamDomainCore {
    pub(in crate::domains::stream::sink) fn encode_optional_bytes(
        encoder: &mut PayloadEncoder,
        value: Option<&bytes::Bytes>,
    ) {
        match value {
            Some(bytes) => {
                encoder.put_u8(1);
                encoder.put_bytes(bytes.as_ref());
            }
            None => encoder.put_u8(0),
        }
    }

    pub(in crate::domains::stream::sink) fn encode_stream_record(
        encoder: &mut PayloadEncoder,
        record: &StreamRecord,
        extended: bool,
    ) {
        encoder.put_u64(record.resource_offset);
        encoder.put_optional_u64(record.area_offset);
        encoder.put_optional_u64(record.realm_offset);
        if extended {
            encoder.put_optional_u64(record.global_offset);
        }
        encoder.put_bytes(record.body.as_ref());
        Self::encode_optional_bytes(encoder, record.metadata.as_ref());
        encoder.put_u64(record.created_at);
    }

    pub(in crate::domains::stream::sink) fn encode_stream_filtered_reason(
        encoder: &mut PayloadEncoder,
        reason: Option<&StreamFilteredReason>,
    ) {
        match reason {
            Some(StreamFilteredReason::ServerFilter) => encoder.put_u8(1),
            Some(StreamFilteredReason::Permission) => encoder.put_u8(2),
            Some(StreamFilteredReason::Projection) => encoder.put_u8(3),
            None => encoder.put_u8(0),
        }
    }

    pub(in crate::domains::stream::sink) fn encode_stream_read_item(
        encoder: &mut PayloadEncoder,
        item: &StreamReadItem,
        extended: bool,
    ) {
        match item {
            StreamReadItem::Event(record) => {
                encoder.put_u8(0);
                Self::encode_stream_record(encoder, record, extended);
            }
            StreamReadItem::Filtered { offset, reason, .. } => {
                encoder.put_u8(1);
                encoder.put_u64(*offset);
                Self::encode_stream_filtered_reason(encoder, reason.as_ref());
            }
            StreamReadItem::FilteredRange {
                from_offset,
                to_offset,
                reason,
                ..
            } => {
                encoder.put_u8(2);
                encoder.put_u64(*from_offset);
                encoder.put_u64(*to_offset);
                Self::encode_stream_filtered_reason(encoder, reason.as_ref());
            }
        }
    }

    pub(in crate::domains::stream::sink) fn encode_stream_cursor(
        encoder: &mut PayloadEncoder,
        cursor: &crate::domains::stream::protocol::ReadCursor,
        extended: bool,
    ) {
        encoder.put_u64(cursor.last_resource_offset);
        encoder.put_optional_u64(cursor.last_area_offset);
        encoder.put_optional_u64(cursor.last_realm_offset);
        if extended {
            encoder.put_optional_u64(cursor.last_global_offset);
        }
        encoder.put_u8(u8::from(cursor.has_more));
        if extended {
            encoder.put_optional_u64(cursor.cursor_fingerprint);
            encoder.put_optional_u64(cursor.captured_watermark);
        }
    }

    pub(in crate::domains::stream::sink) fn encode_stream_read_data(
        items: &[StreamReadItem],
        cursor: &crate::domains::stream::protocol::ReadCursor,
        extended: bool,
    ) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u32(usize_to_u32_saturating(items.len()));
        for item in items {
            encoder.put_string(item.route());
            Self::encode_stream_read_item(&mut encoder, item, extended);
        }
        Self::encode_stream_cursor(&mut encoder, cursor, extended);
        encoder.finish()
    }

    pub(in crate::domains::stream::sink) fn encode_stream_last_data(
        record: &StreamRecord,
    ) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        Self::encode_stream_record(&mut encoder, record, false);
        encoder.finish()
    }

    pub(in crate::domains::stream::sink) fn encode_stream_metadata_data(
        metadata: &StreamMetadata,
    ) -> Vec<u8> {
        let mut encoder = PayloadEncoder::new();
        encoder.put_optional_u64(metadata.first_resource_offset);
        encoder.put_optional_u64(metadata.last_resource_offset);
        encoder.put_u64(metadata.resource_count);
        encoder.put_u64(usize_to_u64_saturating(metadata.max_batch_events));
        encoder.put_u64(usize_to_u64_saturating(metadata.max_batch_bytes));
        encoder.put_optional_u64(metadata.ttl_seconds);
        encoder.put_u64(metadata.area_watermark);
        encoder.put_u64(metadata.realm_watermark);
        encoder.finish()
    }

    pub(in crate::domains::stream::sink) fn encode_stream_commit_notify_payload(
        commit: &crate::domains::stream::protocol::CommitSessionResponse,
    ) -> bytes::Bytes {
        bytes::Bytes::from(
            serde_json::json!({
                "event": "committed",
                "first_resource_offset": commit.first_resource_offset,
                "last_resource_offset": commit.last_resource_offset,
                "first_area_offset": commit.first_area_offset,
                "last_area_offset": commit.last_area_offset,
                "first_realm_offset": commit.first_realm_offset,
                "last_realm_offset": commit.last_realm_offset,
                "first_global_offset": commit.first_global_offset,
                "last_global_offset": commit.last_global_offset,
                "batch_size": commit.batch_size,
            })
            .to_string(),
        )
    }

    pub(in crate::domains::stream::sink) fn stream_error_response(
        error: impl Into<String>,
    ) -> StreamClientResponseBody {
        StreamClientResponseBody::Error(error.into())
    }
}
