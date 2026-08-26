use crate::domains::stream::protocol::{ReadResponse, StreamReadItem};

pub(super) fn apply_global_snapshot_boundary(
    from_offset: u64,
    captured_watermark: u64,
    response: &mut ReadResponse,
) {
    let item_offset = |item: &StreamReadItem| match item {
        StreamReadItem::Event(record) => record.global_offset.unwrap_or(u64::MAX),
        StreamReadItem::Filtered { offset, .. }
        | StreamReadItem::FilteredRange {
            from_offset: offset,
            ..
        } => *offset,
    };
    response
        .items
        .retain(|item| item_offset(item) < captured_watermark);
    let cursor_reached_snapshot_end = response
        .cursor
        .last_global_offset
        .is_some_and(|offset| offset >= captured_watermark);
    if !response.cursor.has_more || cursor_reached_snapshot_end {
        response.cursor.last_global_offset = if from_offset < captured_watermark {
            captured_watermark.checked_sub(1)
        } else {
            from_offset.checked_sub(1)
        };
        response.cursor.has_more = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::stream::protocol::{ReadCursor, StreamRecord};
    use crate::runtime::routing::Route;
    use bytes::Bytes;

    #[test]
    fn should_gate_global_read_items_at_the_captured_watermark() {
        // Arrange
        let mut response = ReadResponse {
            items: vec![StreamReadItem::Event(StreamRecord {
                route: Route::new("stream://bench/events/orders"),
                resource_offset: 1,
                area_offset: Some(4),
                realm_offset: Some(7),
                global_offset: Some(8),
                body: Bytes::from_static(b"after-snapshot"),
                metadata: None,
                created_at: 1,
            })],
            cursor: ReadCursor {
                last_resource_offset: 1,
                last_area_offset: None,
                last_realm_offset: None,
                last_global_offset: Some(8),
                has_more: true,
                cursor_fingerprint: None,
                captured_watermark: None,
            },
        };

        // Act
        apply_global_snapshot_boundary(0, 8, &mut response);

        // Assert
        assert!(response.items.is_empty());
        assert_eq!(response.cursor.last_resource_offset, 1);
        assert_eq!(response.cursor.last_global_offset, Some(7));
        assert!(!response.cursor.has_more);
    }
}
