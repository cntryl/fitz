use super::{kv_scan_item_wire_bytes, kv_scan_response_byte_ceiling};
use crate::domains::kv::{KvPair, KvResponse};
use bytes::Bytes;

fn encoded_len(items: Vec<KvPair>) -> usize {
    crate::dispatch::protocol::kv::encode_response(&KvResponse::ScanResult {
        items,
        has_more: false,
    })
    .len()
}

#[test]
fn should_match_the_codec_exactly_for_a_single_pair() {
    // Arrange
    // Budgeting more than the codec writes rejects wire-valid responses at
    // the boundary; budgeting less emits unframable ones. Both are bugs, so
    // the arithmetic is pinned to the encoder.
    let key = Bytes::from(vec![b'k'; 300]);
    let value = Bytes::from(vec![b'v'; 1_024]);

    // Act
    let budgeted =
        kv_scan_item_wire_bytes(key.len(), value.len()) + super::KV_SCAN_ENVELOPE_OVERHEAD_BYTES;
    let actual = encoded_len(vec![KvPair { key, value }]);

    // Assert
    assert_eq!(budgeted, actual, "budget must equal the encoded length");
}

#[test]
fn should_admit_the_largest_wire_valid_single_pair() {
    // Arrange
    // The exact case an over-generous budget rejected.
    let key = Bytes::from(vec![b'k'; 300]);
    let value = Bytes::from(vec![b'v'; 65_200]);
    let pair = KvPair {
        key: key.clone(),
        value: value.clone(),
    };

    // Act
    let cost = kv_scan_item_wire_bytes(key.len(), value.len());
    let actual = encoded_len(vec![pair]);

    // Assert
    assert!(
        u16::try_from(actual).is_ok(),
        "this response is wire-valid at {actual} bytes"
    );
    assert!(
        cost <= kv_scan_response_byte_ceiling(),
        "a wire-valid pair must not be rejected: {cost} charged against {}",
        kv_scan_response_byte_ceiling()
    );
}

#[test]
fn should_match_the_codec_exactly_across_many_pairs() {
    // Arrange
    let items = (0..50)
        .map(|index| KvPair {
            key: Bytes::from(format!("key-{index:03}")),
            value: Bytes::from(vec![b'v'; 100 + index]),
        })
        .collect::<Vec<_>>();

    // Act
    let budgeted = items
        .iter()
        .map(|item| kv_scan_item_wire_bytes(item.key.len(), item.value.len()))
        .sum::<usize>()
        + super::KV_SCAN_ENVELOPE_OVERHEAD_BYTES;
    let actual = encoded_len(items);

    // Assert
    assert_eq!(budgeted, actual);
}
