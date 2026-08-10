use super::*;
use crate::domains::stream::store::EventPayload;
use crate::utils::storage_key::{self, DomainKeyspace};
use bytes::Bytes;

#[test]
fn should_encode_resource_key_with_proper_ordering() {
    // Arrange
    let key1 = encode_resource_key("realm", "area", "res1", 0);
    let key2 = encode_resource_key("realm", "area", "res1", 1);
    let key3 = encode_resource_key("realm", "area", "res1", 10);

    // Act
    // (keys are already encoded)

    // Assert
    assert!(key1 < key2);
    assert!(key2 < key3);
}

#[test]
fn should_encode_area_key_with_proper_ordering() {
    // Arrange
    let key1 = encode_area_key("realm", "area", 0);
    let key2 = encode_area_key("realm", "area", 1);
    let key3 = encode_area_key("realm", "area", 100);

    // Act
    // (keys are already encoded)

    // Assert
    assert!(key1 < key2);
    assert!(key2 < key3);
}

#[test]
fn should_isolate_keys_by_prefix() {
    // Arrange
    let resource_key = encode_resource_key("realm", "area", "res", 0);
    let area_key = encode_area_key("realm", "area", 0);
    let realm_key = encode_realm_key("realm", 0);

    // Act
    // (keys are already encoded)

    // Assert
    assert_eq!(
        stream_key_suffix(&resource_key)[0],
        KeyPrefix::Resource as u8
    );
    assert_eq!(stream_key_suffix(&area_key)[0], KeyPrefix::Area as u8);
    assert_eq!(stream_key_suffix(&realm_key)[0], KeyPrefix::Realm as u8);
}

#[test]
fn should_encode_resource_key_with_typed_lexkey_segments() {
    // Arrange
    let mut expected = storage_key::domain_marker_encoder(
        "realm",
        DomainKeyspace::Stream,
        KeyPrefix::Resource as u8,
        16,
    );
    storage_key::encode_segment_into(&mut expected, "area");
    storage_key::encode_segment_into(&mut expected, "resource");
    expected.encode_u64_into(42);

    // Act
    let key = encode_resource_key("realm", "area", "resource", 42);

    // Assert
    assert_eq!(key, expected.into_vec());
}

#[test]
fn should_encode_compact_area_page_key_with_proper_ordering() {
    // Arrange
    let key1 = encode_compact_area_page_key("realm", "area", 0);
    let key2 = encode_compact_area_page_key("realm", "area", 1);
    let key3 = encode_compact_area_page_key("realm", "area", 10);

    // Act
    // (keys are already encoded)

    // Assert
    assert!(key1 < key2);
    assert!(key2 < key3);
}

#[test]
fn should_encode_compact_resource_page_key_with_proper_ordering() {
    // Arrange
    let key1 = encode_compact_resource_page_key("realm", "area", "resource", 0);
    let key2 = encode_compact_resource_page_key("realm", "area", "resource", 1);
    let key3 = encode_compact_resource_page_key("realm", "area", "resource", 10);

    // Act
    // (keys are already encoded)

    // Assert
    assert!(key1 < key2);
    assert!(key2 < key3);
}

#[test]
fn should_decode_resource_offset_from_compact_resource_page_key() {
    // Arrange
    let key = encode_compact_resource_page_key("realm", "area", "resource", 42);

    // Act
    let decoded = decode_resource_offset_from_key(&key).expect("decode resource offset");

    // Assert
    assert_eq!(decoded, 42);
}

#[test]
fn should_order_resource_fragment_generations_after_their_logical_offset() {
    // Arrange
    let generation_zero = encode_compact_resource_fragment_key("realm", "area", "resource", 42, 0);
    let generation_one = encode_compact_resource_fragment_key("realm", "area", "resource", 42, 1);
    let next_offset = encode_compact_resource_fragment_key("realm", "area", "resource", 43, 0);

    // Act
    // Assert
    assert!(generation_zero < generation_one);
    assert!(generation_one < next_offset);
    assert_eq!(decode_resource_offset_from_key(&generation_one), Ok(42));
}

#[test]
fn should_roundtrip_resource_value() {
    // Arrange
    let value = ResourceValue {
        resource_offset: 42,
        body: Bytes::from("test"),
        metadata: Some(Bytes::from("meta")),
        created_at: 1_234_567_890,
        area_offset: Some(10),
        realm_offset: Some(5),
    };

    // Act
    let encoded = value.encode();
    let decoded = ResourceValue::decode(&encoded);

    // Assert

    assert_eq!(decoded.resource_offset, 42);
    assert_eq!(decoded.body, Bytes::from("test"));
    assert_eq!(decoded.area_offset, Some(10));
}

#[test]
fn should_reject_resource_value_with_wrong_marker() {
    // Arrange
    let mut encoded = ResourceValue {
        resource_offset: 42,
        body: Bytes::from("test"),
        metadata: Some(Bytes::from("meta")),
        created_at: 1_234_567_890,
        area_offset: Some(10),
        realm_offset: Some(5),
    }
    .encode();
    encoded[0] = 0xFF;

    // Act
    let error = ResourceValue::try_decode(&encoded).expect_err("reject wrong marker");

    // Assert
    assert_eq!(error, "decode resource value: missing marker");
}

#[test]
fn should_roundtrip_staging_value_with_metadata() {
    // Arrange
    let event = EventPayload {
        body: Bytes::from("body"),
        metadata: Some(Bytes::from("meta")),
        discriminator: None,
    };

    // Act
    let encoded = encode_staging_value(&event);
    let decoded = decode_staging_value(&encoded).expect("decode staging value");

    // Assert
    assert_eq!(decoded.body, event.body);
    assert_eq!(decoded.metadata, event.metadata);
}

#[test]
fn should_roundtrip_staging_value_without_metadata() {
    // Arrange
    let event = EventPayload {
        body: Bytes::from("body"),
        metadata: None,
        discriminator: None,
    };

    // Act
    let encoded = encode_staging_value(&event);
    let decoded = decode_staging_value(&encoded).expect("decode staging value");

    // Assert
    assert_eq!(decoded.body, event.body);
    assert_eq!(decoded.metadata, None);
}

#[test]
fn should_reject_staging_value_with_trailing_bytes() {
    // Arrange
    let event = EventPayload {
        body: Bytes::from("body"),
        metadata: Some(Bytes::from("meta")),
        discriminator: None,
    };
    let mut encoded = encode_staging_value(&event);
    encoded.push(0);

    // Act
    let error = decode_staging_value(&encoded).expect_err("reject trailing bytes");

    // Assert
    assert_eq!(error, "decode_staging_value: trailing bytes");
}

#[test]
fn should_roundtrip_compact_area_value() {
    // Arrange
    let value = AreaValue {
        resource_offset: 42,
        body: Bytes::from("body"),
        metadata: Some(Bytes::from("meta")),
        created_at: 123,
    };

    // Act
    let encoded = value.encode();
    let decoded = AreaValue::decode(&encoded);

    // Assert
    assert_eq!(decoded.resource_offset, value.resource_offset);
    assert_eq!(decoded.body, value.body);
    assert_eq!(decoded.metadata, value.metadata);
    assert_eq!(decoded.created_at, value.created_at);
}

#[test]
fn should_reject_area_value_with_wrong_marker() {
    // Arrange
    let mut encoded = AreaValue {
        resource_offset: 7,
        body: Bytes::from("body"),
        metadata: Some(Bytes::from("meta")),
        created_at: 321,
    }
    .encode();
    encoded[0] = 0xFF;

    // Act
    let error = AreaValue::try_decode(&encoded).expect_err("reject wrong marker");

    // Assert
    assert_eq!(error, "decode area value: missing marker");
}

#[test]
fn should_roundtrip_compact_realm_value() {
    // Arrange
    let value = RealmValue {
        area_offset: 11,
        resource_offset: 42,
        body: Bytes::from("body"),
        metadata: Some(Bytes::from("meta")),
        created_at: 123,
    };

    // Act
    let encoded = value.encode();
    let decoded = RealmValue::decode(&encoded);

    // Assert
    assert_eq!(decoded.area_offset, value.area_offset);
    assert_eq!(decoded.resource_offset, value.resource_offset);
    assert_eq!(decoded.body, value.body);
    assert_eq!(decoded.metadata, value.metadata);
    assert_eq!(decoded.created_at, value.created_at);
}

#[test]
fn should_reject_realm_value_with_wrong_marker() {
    // Arrange
    let mut encoded = RealmValue {
        area_offset: 11,
        resource_offset: 7,
        body: Bytes::from("body"),
        metadata: Some(Bytes::from("meta")),
        created_at: 321,
    }
    .encode();
    encoded[0] = 0xFF;

    // Act
    let error = RealmValue::try_decode(&encoded).expect_err("reject wrong marker");

    // Assert
    assert_eq!(error, "decode realm value: missing marker");
}

#[test]
fn should_roundtrip_simple_storage_values() {
    // Arrange
    let watermark = WatermarkValue { watermark: 42 };
    let offset_counter = OffsetCounterValue { next_offset: 7 };
    let resource_meta = ResourceMetaValue {
        next_offset: 11,
        committed_size_bytes: 99,
    };
    let area_counter = AreaCounterValue { next_offset: 19 };
    let realm_counter = RealmCounterValue { next_offset: 23 };

    // Act
    let decoded_watermark = WatermarkValue::decode(&watermark.encode()).expect("decode watermark");
    let decoded_offset_counter =
        OffsetCounterValue::decode(&offset_counter.encode()).expect("decode offset counter");
    let decoded_resource_meta =
        ResourceMetaValue::decode(&resource_meta.encode()).expect("decode resource meta");
    let decoded_area_counter =
        AreaCounterValue::decode(&area_counter.encode()).expect("decode area counter");
    let decoded_realm_counter =
        RealmCounterValue::decode(&realm_counter.encode()).expect("decode realm counter");

    // Assert
    assert_eq!(decoded_watermark.watermark, 42);
    assert_eq!(decoded_offset_counter.next_offset, 7);
    assert_eq!(decoded_resource_meta.next_offset, 11);
    assert_eq!(decoded_resource_meta.committed_size_bytes, 99);
    assert_eq!(decoded_area_counter.next_offset, 19);
    assert_eq!(decoded_realm_counter.next_offset, 23);
}

#[test]
fn should_reject_simple_storage_values_with_truncation() {
    // Arrange
    let mut watermark = WatermarkValue { watermark: 42 }.encode();
    watermark.pop();
    let mut offset_counter = OffsetCounterValue { next_offset: 7 }.encode();
    offset_counter.pop();
    let mut resource_meta = ResourceMetaValue {
        next_offset: 11,
        committed_size_bytes: 99,
    }
    .encode();
    resource_meta.pop();
    let mut area_counter = AreaCounterValue { next_offset: 19 }.encode();
    area_counter.pop();
    let mut realm_counter = RealmCounterValue { next_offset: 23 }.encode();
    realm_counter.pop();

    // Act
    let watermark_error =
        WatermarkValue::decode(&watermark).expect_err("reject truncated watermark");
    let offset_counter_error =
        OffsetCounterValue::decode(&offset_counter).expect_err("reject truncated offset");
    let resource_meta_error =
        ResourceMetaValue::decode(&resource_meta).expect_err("reject truncated resource meta");
    let area_counter_error =
        AreaCounterValue::decode(&area_counter).expect_err("reject truncated area counter");
    let realm_counter_error =
        RealmCounterValue::decode(&realm_counter).expect_err("reject truncated realm counter");

    // Assert
    assert_eq!(watermark_error, "decode watermark value: invalid length");
    assert_eq!(
        offset_counter_error,
        "decode offset counter value: invalid length"
    );
    assert_eq!(
        resource_meta_error,
        "decode resource metadata value: invalid length"
    );
    assert_eq!(
        area_counter_error,
        "decode area counter value: invalid length"
    );
    assert_eq!(
        realm_counter_error,
        "decode realm counter value: invalid length"
    );
}

#[test]
fn should_roundtrip_compact_realm_page_value() {
    // Arrange
    let value = CompactRealmPageValue {
        records: vec![
            CompactRealmPageRecord {
                area: "events".into(),
                resource: "orders".into(),
                area_offset: 11,
                resource_offset: 42,
                body: Bytes::from("body"),
                metadata: Some(Bytes::from("meta")),
                created_at: 123,
                expires_at: None,
            },
            CompactRealmPageRecord {
                area: "events".into(),
                resource: "audits".into(),
                area_offset: 12,
                resource_offset: 43,
                body: Bytes::from("body-2"),
                metadata: None,
                created_at: 124,
                expires_at: Some(456),
            },
        ],
    };

    // Act
    let encoded = value.encode();
    let decoded = CompactRealmPageValue::decode(&encoded);

    // Assert
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].area_offset, 11);
    assert_eq!(decoded.records[0].area.as_ref(), "events");
    assert_eq!(decoded.records[0].resource.as_ref(), "orders");
    assert_eq!(decoded.records[0].resource_offset, 42);
    assert_eq!(decoded.records[0].body, Bytes::from("body"));
    assert_eq!(decoded.records[0].metadata, Some(Bytes::from("meta")));
    assert_eq!(decoded.records[1].area_offset, 12);
    assert_eq!(decoded.records[1].resource_offset, 43);
    assert_eq!(decoded.records[1].body, Bytes::from("body-2"));
    assert_eq!(decoded.records[1].metadata, None);
    assert_eq!(decoded.records[1].expires_at, Some(456));
}

#[test]
fn should_return_error_given_truncated_compact_realm_page_value() {
    // Arrange
    let encoded = vec![
        COMPACT_REALM_PAGE_VALUE_V2_MARKER[0],
        COMPACT_REALM_PAGE_VALUE_V2_MARKER[1],
        1,
        0,
        0,
        0,
    ];

    // Act
    let result = CompactRealmPageValue::try_decode(&encoded);

    // Assert
    let error = result.expect_err("truncated compact realm page should fail to decode");
    assert!(error.contains("route identity length truncated"));
}

#[test]
fn should_roundtrip_compact_area_page_value() {
    // Arrange
    let value = CompactAreaPageValue {
        records: vec![
            CompactAreaPageRecord {
                resource: "orders".into(),
                resource_offset: 42,
                body: Bytes::from("body"),
                metadata: Some(Bytes::from("meta")),
                created_at: 123,
                expires_at: None,
            },
            CompactAreaPageRecord {
                resource: "audits".into(),
                resource_offset: 43,
                body: Bytes::from("body-2"),
                metadata: None,
                created_at: 124,
                expires_at: Some(456),
            },
        ],
    };

    // Act
    let encoded = value.encode();
    let decoded = CompactAreaPageValue::decode(&encoded);

    // Assert
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].resource_offset, 42);
    assert_eq!(decoded.records[0].resource.as_ref(), "orders");
    assert_eq!(decoded.records[0].body, Bytes::from("body"));
    assert_eq!(decoded.records[0].metadata, Some(Bytes::from("meta")));
    assert_eq!(decoded.records[1].resource_offset, 43);
    assert_eq!(decoded.records[1].body, Bytes::from("body-2"));
    assert_eq!(decoded.records[1].metadata, None);
    assert_eq!(decoded.records[1].expires_at, Some(456));
}

#[test]
fn should_roundtrip_compact_resource_page_value() {
    // Arrange
    let value = CompactResourcePageValue {
        records: vec![
            CompactResourcePageRecord {
                area_offset: 11,
                realm_offset: 21,
                body: Bytes::from("body"),
                metadata: Some(Bytes::from("meta")),
                created_at: 123,
                expires_at: None,
            },
            CompactResourcePageRecord {
                area_offset: 12,
                realm_offset: 22,
                body: Bytes::from("body-2"),
                metadata: None,
                created_at: 124,
                expires_at: Some(456),
            },
        ],
    };

    // Act
    let encoded = value.encode();
    let decoded = CompactResourcePageValue::decode(&encoded);

    // Assert
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].area_offset, 11);
    assert_eq!(decoded.records[0].realm_offset, 21);
    assert_eq!(decoded.records[0].body, Bytes::from("body"));
    assert_eq!(decoded.records[0].metadata, Some(Bytes::from("meta")));
    assert_eq!(decoded.records[1].area_offset, 12);
    assert_eq!(decoded.records[1].realm_offset, 22);
    assert_eq!(decoded.records[1].body, Bytes::from("body-2"));
    assert_eq!(decoded.records[1].metadata, None);
    assert_eq!(decoded.records[1].expires_at, Some(456));
}

#[test]
fn should_roundtrip_compressed_compact_realm_page_value() {
    // Arrange
    let value = CompressedCompactRealmPageValue {
        records: vec![
            CompactRealmPageRecord {
                area: "events".into(),
                resource: "orders".into(),
                area_offset: 11,
                resource_offset: 42,
                body: Bytes::from("body"),
                metadata: Some(Bytes::from("meta")),
                created_at: 123,
                expires_at: None,
            },
            CompactRealmPageRecord {
                area: "events".into(),
                resource: "audits".into(),
                area_offset: 12,
                resource_offset: 43,
                body: Bytes::from("body-2"),
                metadata: None,
                created_at: 124,
                expires_at: Some(456),
            },
        ],
    };

    // Act
    let encoded = value.encode();
    let decoded = CompressedCompactRealmPageValue::decode(&encoded);
    let decoded_page = decoded.into_compact_realm_page();

    // Assert
    assert_eq!(decoded_page.records.len(), 2);
    assert_eq!(decoded_page.records[0].area_offset, 11);
    assert_eq!(decoded_page.records[0].resource_offset, 42);
    assert_eq!(decoded_page.records[0].body, Bytes::from("body"));
    assert_eq!(decoded_page.records[0].metadata, Some(Bytes::from("meta")));
    assert_eq!(decoded_page.records[1].area_offset, 12);
    assert_eq!(decoded_page.records[1].resource_offset, 43);
    assert_eq!(decoded_page.records[1].body, Bytes::from("body-2"));
    assert_eq!(decoded_page.records[1].metadata, None);
}

#[test]
fn should_compress_repeated_global_page_routes() {
    // Arrange
    let realm = std::sync::Arc::<str>::from("north-america-production");
    let area = std::sync::Arc::<str>::from("payments-and-settlement");
    let resource = std::sync::Arc::<str>::from("completed-transactions");
    let records = (0..64)
        .map(|offset| CompactGlobalPageRecord {
            realm: realm.clone(),
            area: area.clone(),
            resource: resource.clone(),
            resource_offset: offset,
            area_offset: offset,
            realm_offset: offset,
            body: Bytes::from_static(b"x"),
            metadata: None,
            created_at: 1,
            expires_at: None,
        })
        .collect();
    let page = CompactGlobalPageValue { records };
    let legacy_route_bytes = 64 * (12 + realm.len() + area.len() + resource.len());

    // Act
    let encoded = page.encode();
    let decoded = CompactGlobalPageValue::try_decode(&encoded).expect("decode compressed page");
    let mut legacy = COMPACT_GLOBAL_PAGE_VALUE_V1_MARKER.to_vec();
    legacy.extend_from_slice(
        &lz4_flex::block::decompress_size_prepended(&encoded[2..]).expect("recover legacy payload"),
    );
    let legacy_decoded = CompactGlobalPageValue::try_decode(&legacy).expect("decode legacy page");

    // Assert
    assert!(encoded.len() < legacy_route_bytes);
    assert_eq!(decoded.records.len(), 64);
    assert_eq!(legacy_decoded.records.len(), 64);
    assert_eq!(decoded.records[63].resource.as_ref(), resource.as_ref());
}

#[test]
fn should_preserve_compact_page_codec_bytes() {
    // Arrange
    let global = CompactGlobalPageValue {
        records: vec![CompactGlobalPageRecord {
            realm: "r".into(),
            area: "a".into(),
            resource: "x".into(),
            resource_offset: 1,
            area_offset: 2,
            realm_offset: 3,
            body: Bytes::from_static(b"b"),
            metadata: Some(Bytes::from_static(b"m")),
            created_at: 4,
            expires_at: Some(5),
        }],
    };
    let realm = CompactRealmPageValue {
        records: vec![CompactRealmPageRecord {
            area: "a".into(),
            resource: "x".into(),
            area_offset: 2,
            resource_offset: 1,
            body: Bytes::from_static(b"b"),
            metadata: Some(Bytes::from_static(b"m")),
            created_at: 4,
            expires_at: Some(5),
        }],
    };
    let area = CompactAreaPageValue {
        records: vec![CompactAreaPageRecord {
            resource: "x".into(),
            resource_offset: 1,
            body: Bytes::from_static(b"b"),
            metadata: Some(Bytes::from_static(b"m")),
            created_at: 4,
            expires_at: Some(5),
        }],
    };
    let resource = CompactResourcePageValue {
        records: vec![CompactResourcePageRecord {
            area_offset: 2,
            realm_offset: 3,
            body: Bytes::from_static(b"b"),
            metadata: Some(Bytes::from_static(b"m")),
            created_at: 4,
            expires_at: Some(5),
        }],
    };

    // Act
    let encoded = [
        hex::encode(global.encode()),
        hex::encode(realm.encode()),
        hex::encode(area.encode()),
        hex::encode(resource.encode()),
    ];

    // Assert
    assert_eq!(
        encoded,
        [
            "00ed45000000400100000004001072050010610500107805000002001002050041000000030600300000040500410000000506000237006001000000626d",
            "00b3010000000100000061010000007802000000000000000100000000000000040000000000000005000000000000000100000001000000626d",
            "00e50100000001000000780100000000000000040000000000000005000000000000000100000001000000626d",
            "00ea0100000002000000000000000300000000000000040000000000000005000000000000000100000001000000626d",
        ]
    );
}

#[test]
fn should_roundtrip_posting_expirations() {
    // Arrange
    let value = PostingPageValue {
        entries: vec![
            PostingEntry {
                offset: 7,
                parent_fragment_start: 4,
                expires_at: None,
            },
            PostingEntry {
                offset: 8,
                parent_fragment_start: 4,
                expires_at: Some(12_345),
            },
        ],
    };

    // Act
    let decoded = PostingPageValue::try_decode(&value.encode()).expect("decode posting page");

    // Assert
    assert_eq!(decoded, value);
}
