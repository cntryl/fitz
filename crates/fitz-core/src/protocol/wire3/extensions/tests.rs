use super::*;
use proptest::prelude::*;

fn capability_list(ids: &[u32]) -> Vec<u8> {
    let mut value = Vec::with_capacity(2 + ids.len() * 4);
    value.extend_from_slice(&u16::try_from(ids.len()).unwrap().to_be_bytes());
    for id in ids {
        value.extend_from_slice(&id.to_be_bytes());
    }
    value
}

fn payload(core: &[u8], fields: &[(u16, &[u8])]) -> Vec<u8> {
    let mut value = Vec::new();
    value.extend_from_slice(&u32::try_from(core.len()).unwrap().to_be_bytes());
    value.extend_from_slice(core);
    for (field_id, field_value) in fields {
        value.extend_from_slice(&field_id.to_be_bytes());
        value.extend_from_slice(&u32::try_from(field_value.len()).unwrap().to_be_bytes());
        value.extend_from_slice(field_value);
    }
    value
}

#[test]
fn should_keep_core_only_payload_unchanged() {
    // Arrange
    let core = b"core";
    let encoded = encode_payload(core, 104, ExtensionDirection::ClientToBroker, &[]).unwrap();

    // Act
    let range = decode_core_payload(
        &encoded.value,
        104,
        ExtensionDirection::ClientToBroker,
        encoded.extensions_present,
    )
    .unwrap();

    // Assert
    assert!(!encoded.extensions_present);
    assert!(matches!(encoded.value, Cow::Borrowed(_)));
    assert_eq!(range.get(&encoded.value), Some(core.as_slice()));
}

#[test]
fn should_not_panic_when_a_core_range_does_not_fit_the_payload() {
    // Arrange
    let ranges = [
        CorePayloadRange {
            offset: 1,
            length: 1,
        },
        CorePayloadRange {
            offset: usize::MAX,
            length: 1,
        },
    ];

    // Act
    let results = ranges.map(|range| range.get(&[]));

    // Assert
    assert_eq!(results, [None, None]);
}

#[test]
fn should_not_panic_when_a_core_range_end_overflows() {
    // Arrange
    let range = CorePayloadRange {
        offset: usize::MAX,
        length: 1,
    };

    // Act
    let result = range.as_range();

    // Assert
    assert_eq!(result, None);
}

#[test]
fn should_skip_bounded_unknown_extensions_without_changing_core_range() {
    // Arrange
    let encoded = payload(b"core", &[(8, b"future"), (9, b"")]);

    // Act
    let range =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap();

    // Assert
    assert_eq!(range.get(&encoded), Some(&b"core"[..]));
}

#[test]
fn should_reject_truncated_extension_header() {
    // Arrange
    let mut encoded = payload(b"core", &[]);
    encoded.extend_from_slice(&[0, 8, 0, 0]);

    // Act
    let error =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::FieldHeaderTruncated);
}

#[test]
fn should_reject_every_truncated_core_length_prefix() {
    // Arrange
    let direction = ExtensionDirection::ClientToBroker;

    // Act
    let errors = (0..4)
        .map(|length| decode_core_payload(&vec![0; length], 104, direction, true).unwrap_err())
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(errors, vec![ExtensionError::CoreLengthMissing; 4]);
}

#[test]
fn should_reject_every_truncated_extension_field_header() {
    // Arrange
    let direction = ExtensionDirection::ClientToBroker;

    // Act
    let errors = (1..EXTENSION_HEADER_LEN)
        .map(|length| {
            let mut encoded = payload(b"", &[]);
            encoded.extend(std::iter::repeat_n(0, length));
            decode_core_payload(&encoded, 104, direction, true).unwrap_err()
        })
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(
        errors,
        vec![ExtensionError::FieldHeaderTruncated; EXTENSION_HEADER_LEN - 1]
    );
}

#[test]
fn should_reject_extension_flag_without_any_fields() {
    // Arrange
    let encoded = payload(b"core", &[]);

    // Act
    let error =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::NoExtensionFields);
}

#[test]
fn should_reject_truncated_extension_value() {
    // Arrange
    let mut encoded = payload(b"core", &[]);
    encoded.extend_from_slice(&[0, 8, 0, 0, 0, 4, b'x']);

    // Act
    let error =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::FieldValueTruncated);
}

#[test]
fn should_reject_duplicate_known_capability_fields() {
    // Arrange
    let caps = capability_list(&[7]);
    let encoded = payload(b"connect", &[(1, &caps), (1, &caps)]);

    // Act
    let error = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::KnownSingletonRepeated);
}

#[test]
fn should_reject_capability_list_count_mismatch() {
    // Arrange
    let caps = [0, 2, 0, 0, 0, 7];
    let encoded = payload(b"connect", &[(CAPABILITY_IDS_FIELD_ID, &caps)]);

    // Act
    let error = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityListLengthMismatch);
}

#[test]
fn should_reject_core_length_beyond_payload_bounds() {
    // Arrange
    let encoded = [0, 0, 0, 5, b'c', b'o', b'r', b'e'];

    // Act
    let error =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CoreLengthOutOfBounds);
}

#[test]
fn should_reject_zero_extension_field_id() {
    // Arrange
    let encoded = payload(b"core", &[(0, b"")]);

    // Act
    let error =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::FieldIdReserved);
}

#[test]
fn should_reject_duplicate_fields_from_encoder() {
    // Arrange
    let fields = [
        ExtensionField {
            field_id: 8,
            value: b"first",
        },
        ExtensionField {
            field_id: 8,
            value: b"second",
        },
    ];

    // Act
    let error =
        encode_payload(b"core", 104, ExtensionDirection::ClientToBroker, &fields).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::DuplicateField);
}

#[test]
fn should_reject_out_of_order_fields_from_encoder() {
    // Arrange
    let fields = [
        ExtensionField {
            field_id: 9,
            value: b"nine",
        },
        ExtensionField {
            field_id: 3,
            value: b"three",
        },
    ];

    // Act
    let error =
        encode_payload(b"core", 104, ExtensionDirection::ClientToBroker, &fields).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::FieldsOutOfOrder);
}

#[test]
fn should_validate_hello_capability_field_in_server_to_client_scope() {
    // Arrange
    let caps = capability_list(&[3, 9]);
    let encoded = payload(b"hello", &[(CAPABILITY_IDS_FIELD_ID, &caps)]);

    // Act
    let range = decode_core_payload(
        &encoded,
        HELLO_MESSAGE_TYPE,
        ExtensionDirection::BrokerToClient,
        true,
    )
    .unwrap();

    // Assert
    assert_eq!(range.get(&encoded), Some(&b"hello"[..]));
}

#[test]
fn should_encode_fields_in_declared_ascending_order() {
    // Arrange
    let fields = [
        ExtensionField {
            field_id: 4,
            value: b"four",
        },
        ExtensionField {
            field_id: 9,
            value: b"nine",
        },
    ];

    let encoded =
        encode_payload(b"core", 104, ExtensionDirection::ClientToBroker, &fields).unwrap();

    // Act
    let range = decode_core_payload(
        &encoded.value,
        104,
        ExtensionDirection::ClientToBroker,
        encoded.extensions_present,
    )
    .unwrap();

    // Assert
    assert!(encoded.extensions_present);
    assert_eq!(range.get(&encoded.value), Some(&b"core"[..]));
}

#[test]
fn should_require_a_capability_set_in_connect() {
    // Arrange
    let encoded = payload(b"handshake", &[]);

    // Act
    let error = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::NoExtensionFields);
}

#[test]
fn should_require_a_capability_set_in_hello() {
    // Arrange
    let encoded = payload(b"handshake", &[]);

    // Act
    let error = decode_core_payload(
        &encoded,
        HELLO_MESSAGE_TYPE,
        ExtensionDirection::BrokerToClient,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::NoExtensionFields);
}

#[test]
fn should_reject_handshake_without_extension_flag() {
    // Arrange
    let core = b"handshake";

    // Act
    let error = decode_core_payload(
        core,
        HELLO_MESSAGE_TYPE,
        ExtensionDirection::BrokerToClient,
        false,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityListMissing);
}

#[test]
fn should_require_capability_field_in_handshake_extensions() {
    // Arrange
    let encoded = payload(b"handshake", &[(2, b"future")]);

    // Act
    let error = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityListMissing);
}

#[test]
fn should_reject_capability_ids_that_are_not_strictly_ascending() {
    // Arrange
    let capabilities = capability_list(&[8, 7]);
    let encoded = payload(b"connect", &[(CAPABILITY_IDS_FIELD_ID, &capabilities)]);

    // Act
    let error = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityIdsOutOfOrder);
}

#[test]
fn should_reject_capability_lists_shorter_than_the_count_field() {
    // Arrange
    let truncated_values: [&[u8]; 2] = [&[], &[0]];

    // Act
    let errors = truncated_values
        .iter()
        .map(|value| {
            let encoded = payload(b"connect", &[(CAPABILITY_IDS_FIELD_ID, value)]);
            decode_core_payload(
                &encoded,
                CONNECT_MESSAGE_TYPE,
                ExtensionDirection::ClientToBroker,
                true,
            )
            .unwrap_err()
        })
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(
        errors,
        vec![ExtensionError::CapabilityListTruncated; truncated_values.len()]
    );
}

#[test]
fn should_reject_reserved_capability_id_in_decoder() {
    // Arrange
    let capabilities = capability_list(&[0]);
    let encoded = payload(b"connect", &[(CAPABILITY_IDS_FIELD_ID, &capabilities)]);

    // Act
    let error = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityIdReserved);
}

#[test]
fn should_reject_reserved_capability_id_in_encoder() {
    // Arrange

    // Act
    let error = encode_capability_ids(&[0]).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityIdReserved);
}

#[test]
fn should_reject_more_than_u16_max_capability_ids_in_encoder() {
    // Arrange
    let ids = vec![1; usize::from(u16::MAX) + 1];

    // Act
    let error = encode_capability_ids(&ids).unwrap_err();

    // Assert
    assert_eq!(error, ExtensionError::CapabilityListLengthMismatch);
}

#[test]
fn should_accept_maximum_u16_capability_count() {
    // Arrange
    let ids = (1..=u32::from(u16::MAX)).collect::<Vec<_>>();
    let capabilities = encode_capability_ids(&ids).unwrap();
    let encoded = payload(b"connect", &[(CAPABILITY_IDS_FIELD_ID, &capabilities)]);

    // Act
    let range = decode_core_payload(
        &encoded,
        CONNECT_MESSAGE_TYPE,
        ExtensionDirection::ClientToBroker,
        true,
    )
    .unwrap();

    // Assert
    assert_eq!(range.get(&encoded), Some(&b"connect"[..]));
}

#[test]
fn should_encode_the_empty_capability_set_with_a_zero_count() {
    // Arrange

    // Act
    let encoded = encode_capability_ids(&[]).unwrap();

    // Assert
    assert_eq!(encoded, [0, 0]);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn should_reject_complete_payloads_above_the_header_length_limit() {
    // Arrange
    let max_length = usize::try_from(u32::MAX).expect("u32 fits usize");
    let too_large = usize::try_from(u64::from(u32::MAX) + 1).expect("u64 value fits usize");

    // Act
    let at_limit = validate_payload_length(max_length);
    let over_limit = validate_payload_length(too_large);

    // Assert
    assert_eq!(at_limit, Ok(()));
    assert_eq!(over_limit, Err(ExtensionError::PayloadTooLarge));
}

#[test]
fn should_accept_extension_fields_in_any_order() {
    // Arrange
    let encoded = payload(b"core", &[(9, b"nine"), (3, b"three")]);

    // Act
    let range =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap();

    // Assert
    assert_eq!(range.get(&encoded), Some(&b"core"[..]));
}

#[test]
fn should_scope_capability_field_ids_to_handshake_messages() {
    // Arrange
    let encoded = payload(b"core", &[(CAPABILITY_IDS_FIELD_ID, b"bad"), (1, b"")]);

    // Act
    let range =
        decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap();

    // Assert
    assert_eq!(range.get(&encoded), Some(&b"core"[..]));
}

proptest! {
    #[test]
    fn should_return_only_in_bounds_core_ranges_for_arbitrary_payloads(
        payload in prop::collection::vec(any::<u8>(), 0..1024),
        message_type in any::<u16>(),
        client_to_broker in any::<bool>(),
        extensions_present in any::<bool>(),
    ) {
        // Arrange
        let direction = if client_to_broker {
            ExtensionDirection::ClientToBroker
        } else {
            ExtensionDirection::BrokerToClient
        };

        // Act
        let decoded = decode_core_payload(
            &payload,
            message_type,
            direction,
            extensions_present,
        );

        // Assert
        if let Ok(range) = decoded {
            prop_assert!(range.offset <= payload.len());
            prop_assert!(range.length <= payload.len() - range.offset);
            prop_assert_eq!(range.get(&payload).map(<[u8]>::len), Some(range.length));
        }
    }
}
