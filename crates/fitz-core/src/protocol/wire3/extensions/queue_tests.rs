use super::*;

fn ttl_payload(value: &[u8], repeated: bool) -> Vec<u8> {
    let mut bytes = vec![0, 0, 0, 0];
    for _ in 0..if repeated { 2 } else { 1 } {
        bytes.extend_from_slice(&QUEUE_TTL_HOURS_FIELD_ID.to_be_bytes());
        bytes.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(value);
    }
    bytes
}

#[test]
fn should_decode_queue_ttl_as_positive_hours() {
    // Arrange
    let bytes = ttl_payload(&2_u32.to_be_bytes(), false);
    // Act
    let decoded = decode_payload(&bytes, 200, ExtensionDirection::ClientToBroker, true).unwrap();
    // Assert
    assert_eq!(
        decoded
            .fields
            .queue_ttl_hours
            .map(std::num::NonZeroU32::get),
        Some(2)
    );
}

#[test]
fn should_reject_zero_queue_ttl_hours() {
    // Arrange
    let bytes = ttl_payload(&0_u32.to_be_bytes(), false);
    // Act
    let error = decode_payload(&bytes, 200, ExtensionDirection::ClientToBroker, true)
        .err()
        .unwrap();
    // Assert
    assert_eq!(error, ExtensionError::InvalidQueueTtl);
}

#[test]
fn should_reject_truncated_queue_ttl_hours() {
    // Arrange
    let bytes = ttl_payload(&[1, 2, 3], false);
    // Act
    let error = decode_payload(&bytes, 200, ExtensionDirection::ClientToBroker, true)
        .err()
        .unwrap();
    // Assert
    assert_eq!(error, ExtensionError::InvalidQueueTtl);
}

#[test]
fn should_reject_duplicate_queue_ttl_hours() {
    // Arrange
    let bytes = ttl_payload(&2_u32.to_be_bytes(), true);
    // Act
    let error = decode_payload(&bytes, 200, ExtensionDirection::ClientToBroker, true)
        .err()
        .unwrap();
    // Assert
    assert_eq!(error, ExtensionError::KnownSingletonRepeated);
}

#[test]
fn should_ignore_queue_ttl_field_id_on_an_enqueue_response() {
    // Arrange
    let bytes = ttl_payload(&[255], false);
    // Act
    let decoded = decode_payload(&bytes, 200, ExtensionDirection::BrokerToClient, true).unwrap();
    // Assert
    assert_eq!(decoded.fields, KnownFields::none());
}

#[test]
fn should_reject_zero_queue_ttl_in_the_encoder() {
    // Arrange
    let value = 0_u32.to_be_bytes();
    let fields = [ExtensionField {
        field_id: QUEUE_TTL_HOURS_FIELD_ID,
        value: &value,
    }];
    // Act
    let error =
        encode_payload(b"core", 200, ExtensionDirection::ClientToBroker, &fields).unwrap_err();
    // Assert
    assert_eq!(error, ExtensionError::InvalidQueueTtl);
}
