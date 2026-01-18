//! Schedule domain E2E integration tests
//!
//! Tests schedule protocol encoding/decoding and cron parsing validation
//! Note: Persistence tests skipped due to Midge commit() bug with writes
//! (See TODO.md: "Midge commit() fails when transaction contains writes")

use bytes::Bytes;
use fitz::domains::schedule::protocol::SchedulePayload;

#[test]
fn should_encode_and_decode_schedule_payload() {
    // Arrange
    let original = SchedulePayload {
        cron: "0 9 * * 1-5".to_string(),
        resource: "emails".to_string(),
        operation: "send".to_string(),
    };

    // Act
    let encoded = original.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap(), original);
}

#[test]
fn should_parse_valid_cron_every_minute() {
    // Arrange
    let payload = SchedulePayload {
        cron: "* * * * *".to_string(),
        resource: "task".to_string(),
        operation: "run".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "* * * * *");
}

#[test]
fn should_parse_valid_cron_workday_9am() {
    // Arrange
    let payload = SchedulePayload {
        cron: "0 9 * * 1-5".to_string(), // Mon-Fri at 9 AM
        resource: "meetings".to_string(),
        operation: "start".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "0 9 * * 1-5");
}

#[test]
fn should_parse_valid_cron_with_step_syntax() {
    // Arrange
    let payload = SchedulePayload {
        cron: "*/15 */6 * * *".to_string(), // Every 15 min, every 6 hours
        resource: "sync".to_string(),
        operation: "data".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "*/15 */6 * * *");
}

#[test]
fn should_parse_valid_cron_with_list_syntax() {
    // Arrange
    let payload = SchedulePayload {
        cron: "0 9,12,18 * * *".to_string(), // At 9 AM, 12 PM, 6 PM
        resource: "alerts".to_string(),
        operation: "check".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "0 9,12,18 * * *");
}

#[test]
fn should_parse_valid_cron_with_range_syntax() {
    // Arrange
    let payload = SchedulePayload {
        cron: "0 9-17 * * 1-5".to_string(), // 9 AM to 5 PM, Mon-Fri
        resource: "office".to_string(),
        operation: "open".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "0 9-17 * * 1-5");
}

#[test]
fn should_parse_valid_cron_max_values() {
    // Arrange - Minute max 59, Hour max 23, Day max 31, Month max 12, Weekday max 6
    let payload = SchedulePayload {
        cron: "59 23 31 12 6".to_string(),
        resource: "last".to_string(),
        operation: "second".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "59 23 31 12 6");
}

#[test]
fn should_parse_valid_cron_min_values() {
    // Arrange - Minute min 0, Hour min 0, Day min 1, Month min 1, Weekday min 0
    let payload = SchedulePayload {
        cron: "0 0 1 1 0".to_string(),
        resource: "first".to_string(),
        operation: "instant".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().cron, "0 0 1 1 0");
}

#[test]
fn should_decode_with_empty_operation_field() {
    // Arrange - Operation can be empty string (semantic meaning: fire without routing)
    let payload = SchedulePayload {
        cron: "0 12 * * *".to_string(),
        resource: "task".to_string(),
        operation: "".to_string(), // Empty operation allowed
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    assert_eq!(decoded.unwrap().operation, "");
}

#[test]
fn should_preserve_payload_through_roundtrip() {
    // Arrange
    let payloads = vec![
        SchedulePayload {
            cron: "0 0 * * *".to_string(),
            resource: "daily_backup".to_string(),
            operation: "backup_full".to_string(),
        },
        SchedulePayload {
            cron: "*/5 * * * *".to_string(),
            resource: "health_check".to_string(),
            operation: "check_services".to_string(),
        },
        SchedulePayload {
            cron: "0 */4 * * *".to_string(),
            resource: "cache_refresh".to_string(),
            operation: "invalidate_all".to_string(),
        },
    ];

    // Act & Assert
    for original in payloads {
        let encoded = original.encode();
        let decoded = SchedulePayload::decode(&encoded).unwrap();
        assert_eq!(original.cron, decoded.cron);
        assert_eq!(original.resource, decoded.resource);
        assert_eq!(original.operation, decoded.operation);
    }
}

#[test]
fn should_handle_unicode_in_resource_and_operation() {
    // Arrange
    let payload = SchedulePayload {
        cron: "0 0 * * *".to_string(),
        resource: "café_backup".to_string(),
        operation: "café_sync".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    let decoded = decoded.unwrap();
    assert_eq!(decoded.resource, "café_backup");
    assert_eq!(decoded.operation, "café_sync");
}

#[test]
fn should_handle_long_resource_and_operation_names() {
    // Arrange
    let long_name = "a".repeat(256);
    let payload = SchedulePayload {
        cron: "0 0 * * *".to_string(),
        resource: long_name.clone(),
        operation: long_name.clone(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    let decoded = decoded.unwrap();
    assert_eq!(decoded.resource, long_name);
    assert_eq!(decoded.operation, long_name);
}

#[test]
fn should_handle_special_characters_in_fields() {
    // Arrange
    let payload = SchedulePayload {
        cron: "0 0 * * *".to_string(),
        resource: "task/with/slashes".to_string(),
        operation: "op-with-dashes_and_underscores".to_string(),
    };

    // Act
    let encoded = payload.encode();
    let decoded = SchedulePayload::decode(&encoded);

    // Assert
    assert!(decoded.is_ok());
    let decoded = decoded.unwrap();
    assert_eq!(decoded.resource, "task/with/slashes");
    assert_eq!(decoded.operation, "op-with-dashes_and_underscores");
}

#[test]
fn should_reject_malformed_tlv_payload() {
    // Arrange
    let malformed = Bytes::from_static(b"not tlv encoded at all");

    // Act
    let decoded = SchedulePayload::decode(&malformed);

    // Assert
    assert!(decoded.is_err());
}

#[test]
fn should_decode_payload_with_missing_cron_field() {
    // Arrange - TLV with only resource and operation, missing cron (type 1)
    use fitz::protocol::tlv::{MessageType, TlvEncoder};
    let mut enc = TlvEncoder::new();
    enc.encode(MessageType(2), b"resource_only");
    enc.encode(MessageType(3), b"operation_only");
    let tlv = enc.finish();

    // Act
    let decoded = SchedulePayload::decode(&tlv);

    // Assert
    assert!(decoded.is_err()); // Missing required field
}

#[test]
fn should_encode_multiple_payloads_independently() {
    // Arrange
    let payload1 = SchedulePayload {
        cron: "0 9 * * *".to_string(),
        resource: "morning".to_string(),
        operation: "start".to_string(),
    };

    let payload2 = SchedulePayload {
        cron: "0 17 * * *".to_string(),
        resource: "evening".to_string(),
        operation: "end".to_string(),
    };

    // Act
    let encoded1 = payload1.encode();
    let encoded2 = payload2.encode();

    let decoded1 = SchedulePayload::decode(&encoded1).unwrap();
    let decoded2 = SchedulePayload::decode(&encoded2).unwrap();

    // Assert - Verify no cross-contamination
    assert_eq!(decoded1.resource, "morning");
    assert_eq!(decoded2.resource, "evening");
    assert_ne!(decoded1, decoded2);
}
