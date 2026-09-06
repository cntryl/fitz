use super::*;
use crate::dispatch::wire::kv::{KvError, KvMessage, KvResponse};
use crate::runtime::routing::RouteFamily;
use bytes::{BufMut, Bytes};

fn len_to_u32(len: usize) -> u32 {
    u32::try_from(len).expect("test payload length should fit in u32")
}

fn put_route(payload: &mut Vec<u8>, route: &str) {
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
}

fn canonical_operation_payloads() -> Vec<(u16, Vec<u8>)> {
    let route = "kv://acme/kv/users";
    let mut begin = Vec::new();
    put_route(&mut begin, route);
    begin.extend_from_slice(&[1, 0]);

    let mut transaction_only = Vec::new();
    transaction_only.put_u64(1);
    put_route(&mut transaction_only, route);

    let mut keyed = transaction_only.clone();
    keyed.put_u32(1);
    keyed.put_u8(b'k');

    let mut key_value = keyed.clone();
    key_value.put_u32(1);
    key_value.put_u8(b'v');

    let mut range = keyed.clone();
    range.put_u32(1);
    range.put_u8(b'z');

    let mut scan = transaction_only.clone();
    scan.extend_from_slice(&[0, 0, 0, 0]);

    vec![
        (msg_type::BEGIN, begin),
        (msg_type::COMMIT, transaction_only.clone()),
        (msg_type::ROLLBACK, transaction_only),
        (msg_type::GET, keyed.clone()),
        (msg_type::PUT, key_value.clone()),
        (msg_type::INSERT, key_value),
        (msg_type::DELETE, keyed),
        (msg_type::DELETE_RANGE, range),
        (msg_type::SCAN, scan),
    ]
}

#[test]
fn should_parse_begin_read_write_buffered() {
    // Arrange
    let route = "kv://acme/kv/users";
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(1); // ReadWrite
    payload.put_u8(0); // buffered (per CLIENT_SPEC: 0=buffered, 1=sync)

    // Act
    let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

    // Assert
    assert!(matches!(result, Ok(KvMessage::Begin { .. })));
}

#[test]
fn should_parse_get_with_key() {
    // Arrange
    let route = "kv://acme/kv/users";
    let key = b"user:1001";
    let mut payload = Vec::new();
    payload.put_u64(1); // tx_id
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(len_to_u32(key.len()));
    payload.put_slice(key);

    // Act
    let result = parse_request(msg_type::GET, RouteFamily::new(1), &payload);

    // Assert
    let Ok(KvMessage::Get { tx_id, scope, .. }) = result else {
        panic!("expected parsed GET");
    };
    assert_eq!(tx_id, 1);
    assert_eq!(scope.route_family, RouteFamily::new(1));
    assert_eq!(scope.realm, "acme");
    assert_eq!(scope.area, "kv");
    assert_eq!(scope.resource, "users");
}

#[test]
fn should_reject_trailing_bytes_for_every_kv_operation() {
    // Arrange
    let payloads = canonical_operation_payloads();

    // Act
    let results = payloads
        .into_iter()
        .map(|(message_type, mut payload)| {
            payload.push(0xff);
            parse_request(message_type, RouteFamily::new(1), &payload)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(results.iter().enumerate().all(|(index, result)| {
        if result.is_err() {
            true
        } else {
            panic!("noncanonical scan flag set {index} was accepted: {result:?}");
        }
    }));
}

#[test]
fn should_reject_noncanonical_scan_flags() {
    // Arrange
    let mut prefix = Vec::new();
    prefix.put_u64(1);
    put_route(&mut prefix, "kv://acme/kv/users");
    let flag_sets = [[2, 0, 0, 0], [0, 2, 0, 0], [0, 0, 2, 0], [0, 0, 0, 2]];

    // Act
    let results = flag_sets.map(|flags| {
        let mut payload = prefix.clone();
        payload.extend_from_slice(&flags);
        parse_request(msg_type::SCAN, RouteFamily::new(1), &payload)
    });

    // Assert
    for (index, result) in results.iter().enumerate() {
        assert!(
            result.is_err(),
            "noncanonical scan flag set {index} was accepted: {result:?}"
        );
    }
}

#[test]
fn should_reject_scan_with_any_required_flag_missing() {
    // Arrange
    let mut prefix = Vec::new();
    prefix.put_u64(1);
    put_route(&mut prefix, "kv://acme/kv/users");

    // Act
    let results = (0..4)
        .map(|present_flags| {
            let mut payload = prefix.clone();
            payload.resize(payload.len() + present_flags, 0);
            parse_request(msg_type::SCAN, RouteFamily::new(1), &payload)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(results.iter().all(Result::is_err));
}

proptest::proptest! {
    #[test]
    fn should_never_panic_given_arbitrary_kv_payload(
        message_type in msg_type::BEGIN..=msg_type::SCAN,
        payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
    ) {
        // Arrange
        // Act
        let _ = parse_request(message_type, RouteFamily::new(1), &payload);
        // Assert
    }
}

#[test]
fn should_encode_get_result_found() {
    // Arrange
    let response = KvResponse::GetResult {
        found: true,
        value: Some(Bytes::from("test_value")),
    };

    // Act
    let encoded = encode_response(&response);

    // Assert
    assert!(!encoded.is_empty());
    assert_eq!(encoded[0], 0); // status: success
    assert_eq!(encoded[1], 1); // found flag
}

#[test]
fn should_encode_get_result_not_found() {
    // Arrange
    let response = KvResponse::GetResult {
        found: false,
        value: None,
    };

    // Act
    let encoded = encode_response(&response);

    // Assert
    assert!(!encoded.is_empty());
    assert_eq!(encoded[0], 0); // status: success
    assert_eq!(encoded[1], 0); // not found flag
}

#[test]
fn should_parse_begin_with_sync_durability() {
    // Arrange - Per CLIENT_SPEC, durability byte: 0=buffered, 1=sync
    let route = "kv://acme/kv/users";
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(1); // ReadWrite
    payload.put_u8(1); // sync durability (per CLIENT_SPEC: 1=sync)

    // Act
    let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

    // Assert
    match result {
        Ok(KvMessage::Begin { write_options, .. }) => {
            // Verify that durability byte 1 maps to sync
            assert_eq!(
                write_options,
                crate::domains::WritePolicy::Sync,
                "Durability byte 1 should map to sync"
            );
        }
        _ => panic!("Expected KvMessage::Begin with sync write options"),
    }
}

#[test]
fn should_parse_begin_with_buffered_durability() {
    // Arrange - Per CLIENT_SPEC, durability byte: 0=buffered, 1=sync
    let route = "kv://acme/kv/users";
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(1); // ReadWrite
    payload.put_u8(0); // buffered durability (per CLIENT_SPEC: 0=buffered)

    // Act
    let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

    // Assert
    match result {
        Ok(KvMessage::Begin { write_options, .. }) => {
            // Verify that durability byte 0 maps to buffered
            assert_eq!(
                write_options,
                crate::domains::WritePolicy::Buffered,
                "Durability byte 0 should map to buffered"
            );
        }
        _ => panic!("Expected KvMessage::Begin with buffered write options"),
    }
}

#[test]
fn should_reject_begin_with_invalid_durability() {
    // Arrange
    let route = "kv://acme/kv/users";
    let mut base_payload = Vec::new();
    base_payload.put_u32(len_to_u32(route.len()));
    base_payload.put_slice(route.as_bytes());
    base_payload.put_u8(1); // ReadWrite

    // Act
    let results = [2_u8, 255_u8]
        .into_iter()
        .map(|durability| {
            let mut payload = base_payload.clone();
            payload.put_u8(durability);
            parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload)
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(results.iter().all(Result::is_err));
    assert!(results.iter().all(|result| result
        .as_ref()
        .unwrap_err()
        .contains("Invalid durability mode")));
}

#[test]
fn should_reject_begin_with_too_few_route_segments() {
    // Arrange
    let route = "kv://acme/kv";
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(1); // ReadWrite
    payload.put_u8(0); // buffered

    // Act
    let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_begin_given_nested_resource_path() {
    // Arrange
    let route = "kv://acme/kv/users/by/id";
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(1);
    payload.put_u8(0);

    // Act
    let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_encode_subscribe_ok_response() {
    // Arrange
    let response = KvResponse::SubscribeOk { subscription_id: 9 };

    // Act
    let encoded = encode_response(&response);

    // Assert
    assert_eq!(encoded.len(), 9);
    assert_eq!(encoded[0], 0);
    assert_eq!(u64::from_be_bytes(encoded[1..9].try_into().unwrap()), 9);
}

#[test]
fn should_encode_read_only_write_with_canonical_error_code() {
    // Arrange
    // Act
    // Assert
    let response = KvResponse::Error {
        error: KvError::BackendError("Cannot write in ReadOnly transaction".to_string()),
    };

    let encoded = encode_response(&response);

    assert_eq!(encoded[0], 1);
    assert_eq!(u32::from_be_bytes(encoded[1..5].try_into().unwrap()), 1005);
}
