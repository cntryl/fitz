use super::*;
use crate::dispatch::wire::kv::{KvMessage, KvResponse};
use crate::runtime::routing::RouteFamily;
use bytes::{BufMut, Bytes};

fn len_to_u32(len: usize) -> u32 {
    u32::try_from(len).expect("test payload length should fit in u32")
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
    assert!(matches!(result, Ok(KvMessage::Get { tx_id: 1, .. })));
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
            assert!(
                write_options.is_sync(),
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
            assert!(
                !write_options.is_sync(),
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
