use fitz::protocol::wire3::extensions::{
    decode_core_payload, encode_capability_ids, encode_payload, ExtensionDirection, ExtensionField,
    CAPABILITY_IDS_FIELD_ID, HELLO_MESSAGE_TYPE,
};
use fitz::protocol::wire3::handshake::{Connect, Hello};
use fitz::protocol::wire3::header::{decode_message, encode_message, HeaderFlags};
use std::num::NonZeroU64;

const VECTORS: &str = include_str!("fixtures/golden/wire3_vectors.txt");

#[test]
fn should_encode_hello_with_queue_capabilities() {
    // Arrange
    let core = Hello {
        version: 3,
        broker_caps: 0,
        max_message_bytes: 1_048_576,
        max_in_flight: 64,
        session_id: 42,
    }
    .encode();
    let capabilities = encode_capability_ids(fitz::protocol::wire3::BROKER_CAPABILITY_IDS).unwrap();
    let payload = encode_payload(
        &core,
        2,
        ExtensionDirection::BrokerToClient,
        &[ExtensionField {
            field_id: 1,
            value: &capabilities,
        }],
    )
    .unwrap();
    // Act
    let frame = encode_message(
        2,
        HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    );
    // Assert
    assert_matches_vector("hello_wire3_queue_capabilities", &frame);
}

#[test]
fn should_encode_enqueue_with_a_two_hour_ttl() {
    // Arrange
    let mut core = fitz::protocol::payload_codec::PayloadEncoder::new();
    core.put_string("queue://acme/jobs/q");
    core.put_bytes(b"job");
    core.put_u8(0);
    let core = core.finish();
    let ttl = 2_u32.to_be_bytes();
    let payload = encode_payload(
        &core,
        200,
        ExtensionDirection::ClientToBroker,
        &[ExtensionField {
            field_id: 1,
            value: &ttl,
        }],
    )
    .unwrap();
    // Act
    let frame = encode_message(
        200,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(1),
        payload.value.as_ref(),
    );
    // Assert
    assert_matches_vector("enqueue_wire3_ttl2h_corr1", &frame);
}

fn nack_core(visibility_seconds: u64) -> Vec<u8> {
    let mut core = fitz::protocol::payload_codec::PayloadEncoder::new();
    core.put_string("queue://acme/jobs/q");
    core.put_u64(1);
    core.put_u64(9);
    core.put_u64(visibility_seconds);
    core.finish()
}

#[test]
fn should_encode_an_immediate_nack() {
    // Arrange
    let core = nack_core(0);
    // Act
    let frame = encode_message(210, HeaderFlags::none(), NonZeroU64::new(2), &core);
    // Assert
    assert_matches_vector("nack_wire3_immediate_corr2", &frame);
}

#[test]
fn should_encode_a_delayed_nack() {
    // Arrange
    let core = nack_core(60);
    // Act
    let frame = encode_message(210, HeaderFlags::none(), NonZeroU64::new(3), &core);
    // Assert
    assert_matches_vector("nack_wire3_delay60_corr3", &frame);
}

fn vector(name: &str) -> Vec<u8> {
    let hex = VECTORS
        .lines()
        .find_map(|line| {
            let (vector_name, hex) = line.split_once(' ')?;
            (vector_name == name).then_some(hex)
        })
        .expect("named Wire 3 golden vector");
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex byte"))
        .collect()
}

fn assert_matches_vector(name: &str, actual: &[u8]) {
    let actual = hex::encode(actual);
    let expected = VECTORS
        .lines()
        .find_map(|line| {
            let (vector_name, hex) = line.split_once(' ')?;
            (vector_name == name).then_some(hex)
        })
        .expect("named Wire 3 golden vector");
    assert_eq!(actual, expected, "{name}");
}

#[test]
fn should_preserve_core_only_frame_bytes() {
    // Arrange
    let correlation = NonZeroU64::new(1);

    // Act
    let frame = encode_message(104, HeaderFlags::none(), correlation, b"hi");

    // Assert
    assert_matches_vector("core_only_type104_corr1_value_hi", &frame);
}

#[test]
fn should_encode_connect_with_an_explicit_empty_capability_set() {
    // Arrange
    let core = Connect {
        min_version: 3,
        max_version: 3,
        client_caps: 0,
        jwt: String::new(),
        service_name: String::new(),
        client_build: "fitz-testkit".to_owned(),
    }
    .encode();
    let capabilities = encode_capability_ids(&[]).unwrap();
    let fields = [ExtensionField {
        field_id: CAPABILITY_IDS_FIELD_ID,
        value: &capabilities,
    }];
    let payload = encode_payload(&core, 1, ExtensionDirection::ClientToBroker, &fields).unwrap();

    // Act
    let frame = encode_message(
        1,
        HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    );

    // Assert
    assert_matches_vector("connect_wire3_empty_capabilities", &frame);
}

#[test]
fn should_encode_hello_with_an_explicit_empty_capability_set() {
    // Arrange
    let core = Hello {
        version: 3,
        broker_caps: 0,
        max_message_bytes: 1_048_576,
        max_in_flight: 64,
        session_id: 42,
    }
    .encode();
    let capabilities = encode_capability_ids(&[]).unwrap();
    let fields = [ExtensionField {
        field_id: CAPABILITY_IDS_FIELD_ID,
        value: &capabilities,
    }];
    let payload = encode_payload(
        &core,
        HELLO_MESSAGE_TYPE,
        ExtensionDirection::BrokerToClient,
        &fields,
    )
    .unwrap();

    // Act
    let frame = encode_message(
        HELLO_MESSAGE_TYPE,
        HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    );

    // Assert
    assert_matches_vector("hello_wire3_empty_capabilities_session42", &frame);
}

#[test]
fn should_decode_core_from_golden_vector_with_unknown_extension() {
    // Arrange
    let frame = vector("unknown_extension_type104_corr1_core_hi");

    // Act
    let (header, payload) = decode_message(&frame, u32::MAX).unwrap();
    let core = decode_core_payload(
        payload,
        104,
        ExtensionDirection::ClientToBroker,
        header.flags.is_extensions_present(),
    )
    .unwrap();

    // Assert
    assert_eq!(header.correlation, NonZeroU64::new(1));
    assert_eq!(core.get(payload), Some(&b"hi"[..]));
}
