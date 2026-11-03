use fitz::protocol::wire3::extensions::{
    decode_core_payload, encode_capability_ids, encode_payload, ExtensionDirection, ExtensionField,
    CAPABILITY_IDS_FIELD_ID, HELLO_MESSAGE_TYPE,
};
use fitz::protocol::wire3::handshake::{Connect, Hello};
use fitz::protocol::wire3::header::{decode_message, encode_message, HeaderFlags};
use std::num::NonZeroU64;

const VECTORS: &str = include_str!("fixtures/golden/wire3_vectors.txt");

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
    assert_eq!(core.get(payload), b"hi");
}
