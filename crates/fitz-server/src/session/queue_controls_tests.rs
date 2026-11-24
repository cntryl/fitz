use super::*;

#[test]
fn should_reject_nack_without_its_own_capability() {
    // Arrange
    let mut session = queue_session(&[crate::protocol::wire3::QUEUE_MESSAGE_TTL_CAPABILITY_ID]);
    let frame = Bytes::from(crate::protocol::wire3::header::encode_message(
        210,
        HeaderFlags::none(),
        None,
        b"nack-core",
    ));
    // Act
    let result = session.decode_frame(&frame);
    // Assert
    assert!(matches!(result, Err(SessionError::Unsupported { .. })));
}

#[test]
fn should_dispatch_nack_with_its_negotiated_capability() {
    // Arrange
    let mut session = queue_session(&[crate::protocol::wire3::QUEUE_NACK_CAPABILITY_ID]);
    let frame = Bytes::from(crate::protocol::wire3::header::encode_message(
        210,
        HeaderFlags::none(),
        None,
        b"nack-core",
    ));
    // Act
    let message = session.decode_frame(&frame).unwrap();
    // Assert
    assert_eq!(message.payload.as_ref(), b"nack-core");
}
use crate::protocol::wire3::extensions::{
    encode_capability_ids, encode_payload, ExtensionDirection, ExtensionField,
};
use crate::protocol::wire3::header::{encode_message, HeaderFlags};

fn queue_session(capabilities: &[u32]) -> Session {
    let mut session = Session::new(
        42,
        NewSessionConfig::unauthenticated(
            TransportKind::Tcp,
            None,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            16,
            None,
            RouteFamily::new(1),
        ),
    );
    let core = crate::protocol::wire3::handshake::Connect {
        min_version: 3,
        max_version: 3,
        client_caps: 0,
        jwt: String::new(),
        service_name: String::new(),
        client_build: String::new(),
    }
    .encode();
    let capabilities = encode_capability_ids(capabilities).unwrap();
    let payload = encode_payload(
        &core,
        1,
        ExtensionDirection::ClientToBroker,
        &[ExtensionField {
            field_id: 1,
            value: &capabilities,
        }],
    )
    .unwrap();
    let frame = Bytes::from(encode_message(
        1,
        HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    ));
    let message = session.decode_frame(&frame).unwrap();
    session.release_channel(message.channel);
    session
}

fn ttl_frame() -> Bytes {
    let ttl = 2_u32.to_be_bytes();
    let payload = encode_payload(
        b"enqueue-core",
        200,
        ExtensionDirection::ClientToBroker,
        &[ExtensionField {
            field_id: 1,
            value: &ttl,
        }],
    )
    .unwrap();
    Bytes::from(encode_message(
        200,
        HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    ))
}

#[test]
fn should_reject_queue_ttl_without_negotiated_support() {
    // Arrange
    let mut session = queue_session(&[]);
    // Act
    let result = session.decode_frame(&ttl_frame());
    // Assert
    assert!(matches!(result, Err(SessionError::Unsupported { .. })));
}

#[test]
fn should_forward_negotiated_queue_ttl_with_unchanged_core_bytes() {
    // Arrange
    let mut session = queue_session(&[crate::protocol::wire3::QUEUE_MESSAGE_TTL_CAPABILITY_ID]);
    // Act
    let message = session.decode_frame(&ttl_frame()).unwrap();
    // Assert
    assert_eq!(message.payload.as_ref(), b"enqueue-core");
    assert_eq!(
        message
            .extensions
            .queue_ttl_hours
            .map(std::num::NonZeroU32::get),
        Some(2)
    );
}

#[test]
fn should_ignore_unknown_client_capabilities_when_gating_queue_ttl() {
    // Arrange
    let mut session = queue_session(&[u32::MAX]);
    // Act
    let result = session.decode_frame(&ttl_frame());
    // Assert
    assert!(matches!(result, Err(SessionError::Unsupported { .. })));
}
