//! Length-delimited Wire 3 core and extension payloads.

use std::borrow::Cow;
use std::fmt;

pub const CONNECT_MESSAGE_TYPE: u16 = 1;
pub const HELLO_MESSAGE_TYPE: u16 = 2;
pub const CAPABILITY_IDS_FIELD_ID: u16 = 1;
const EXTENSION_HEADER_LEN: usize = 6;
const CAPABILITY_COUNT_LEN: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionDirection {
    ClientToBroker,
    BrokerToClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionError {
    CoreTooLarge,
    CoreLengthMissing,
    CoreLengthOutOfBounds,
    FieldHeaderTruncated,
    FieldIdReserved,
    FieldLengthOverflow,
    FieldValueTruncated,
    NoExtensionFields,
    KnownSingletonRepeated,
    CapabilityListTruncated,
    CapabilityListLengthMismatch,
    CapabilityListMissing,
    CapabilityIdReserved,
    CapabilityIdsOutOfOrder,
    FieldsOutOfOrder,
    DuplicateField,
    FieldTooLarge,
    PayloadTooLarge,
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::CoreTooLarge => "core exceeds the u32 length field",
            Self::CoreLengthMissing => "core length is truncated",
            Self::CoreLengthOutOfBounds => "core length exceeds payload bounds",
            Self::FieldHeaderTruncated => "extension field header is truncated",
            Self::FieldIdReserved => "extension field ID zero is reserved",
            Self::FieldLengthOverflow => "extension field length overflows address space",
            Self::FieldValueTruncated => "extension field value exceeds payload bounds",
            Self::NoExtensionFields => "extension flag is set but no fields are present",
            Self::KnownSingletonRepeated => "known singleton field occurs more than once",
            Self::CapabilityListTruncated => "capability list is shorter than its count field",
            Self::CapabilityListLengthMismatch => "capability list length does not match its count",
            Self::CapabilityListMissing => "handshake capability-ID field is missing",
            Self::CapabilityIdReserved => "capability ID zero is reserved",
            Self::CapabilityIdsOutOfOrder => "capability IDs are not strictly ascending",
            Self::FieldsOutOfOrder => "extension fields are not in ascending field-ID order",
            Self::DuplicateField => {
                "encoder repeated a field without a repeatable-field registration"
            }
            Self::FieldTooLarge => "extension field exceeds the u32 length field",
            Self::PayloadTooLarge => "complete payload exceeds the u32 length field",
        };
        f.write_str(message)
    }
}

impl std::error::Error for ExtensionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorePayloadRange {
    pub offset: usize,
    pub length: usize,
}

impl CorePayloadRange {
    #[must_use]
    pub fn get(self, payload: &[u8]) -> &[u8] {
        &payload[self.offset..self.offset + self.length]
    }

    #[must_use]
    pub const fn as_range(self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.length
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtensionField<'a> {
    pub field_id: u16,
    pub value: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPayload<'a> {
    pub value: Cow<'a, [u8]>,
    pub extensions_present: bool,
}

fn is_capability_field(direction: ExtensionDirection, message_type: u16, field_id: u16) -> bool {
    field_id == CAPABILITY_IDS_FIELD_ID
        && match direction {
            ExtensionDirection::ClientToBroker => message_type == CONNECT_MESSAGE_TYPE,
            ExtensionDirection::BrokerToClient => message_type == HELLO_MESSAGE_TYPE,
        }
}

fn requires_capability_field(direction: ExtensionDirection, message_type: u16) -> bool {
    matches!(
        (direction, message_type),
        (ExtensionDirection::ClientToBroker, CONNECT_MESSAGE_TYPE)
            | (ExtensionDirection::BrokerToClient, HELLO_MESSAGE_TYPE)
    )
}

fn validate_payload_length(length: usize) -> Result<(), ExtensionError> {
    u32::try_from(length)
        .map(|_| ())
        .map_err(|_| ExtensionError::PayloadTooLarge)
}

fn validate_capability_ids(value: &[u8]) -> Result<(), ExtensionError> {
    if value.len() < CAPABILITY_COUNT_LEN {
        return Err(ExtensionError::CapabilityListTruncated);
    }
    let count = usize::from(u16::from_be_bytes([value[0], value[1]]));
    let expected = count
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(CAPABILITY_COUNT_LEN))
        .ok_or(ExtensionError::CapabilityListLengthMismatch)?;
    if value.len() != expected {
        return Err(ExtensionError::CapabilityListLengthMismatch);
    }
    let mut previous = 0;
    let (capability_chunks, _) = value[CAPABILITY_COUNT_LEN..].as_chunks::<4>();
    for chunk in capability_chunks {
        let capability_id = u32::from_be_bytes(*chunk);
        if capability_id == 0 {
            return Err(ExtensionError::CapabilityIdReserved);
        }
        if capability_id <= previous {
            return Err(ExtensionError::CapabilityIdsOutOfOrder);
        }
        previous = capability_id;
    }
    Ok(())
}

/// Encode a capability set in its canonical ascending-ID representation.
///
/// # Errors
///
/// Returns an error for too many IDs, the reserved ID zero, or unsorted IDs.
pub fn encode_capability_ids(ids: &[u32]) -> Result<Vec<u8>, ExtensionError> {
    let count =
        u16::try_from(ids.len()).map_err(|_| ExtensionError::CapabilityListLengthMismatch)?;
    let capacity = ids
        .len()
        .checked_mul(4)
        .and_then(|length| length.checked_add(CAPABILITY_COUNT_LEN))
        .ok_or(ExtensionError::CapabilityListLengthMismatch)?;
    let mut value = Vec::with_capacity(capacity);
    value.extend_from_slice(&count.to_be_bytes());
    let mut previous = 0;
    for capability_id in ids {
        if *capability_id == 0 {
            return Err(ExtensionError::CapabilityIdReserved);
        }
        if *capability_id <= previous {
            return Err(ExtensionError::CapabilityIdsOutOfOrder);
        }
        value.extend_from_slice(&capability_id.to_be_bytes());
        previous = *capability_id;
    }
    Ok(value)
}

/// Validate a Wire 3 payload and return the core's borrowed byte range.
/// Unknown extension values are only inspected for their declared bounds;
/// they are neither copied nor retained.
///
/// # Errors
///
/// Returns an error for truncated or out-of-bounds core and extension lengths,
/// reserved field IDs, malformed known fields (including unordered capability
/// IDs), or a missing required handshake capability list. A core-only CONNECT
/// is returned for version-mismatch handling; the session rejects it if its
/// version range includes Wire 3.
pub fn decode_core_payload(
    payload: &[u8],
    message_type: u16,
    direction: ExtensionDirection,
    extensions_present: bool,
) -> Result<CorePayloadRange, ExtensionError> {
    if !extensions_present {
        if direction == ExtensionDirection::BrokerToClient && message_type == HELLO_MESSAGE_TYPE {
            return Err(ExtensionError::CapabilityListMissing);
        }
        // Preserve legacy core-only CONNECT so the session can return a
        // structured VERSION_MISMATCH to clients that cannot negotiate Wire 3.
        return Ok(CorePayloadRange {
            offset: 0,
            length: payload.len(),
        });
    }
    if payload.len() < 4 {
        return Err(ExtensionError::CoreLengthMissing);
    }
    let core_length = usize::try_from(u32::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
    .map_err(|_| ExtensionError::CoreLengthOutOfBounds)?;
    let core_start: usize = 4;
    let core_end = core_start
        .checked_add(core_length)
        .ok_or(ExtensionError::CoreLengthOutOfBounds)?;
    if core_end > payload.len() {
        return Err(ExtensionError::CoreLengthOutOfBounds);
    }

    let mut cursor = core_end;
    let mut capability_field_seen = false;
    let mut extension_field_seen = false;
    while cursor < payload.len() {
        let remaining = payload.len() - cursor;
        if remaining < EXTENSION_HEADER_LEN {
            return Err(ExtensionError::FieldHeaderTruncated);
        }
        let field_id = u16::from_be_bytes([payload[cursor], payload[cursor + 1]]);
        if field_id == 0 {
            return Err(ExtensionError::FieldIdReserved);
        }
        let value_length = usize::try_from(u32::from_be_bytes([
            payload[cursor + 2],
            payload[cursor + 3],
            payload[cursor + 4],
            payload[cursor + 5],
        ]))
        .map_err(|_| ExtensionError::FieldLengthOverflow)?;
        let value_start = cursor + EXTENSION_HEADER_LEN;
        let value_end = value_start
            .checked_add(value_length)
            .ok_or(ExtensionError::FieldLengthOverflow)?;
        if value_end > payload.len() {
            return Err(ExtensionError::FieldValueTruncated);
        }

        if is_capability_field(direction, message_type, field_id) {
            if capability_field_seen {
                return Err(ExtensionError::KnownSingletonRepeated);
            }
            capability_field_seen = true;
            validate_capability_ids(&payload[value_start..value_end])?;
        }
        extension_field_seen = true;
        cursor = value_end;
    }

    if !extension_field_seen {
        return Err(ExtensionError::NoExtensionFields);
    }
    if requires_capability_field(direction, message_type) && !capability_field_seen {
        return Err(ExtensionError::CapabilityListMissing);
    }

    Ok(CorePayloadRange {
        offset: core_start,
        length: core_length,
    })
}

/// Encode a core and sorted extension fields. With no extensions, the core is
/// borrowed unchanged and no payload prefix is emitted.
///
/// # Errors
///
/// Returns an error for reserved, duplicated, or out-of-order field IDs,
/// oversized core/field/payload lengths, malformed capability lists, or a
/// missing required handshake capability list.
pub fn encode_payload<'a>(
    core: &'a [u8],
    message_type: u16,
    direction: ExtensionDirection,
    fields: &[ExtensionField<'_>],
) -> Result<EncodedPayload<'a>, ExtensionError> {
    if fields.is_empty() {
        if requires_capability_field(direction, message_type) {
            return Err(ExtensionError::CapabilityListMissing);
        }
        u32::try_from(core.len()).map_err(|_| ExtensionError::CoreTooLarge)?;
        return Ok(EncodedPayload {
            value: Cow::Borrowed(core),
            extensions_present: false,
        });
    }
    let core_length = u32::try_from(core.len()).map_err(|_| ExtensionError::CoreTooLarge)?;
    let mut total_length = 4_usize
        .checked_add(core.len())
        .ok_or(ExtensionError::PayloadTooLarge)?;
    let mut previous_field_id = 0;
    let mut capability_field_seen = false;
    for field in fields {
        if field.field_id == 0 {
            return Err(ExtensionError::FieldIdReserved);
        }
        if field.field_id < previous_field_id {
            return Err(ExtensionError::FieldsOutOfOrder);
        }
        if field.field_id == previous_field_id {
            return Err(ExtensionError::DuplicateField);
        }
        previous_field_id = field.field_id;
        let value_length =
            u32::try_from(field.value.len()).map_err(|_| ExtensionError::FieldTooLarge)?;
        if is_capability_field(direction, message_type, field.field_id) {
            if capability_field_seen {
                return Err(ExtensionError::KnownSingletonRepeated);
            }
            capability_field_seen = true;
            validate_capability_ids(field.value)?;
        }
        total_length = total_length
            .checked_add(EXTENSION_HEADER_LEN)
            .and_then(|length| length.checked_add(usize::try_from(value_length).ok()?))
            .ok_or(ExtensionError::PayloadTooLarge)?;
    }

    if requires_capability_field(direction, message_type) && !capability_field_seen {
        return Err(ExtensionError::CapabilityListMissing);
    }
    validate_payload_length(total_length)?;

    let mut value = Vec::with_capacity(total_length);
    value.extend_from_slice(&core_length.to_be_bytes());
    value.extend_from_slice(core);
    for field in fields {
        value.extend_from_slice(&field.field_id.to_be_bytes());
        let value_length =
            u32::try_from(field.value.len()).map_err(|_| ExtensionError::FieldTooLarge)?;
        value.extend_from_slice(&value_length.to_be_bytes());
        value.extend_from_slice(field.value);
    }
    Ok(EncodedPayload {
        value: Cow::Owned(value),
        extensions_present: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(range.get(&encoded.value), core);
    }

    #[test]
    fn should_skip_bounded_unknown_extensions_without_changing_core_range() {
        // Arrange
        let encoded = payload(b"core", &[(8, b"future"), (9, b"")]);

        // Act
        let range =
            decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true).unwrap();

        // Assert
        assert_eq!(range.get(&encoded), b"core");
    }

    #[test]
    fn should_reject_truncated_extension_header() {
        // Arrange
        let mut encoded = payload(b"core", &[]);
        encoded.extend_from_slice(&[0, 8, 0, 0]);

        // Act
        let error = decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true)
            .unwrap_err();

        // Assert
        assert_eq!(error, ExtensionError::FieldHeaderTruncated);
    }

    #[test]
    fn should_reject_extension_flag_without_any_fields() {
        // Arrange
        let encoded = payload(b"core", &[]);

        // Act
        let error = decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true)
            .unwrap_err();

        // Assert
        assert_eq!(error, ExtensionError::NoExtensionFields);
    }

    #[test]
    fn should_reject_truncated_extension_value() {
        // Arrange
        let mut encoded = payload(b"core", &[]);
        encoded.extend_from_slice(&[0, 8, 0, 0, 0, 4, b'x']);

        // Act
        let error = decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true)
            .unwrap_err();

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
        let error = decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true)
            .unwrap_err();

        // Assert
        assert_eq!(error, ExtensionError::CoreLengthOutOfBounds);
    }

    #[test]
    fn should_reject_zero_extension_field_id() {
        // Arrange
        let encoded = payload(b"core", &[(0, b"")]);

        // Act
        let error = decode_core_payload(&encoded, 104, ExtensionDirection::ClientToBroker, true)
            .unwrap_err();

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
        assert_eq!(range.get(&encoded), b"hello");
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
        assert_eq!(range.get(&encoded.value), b"core");
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
        assert_eq!(range.get(&encoded), b"core");
    }
}
