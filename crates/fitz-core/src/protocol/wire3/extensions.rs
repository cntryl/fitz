//! Length-delimited Wire 3 core and extension payloads.

use std::borrow::Cow;
use std::fmt;

pub const CONNECT_MESSAGE_TYPE: u16 = 1;
pub const HELLO_MESSAGE_TYPE: u16 = 2;
pub const CAPABILITY_IDS_FIELD_ID: u16 = 1;
pub const QUEUE_TTL_HOURS_FIELD_ID: u16 = 1;
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
    InvalidQueueTtl,
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
            Self::InvalidQueueTtl => "queue TTL must be a positive u32 hour count",
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

/// Known optional fields retained after bounds validation. Unknown fields are skipped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KnownFields {
    pub queue_ttl_hours: Option<std::num::NonZeroU32>,
}

impl KnownFields {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            queue_ttl_hours: None,
        }
    }
}

pub struct DecodedPayload<'a> {
    pub core: CorePayloadRange,
    pub fields: KnownFields,
    pub capability_ids: Option<&'a [u8]>,
}

impl CorePayloadRange {
    /// Return the core slice when this range fits the supplied payload.
    #[must_use]
    pub fn get(self, payload: &[u8]) -> Option<&[u8]> {
        self.as_range().and_then(|range| payload.get(range))
    }

    /// Return the byte range when its end can be represented.
    #[must_use]
    pub fn as_range(self) -> Option<std::ops::Range<usize>> {
        self.offset
            .checked_add(self.length)
            .map(|end| self.offset..end)
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
    decode_payload(payload, message_type, direction, extensions_present).map(|decoded| decoded.core)
}

/// Validate the envelope and retain only registered fields.
///
/// # Errors
/// Returns the same malformed-envelope errors as [`decode_core_payload`].
pub fn decode_payload(
    payload: &[u8],
    message_type: u16,
    direction: ExtensionDirection,
    extensions_present: bool,
) -> Result<DecodedPayload<'_>, ExtensionError> {
    if !extensions_present {
        if direction == ExtensionDirection::BrokerToClient && message_type == HELLO_MESSAGE_TYPE {
            return Err(ExtensionError::CapabilityListMissing);
        }
        // Preserve legacy core-only CONNECT so the session can return a
        // structured VERSION_MISMATCH to clients that cannot negotiate Wire 3.
        return Ok(DecodedPayload {
            core: CorePayloadRange {
                offset: 0,
                length: payload.len(),
            },
            fields: KnownFields::default(),
            capability_ids: None,
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
    let mut fields = KnownFields::default();
    let mut capability_ids = None;
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
            capability_ids = Some(&payload[value_start + 2..value_end]);
        }
        if is_queue_ttl_field(direction, message_type, field_id) {
            if fields.queue_ttl_hours.is_some() {
                return Err(ExtensionError::KnownSingletonRepeated);
            }
            fields.queue_ttl_hours = Some(queue_ttl_hours(&payload[value_start..value_end])?);
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

    Ok(DecodedPayload {
        core: CorePayloadRange {
            offset: core_start,
            length: core_length,
        },
        fields,
        capability_ids,
    })
}

fn is_queue_ttl_field(direction: ExtensionDirection, message_type: u16, field_id: u16) -> bool {
    direction == ExtensionDirection::ClientToBroker
        && message_type == 200
        && field_id == QUEUE_TTL_HOURS_FIELD_ID
}

fn queue_ttl_hours(value: &[u8]) -> Result<std::num::NonZeroU32, ExtensionError> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| ExtensionError::InvalidQueueTtl)?;
    std::num::NonZeroU32::new(u32::from_be_bytes(bytes)).ok_or(ExtensionError::InvalidQueueTtl)
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
        if is_queue_ttl_field(direction, message_type, field.field_id) {
            queue_ttl_hours(field.value)?;
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
mod queue_tests;
#[cfg(test)]
mod tests;
