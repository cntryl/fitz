//! The one response envelope every verb in every domain uses.

use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};

/// The shared meaning of an error, independent of its domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ErrorClass {
    Domain = 0,
    Unauthorized = 1,
    InvalidRoute = 2,
    InvalidPattern = 3,
    RegistrationLimit = 4,
    Busy = 5,
    Backend = 6,
    Timeout = 7,
    BadRequest = 8,
    NotFound = 9,
    Unsupported = 10,
}

impl ErrorClass {
    /// Every class in wire order.
    pub const ALL: [Self; 11] = [
        Self::Domain,
        Self::Unauthorized,
        Self::InvalidRoute,
        Self::InvalidPattern,
        Self::RegistrationLimit,
        Self::Busy,
        Self::Backend,
        Self::Timeout,
        Self::BadRequest,
        Self::NotFound,
        Self::Unsupported,
    ];

    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Domain),
            1 => Some(Self::Unauthorized),
            2 => Some(Self::InvalidRoute),
            3 => Some(Self::InvalidPattern),
            4 => Some(Self::RegistrationLimit),
            5 => Some(Self::Busy),
            6 => Some(Self::Backend),
            7 => Some(Self::Timeout),
            8 => Some(Self::BadRequest),
            9 => Some(Self::NotFound),
            10 => Some(Self::Unsupported),
            _ => None,
        }
    }

    /// The low two digits of a domain-block code that carries this class, or
    /// `None` for `Domain`, which has no shared slot.
    #[must_use]
    pub const fn shared_slot(self) -> Option<u16> {
        match self {
            Self::Domain => None,
            other => Some(other as u16),
        }
    }
}

/// Whether the failed request took effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Effect {
    NotApplied = 0,
    Indeterminate = 1,
    Applied = 2,
}

impl Effect {
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::NotApplied),
            1 => Some(Self::Indeterminate),
            2 => Some(Self::Applied),
            _ => None,
        }
    }
}

/// An error as the client sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    pub code: u16,
    pub class: ErrorClass,
    pub effect: Effect,
    /// Diagnostic only. Clients must not branch on it.
    pub message: String,
}

/// A decoded response body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response<'a> {
    /// The domain payload that follows the status byte.
    Success(&'a [u8]),
    Error(WireError),
}

/// Why a response body could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeError {
    Empty,
    UnknownStatus(u8),
    UnknownClass(u8),
    UnknownEffect(u8),
    Malformed(String),
    TrailingBytes,
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "response body is empty"),
            Self::UnknownStatus(status) => write!(f, "unknown response status {status}"),
            Self::UnknownClass(class) => write!(f, "unknown error class {class}"),
            Self::UnknownEffect(effect) => write!(f, "unknown error effect {effect}"),
            Self::Malformed(reason) => write!(f, "malformed error envelope: {reason}"),
            Self::TrailingBytes => write!(f, "trailing bytes after error envelope"),
        }
    }
}

impl std::error::Error for EnvelopeError {}

const STATUS_SUCCESS: u8 = 0;
const STATUS_ERROR: u8 = 1;

/// Encode a success response: the status byte followed by `payload`.
#[must_use]
pub fn encode_success(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(STATUS_SUCCESS);
    out.extend_from_slice(payload);
    out
}

/// Encode an error response.
#[must_use]
pub fn encode_error(error: &WireError) -> Vec<u8> {
    let mut enc = PayloadEncoder::with_capacity(1 + 2 + 1 + 1 + 4 + error.message.len());
    enc.put_u8(STATUS_ERROR);
    enc.put_u16(error.code);
    enc.put_u8(error.class as u8);
    enc.put_u8(error.effect as u8);
    enc.put_string(&error.message);
    enc.finish()
}

/// Decode a response body. A success returns the payload slice untouched; an
/// error must be exactly one envelope.
///
/// # Errors
///
/// Every [`EnvelopeError`] variant.
pub fn decode_response(body: &[u8]) -> Result<Response<'_>, EnvelopeError> {
    let (&status, rest) = body.split_first().ok_or(EnvelopeError::Empty)?;
    match status {
        STATUS_SUCCESS => Ok(Response::Success(rest)),
        STATUS_ERROR => {
            let mut dec = PayloadDecoder::new(rest);
            let code = dec.get_u16().map_err(EnvelopeError::Malformed)?;
            let class = dec.get_u8().map_err(EnvelopeError::Malformed)?;
            let class = ErrorClass::from_u8(class).ok_or(EnvelopeError::UnknownClass(class))?;
            let effect = dec.get_u8().map_err(EnvelopeError::Malformed)?;
            let effect = Effect::from_u8(effect).ok_or(EnvelopeError::UnknownEffect(effect))?;
            let message = dec.get_string().map_err(EnvelopeError::Malformed)?;
            if !dec.is_complete() {
                return Err(EnvelopeError::TrailingBytes);
            }
            Ok(Response::Error(WireError {
                code,
                class,
                effect,
                message,
            }))
        }
        other => Err(EnvelopeError::UnknownStatus(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_error() -> WireError {
        WireError {
            code: 2050,
            class: ErrorClass::Domain,
            effect: Effect::NotApplied,
            message: "concurrency conflict".to_owned(),
        }
    }

    #[test]
    fn should_encode_the_documented_error_layout() {
        // Arrange
        let error = WireError {
            code: 7005,
            class: ErrorClass::Busy,
            effect: Effect::NotApplied,
            message: "ok".to_owned(),
        };

        // Act
        let bytes = encode_error(&error);

        // Assert
        assert_eq!(bytes, [1, 0x1B, 0x5D, 5, 0, 0, 0, 0, 2, b'o', b'k']);
    }

    #[test]
    fn should_round_trip_an_error_envelope() {
        // Arrange
        let error = sample_error();

        let bytes = encode_error(&error);

        // Act
        let decoded = decode_response(&bytes).expect("decode");

        // Assert
        assert_eq!(decoded, Response::Error(error));
    }

    #[test]
    fn should_return_the_success_payload_untouched() {
        // Arrange
        let body = encode_success(&[9, 8, 7]);

        // Act
        let decoded = decode_response(&body).expect("decode");

        // Assert
        assert_eq!(decoded, Response::Success(&[9, 8, 7][..]));
    }

    #[test]
    fn should_reject_unknown_status_class_effect_and_trailing_bytes() {
        // Arrange
        let mut unknown_class = encode_error(&sample_error());
        unknown_class[3] = 11;
        let mut unknown_effect = encode_error(&sample_error());
        unknown_effect[4] = 3;
        let mut trailing = encode_error(&sample_error());
        trailing.push(0);

        // Act
        let results = [
            decode_response(&[]),
            decode_response(&[2]),
            decode_response(&unknown_class),
            decode_response(&unknown_effect),
            decode_response(&trailing),
        ];

        // Assert
        assert_eq!(
            results,
            [
                Err(EnvelopeError::Empty),
                Err(EnvelopeError::UnknownStatus(2)),
                Err(EnvelopeError::UnknownClass(11)),
                Err(EnvelopeError::UnknownEffect(3)),
                Err(EnvelopeError::TrailingBytes),
            ]
        );
    }

    #[test]
    fn should_map_every_class_to_its_shared_slot() {
        // Arrange
        let classes = ErrorClass::ALL;

        // Act
        let slots = classes.map(ErrorClass::shared_slot);

        // Assert
        assert_eq!(
            slots,
            [
                None,
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(10)
            ]
        );
        for class in classes {
            assert_eq!(ErrorClass::from_u8(class as u8), Some(class));
        }
    }
}
