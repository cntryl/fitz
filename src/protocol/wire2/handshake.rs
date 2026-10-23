//! `CONNECT`, `HELLO`, `CONNECT_REJECTED`, and `GOAWAY` bodies, plus version
//! negotiation.

use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};

/// What a client says first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub min_version: u16,
    pub max_version: u16,
    pub client_caps: u32,
    /// Compact JWT; empty in anonymous mode.
    pub jwt: String,
    pub service_name: String,
    pub client_build: String,
}

/// What the broker answers when it accepts a `CONNECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    pub broker_caps: u32,
    pub max_message_bytes: u32,
    pub max_in_flight: u32,
    pub session_id: u64,
}

/// What the broker answers when it refuses a `CONNECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRejected {
    pub code: u16,
    pub message: String,
}

/// A broker-initiated notice that the connection is ending or changing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoAway {
    pub code: u16,
    /// How long the broker keeps serving before it closes; 0 means now.
    pub grace_ms: u32,
    pub reason: String,
}

/// The minimum size of a `HELLO` body. Readers ignore bytes beyond the fields
/// they know, so a later broker can append fields.
pub const HELLO_MIN_LEN: usize = 2 + 4 + 4 + 4 + 8;

/// Pick the wire version both sides speak: the highest common value.
#[must_use]
pub const fn negotiate_version(
    client_min: u16,
    client_max: u16,
    broker_min: u16,
    broker_max: u16,
) -> Option<u16> {
    let low = if client_min > broker_min {
        client_min
    } else {
        broker_min
    };
    let high = if client_max < broker_max {
        client_max
    } else {
        broker_max
    };
    if low <= high {
        Some(high)
    } else {
        None
    }
}

impl Connect {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = PayloadEncoder::new();
        enc.put_u16(self.min_version);
        enc.put_u16(self.max_version);
        enc.put_u32(self.client_caps);
        enc.put_string(&self.jwt);
        enc.put_string(&self.service_name);
        enc.put_string(&self.client_build);
        enc.finish()
    }

    /// # Errors
    ///
    /// Returns a description when the body is truncated, malformed, has an
    /// inverted version range, or carries trailing bytes.
    pub fn decode(value: &[u8]) -> Result<Self, String> {
        let mut dec = PayloadDecoder::new(value);
        let connect = Self {
            min_version: dec.get_u16()?,
            max_version: dec.get_u16()?,
            client_caps: dec.get_u32()?,
            jwt: dec.get_string()?,
            service_name: dec.get_string()?,
            client_build: dec.get_string()?,
        };
        if !dec.is_complete() {
            return Err("trailing bytes after CONNECT".to_owned());
        }
        if connect.min_version > connect.max_version {
            return Err(format!(
                "CONNECT version range is inverted: {}..{}",
                connect.min_version, connect.max_version
            ));
        }
        Ok(connect)
    }
}

impl Hello {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = PayloadEncoder::new();
        enc.put_u16(self.version);
        enc.put_u32(self.broker_caps);
        enc.put_u32(self.max_message_bytes);
        enc.put_u32(self.max_in_flight);
        enc.put_u64(self.session_id);
        enc.finish()
    }

    /// Decode the known fields and ignore anything a newer broker appended.
    ///
    /// # Errors
    ///
    /// Returns a description when the body is shorter than [`HELLO_MIN_LEN`].
    pub fn decode(value: &[u8]) -> Result<Self, String> {
        if value.len() < HELLO_MIN_LEN {
            return Err(format!(
                "HELLO needs at least {HELLO_MIN_LEN} bytes, got {}",
                value.len()
            ));
        }
        let mut dec = PayloadDecoder::new(value);
        Ok(Self {
            version: dec.get_u16()?,
            broker_caps: dec.get_u32()?,
            max_message_bytes: dec.get_u32()?,
            max_in_flight: dec.get_u32()?,
            session_id: dec.get_u64()?,
        })
    }
}

impl ConnectRejected {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = PayloadEncoder::new();
        enc.put_u16(self.code);
        enc.put_string(&self.message);
        enc.finish()
    }

    /// # Errors
    ///
    /// Returns a description when the body is truncated or carries trailing bytes.
    pub fn decode(value: &[u8]) -> Result<Self, String> {
        let mut dec = PayloadDecoder::new(value);
        let rejected = Self {
            code: dec.get_u16()?,
            message: dec.get_string()?,
        };
        if !dec.is_complete() {
            return Err("trailing bytes after CONNECT_REJECTED".to_owned());
        }
        Ok(rejected)
    }
}

impl GoAway {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = PayloadEncoder::new();
        enc.put_u16(self.code);
        enc.put_u32(self.grace_ms);
        enc.put_string(&self.reason);
        enc.finish()
    }

    /// # Errors
    ///
    /// Returns a description when the body is truncated or carries trailing bytes.
    pub fn decode(value: &[u8]) -> Result<Self, String> {
        let mut dec = PayloadDecoder::new(value);
        let goaway = Self {
            code: dec.get_u16()?,
            grace_ms: dec.get_u32()?,
            reason: dec.get_string()?,
        };
        if !dec.is_complete() {
            return Err("trailing bytes after GOAWAY".to_owned());
        }
        Ok(goaway)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_connect() -> Connect {
        Connect {
            min_version: 2,
            max_version: 3,
            client_caps: 0b101,
            jwt: "eyJ.header.sig".to_owned(),
            service_name: "orders".to_owned(),
            client_build: "fitz-ts/2.0.0".to_owned(),
        }
    }

    #[test]
    fn should_round_trip_connect() {
        // Arrange
        let connect = sample_connect();

        // Act
        let decoded = Connect::decode(&connect.encode()).expect("decode");

        // Assert
        assert_eq!(decoded, connect);
    }

    #[test]
    fn should_reject_connect_with_trailing_bytes() {
        // Arrange
        let mut bytes = sample_connect().encode();
        bytes.push(0);

        // Act
        let error = Connect::decode(&bytes).expect_err("trailing");

        // Assert
        assert!(error.contains("trailing"));
    }

    #[test]
    fn should_reject_connect_with_inverted_version_range() {
        // Arrange
        let mut connect = sample_connect();
        connect.min_version = 4;
        connect.max_version = 2;

        // Act
        let error = Connect::decode(&connect.encode()).expect_err("inverted");

        // Assert
        assert!(error.contains("inverted"));
    }

    #[test]
    fn should_negotiate_the_highest_common_version() {
        // Arrange
        let cases = [
            ((2, 3), (2, 2), Some(2)),
            ((2, 5), (3, 4), Some(4)),
            ((2, 2), (3, 3), None),
            ((4, 6), (2, 3), None),
        ];

        // Act
        let results = cases
            .iter()
            .map(|((cmin, cmax), (bmin, bmax), _)| negotiate_version(*cmin, *cmax, *bmin, *bmax))
            .collect::<Vec<_>>();

        // Assert
        let expected = cases.iter().map(|(_, _, want)| *want).collect::<Vec<_>>();
        assert_eq!(results, expected);
    }

    #[test]
    fn should_decode_hello_while_ignoring_appended_fields() {
        // Arrange
        let hello = Hello {
            version: 2,
            broker_caps: 7,
            max_message_bytes: 1 << 20,
            max_in_flight: 64,
            session_id: 99,
        };
        let mut bytes = hello.encode();
        bytes.extend_from_slice(&[1, 2, 3, 4, 5]);

        // Act
        let decoded = Hello::decode(&bytes).expect("decode");

        // Assert
        assert_eq!(decoded, hello);
        assert_eq!(hello.encode().len(), HELLO_MIN_LEN);
    }

    #[test]
    fn should_reject_a_hello_shorter_than_its_fixed_fields() {
        // Arrange
        let bytes = [0_u8; HELLO_MIN_LEN - 1];

        // Act
        let error = Hello::decode(&bytes).expect_err("short");

        // Assert
        assert!(error.contains("at least"));
    }

    #[test]
    fn should_round_trip_connect_rejected_and_goaway() {
        // Arrange
        let rejected = ConnectRejected {
            code: 50,
            message: "no common version".to_owned(),
        };
        let goaway = GoAway {
            code: 54,
            grace_ms: 25_000,
            reason: "broker draining for redeploy".to_owned(),
        };

        // Act
        let rejected_decoded = ConnectRejected::decode(&rejected.encode()).expect("rejected");
        let goaway_decoded = GoAway::decode(&goaway.encode()).expect("goaway");

        // Assert
        assert_eq!(rejected_decoded, rejected);
        assert_eq!(goaway_decoded, goaway);
    }
}
