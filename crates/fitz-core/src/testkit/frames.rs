fn next_correlation() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .max(1)
}

/// Wire 3 message builder for protocol test frames.
/// Builds one message with a 15-byte header followed by the value.
/// One message per transport frame, so `encode_field` may be called once.
pub struct FrameBuilder {
    message: Option<Vec<u8>>,
    correlation: Option<std::num::NonZeroU64>,
    flags: crate::protocol::wire2::header::HeaderFlags,
}

impl FrameBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            message: None,
            correlation: std::num::NonZeroU64::new(next_correlation()),
            flags: crate::protocol::wire2::header::HeaderFlags::none(),
        }
    }

    /// Label the message with a client correlation; zero means none.
    #[must_use]
    pub fn with_correlation(mut self, correlation: u64) -> Self {
        self.correlation = std::num::NonZeroU64::new(correlation);
        self
    }

    /// Set the header flags on the message.
    #[must_use]
    pub fn with_flags(mut self, flags: crate::protocol::wire2::header::HeaderFlags) -> Self {
        self.flags = flags;
        self
    }

    /// # Panics
    ///
    /// Panics when called twice: a frame carries exactly one message.
    pub fn encode_field(&mut self, msg_type: u16, value: &[u8]) {
        assert!(
            self.message.is_none(),
            "Wire 3 carries one message per frame"
        );
        self.message = Some(crate::protocol::wire2::header::encode_message(
            msg_type,
            self.flags,
            if crate::protocol::manifest::client_entry(crate::protocol::message::MessageType::new(
                msg_type,
            ))
            .is_ok()
                && msg_type != 1
                && msg_type != 303
            {
                self.correlation
            } else {
                None
            },
            value,
        ));
    }

    /// # Panics
    ///
    /// Panics when no message was encoded.
    #[must_use]
    pub fn build(self) -> Vec<u8> {
        self.message.expect("a frame needs exactly one message")
    }
}

/// Reads the one Wire 3 message in a frame.
pub struct FrameParser<'a> {
    buf: &'a [u8],
    consumed: bool,
}

impl<'a> FrameParser<'a> {
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            consumed: false,
        }
    }

    /// The message's header, when the frame is well formed.
    #[must_use]
    pub fn header(&self) -> Option<crate::protocol::wire2::header::MessageHeader> {
        crate::protocol::wire3::header::MessageHeader::decode_wire3(self.buf).ok()
    }

    pub fn next_field_ref(&mut self) -> Option<(u16, &'a [u8])> {
        if self.consumed {
            return None;
        }
        self.consumed = true;
        let (header, value) =
            crate::protocol::wire3::header::decode_message(self.buf, u32::MAX).ok()?;
        Some((header.message_type, value))
    }

    pub fn next_field(&mut self) -> Option<(u16, Vec<u8>)> {
        self.next_field_ref()
            .map(|(msg_type, value)| (msg_type, value.to_vec()))
    }

    pub fn parse_all(&mut self) -> Vec<(u16, Vec<u8>)> {
        let mut fields = Vec::new();
        while let Some(field) = self.next_field() {
            fields.push(field);
        }
        fields
    }
}

impl Default for FrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

use bytes::Bytes;

/// Extract the message value from a test frame for direct-to-sink delivery.
/// # Panics
/// This function panics when the frame is not exactly one valid Fitz message.
#[must_use]
pub fn extract_single_tlv_field(frame: &[u8]) -> (u16, Bytes) {
    let mut parser = FrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref().expect("single Fitz message");
    (msg_type, Bytes::copy_from_slice(payload))
}
