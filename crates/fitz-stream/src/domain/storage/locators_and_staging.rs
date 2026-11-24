use super::super::store::EventPayload;
use super::{
    decode_single_u64_value, decode_two_u64_value, encode_single_u64_value, encode_two_u64_value,
    AreaCounterValue, CompactRealmPageValue, CompressedCompactRealmPageValue, KeyPrefix,
    OffsetCounterValue, RealmCounterValue, ResourceMetaValue, WatermarkValue,
    COMPRESSED_COMPACT_REALM_PAGE_VALUE_V2_MARKER,
};
use bytes::Bytes;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl CompressedCompactRealmPageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V2_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let compact_page = CompactRealmPageValue {
            records: self.records.clone(),
        };
        let compressed_payload = compress_prepend_size(&compact_page.encode());

        let mut bytes = Vec::with_capacity(2 + compressed_payload.len());
        bytes.extend_from_slice(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V2_MARKER);
        bytes.extend_from_slice(&compressed_payload);
        bytes
    }

    #[cfg(test)]
    /// # Panics
    ///
    /// Panics when test fixtures contain an invalid compressed page.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compressed compact realm page value")
    }

    #[must_use]
    pub fn into_compact_realm_page(self) -> CompactRealmPageValue {
        CompactRealmPageValue {
            records: self.records,
        }
    }

    /// # Errors
    ///
    /// Returns an error if the marker is missing, the compressed payload is
    /// absent, decompression fails, or the decoded compact realm page is
    /// invalid.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compressed compact realm page value: missing marker".to_string());
        }
        if bytes.len() <= 2 {
            return Err("decode compressed compact realm page value: payload missing".to_string());
        }

        let decompressed = decompress_size_prepended(&bytes[2..]).map_err(|error| {
            format!("decode compressed compact realm page value: decompress failed: {error}")
        })?;
        let page = CompactRealmPageValue::try_decode(&decompressed)?;

        Ok(Self {
            records: page.records,
        })
    }
}

impl WatermarkValue {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_single_u64_value(0x01, self.watermark)
    }

    /// # Errors
    ///
    /// Returns an error if `bytes` are not a valid encoded watermark value.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        Ok(Self {
            watermark: decode_single_u64_value(bytes, 0x01, "decode watermark value")?,
        })
    }
}

impl OffsetCounterValue {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_single_u64_value(0x02, self.next_offset)
    }

    /// # Errors
    ///
    /// Returns an error if `bytes` are not a valid encoded offset counter
    /// value.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        Ok(Self {
            next_offset: decode_single_u64_value(bytes, 0x02, "decode offset counter value")?,
        })
    }
}

impl ResourceMetaValue {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_two_u64_value(0x03, self.next_offset, self.committed_size_bytes)
    }

    /// # Errors
    ///
    /// Returns an error if `bytes` are not a valid encoded resource metadata
    /// value.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let (next_offset, committed_size_bytes) =
            decode_two_u64_value(bytes, 0x03, "decode resource metadata value")?;
        Ok(Self {
            next_offset,
            committed_size_bytes,
        })
    }
}

impl AreaCounterValue {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_single_u64_value(0x04, self.next_offset)
    }

    /// # Errors
    ///
    /// Returns an error if `bytes` are not a valid encoded area counter value.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        Ok(Self {
            next_offset: decode_single_u64_value(bytes, 0x04, "decode area counter value")?,
        })
    }
}

impl RealmCounterValue {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_single_u64_value(0x05, self.next_offset)
    }

    /// # Errors
    ///
    /// Returns an error if `bytes` are not a valid encoded realm counter
    /// value.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        Ok(Self {
            next_offset: decode_single_u64_value(bytes, 0x05, "decode realm counter value")?,
        })
    }
}

/// Create an in-memory Midge database for tests
#[cfg(test)]
#[must_use]
/// # Panics
///
/// Panics if the in-memory test database cannot be created.
pub fn create_test_db() -> std::sync::Arc<cntryl_midge::Engine> {
    use std::sync::Arc;
    Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("create in-memory db"),
    )
}

/// Staging key/value encoding for Transaction
#[must_use]
pub fn encode_staging_key(session_id: u64, event_index: usize) -> Vec<u8> {
    let mut encoder = lexkey::Encoder::with_capacity(18);
    encoder.push_byte(KeyPrefix::Staging as u8);
    encoder.encode_u64_into(session_id);
    encoder.push_separator();
    encoder.encode_u64_into(u64::try_from(event_index).unwrap_or(u64::MAX));
    encoder.into_vec()
}

pub fn encode_staging_value(event: &EventPayload) -> Vec<u8> {
    let metadata_len = event.metadata.as_ref().map_or(0, Bytes::len);
    let mut buf = Vec::with_capacity(8 + event.body.len() + metadata_len);
    buf.extend_from_slice(&usize_to_u32_saturating(event.body.len()).to_le_bytes());
    buf.extend_from_slice(
        &event
            .metadata
            .as_ref()
            .map_or(u32::MAX, |m| usize_to_u32_saturating(m.len()))
            .to_le_bytes(),
    );
    buf.extend_from_slice(&event.body);
    if let Some(metadata) = &event.metadata {
        buf.extend_from_slice(metadata);
    }
    buf
}

/// # Errors
///
/// Returns an error if the staging payload is truncated or contains trailing
/// bytes after decoding.
///
/// # Panics
///
/// Panics if fixed-width header slices fail to convert into arrays after the
/// preceding length checks.
pub fn decode_staging_value(data: &[u8]) -> Result<EventPayload, String> {
    if data.len() < 8 {
        return Err("decode_staging_value: header too short".to_string());
    }

    let body_len = u32_to_usize(u32::from_le_bytes(data[0..4].try_into().unwrap()));
    let metadata_len_raw = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let metadata_len = if metadata_len_raw == u32::MAX {
        None
    } else {
        Some(u32_to_usize(metadata_len_raw))
    };

    let mut offset = 8;
    if data.len() < offset + body_len {
        return Err("decode_staging_value: truncated body".to_string());
    }
    let body = data[offset..offset + body_len].to_vec();
    offset += body_len;

    let metadata = if let Some(metadata_len) = metadata_len {
        if data.len() < offset + metadata_len {
            return Err("decode_staging_value: truncated metadata".to_string());
        }
        let metadata = Some(data[offset..offset + metadata_len].to_vec());
        offset += metadata_len;
        metadata
    } else {
        None
    };
    if offset != data.len() {
        return Err("decode_staging_value: trailing bytes".to_string());
    }

    Ok(EventPayload {
        body: Bytes::from(body),
        metadata: metadata.map(Bytes::from),
        discriminator: None,
    })
}
