use super::super::store::StreamStorageLayout;
use super::{
    AreaValue, RealmValue, ResourceValue, StreamLayoutMarkerValue, AREA_VALUE_V2_MARKER,
    OPTIONAL_BYTES_ABSENT, OPTIONAL_OFFSET_ABSENT, REALM_VALUE_V2_MARKER, RESOURCE_VALUE_V2_MARKER,
    STREAM_LAYOUT_MARKER_VALUE_V2_MARKER,
};
use bytes::Bytes;

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl StreamLayoutMarkerValue {
    #[must_use]
    pub fn new(layout: StreamStorageLayout) -> Self {
        Self { layout }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        vec![
            STREAM_LAYOUT_MARKER_VALUE_V2_MARKER[0],
            STREAM_LAYOUT_MARKER_VALUE_V2_MARKER[1],
            match self.layout {
                StreamStorageLayout::PromotionFrontier => 1,
            },
        ]
    }

    /// # Errors
    ///
    /// Returns an error if the marker is missing or the encoded layout id is
    /// unknown.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 3 || !bytes.starts_with(&STREAM_LAYOUT_MARKER_VALUE_V2_MARKER) {
            return Err("decode stream layout marker: invalid encoding".to_string());
        }

        let layout = match bytes[2] {
            1 => StreamStorageLayout::PromotionFrontier,
            other => {
                return Err(format!(
                    "decode stream layout marker: unknown layout id {other}"
                ));
            }
        };

        Ok(Self { layout })
    }
}

impl ResourceValue {
    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map_or(0, Bytes::len);
        let mut buf = Vec::with_capacity(42 + self.body.len() + metadata_len);
        buf.extend_from_slice(&RESOURCE_VALUE_V2_MARKER);
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(
            &self
                .area_offset
                .unwrap_or(OPTIONAL_OFFSET_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(
            &self
                .realm_offset
                .unwrap_or(OPTIONAL_OFFSET_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&usize_to_u32_saturating(self.body.len()).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map_or(OPTIONAL_BYTES_ABSENT, |m| usize_to_u32_saturating(m.len()))
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    /// # Errors
    ///
    /// Returns an error if the resource value is malformed, truncated, or has
    /// trailing bytes.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        Self::decode_v2(bytes)
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid resource value encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize resource value")
    }

    fn decode_v2(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&RESOURCE_VALUE_V2_MARKER) {
            return Err("decode resource value: missing marker".to_string());
        }
        if bytes.len() < 42 {
            return Err("decode resource value: header too short".to_string());
        }

        let resource_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let area_offset_raw = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
        let realm_offset_raw = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
        let body_len = u32_to_usize(u32::from_le_bytes(bytes[34..38].try_into().unwrap()));
        let metadata_len_raw = u32::from_le_bytes(bytes[38..42].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(u32_to_usize(metadata_len_raw))
        };

        let mut offset = 42;
        if bytes.len().saturating_sub(offset) < body_len {
            return Err("decode resource value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len().saturating_sub(offset) < metadata_len {
                return Err("decode resource value: truncated metadata".to_string());
            }
            let metadata = Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ));
            offset += metadata_len;
            metadata
        } else {
            None
        };
        if offset != bytes.len() {
            return Err("decode resource value: trailing bytes".to_string());
        }

        Ok(Self {
            resource_offset,
            body,
            metadata,
            created_at,
            area_offset: (area_offset_raw != OPTIONAL_OFFSET_ABSENT).then_some(area_offset_raw),
            realm_offset: (realm_offset_raw != OPTIONAL_OFFSET_ABSENT).then_some(realm_offset_raw),
        })
    }
}

impl AreaValue {
    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map_or(0, Bytes::len);
        let mut buf = Vec::with_capacity(26 + self.body.len() + metadata_len);
        buf.extend_from_slice(&AREA_VALUE_V2_MARKER);
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(&usize_to_u32_saturating(self.body.len()).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map_or(OPTIONAL_BYTES_ABSENT, |m| usize_to_u32_saturating(m.len()))
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    /// # Errors
    ///
    /// Returns an error if the area value is malformed, truncated, or has
    /// trailing bytes.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        Self::decode_v2(bytes)
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid area value encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize area value")
    }

    fn decode_v2(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&AREA_VALUE_V2_MARKER) {
            return Err("decode area value: missing marker".to_string());
        }
        if bytes.len() < 26 {
            return Err("decode area value: header too short".to_string());
        }

        let resource_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let body_len = u32_to_usize(u32::from_le_bytes(bytes[18..22].try_into().unwrap()));
        let metadata_len_raw = u32::from_le_bytes(bytes[22..26].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(u32_to_usize(metadata_len_raw))
        };

        let mut offset = 26;
        if bytes.len().saturating_sub(offset) < body_len {
            return Err("decode area value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len().saturating_sub(offset) < metadata_len {
                return Err("decode area value: truncated metadata".to_string());
            }
            let metadata = Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ));
            offset += metadata_len;
            metadata
        } else {
            None
        };

        if offset != bytes.len() {
            return Err("decode area value: trailing bytes".to_string());
        }

        Ok(Self {
            resource_offset,
            body,
            metadata,
            created_at,
        })
    }
}

impl RealmValue {
    /// # Errors
    ///
    /// Returns an error if the realm value is malformed, truncated, or has
    /// trailing bytes.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        Self::decode_v2(bytes)
    }

    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map_or(0, Bytes::len);
        let mut buf = Vec::with_capacity(34 + self.body.len() + metadata_len);
        buf.extend_from_slice(&REALM_VALUE_V2_MARKER);
        buf.extend_from_slice(&self.area_offset.to_le_bytes());
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(&usize_to_u32_saturating(self.body.len()).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map_or(OPTIONAL_BYTES_ABSENT, |m| usize_to_u32_saturating(m.len()))
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid realm value encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize realm value")
    }

    fn decode_v2(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&REALM_VALUE_V2_MARKER) {
            return Err("decode realm value: missing marker".to_string());
        }
        if bytes.len() < 34 {
            return Err("decode realm value: header too short".to_string());
        }

        let area_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let resource_offset = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
        let body_len = u32_to_usize(u32::from_le_bytes(bytes[26..30].try_into().unwrap()));
        let metadata_len_raw = u32::from_le_bytes(bytes[30..34].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(u32_to_usize(metadata_len_raw))
        };

        let mut offset = 34;
        if bytes.len().saturating_sub(offset) < body_len {
            return Err("decode realm value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len().saturating_sub(offset) < metadata_len {
                return Err("decode realm value: truncated metadata".to_string());
            }
            let metadata = Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ));
            offset += metadata_len;
            metadata
        } else {
            None
        };
        if offset != bytes.len() {
            return Err("decode realm value: trailing bytes".to_string());
        }

        Ok(Self {
            area_offset,
            resource_offset,
            body,
            metadata,
            created_at,
        })
    }
}
