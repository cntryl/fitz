use super::super::store::StreamStorageLayout;
#[cfg(test)]
use super::{AreaValue, RealmValue, ResourceValue, OPTIONAL_OFFSET_ABSENT};
use super::{
    StreamLayoutMarkerValue, AREA_VALUE_V2_MARKER, OPTIONAL_BYTES_ABSENT, REALM_VALUE_V2_MARKER,
    RESOURCE_VALUE_V2_MARKER, STREAM_LAYOUT_DROP_MARKER, STREAM_LAYOUT_MARKER_VALUE_V2_MARKER,
};
#[cfg(test)]
use bytes::Bytes;

#[cfg(test)]
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
    pub fn encode(self) -> Vec<u8> {
        self.encode_marker(STREAM_LAYOUT_MARKER_VALUE_V2_MARKER)
    }

    #[must_use]
    pub(crate) fn encode_drop_aware(self) -> Vec<u8> {
        self.encode_marker(STREAM_LAYOUT_DROP_MARKER)
    }

    fn encode_marker(self, marker: [u8; 2]) -> Vec<u8> {
        vec![
            marker[0],
            marker[1],
            match self.layout {
                StreamStorageLayout::PromotionFrontier => 2,
            },
        ]
    }

    /// # Errors
    ///
    /// Returns an error if the marker is missing or the encoded layout id is
    /// unknown.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 3
            || !(bytes.starts_with(&STREAM_LAYOUT_MARKER_VALUE_V2_MARKER)
                || bytes.starts_with(&STREAM_LAYOUT_DROP_MARKER))
        {
            return Err("decode stream layout marker: invalid encoding".to_string());
        }

        let layout = match bytes[2] {
            2 => StreamStorageLayout::PromotionFrontier,
            other => {
                return Err(format!(
                    "decode stream layout marker: unknown layout id {other}"
                ));
            }
        };

        Ok(Self { layout })
    }
}

/// Inline index rows require validation at startup. Only fixtures read or emit their payloads.
pub(crate) enum InlineIndexValue {
    Resource,
    Area,
    Realm,
}

impl InlineIndexValue {
    pub(crate) fn validate(self, bytes: &[u8]) -> Result<(), String> {
        self.payload(bytes).map(|_| ())
    }

    fn payload(self, bytes: &[u8]) -> Result<(&[u8], Option<&[u8]>), String> {
        let (marker, header_len, scope) = match self {
            Self::Resource => (RESOURCE_VALUE_V2_MARKER, 42, "resource"),
            Self::Area => (AREA_VALUE_V2_MARKER, 26, "area"),
            Self::Realm => (REALM_VALUE_V2_MARKER, 34, "realm"),
        };
        if !bytes.starts_with(&marker) {
            return Err(format!("decode {scope} value: missing marker"));
        }
        if bytes.len() < header_len {
            return Err(format!("decode {scope} value: header too short"));
        }

        let body_len = u32_to_usize(u32::from_le_bytes(
            bytes[header_len - 8..header_len - 4].try_into().unwrap(),
        ));
        let metadata_len =
            u32::from_le_bytes(bytes[header_len - 4..header_len].try_into().unwrap());
        let (body, rest) = bytes[header_len..]
            .split_at_checked(body_len)
            .ok_or_else(|| format!("decode {scope} value: truncated body"))?;
        let (metadata, trailing) = if metadata_len == OPTIONAL_BYTES_ABSENT {
            (None, rest)
        } else {
            let (metadata, trailing) = rest
                .split_at_checked(u32_to_usize(metadata_len))
                .ok_or_else(|| format!("decode {scope} value: truncated metadata"))?;
            (Some(metadata), trailing)
        };
        if !trailing.is_empty() {
            return Err(format!("decode {scope} value: trailing bytes"));
        }
        Ok((body, metadata))
    }
}

#[cfg(test)]
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
        let (body, metadata) = InlineIndexValue::Resource.payload(bytes)?;
        let area_offset = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
        let realm_offset = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
        Ok(Self {
            resource_offset: u64::from_le_bytes(bytes[2..10].try_into().unwrap()),
            body: Bytes::copy_from_slice(body),
            metadata: metadata.map(Bytes::copy_from_slice),
            created_at: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
            area_offset: (area_offset != OPTIONAL_OFFSET_ABSENT).then_some(area_offset),
            realm_offset: (realm_offset != OPTIONAL_OFFSET_ABSENT).then_some(realm_offset),
        })
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid resource value encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize resource value")
    }
}

#[cfg(test)]
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
        let (body, metadata) = InlineIndexValue::Area.payload(bytes)?;
        Ok(Self {
            resource_offset: u64::from_le_bytes(bytes[2..10].try_into().unwrap()),
            body: Bytes::copy_from_slice(body),
            metadata: metadata.map(Bytes::copy_from_slice),
            created_at: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
        })
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid area value encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize area value")
    }
}

#[cfg(test)]
impl RealmValue {
    /// # Errors
    ///
    /// Returns an error if the realm value is malformed, truncated, or has
    /// trailing bytes.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        let (body, metadata) = InlineIndexValue::Realm.payload(bytes)?;
        Ok(Self {
            area_offset: u64::from_le_bytes(bytes[2..10].try_into().unwrap()),
            resource_offset: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
            body: Bytes::copy_from_slice(body),
            metadata: metadata.map(Bytes::copy_from_slice),
            created_at: u64::from_le_bytes(bytes[18..26].try_into().unwrap()),
        })
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
}
