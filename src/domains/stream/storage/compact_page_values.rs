use super::{
    CompactAreaPageRecord, CompactAreaPageValue, CompactRealmPageRecord, CompactRealmPageValue,
    CompactResourcePageRecord, CompactResourcePageValue, COMPACT_AREA_PAGE_VALUE_V1_MARKER,
    COMPACT_REALM_PAGE_VALUE_V1_MARKER, COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER,
    OPTIONAL_BYTES_ABSENT,
};
use bytes::Bytes;

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl CompactRealmPageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_REALM_PAGE_VALUE_V1_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_REALM_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.records.len()).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&usize_to_u32_saturating(record.body.len()).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(OPTIONAL_BYTES_ABSENT, |metadata| {
                        usize_to_u32_saturating(metadata.len())
                    })
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid compact realm page encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compact realm page value")
    }

    /// # Errors
    ///
    /// Returns an error if the page marker is missing, the payload is
    /// truncated, or trailing bytes remain after decoding.
    ///
    /// # Panics
    ///
    /// Panics if fixed-width header slices fail to convert into arrays after
    /// preceding length checks.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compact realm page value: missing marker".to_string());
        }
        if bytes.len() < 6 {
            return Err("decode compact realm page value: header too short".to_string());
        }

        let mut offset = 2usize;
        let record_count = u32_to_usize(u32::from_le_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if bytes.len().saturating_sub(offset) < 32 {
                return Err("decode compact realm page value: record header truncated".to_string());
            }

            let area_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let resource_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let created_at = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let body_len = u32_to_usize(u32::from_le_bytes(
                bytes[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
            let metadata_len_raw =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(u32_to_usize(metadata_len_raw))
            };

            if bytes.len().saturating_sub(offset) < body_len {
                return Err("decode compact realm page value: truncated body".to_string());
            }
            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;

            let metadata = if let Some(metadata_len) = metadata_len {
                if bytes.len().saturating_sub(offset) < metadata_len {
                    return Err("decode compact realm page value: truncated metadata".to_string());
                }
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(CompactRealmPageRecord {
                area_offset,
                resource_offset,
                body,
                metadata,
                created_at,
            });
        }
        if offset != bytes.len() {
            return Err("decode compact realm page value: trailing bytes".to_string());
        }

        Ok(Self { records })
    }
}

impl CompactAreaPageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_AREA_PAGE_VALUE_V1_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_AREA_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.records.len()).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&usize_to_u32_saturating(record.body.len()).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(OPTIONAL_BYTES_ABSENT, |metadata| {
                        usize_to_u32_saturating(metadata.len())
                    })
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid compact area page encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compact area page value")
    }

    /// # Errors
    ///
    /// Returns an error if the page marker is missing, the payload is
    /// truncated, or trailing bytes remain after decoding.
    ///
    /// # Panics
    ///
    /// Panics if fixed-width header slices fail to convert into arrays after
    /// preceding length checks.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compact area page value: missing marker".to_string());
        }
        if bytes.len() < 6 {
            return Err("decode compact area page value: header too short".to_string());
        }

        let mut offset = 2usize;
        let record_count = u32_to_usize(u32::from_le_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if bytes.len().saturating_sub(offset) < 24 {
                return Err("decode compact area page value: record header truncated".to_string());
            }

            let resource_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let created_at = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let body_len = u32_to_usize(u32::from_le_bytes(
                bytes[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
            let metadata_len_raw =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(u32_to_usize(metadata_len_raw))
            };

            if bytes.len().saturating_sub(offset) < body_len {
                return Err("decode compact area page value: truncated body".to_string());
            }
            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;

            let metadata = if let Some(metadata_len) = metadata_len {
                if bytes.len().saturating_sub(offset) < metadata_len {
                    return Err("decode compact area page value: truncated metadata".to_string());
                }
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(CompactAreaPageRecord {
                resource_offset,
                body,
                metadata,
                created_at,
            });
        }
        if offset != bytes.len() {
            return Err("decode compact area page value: trailing bytes".to_string());
        }

        Ok(Self { records })
    }
}

impl CompactResourcePageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.records.len()).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.realm_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&usize_to_u32_saturating(record.body.len()).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(OPTIONAL_BYTES_ABSENT, |metadata| {
                        usize_to_u32_saturating(metadata.len())
                    })
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid compact resource page encoding.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compact resource page value")
    }

    /// # Errors
    ///
    /// Returns an error if the page marker is missing, the payload is
    /// truncated, or trailing bytes remain after decoding.
    ///
    /// # Panics
    ///
    /// Panics if fixed-width header slices fail to convert into arrays after
    /// preceding length checks.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compact resource page value: missing marker".to_string());
        }
        if bytes.len() < 6 {
            return Err("decode compact resource page value: header too short".to_string());
        }

        let mut offset = 2usize;
        let record_count = u32_to_usize(u32::from_le_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if bytes.len().saturating_sub(offset) < 32 {
                return Err(
                    "decode compact resource page value: record header truncated".to_string(),
                );
            }

            let area_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let realm_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let created_at = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let body_len = u32_to_usize(u32::from_le_bytes(
                bytes[offset..offset + 4].try_into().unwrap(),
            ));
            offset += 4;
            let metadata_len_raw =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(u32_to_usize(metadata_len_raw))
            };

            if bytes.len().saturating_sub(offset) < body_len {
                return Err("decode compact resource page value: truncated body".to_string());
            }
            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;

            let metadata = if let Some(metadata_len) = metadata_len {
                if bytes.len().saturating_sub(offset) < metadata_len {
                    return Err(
                        "decode compact resource page value: truncated metadata".to_string()
                    );
                }
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(CompactResourcePageRecord {
                area_offset,
                realm_offset,
                body,
                metadata,
                created_at,
            });
        }
        if offset != bytes.len() {
            return Err("decode compact resource page value: trailing bytes".to_string());
        }

        Ok(Self { records })
    }
}
