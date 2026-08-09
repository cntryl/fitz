use super::{
    CompactAreaPageRecord, CompactAreaPageValue, CompactGlobalPageRecord, CompactGlobalPageValue,
    CompactRealmPageRecord, CompactRealmPageValue, CompactResourcePageRecord,
    CompactResourcePageValue, PostingEntry, PostingPageValue, COMPACT_AREA_PAGE_VALUE_V2_MARKER,
    COMPACT_GLOBAL_PAGE_VALUE_V1_MARKER, COMPACT_GLOBAL_PAGE_VALUE_V2_MARKER,
    COMPACT_REALM_PAGE_VALUE_V2_MARKER, COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER,
    OPTIONAL_BYTES_ABSENT, OPTIONAL_OFFSET_ABSENT,
};
use bytes::Bytes;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn encoded_string_len(value: &str) -> usize {
    4 + value.len()
}

fn encode_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&usize_to_u32_saturating(value.len()).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn decode_string(bytes: &[u8], offset: &mut usize, context: &str) -> Result<String, String> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(format!("{context}: route identity length truncated"));
    }
    let raw = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let length = u32_to_usize(raw);
    if bytes.len().saturating_sub(*offset) < length {
        return Err(format!("{context}: route identity truncated"));
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + length])
        .map_err(|_| format!("{context}: route identity is not UTF-8"))?
        .to_string();
    *offset += length;
    Ok(value)
}

trait PageRecordCodec: Sized {
    type Specific;

    fn encoded_specific_len(&self) -> usize;
    fn encode_specific(&self, bytes: &mut Vec<u8>);
    fn decode_specific(
        bytes: &[u8],
        offset: &mut usize,
        context: &str,
    ) -> Result<Self::Specific, String>;
    fn body(&self) -> &Bytes;
    fn metadata(&self) -> Option<&Bytes>;
    fn created_at(&self) -> u64;
    fn expires_at(&self) -> Option<u64>;
    fn from_parts(
        specific: Self::Specific,
        body: Bytes,
        metadata: Option<Bytes>,
        created_at: u64,
        expires_at: Option<u64>,
    ) -> Self;
}

fn encoded_record_len<R: PageRecordCodec>(record: &R) -> usize {
    record
        .encoded_specific_len()
        .saturating_add(24)
        .saturating_add(record.body().len())
        .saturating_add(record.metadata().map_or(0, Bytes::len))
}

fn encode_record<R: PageRecordCodec>(bytes: &mut Vec<u8>, record: &R) {
    record.encode_specific(bytes);
    bytes.extend_from_slice(&record.created_at().to_le_bytes());
    bytes.extend_from_slice(
        &record
            .expires_at()
            .unwrap_or(OPTIONAL_OFFSET_ABSENT)
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&usize_to_u32_saturating(record.body().len()).to_le_bytes());
    bytes.extend_from_slice(
        &record
            .metadata()
            .map_or(OPTIONAL_BYTES_ABSENT, |value| {
                usize_to_u32_saturating(value.len())
            })
            .to_le_bytes(),
    );
    bytes.extend_from_slice(record.body());
    if let Some(metadata) = record.metadata() {
        bytes.extend_from_slice(metadata);
    }
}

fn decode_record<R: PageRecordCodec>(
    bytes: &[u8],
    offset: &mut usize,
    context: &str,
) -> Result<R, String> {
    let specific = R::decode_specific(bytes, offset, context)?;
    if bytes.len().saturating_sub(*offset) < 24 {
        return Err(format!("{context}: record header truncated"));
    }
    let created_at = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    let expires_at = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    let body_len = u32_to_usize(u32::from_le_bytes(
        bytes[*offset..*offset + 4].try_into().unwrap(),
    ));
    *offset += 4;
    let metadata_raw = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    let metadata_len = (metadata_raw != OPTIONAL_BYTES_ABSENT).then(|| u32_to_usize(metadata_raw));
    if bytes.len().saturating_sub(*offset) < body_len {
        return Err(format!("{context}: body truncated"));
    }
    let body = Bytes::copy_from_slice(&bytes[*offset..*offset + body_len]);
    *offset += body_len;
    let metadata = if let Some(len) = metadata_len {
        if bytes.len().saturating_sub(*offset) < len {
            return Err(format!("{context}: metadata truncated"));
        }
        let value = Bytes::copy_from_slice(&bytes[*offset..*offset + len]);
        *offset += len;
        Some(value)
    } else {
        None
    };
    Ok(R::from_parts(
        specific,
        body,
        metadata,
        created_at,
        (expires_at != OPTIONAL_OFFSET_ABSENT).then_some(expires_at),
    ))
}

fn encode_page_payload<R: PageRecordCodec>(records: &[R]) -> Vec<u8> {
    let capacity = 4usize.saturating_add(records.iter().map(encoded_record_len).sum::<usize>());
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(&usize_to_u32_saturating(records.len()).to_le_bytes());
    for record in records {
        encode_record(&mut bytes, record);
    }
    bytes
}

fn decode_page_payload<R: PageRecordCodec>(bytes: &[u8], context: &str) -> Result<Vec<R>, String> {
    if bytes.len() < 4 {
        return Err(format!("{context}: invalid header"));
    }
    let mut offset = 4;
    let count = u32_to_usize(u32::from_le_bytes(bytes[..4].try_into().unwrap()));
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(decode_record(bytes, &mut offset, context)?);
    }
    if offset != bytes.len() {
        return Err(format!("{context}: trailing bytes"));
    }
    Ok(records)
}

fn encode_marked_page<R: PageRecordCodec>(marker: [u8; 2], records: &[R]) -> Vec<u8> {
    let payload = encode_page_payload(records);
    let mut bytes = Vec::with_capacity(2 + payload.len());
    bytes.extend_from_slice(&marker);
    bytes.extend_from_slice(&payload);
    bytes
}

fn decode_marked_page<R: PageRecordCodec>(
    marker: [u8; 2],
    bytes: &[u8],
    context: &str,
) -> Result<Vec<R>, String> {
    if !bytes.starts_with(&marker) {
        return Err(format!("{context}: missing marker"));
    }
    decode_page_payload(bytes.get(2..).unwrap_or_default(), context)
}

fn decode_u64(bytes: &[u8], offset: &mut usize, context: &str) -> Result<u64, String> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(format!("{context}: record header truncated"));
    }
    let value = u64::from_le_bytes(bytes[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(value)
}

macro_rules! common_page_record_accessors {
    () => {
        fn body(&self) -> &Bytes {
            &self.body
        }

        fn metadata(&self) -> Option<&Bytes> {
            self.metadata.as_ref()
        }

        fn created_at(&self) -> u64 {
            self.created_at
        }

        fn expires_at(&self) -> Option<u64> {
            self.expires_at
        }
    };
}

impl PageRecordCodec for CompactGlobalPageRecord {
    type Specific = (String, String, String, u64, u64, u64);

    fn encoded_specific_len(&self) -> usize {
        encoded_string_len(&self.realm)
            + encoded_string_len(&self.area)
            + encoded_string_len(&self.resource)
            + 24
    }

    fn encode_specific(&self, bytes: &mut Vec<u8>) {
        encode_string(bytes, &self.realm);
        encode_string(bytes, &self.area);
        encode_string(bytes, &self.resource);
        bytes.extend_from_slice(&self.resource_offset.to_le_bytes());
        bytes.extend_from_slice(&self.area_offset.to_le_bytes());
        bytes.extend_from_slice(&self.realm_offset.to_le_bytes());
    }

    fn decode_specific(
        bytes: &[u8],
        offset: &mut usize,
        context: &str,
    ) -> Result<Self::Specific, String> {
        Ok((
            decode_string(bytes, offset, context)?,
            decode_string(bytes, offset, context)?,
            decode_string(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
        ))
    }

    common_page_record_accessors!();

    fn from_parts(
        (realm, area, resource, resource_offset, area_offset, realm_offset): Self::Specific,
        body: Bytes,
        metadata: Option<Bytes>,
        created_at: u64,
        expires_at: Option<u64>,
    ) -> Self {
        Self {
            realm: realm.into(),
            area: area.into(),
            resource: resource.into(),
            resource_offset,
            area_offset,
            realm_offset,
            body,
            metadata,
            created_at,
            expires_at,
        }
    }
}

impl PageRecordCodec for CompactRealmPageRecord {
    type Specific = (String, String, u64, u64);

    fn encoded_specific_len(&self) -> usize {
        encoded_string_len(&self.area) + encoded_string_len(&self.resource) + 16
    }

    fn encode_specific(&self, bytes: &mut Vec<u8>) {
        encode_string(bytes, &self.area);
        encode_string(bytes, &self.resource);
        bytes.extend_from_slice(&self.area_offset.to_le_bytes());
        bytes.extend_from_slice(&self.resource_offset.to_le_bytes());
    }

    fn decode_specific(
        bytes: &[u8],
        offset: &mut usize,
        context: &str,
    ) -> Result<Self::Specific, String> {
        Ok((
            decode_string(bytes, offset, context)?,
            decode_string(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
        ))
    }

    common_page_record_accessors!();

    fn from_parts(
        (area, resource, area_offset, resource_offset): Self::Specific,
        body: Bytes,
        metadata: Option<Bytes>,
        created_at: u64,
        expires_at: Option<u64>,
    ) -> Self {
        Self {
            area: area.into(),
            resource: resource.into(),
            area_offset,
            resource_offset,
            body,
            metadata,
            created_at,
            expires_at,
        }
    }
}

impl PageRecordCodec for CompactAreaPageRecord {
    type Specific = (String, u64);

    fn encoded_specific_len(&self) -> usize {
        encoded_string_len(&self.resource) + 8
    }

    fn encode_specific(&self, bytes: &mut Vec<u8>) {
        encode_string(bytes, &self.resource);
        bytes.extend_from_slice(&self.resource_offset.to_le_bytes());
    }

    fn decode_specific(
        bytes: &[u8],
        offset: &mut usize,
        context: &str,
    ) -> Result<Self::Specific, String> {
        Ok((
            decode_string(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
        ))
    }

    common_page_record_accessors!();

    fn from_parts(
        (resource, resource_offset): Self::Specific,
        body: Bytes,
        metadata: Option<Bytes>,
        created_at: u64,
        expires_at: Option<u64>,
    ) -> Self {
        Self {
            resource: resource.into(),
            resource_offset,
            body,
            metadata,
            created_at,
            expires_at,
        }
    }
}

impl PageRecordCodec for CompactResourcePageRecord {
    type Specific = (u64, u64);

    fn encoded_specific_len(&self) -> usize {
        16
    }

    fn encode_specific(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.area_offset.to_le_bytes());
        bytes.extend_from_slice(&self.realm_offset.to_le_bytes());
    }

    fn decode_specific(
        bytes: &[u8],
        offset: &mut usize,
        context: &str,
    ) -> Result<Self::Specific, String> {
        Ok((
            decode_u64(bytes, offset, context)?,
            decode_u64(bytes, offset, context)?,
        ))
    }

    common_page_record_accessors!();

    fn from_parts(
        (area_offset, realm_offset): Self::Specific,
        body: Bytes,
        metadata: Option<Bytes>,
        created_at: u64,
        expires_at: Option<u64>,
    ) -> Self {
        Self {
            area_offset,
            realm_offset,
            body,
            metadata,
            created_at,
            expires_at,
        }
    }
}

impl PostingPageValue {
    const MARKER: [u8; 2] = [0, 0xEC];

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(6 + self.entries.len() * 24);
        bytes.extend_from_slice(&Self::MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.entries.len()).to_le_bytes());
        for entry in &self.entries {
            bytes.extend_from_slice(&entry.offset.to_le_bytes());
            bytes.extend_from_slice(&entry.parent_fragment_start.to_le_bytes());
            bytes.extend_from_slice(
                &entry
                    .expires_at
                    .unwrap_or(OPTIONAL_OFFSET_ABSENT)
                    .to_le_bytes(),
            );
        }
        bytes
    }

    /// # Errors
    ///
    /// Returns an error for an invalid marker, truncated offset list, or
    /// trailing bytes.
    ///
    /// # Panics
    ///
    /// Panics only if a fixed-width posting slice fails conversion after the
    /// encoded length has been validated.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&Self::MARKER) || bytes.len() < 6 {
            return Err("decode posting page value: invalid header".to_string());
        }
        let mut count_bytes = [0u8; 4];
        count_bytes.copy_from_slice(&bytes[2..6]);
        let count = u32_to_usize(u32::from_le_bytes(count_bytes));
        let expected = 6usize.saturating_add(count.saturating_mul(24));
        if bytes.len() != expected {
            return Err("decode posting page value: invalid offset payload".to_string());
        }
        let entries = bytes[6..]
            .chunks_exact(24)
            .map(|chunk| {
                let mut offset = [0u8; 8];
                let mut parent = [0u8; 8];
                offset.copy_from_slice(&chunk[..8]);
                parent.copy_from_slice(&chunk[8..16]);
                let expires_at = u64::from_le_bytes(chunk[16..24].try_into().unwrap());
                PostingEntry {
                    offset: u64::from_le_bytes(offset),
                    parent_fragment_start: u64::from_le_bytes(parent),
                    expires_at: (expires_at != OPTIONAL_OFFSET_ABSENT).then_some(expires_at),
                }
            })
            .collect();
        Ok(Self { entries })
    }
}

impl CompactGlobalPageValue {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let payload = encode_page_payload(&self.records);
        let compressed = compress_prepend_size(&payload);
        let mut bytes = Vec::with_capacity(2 + compressed.len());
        bytes.extend_from_slice(&COMPACT_GLOBAL_PAGE_VALUE_V2_MARKER);
        bytes.extend_from_slice(&compressed);
        bytes
    }

    /// # Errors
    ///
    /// Returns an error for an invalid marker, truncated record, invalid UTF-8,
    /// or trailing bytes.
    ///
    /// # Panics
    ///
    /// Panics only if fixed-width slices fail conversion after their lengths
    /// have been validated.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        let decompressed;
        let bytes = if bytes.starts_with(&COMPACT_GLOBAL_PAGE_VALUE_V1_MARKER) {
            bytes
                .get(2..)
                .ok_or_else(|| "decode compact global page value: invalid header".to_string())?
        } else if bytes.starts_with(&COMPACT_GLOBAL_PAGE_VALUE_V2_MARKER) {
            decompressed = decompress_size_prepended(bytes.get(2..).unwrap_or_default())
                .map_err(|error| format!("decode compact global page value: {error}"))?;
            decompressed.as_slice()
        } else {
            return Err("decode compact global page value: invalid header".to_string());
        };
        decode_page_payload(bytes, "decode compact global page value")
            .map(|records| Self { records })
    }
}

impl CompactRealmPageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_REALM_PAGE_VALUE_V2_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_marked_page(COMPACT_REALM_PAGE_VALUE_V2_MARKER, &self.records)
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid compact realm page encoding.
    #[must_use]
    #[cfg(test)]
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
        decode_marked_page(
            COMPACT_REALM_PAGE_VALUE_V2_MARKER,
            bytes,
            "decode compact realm page value",
        )
        .map(|records| Self { records })
    }
}

impl CompactAreaPageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_AREA_PAGE_VALUE_V2_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_marked_page(COMPACT_AREA_PAGE_VALUE_V2_MARKER, &self.records)
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid compact area page encoding.
    #[must_use]
    #[cfg(test)]
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
        decode_marked_page(
            COMPACT_AREA_PAGE_VALUE_V2_MARKER,
            bytes,
            "decode compact area page value",
        )
        .map(|records| Self { records })
    }
}

impl CompactResourcePageValue {
    #[must_use]
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        encode_marked_page(COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER, &self.records)
    }

    /// # Panics
    ///
    /// Panics if `bytes` do not contain a valid compact resource page encoding.
    #[must_use]
    #[cfg(test)]
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
        decode_marked_page(
            COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER,
            bytes,
            "decode compact resource page value",
        )
        .map(|records| Self { records })
    }
}
