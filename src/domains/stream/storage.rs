use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::store::EventPayload;

/// Storage key prefixes for stream data
#[derive(Debug, Clone, Copy)]
pub enum KeyPrefix {
    /// Resource stream entry: [RF][realm][area][resource][resource_offset]
    Resource = 0x01,
    /// Area index entry: [RF][realm][area][area_offset]
    Area = 0x02,
    /// Realm index entry: [RF][realm][realm_offset]
    Realm = 0x03,
    /// Watermark entry: [RF][realm][area]
    Watermark = 0x04,
    /// Staging entry for active sessions: [session_id][event_index]
    Staging = 0x05,
    /// Offset counter: [RF][realm][area][resource] - stores next offset independent of TTL
    OffsetCounter = 0x06,
    /// Realm watermark: [RF][realm] - stores realm-level watermark
    RealmWatermark = 0x07,
}

/// Encodes a resource stream key
pub fn encode_resource_key(
    realm: &str,
    area: &str,
    resource: &str,
    resource_offset: u64,
) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Resource as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0); // separator
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key.push(0);
    key.extend_from_slice(&resource_offset.to_be_bytes());
    key
}

/// Encodes an area index key
pub fn encode_area_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Area as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_offset.to_be_bytes());
    key
}

/// Encodes a realm index key
pub fn encode_realm_key(realm: &str, realm_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Realm as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&realm_offset.to_be_bytes());
    key
}

/// Decode area_offset from area key
pub fn decode_area_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 8..];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Decode realm_offset from realm key
pub fn decode_realm_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 8..];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Encodes a watermark key
pub fn encode_watermark_key(realm: &str, area: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Watermark as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key
}

/// Encodes an offset counter key (metadata, independent of TTL)
pub fn encode_offset_counter_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::OffsetCounter as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key
}

/// Encodes a realm watermark key (metadata, independent of TTL)
pub fn encode_realm_watermark_key(realm: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::RealmWatermark as u8];
    key.extend_from_slice(realm.as_bytes());
    key
}

/// Value stored in resource index (full record)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceValue {
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
    /// Area offset (filled in after commit)
    pub area_offset: Option<u64>,
    /// Realm offset (filled in after commit)
    pub realm_offset: Option<u64>,
}

/// Value stored in area index (covering index with full event)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AreaValue {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

/// Value stored in realm index (covering index with full event)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmValue {
    pub realm: String,
    pub area: String,
    pub area_offset: u64,
    pub resource: String,
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

/// Watermark value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkValue {
    pub watermark: u64,
}

/// Offset counter value (metadata, not subject to TTL)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCounterValue {
    pub next_offset: u64,
}

impl ResourceValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize resource value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize resource value")
    }
}

impl AreaValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize area value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize area value")
    }
}

impl RealmValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize realm value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize realm value")
    }
}

impl WatermarkValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize watermark value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize watermark value")
    }
}

impl OffsetCounterValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize offset counter value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize offset counter value")
    }
}

/// Create an in-memory Midge database for tests
#[cfg(test)]
pub fn create_test_db() -> std::sync::Arc<cntryl_midge::Engine> {
    use cntryl_midge::testkit::MidgeOptions;
    use std::sync::Arc;
    Arc::new(
        cntryl_midge::Engine::open_with_options(MidgeOptions::default())
            .expect("create in-memory db"),
    )
}

/// Staging key/value encoding for Transaction
pub fn encode_staging_key(session_id: u64, event_index: usize) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Staging as u8];
    key.extend_from_slice(&session_id.to_be_bytes());
    key.push(0);
    key.extend_from_slice(&(event_index as u64).to_be_bytes());
    key
}

pub fn encode_staging_value(event: &EventPayload) -> Vec<u8> {
    let metadata_len = event.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
    let mut buf = Vec::with_capacity(8 + event.body.len() + metadata_len);
    buf.extend_from_slice(&(event.body.len() as u32).to_le_bytes());
    buf.extend_from_slice(
        &event
            .metadata
            .as_ref()
            .map(|m| m.len() as u32)
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    buf.extend_from_slice(&event.body);
    if let Some(metadata) = &event.metadata {
        buf.extend_from_slice(metadata);
    }
    buf
}

pub fn decode_staging_value(data: &[u8]) -> Result<EventPayload, String> {
    if data.len() < 8 {
        return Err("decode_staging_value: header too short".to_string());
    }

    let body_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let metadata_len_raw = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let metadata_len = if metadata_len_raw == u32::MAX {
        None
    } else {
        Some(metadata_len_raw as usize)
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
        Some(data[offset..offset + metadata_len].to_vec())
    } else {
        None
    };

    Ok(EventPayload {
        body: Bytes::from(body),
        metadata: metadata.map(Bytes::from),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_resource_key_with_proper_ordering() {
        // Arrange
        let key1 = encode_resource_key("realm", "area", "res1", 0);
        let key2 = encode_resource_key("realm", "area", "res1", 1);
        let key3 = encode_resource_key("realm", "area", "res1", 10);

        // Act
        // (keys are already encoded)

        // Assert
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn should_encode_area_key_with_proper_ordering() {
        // Arrange
        let key1 = encode_area_key("realm", "area", 0);
        let key2 = encode_area_key("realm", "area", 1);
        let key3 = encode_area_key("realm", "area", 100);

        // Act
        // (keys are already encoded)

        // Assert
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn should_isolate_keys_by_prefix() {
        // Arrange
        let resource_key = encode_resource_key("realm", "area", "res", 0);
        let area_key = encode_area_key("realm", "area", 0);
        let realm_key = encode_realm_key("realm", 0);

        // Act
        // (keys are already encoded)

        // Assert
        assert_eq!(resource_key[0], KeyPrefix::Resource as u8);
        assert_eq!(area_key[0], KeyPrefix::Area as u8);
        assert_eq!(realm_key[0], KeyPrefix::Realm as u8);
    }

    #[test]
    fn should_roundtrip_resource_value() {
        // Arrange
        let value = ResourceValue {
            resource_offset: 42,
            body: Bytes::from("test"),
            metadata: Some(Bytes::from("meta")),
            created_at: 1234567890,
            area_offset: Some(10),
            realm_offset: Some(5),
        };

        // Act
        let encoded = value.encode();
        let decoded = ResourceValue::decode(&encoded);

        // Assert

        assert_eq!(decoded.resource_offset, 42);
        assert_eq!(decoded.body, Bytes::from("test"));
        assert_eq!(decoded.area_offset, Some(10));
    }

    #[test]
    fn should_roundtrip_staging_value_with_metadata() {
        // Arrange
        let event = EventPayload {
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
        };

        // Act
        let encoded = encode_staging_value(&event);
        let decoded = decode_staging_value(&encoded).expect("decode staging value");

        // Assert
        assert_eq!(decoded.body, event.body);
        assert_eq!(decoded.metadata, event.metadata);
    }

    #[test]
    fn should_roundtrip_staging_value_without_metadata() {
        // Arrange
        let event = EventPayload {
            body: Bytes::from("body"),
            metadata: None,
        };

        // Act
        let encoded = encode_staging_value(&event);
        let decoded = decode_staging_value(&encoded).expect("decode staging value");

        // Assert
        assert_eq!(decoded.body, event.body);
        assert_eq!(decoded.metadata, None);
    }
}
