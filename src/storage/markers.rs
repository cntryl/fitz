//! Domain-specific key prefixes and markers for storage operations
//!
//! This module centralizes all domain prefixes to avoid conflicts between domains.
//! Each domain gets a unique prefix byte, and within each domain, index types
//! are differentiated by additional marker bytes.

/// Stream domain prefix marker
pub const STREAM_DOMAIN_PREFIX: u8 = 0x01;

/// Stream domain index type markers (second byte after domain prefix)
pub mod stream {
    pub const RESOURCE_EVENT: u8 = 0x01;
    pub const AREA_EVENT: u8 = 0x02;
    pub const WATERMARK: u8 = 0x03;
    pub const AREA_DISCOVERY: u8 = 0x04;
    pub const RESOURCE_DISCOVERY: u8 = 0x05;
}

/// Discovery marker value written to KvStore for stream domain
pub const STREAM_DISCOVERY_MARKER: &[u8] = &[0x01];

// Future domains can be added here:
// pub const QUEUE_DOMAIN_PREFIX: u8 = 0x02;
// pub const KV_DOMAIN_PREFIX: u8 = 0x03;
// etc.
