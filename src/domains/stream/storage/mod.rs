mod compact_page_values;
mod keys_and_models;
mod locators_and_staging;
mod resource_area_realm_values;

use keys_and_models::{
    decode_single_u64_value, decode_two_u64_value, encode_single_u64_value, encode_two_u64_value,
    AREA_VALUE_V2_MARKER, COMPACT_AREA_PAGE_VALUE_V2_MARKER, COMPACT_GLOBAL_PAGE_VALUE_V1_MARKER,
    COMPACT_GLOBAL_PAGE_VALUE_V2_MARKER, COMPACT_REALM_PAGE_VALUE_V2_MARKER,
    COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER, COMPRESSED_COMPACT_REALM_PAGE_VALUE_V2_MARKER,
    OPTIONAL_BYTES_ABSENT, OPTIONAL_OFFSET_ABSENT, REALM_VALUE_V2_MARKER, RESOURCE_VALUE_V2_MARKER,
    STREAM_LAYOUT_MARKER_VALUE_V2_MARKER,
};

#[cfg(test)]
pub(crate) use keys_and_models::encode_compact_resource_fragment_key;
pub use keys_and_models::{
    decode_area_offset_from_key, decode_realm_offset_from_key, decode_resource_offset_from_key,
    encode_area_counter_key, encode_area_discriminator_key, encode_area_key,
    encode_compact_area_page_key, encode_compact_global_page_key, encode_compact_resource_page_key,
    encode_compressed_compact_realm_page_key, encode_cursor_state_key,
    encode_family_writer_epoch_key, encode_global_area_posting_key,
    encode_global_area_resource_posting_key, encode_global_counter_key,
    encode_global_discriminator_key, encode_global_resource_posting_key,
    encode_global_watermark_key, encode_offset_counter_key, encode_payload_blob_key,
    encode_realm_counter_key, encode_realm_discriminator_key, encode_realm_key,
    encode_realm_resource_posting_key, encode_realm_watermark_key,
    encode_resource_discriminator_key, encode_resource_key, encode_resource_meta_key,
    encode_stream_layout_marker_key, encode_watermark_key, stream_key_suffix, AreaCounterValue,
    AreaValue, CompactAreaPageRecord, CompactAreaPageValue, CompactGlobalPageRecord,
    CompactGlobalPageValue, CompactRealmPageRecord, CompactRealmPageValue,
    CompactResourcePageRecord, CompactResourcePageValue, CompressedCompactRealmPageValue,
    KeyPrefix, OffsetCounterValue, PostingEntry, PostingPageValue, RealmCounterValue, RealmValue,
    ResourceMetaValue, ResourceValue, StreamLayoutMarkerValue, WatermarkValue,
    GLOBAL_PAGE_RECORD_LIMIT, REALM_PAGE_RECORD_LIMIT,
};
#[cfg(test)]
pub use locators_and_staging::create_test_db;
pub use locators_and_staging::{decode_staging_value, encode_staging_key, encode_staging_value};

#[cfg(test)]
mod tests;
