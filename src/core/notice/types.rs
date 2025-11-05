//! Notice domain types
//!
//! Notice domain uses TLV encoding for all operations.
//! Operations are determined by the presence of specific TLV tags.

/// Notice operation types based on TLV tags in payload
#[derive(Debug, Clone)]
pub enum NoticeOperation {
    /// Subscribe - TAG_SUBSCRIBE (0x90) present
    Subscribe,
    /// Unsubscribe - TAG_UNSUBSCRIBE (0x91) present  
    Unsubscribe,
    /// Publish - TAG_BODY present without subscribe/unsubscribe tags
    Publish,
}
