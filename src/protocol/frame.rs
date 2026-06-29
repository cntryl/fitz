//! Shared protocol frame definitions
//!
//! This module defines logical channels and low-level protocol helpers used by the
//! TLV decoder and Mux layers.

use std::fmt;

/// Logical channel identifiers for multiplexed messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelId {
    Control = 0,
    Pub = 1,
    Sub = 2,
    Rpc = 3,
    Lease = 4,
    Internal = 5,
}

impl ChannelId {
    /// All supported channels in stable order
    #[must_use]
    pub fn all() -> [Self; 6] {
        [
            ChannelId::Control,
            ChannelId::Pub,
            ChannelId::Sub,
            ChannelId::Rpc,
            ChannelId::Lease,
            ChannelId::Internal,
        ]
    }

    /// Try to convert a numeric value back to a channel
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(ChannelId::Control),
            1 => Some(ChannelId::Pub),
            2 => Some(ChannelId::Sub),
            3 => Some(ChannelId::Rpc),
            4 => Some(ChannelId::Lease),
            5 => Some(ChannelId::Internal),
            _ => None,
        }
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelId::Control => write!(f, "control"),
            ChannelId::Pub => write!(f, "pub"),
            ChannelId::Sub => write!(f, "sub"),
            ChannelId::Rpc => write!(f, "rpc"),
            ChannelId::Lease => write!(f, "lease"),
            ChannelId::Internal => write!(f, "internal"),
        }
    }
}

/// Frame decoding error
#[derive(Debug, Clone)]
pub enum FrameError {
    /// Channel identifier is invalid
    InvalidChannel(u8),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::InvalidChannel(value) => write!(f, "invalid channel id: {value}"),
        }
    }
}

impl std::error::Error for FrameError {}
