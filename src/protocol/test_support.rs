//! Shared protocol test helpers used across domain sink unit tests.

use super::frame::ChannelId;
use crate::runtime::ClientChannel;

/// Map a runtime `ClientChannel` to its wire `ChannelId`, for constructing
/// test frames/envelopes that exercise a specific channel.
#[cfg(test)]
pub(crate) fn channel_id_from_client(channel: ClientChannel) -> ChannelId {
    match channel {
        ClientChannel::Control => ChannelId::Control,
        ClientChannel::Pub => ChannelId::Pub,
        ClientChannel::Sub => ChannelId::Sub,
        ClientChannel::Rpc => ChannelId::Rpc,
        ClientChannel::Lease => ChannelId::Lease,
        ClientChannel::Internal => ChannelId::Internal,
    }
}
