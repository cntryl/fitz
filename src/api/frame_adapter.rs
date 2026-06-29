use crate::protocol::frame::ChannelId;
use crate::runtime::ClientChannel;

pub(crate) fn client_channel_from_protocol(channel: ChannelId) -> ClientChannel {
    match channel {
        ChannelId::Control => ClientChannel::Control,
        ChannelId::Pub => ClientChannel::Pub,
        ChannelId::Sub => ClientChannel::Sub,
        ChannelId::Rpc => ClientChannel::Rpc,
        ChannelId::Lease => ClientChannel::Lease,
        ChannelId::Internal => ClientChannel::Internal,
    }
}
