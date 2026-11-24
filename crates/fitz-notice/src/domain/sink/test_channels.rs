pub(super) fn test_client_channel_from_protocol(
    channel: crate::wire::frame::ChannelId,
) -> fitz_core::runtime::ClientChannel {
    match channel {
        crate::wire::frame::ChannelId::Control => fitz_core::runtime::ClientChannel::Control,
        crate::wire::frame::ChannelId::Pub => fitz_core::runtime::ClientChannel::Pub,
        crate::wire::frame::ChannelId::Sub => fitz_core::runtime::ClientChannel::Sub,
        crate::wire::frame::ChannelId::Rpc => fitz_core::runtime::ClientChannel::Rpc,
        crate::wire::frame::ChannelId::Lease => fitz_core::runtime::ClientChannel::Lease,
        crate::wire::frame::ChannelId::Internal => fitz_core::runtime::ClientChannel::Internal,
    }
}
