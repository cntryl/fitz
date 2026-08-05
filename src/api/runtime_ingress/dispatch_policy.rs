use super::domain_frame_dispatcher::DomainFrameDispatcher;
use super::{
    AuthorizationPolicy, AuthorizationTargets, DispatchDomain, DomainAuthorizationSpec,
    RuntimeIngress,
};

impl RuntimeIngress {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn domain_dispatch_for_msg_type(
        msg_type: crate::protocol::tlv::MessageType,
    ) -> Result<Option<DomainAuthorizationSpec>, &'static str> {
        DomainFrameDispatcher::domain_dispatch_for_msg_type(msg_type)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn resolve_authorization_targets(
        domain: DispatchDomain,
        msg_type: crate::protocol::tlv::MessageType,
        payload: &[u8],
        policy: AuthorizationPolicy,
    ) -> Result<(AuthorizationTargets<'_>, crate::auth::Access), String> {
        DomainFrameDispatcher::resolve_authorization_targets(domain, msg_type, payload, policy)
    }
}
