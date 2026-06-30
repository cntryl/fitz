use super::domain_frame_dispatcher::DomainFrameDispatcher;
use super::{
    AuthorizationPolicy, AuthorizationTargets, DispatchDomain, DomainAuthorizationSpec,
    RuntimeIngress,
};

impl RuntimeIngress {
    #[allow(dead_code)]
    pub(super) fn cached_session_inbox_route(
        &self,
        session_id: u64,
    ) -> crate::runtime::routing::Route {
        self.session_registry().cached_inbox_route(session_id)
    }

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
