mod auth_session_setup;
mod builder_and_sessions;
mod dispatch_policy;
mod domain_frame_dispatcher;
pub(crate) mod domain_registry;
mod session_authenticator;
mod session_cleanup_coordinator;
mod session_registry;
mod trait_impls;
mod types_and_helpers;

use crate::observability as obs;
use crate::protocol::frame::ChannelId;
use crate::runtime::DomainKind as DispatchDomain;
use crate::session::{CloseReason, SessionInfo};
use bytes::Bytes;
use dashmap::DashMap;
use std::borrow::Cow;
use std::sync::atomic::AtomicBool;
use tracing::{debug, info, trace, warn};

use types_and_helpers::{
    canonicalize_dispatch_route_str, dispatch_session_cleanup,
    dispatch_session_cleanup_for_domains, extract_auth_route_for_domain,
    is_subscription_registration_message, AuthorizationFailure, AuthorizationPolicy,
    AuthorizationTargets, DomainAuthorizationSpec, DomainDispatchPayload, DomainDispatchRequest,
    PendingSessionCleanup,
};

pub use types_and_helpers::{Ingress, IngressDecision, RuntimeIngress, SessionEvent, SessionFrame};

#[cfg(test)]
mod tests;
