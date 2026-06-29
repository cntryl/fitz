mod auth_session_setup;
mod authorization_dispatch;
mod builder_and_sessions;
mod dispatch_policy;
mod trait_impls;
mod types_and_helpers;

use crate::observability as obs;
use crate::protocol::frame::ChannelId;
use crate::runtime::DomainKind as DispatchDomain;
use crate::session::{CloseReason, SessionInfo, SessionPermissions};
use bytes::Bytes;
use dashmap::DashMap;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};

use types_and_helpers::{
    canonicalize_dispatch_route_str, dispatch_session_cleanup, extract_auth_route_for_domain,
    AuthorizationFailure, AuthorizationPolicy, AuthorizationTargets, DomainAuthorizationSpec,
    DomainDispatchRequest, PendingSessionCleanup,
};

pub use types_and_helpers::{Ingress, IngressDecision, RuntimeIngress, SessionEvent, SessionFrame};

#[cfg(test)]
mod tests;
