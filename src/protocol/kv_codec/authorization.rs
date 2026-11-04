//! Operation-specific KV route canonicalization at the protocol boundary.

use crate::dispatch::wire::kv::{canonical_auth_route, KvDropScope};
use std::borrow::Cow;

/// Canonicalize a request's authorization target without changing permission grammar.
///
/// # Errors
/// Returns an error when the operation's route is malformed.
pub fn canonical_operation_auth_route(
    message_type: u16,
    route: &str,
) -> Result<Cow<'_, str>, String> {
    if message_type == super::msg_type::DROP {
        KvDropScope::canonical_authorization_route(route).map(Cow::Owned)
    } else {
        canonical_auth_route(route)
    }
}
