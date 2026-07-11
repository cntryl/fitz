//! Synchronous wire-to-domain dispatch boundary.
//!
//! Protocol owns wire codecs and the exact manifest. Domains own commands,
//! responses, and state. This small adapter is the only shared mapping from
//! manifest domain names to runtime domain kinds.

use crate::protocol::manifest::{client_entry, ManifestAuthorization, ManifestDecoder};
use crate::protocol::tlv::MessageType;
use crate::runtime::DomainKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientDispatch {
    pub domain: DomainKind,
    pub authorization: ManifestAuthorization,
    pub decoder: ManifestDecoder,
    pub route_scheme: Option<&'static str>,
}

/// Resolve an inbound client message through the exact protocol manifest.
/// Unknown IDs are errors; numeric range membership is never a fallback.
///
/// # Errors
///
/// Returns an error when the message ID is absent from the manifest or names
/// a domain without a registered runtime adapter.
pub fn client_dispatch(message_type: MessageType) -> Result<Option<ClientDispatch>, &'static str> {
    let entry = client_entry(message_type)?;
    if entry.domain == "control"
        || entry.direction != crate::protocol::manifest::ManifestDirection::ClientToServer
    {
        return Ok(None);
    }

    let domain = match entry.domain {
        "kv" => DomainKind::Kv,
        "queue" => DomainKind::Queue,
        "notice" => DomainKind::Notice,
        "stream" => DomainKind::Stream,
        "rpc" => DomainKind::Rpc,
        "lease" => DomainKind::Lease,
        "schedule" => DomainKind::Schedule,
        _ => return Err("manifest domain has no runtime dispatch adapter"),
    };

    Ok(Some(ClientDispatch {
        domain,
        authorization: entry.authorization,
        decoder: entry.decoder,
        route_scheme: entry.route_scheme,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_resolve_client_message_from_manifest() {
        // Arrange
        let message_type = MessageType::new(crate::protocol::kv::msg_type::BEGIN);

        // Act
        let dispatch = client_dispatch(message_type)
            .expect("manifest entry")
            .expect("client dispatch");

        // Assert
        assert_eq!(dispatch.domain, DomainKind::Kv);
        assert_eq!(dispatch.route_scheme, Some("kv"));
    }

    #[test]
    fn should_reject_unknown_message_without_range_fallback() {
        // Arrange
        let message_type = MessageType::new(9999);

        // Act
        let result = client_dispatch(message_type);

        // Assert
        assert_eq!(result, Err("unsupported message type"));
    }
}
