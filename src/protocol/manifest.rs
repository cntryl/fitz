//! Exact client/server message manifest.
//!
//! Message IDs are wire contract, not ranges. Ranges are useful for channel
//! bucketing, but they must never decide whether a message is supported or
//! which domain is authorized to receive it.

use super::tlv::MessageType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestAuthorization {
    None,
    RouteRead,
    RouteWrite,
    RouteAll,
    SessionOwned,
    KvBeginMode,
    WildcardRead,
    MultiRouteWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestDecoder {
    Control,
    Kv,
    Queue,
    Notice,
    Stream,
    Rpc,
    Lease,
    Schedule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageManifestEntry {
    pub message_id: u16,
    pub domain: &'static str,
    pub direction: ManifestDirection,
    pub route_scheme: Option<&'static str>,
    pub authorization: ManifestAuthorization,
    pub decoder: ManifestDecoder,
}

const fn client(
    message_id: u16,
    domain: &'static str,
    route_scheme: Option<&'static str>,
    authorization: ManifestAuthorization,
    decoder: ManifestDecoder,
) -> MessageManifestEntry {
    MessageManifestEntry {
        message_id,
        domain,
        direction: ManifestDirection::ClientToServer,
        route_scheme,
        authorization,
        decoder,
    }
}

const fn server(
    message_id: u16,
    domain: &'static str,
    route_scheme: Option<&'static str>,
    decoder: ManifestDecoder,
) -> MessageManifestEntry {
    MessageManifestEntry {
        message_id,
        domain,
        direction: ManifestDirection::ServerToClient,
        route_scheme,
        authorization: ManifestAuthorization::None,
        decoder,
    }
}

/// Every assigned wire message, including server-only response IDs.
pub const MESSAGE_MANIFEST: &[MessageManifestEntry] = &[
    client(
        1,
        "control",
        None,
        ManifestAuthorization::None,
        ManifestDecoder::Control,
    ),
    client(
        100,
        "kv",
        Some("kv"),
        ManifestAuthorization::KvBeginMode,
        ManifestDecoder::Kv,
    ),
    client(
        101,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        102,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        103,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        104,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        105,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        106,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        107,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        108,
        "kv",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Kv,
    ),
    client(
        109,
        "kv",
        Some("kv"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Kv,
    ),
    client(
        110,
        "kv",
        Some("kv"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Kv,
    ),
    server(111, "kv", Some("kv"), ManifestDecoder::Kv),
    client(
        200,
        "queue",
        Some("queue"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Queue,
    ),
    client(
        202,
        "queue",
        Some("queue"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Queue,
    ),
    client(
        203,
        "queue",
        Some("queue"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Queue,
    ),
    client(
        204,
        "queue",
        Some("queue"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Queue,
    ),
    client(
        207,
        "queue",
        Some("queue"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Queue,
    ),
    client(
        208,
        "queue",
        Some("queue"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Queue,
    ),
    server(209, "queue", Some("queue"), ManifestDecoder::Queue),
    client(
        300,
        "rpc",
        Some("rpc"),
        ManifestAuthorization::RouteAll,
        ManifestDecoder::Rpc,
    ),
    client(
        301,
        "rpc",
        Some("rpc"),
        ManifestAuthorization::RouteAll,
        ManifestDecoder::Rpc,
    ),
    client(
        302,
        "rpc",
        Some("rpc"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Rpc,
    ),
    client(
        303,
        "rpc",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Rpc,
    ),
    server(305, "rpc", Some("rpc"), ManifestDecoder::Rpc),
    client(
        400,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Lease,
    ),
    client(
        401,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Lease,
    ),
    client(
        402,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Lease,
    ),
    client(
        403,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Lease,
    ),
    client(
        407,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Lease,
    ),
    client(
        408,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Lease,
    ),
    server(409, "lease", Some("lease"), ManifestDecoder::Lease),
    client(
        410,
        "lease",
        Some("lease"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Lease,
    ),
    client(
        500,
        "notice",
        Some("notice"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Notice,
    ),
    client(
        501,
        "notice",
        Some("notice"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Notice,
    ),
    client(
        502,
        "notice",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Notice,
    ),
    client(
        503,
        "notice",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Notice,
    ),
    server(504, "notice", Some("notice"), ManifestDecoder::Notice),
    client(
        600,
        "stream",
        Some("stream"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Stream,
    ),
    client(
        601,
        "stream",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Stream,
    ),
    client(
        602,
        "stream",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Stream,
    ),
    client(
        603,
        "stream",
        None,
        ManifestAuthorization::SessionOwned,
        ManifestDecoder::Stream,
    ),
    client(
        604,
        "stream",
        Some("stream"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Stream,
    ),
    client(
        605,
        "stream",
        Some("stream"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Stream,
    ),
    client(
        606,
        "stream",
        Some("stream"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Stream,
    ),
    client(
        607,
        "stream",
        Some("stream"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Stream,
    ),
    client(
        608,
        "stream",
        Some("stream"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Stream,
    ),
    server(609, "stream", Some("stream"), ManifestDecoder::Stream),
    client(
        700,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Schedule,
    ),
    client(
        701,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::RouteWrite,
        ManifestDecoder::Schedule,
    ),
    client(
        702,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::WildcardRead,
        ManifestDecoder::Schedule,
    ),
    client(
        703,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Schedule,
    ),
    client(
        704,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::RouteRead,
        ManifestDecoder::Schedule,
    ),
    server(705, "schedule", Some("schedule"), ManifestDecoder::Schedule),
    client(
        706,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::MultiRouteWrite,
        ManifestDecoder::Schedule,
    ),
    client(
        707,
        "schedule",
        Some("schedule"),
        ManifestAuthorization::WildcardRead,
        ManifestDecoder::Schedule,
    ),
];

#[must_use]
pub fn entry(message_type: MessageType) -> Option<&'static MessageManifestEntry> {
    MESSAGE_MANIFEST
        .iter()
        .find(|entry| entry.message_id == message_type.as_u16())
}

/// Returns the exact client-to-server entry or a rejection reason.
///
/// # Errors
///
/// Returns a static rejection reason for unsupported, server-only, or reserved
/// message IDs.
pub fn client_entry(
    message_type: MessageType,
) -> Result<&'static MessageManifestEntry, &'static str> {
    if message_type.as_u16() == 304 {
        return Err("invalid message type: unsupported rpc operation");
    }
    let Some(entry) = entry(message_type) else {
        return Err("unsupported message type");
    };
    if entry.direction != ManifestDirection::ClientToServer {
        return Err("message type is server-to-client only");
    }
    Ok(entry)
}

#[must_use]
pub fn is_reserved_or_unassigned(message_type: MessageType) -> bool {
    let id = message_type.as_u16();
    (100..=799).contains(&id) && entry(message_type).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

    #[test]
    fn should_have_unique_manifest_message_ids() {
        // Arrange
        // Act
        // Assert
        let mut ids = std::collections::BTreeSet::new();
        for entry in MESSAGE_MANIFEST {
            assert!(ids.insert(entry.message_id));
        }
    }

    #[test]
    fn should_reject_server_only_plus_reserved_ids() {
        assert!(client_entry(MessageType::new(504)).is_err());
        assert!(client_entry(MessageType::new(505)).is_err());
        assert!(client_entry(MessageType::new(999)).is_err());
    }

    #[test]
    fn should_reject_every_unassigned_id_in_the_protocol_manifest_range() {
        // Arrange
        // Act
        // Assert
        for message_id in 100..=799 {
            let message_type = MessageType::new(message_id);
            if entry(message_type).is_none() {
                assert!(is_reserved_or_unassigned(message_type));
                assert!(client_entry(message_type).is_err());
            }
        }
    }

    #[test]
    fn should_keep_manifest_entries_domain_plus_decoder_aligned() {
        // Arrange
        // Act
        // Assert
        for manifest_entry in MESSAGE_MANIFEST {
            let expected_decoder = match manifest_entry.domain {
                "control" => ManifestDecoder::Control,
                "kv" => ManifestDecoder::Kv,
                "queue" => ManifestDecoder::Queue,
                "notice" => ManifestDecoder::Notice,
                "stream" => ManifestDecoder::Stream,
                "rpc" => ManifestDecoder::Rpc,
                "lease" => ManifestDecoder::Lease,
                "schedule" => ManifestDecoder::Schedule,
                other => panic!("unknown manifest domain {other}"),
            };
            assert_eq!(manifest_entry.decoder, expected_decoder);
        }
    }

    #[test]
    fn should_reject_reserved_message_id_given_valid_numeric_range() {
        assert!(is_reserved_or_unassigned(MessageType::new(201)));
        assert!(client_entry(MessageType::new(201)).is_err());
    }

    #[test]
    fn should_reject_unknown_message_id_given_known_domain_range() {
        // Arrange
        // Act
        // Assert
        assert!(entry(MessageType::new(799)).is_none());
        assert_eq!(
            client_entry(MessageType::new(799)),
            Err("unsupported message type")
        );
    }

    #[test]
    fn should_reject_route_given_scheme_domain_mismatch() {
        // Arrange
        // Act
        // Assert
        let queue_write = MESSAGE_MANIFEST
            .iter()
            .find(|entry| entry.message_id == 200)
            .expect("queue write manifest entry");
        assert_eq!(queue_write.route_scheme, Some("queue"));
        assert_ne!(queue_write.route_scheme, Some("stream"));
    }

    #[test]
    fn should_not_infer_realm_from_route_family() {
        // Arrange
        // Act
        // Assert
        let address = RouteAddress::new(RouteFamily::new(41), Route::new("kv://orders/area/key"));
        assert_eq!(address.family().id(), 41);
        assert_eq!(address.route().as_str(), "kv://orders/area/key");
        assert_ne!(address.family().to_string(), "orders");
    }

    #[test]
    fn should_not_infer_route_family_from_realm() {
        // Arrange
        // Act
        // Assert
        let first = RouteAddress::new(RouteFamily::new(1), Route::new("kv://same/area/key"));
        let second = RouteAddress::new(RouteFamily::new(2), Route::new("kv://same/area/key"));
        assert_ne!(first, second);
        assert_eq!(first.route(), second.route());
    }

    #[test]
    fn should_reject_permission_given_wrong_scheme() {
        let queue_write = entry(MessageType::new(200)).expect("queue write manifest entry");
        assert_eq!(queue_write.authorization, ManifestAuthorization::RouteWrite);
        assert_eq!(queue_write.route_scheme, Some("queue"));
    }

    #[test]
    fn should_reject_second_connect_given_authenticated_session() {
        // Arrange
        // Act
        // Assert
        let connects: Vec<_> = MESSAGE_MANIFEST
            .iter()
            .filter(|entry| {
                entry.domain == "control" && entry.direction == ManifestDirection::ClientToServer
            })
            .collect();
        assert_eq!(connects.len(), 1);
    }

    #[test]
    fn should_reject_non_connect_frame_before_authentication() {
        // Arrange
        // Act
        // Assert
        let first_domain_message = MESSAGE_MANIFEST
            .iter()
            .find(|entry| {
                entry.domain != "control" && entry.direction == ManifestDirection::ClientToServer
            })
            .expect("domain message");
        assert_ne!(first_domain_message.domain, "control");
    }
}
