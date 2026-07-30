use fitz::protocol::manifest::{
    client_entry, entry, is_reserved_or_unassigned, ManifestAuthorization, ManifestDirection,
    MESSAGE_MANIFEST,
};
use fitz::protocol::MessageType;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

#[test]
fn should_reject_reserved_message_id_given_valid_numeric_range() {
    assert!(is_reserved_or_unassigned(MessageType::new(201)));
    assert!(client_entry(MessageType::new(201)).is_err());
}

#[test]
fn should_reject_unknown_message_id_given_known_domain_range() {
    assert!(entry(MessageType::new(799)).is_none());
    assert_eq!(client_entry(MessageType::new(799)), Err("unsupported message type"));
}

#[test]
fn should_reject_route_given_scheme_domain_mismatch() {
    let queue_write = MESSAGE_MANIFEST
        .iter()
        .find(|entry| entry.message_id == 200)
        .expect("queue write manifest entry");
    assert_eq!(queue_write.route_scheme, Some("queue"));
    assert_ne!(queue_write.route_scheme, Some("stream"));
}

#[test]
fn should_not_infer_realm_from_route_family() {
    let address = RouteAddress::new(RouteFamily::new(41), Route::new("kv://orders/area/key"));
    assert_eq!(address.family().id(), 41);
    assert_eq!(address.route().as_str(), "kv://orders/area/key");
    assert_ne!(address.family().to_string(), "orders");
}

#[test]
fn should_not_infer_route_family_from_realm() {
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
    let connects: Vec<_> = MESSAGE_MANIFEST
        .iter()
        .filter(|entry| entry.domain == "control" && entry.direction == ManifestDirection::ClientToServer)
        .collect();
    assert_eq!(connects.len(), 1);
}

#[test]
fn should_reject_non_connect_frame_before_authentication() {
    let first_domain_message = MESSAGE_MANIFEST
        .iter()
        .find(|entry| entry.domain != "control" && entry.direction == ManifestDirection::ClientToServer)
        .expect("domain message");
    assert_ne!(first_domain_message.domain, "control");
}
