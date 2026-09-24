//! Golden contracts for public wire and persistence bytes.
//!
//! These fixtures pin values that clients and on-disk data depend on. A
//! failure means a wire or storage contract changed; fix the code, never the
//! fixture.

use fitz::protocol::manifest::{self, MESSAGE_MANIFEST};
use fitz::protocol::{MessageType, TypeMapping};
use fitz::testkit::golden::{assert_golden, hex};
use fitz::utils::idempotency::{self, Domain};
use fitz::utils::storage_key::{self, DomainKeyspace};
use std::fmt::Write as _;

const KEYSPACES: [DomainKeyspace; 7] = [
    DomainKeyspace::Kv,
    DomainKeyspace::Lease,
    DomainKeyspace::Notice,
    DomainKeyspace::Queue,
    DomainKeyspace::Rpc,
    DomainKeyspace::Schedule,
    DomainKeyspace::Stream,
];

const REALMS: [&str; 5] = ["acme", "", "a\0b", "é", "a/b"];

const DOMAINS: [Domain; 7] = [
    Domain::Kv,
    Domain::Stream,
    Domain::Notice,
    Domain::Queue,
    Domain::Lease,
    Domain::Rpc,
    Domain::Schedule,
];

#[test]
fn should_keep_storage_keyspace_prefixes_stable() {
    // Arrange
    let mut actual = String::new();

    // Act
    for realm in REALMS {
        for keyspace in KEYSPACES {
            let (start, end) = storage_key::domain_range(realm, keyspace);
            let _ = writeln!(
                actual,
                "{realm:?} {keyspace:?} code={} prefix={} range_end={} key={} raw_key={}",
                hex(&keyspace.code()),
                hex(&start),
                hex(&end),
                hex(&storage_key::prefixed_key(realm, keyspace, b"orders")),
                hex(&storage_key::prefixed_key(realm, keyspace, b"\xff\x00raw")),
            );
        }
        let (_, realm_end) = storage_key::realm_range(realm);
        let _ = writeln!(
            actual,
            "{realm:?} realm_prefix={} realm_range_end={} kv={} queue_meta={}",
            hex(&storage_key::realm_prefix(realm)),
            hex(&realm_end),
            hex(&storage_key::realm_domain_prefix(realm, "kv")),
            hex(&storage_key::realm_domain_prefix(realm, "queue_meta")),
        );
    }

    // Assert
    assert_golden("storage_keyspace", &actual);
}

#[test]
fn should_keep_message_manifest_stable() {
    // Arrange
    let mapping = TypeMapping::new();
    let mut actual = String::new();

    // Act
    for entry in MESSAGE_MANIFEST {
        let _ = writeln!(
            actual,
            "{entry:?} channel={:?}",
            mapping.get_channel(entry.message_id)
        );
    }
    for id in 0..=999_u16 {
        let message_type = MessageType::new(id);
        let client = manifest::client_entry(message_type).map(|entry| entry.message_id);
        if client != Err("unsupported message type") {
            let _ = writeln!(actual, "id={id} client={client:?}");
        }
    }

    // Assert
    assert_golden("message_manifest", &actual);
}

#[test]
fn should_keep_idempotency_classification_stable() {
    // Arrange
    let mut actual = String::new();

    // Act
    for domain in DOMAINS {
        for id in 0..=999_u16 {
            let class = idempotency::classify(domain, id);
            if !matches!(class, idempotency::Idempotency::NonIdempotent) {
                let _ = writeln!(actual, "{domain:?} {id} {class}");
            }
        }
    }

    // Assert
    assert_golden("idempotency_classification", &actual);
}
