//! Golden contracts for public wire and persistence bytes.
//!
//! These fixtures pin values that clients and on-disk data depend on. A
//! failure means a wire or storage contract changed; fix the code, never the
//! fixture.

use fitz::protocol::manifest::{self, MESSAGE_MANIFEST};
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::protocol::wire2::handshake::{Connect, ConnectRejected, GoAway, Hello};
use fitz::protocol::wire2::header::{encode_message, HeaderFlags};
use fitz::protocol::{MessageType, TypeMapping};
use fitz::testkit::golden::{assert_golden, hex};
use fitz::utils::idempotency::{self, Domain};
use fitz::utils::storage_key::{self, DomainKeyspace};
use std::fmt::Write as _;
use std::num::NonZeroU64;

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

#[test]
fn should_register_broker_control_answers_as_server_to_client() {
    // Arrange
    let broker_controls = [
        MessageType::HELLO,
        MessageType::CONNECT_REJECTED,
        MessageType::GOAWAY,
    ];

    // Act
    let entries = broker_controls.map(|message_type| manifest::entry(message_type).unwrap());

    // Assert
    assert_eq!(broker_controls.map(|m| m.as_u16()), [2, 3, 4]);
    for entry in entries {
        assert_eq!(entry.domain, "control");
        assert_eq!(entry.direction, manifest::ManifestDirection::ServerToClient);
        assert_eq!(entry.route_scheme, None);
    }
    assert!(manifest::client_entry(MessageType::HELLO).is_err());
}

fn vector(out: &mut String, name: &str, bytes: &[u8]) {
    let _ = writeln!(out, "{name} {}", hex(bytes));
}

fn request(out: &mut String, name: &str, msg_type: u16, correlation: u64, value: &[u8]) {
    vector(
        out,
        name,
        &encode_message(
            msg_type,
            HeaderFlags::none(),
            NonZeroU64::new(correlation),
            value,
        ),
    );
}

fn string_payload(parts: &[&str]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    for part in parts {
        enc.put_string(part);
    }
    enc.finish()
}

fn control_and_envelope_vectors(actual: &mut String) {
    vector(
        actual,
        "header_type104_corr1_value_hi",
        &encode_message(104, HeaderFlags::none(), NonZeroU64::new(1), b"hi"),
    );
    vector(
        actual,
        "header_type500_ack_requested_corr2_empty",
        &encode_message(500, HeaderFlags::ack_requested(), NonZeroU64::new(2), b""),
    );
    vector(
        actual,
        "header_type504_notification_corr0",
        &encode_message(504, HeaderFlags::none(), None, b"x"),
    );
    vector(
        actual,
        "connect_body",
        &Connect {
            min_version: 2,
            max_version: 2,
            client_caps: 0,
            jwt: String::new(),
            service_name: "orders".to_owned(),
            client_build: "fitz-rs/2.0.0".to_owned(),
        }
        .encode(),
    );
    vector(
        actual,
        "hello_body",
        &Hello {
            version: 2,
            broker_caps: 0,
            max_message_bytes: 1_048_576,
            max_in_flight: 64,
            session_id: 42,
        }
        .encode(),
    );
    vector(
        actual,
        "connect_rejected_body",
        &ConnectRejected {
            code: 1,
            message: "jwt expired".to_owned(),
        }
        .encode(),
    );
    vector(
        actual,
        "goaway_body",
        &GoAway {
            code: 54,
            grace_ms: 30_000,
            reason: "redeploy".to_owned(),
        }
        .encode(),
    );
    vector(actual, "success_envelope", &[0]);
    vector(
        actual,
        "error_envelope_1052_key_not_found",
        &fitz::protocol::error_codes::encode_error_body(1052, "Key not found"),
    );
    vector(
        actual,
        "error_envelope_4005_busy",
        &fitz::protocol::error_codes::encode_error_body(4005, "busy"),
    );
}

fn domain_request_vectors(actual: &mut String) {
    let route = "kv://acme/app/users";
    let mut kv_begin = string_payload(&[route]);
    kv_begin.push(1);
    request(actual, "kv_begin_read_write", 100, 1, &kv_begin);
    let mut kv_put = PayloadEncoder::new();
    kv_put.put_u64(7);
    kv_put.put_string(route);
    kv_put.put_bytes(b"bob");
    kv_put.put_bytes(b"alice");
    request(actual, "kv_put", 104, 1, &kv_put.finish());
    let mut kv_commit = PayloadEncoder::new();
    kv_commit.put_u64(7);
    kv_commit.put_string(route);
    kv_commit.put_u8(1);
    request(actual, "kv_commit_sync", 101, 1, &kv_commit.finish());
    let mut kv_scan = PayloadEncoder::new();
    kv_scan.put_u64(7);
    kv_scan.put_string(route);
    kv_scan.put_u8(1);
    kv_scan.put_bytes(b"a");
    kv_scan.put_u8(0);
    kv_scan.put_u8(1);
    kv_scan.put_u32(10);
    kv_scan.put_u8(0b11);
    request(
        actual,
        "kv_scan_reverse_exclusive",
        108,
        1,
        &kv_scan.finish(),
    );

    let mut notice_publish = PayloadEncoder::new();
    notice_publish.put_string("notice://acme/app/events");
    notice_publish.put_bytes(b"hello");
    request(actual, "notice_publish", 500, 1, &notice_publish.finish());
    request(
        actual,
        "notice_subscribe",
        501,
        1,
        &string_payload(&["notice://acme/app/*"]),
    );

    let mut queue_enqueue = PayloadEncoder::new();
    queue_enqueue.put_string("queue://acme/app/tasks");
    queue_enqueue.put_bytes(b"job");
    queue_enqueue.put_u8(0);
    request(actual, "queue_enqueue", 200, 1, &queue_enqueue.finish());

    let mut lease_acquire = PayloadEncoder::new();
    lease_acquire.put_string("lease://acme/app/lock");
    lease_acquire.put_string("worker-1");
    lease_acquire.put_u64(30);
    lease_acquire.put_u32(0);
    request(
        actual,
        "lease_acquire_immediate",
        400,
        1,
        &lease_acquire.finish(),
    );

    let mut rpc_subscribe = PayloadEncoder::new();
    rpc_subscribe.put_string("rpc://acme/app/compute");
    rpc_subscribe.put_u32(4);
    rpc_subscribe.put_u8(1);
    request(
        actual,
        "rpc_subscribe_worker_cancellable",
        300,
        1,
        &rpc_subscribe.finish(),
    );
    let mut rpc_request = PayloadEncoder::new();
    rpc_request.put_raw(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    rpc_request.put_string("rpc://acme/app/compute");
    rpc_request.put_bytes(b"ping");
    rpc_request.put_u8(1);
    rpc_request.put_u32(5_000);
    request(
        actual,
        "rpc_request_with_budget",
        302,
        1,
        &rpc_request.finish(),
    );
}

fn stream_and_schedule_vectors(actual: &mut String) {
    let mut stream_begin = string_payload(&["stream://acme/app/log"]);
    stream_begin.push(0);
    request(actual, "stream_begin", 600, 1, &stream_begin);
    let mut stream_append = PayloadEncoder::new();
    stream_append.put_u64(9);
    stream_append.put_u64(0);
    stream_append.put_bytes(b"evt");
    stream_append.put_u8(0);
    request(actual, "stream_append", 601, 1, &stream_append.finish());
    let mut stream_read = PayloadEncoder::new();
    stream_read.put_string("stream://acme/app/log");
    stream_read.put_u64(0);
    stream_read.put_u64(100);
    stream_read.put_u8(0);
    request(actual, "stream_read", 604, 1, &stream_read.finish());

    let mut schedule_create = PayloadEncoder::new();
    schedule_create.put_string("schedule://acme/app/report/run");
    schedule_create.put_string("0 * * * *");
    schedule_create.put_u8(0);
    schedule_create.put_bytes(b"p");
    request(actual, "schedule_create", 700, 1, &schedule_create.finish());
    request(actual, "schedule_list_first_page", 702, 1, &[]);
}

/// Wire format 2 vectors every SDK must reproduce byte for byte: the header,
/// the handshake bodies, the error envelope, and one request per domain.
/// Requests carry correlation 1 so the header bytes are fixed.
#[test]
fn should_keep_wire2_vectors_stable() {
    // Arrange
    let mut actual = String::new();

    // Act
    control_and_envelope_vectors(&mut actual);
    domain_request_vectors(&mut actual);
    stream_and_schedule_vectors(&mut actual);

    // Assert
    assert_golden("wire2_vectors", &actual);
}
