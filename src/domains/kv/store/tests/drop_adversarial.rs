//! Adversarial regressions for DROP progress while admin estimates are refreshed.

use super::*;
use crate::domains::kv::key_layout;
use crate::domains::kv::KvDomain;
use crate::protocol::frame::ChannelId;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::message::MessageType;
use crate::protocol::payload_codec::PayloadEncoder;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{Envelope, Mailbox, MailboxSink, Router};
use std::time::Duration;

struct RefreshDropOutcome {
    ack_before_release: bool,
    rows_present: bool,
    inventory_present: bool,
}

fn drop_during_stalled_refresh(family_id: u32, route: &str) -> RefreshDropOutcome {
    let (engine, store, _directory) = local_store();
    assert_eq!(engine.create_column_family("cf_2").unwrap().id(), 2);
    let prefix = key_layout::realm_resource_prefix("acme", "app", "rows");
    let mut seed = store.begin(1, TxMode::ReadWrite).unwrap();
    seed.put(
        key_layout::encode_scoped_key(&prefix, b"key"),
        b"value".to_vec(),
    )
    .unwrap();
    seed.put(
        key_layout::inventory_metadata_key("acme", "app", "rows"),
        crate::domains::kv::inventory::encode_estimate(
            crate::domains::kv::inventory::KvInventoryEstimate {
                estimate_complete: false,
                ..Default::default()
            },
        ),
    )
    .unwrap();
    seed.commit(WritePolicy::Sync).unwrap();
    let router = Arc::new(Router::new());
    let sink = Arc::new(KvDomain::new(
        store.clone(),
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));
    let inbox = Arc::new(Mailbox::new(4));
    let family = RouteFamily::new(family_id);
    let source = RouteAddress::new(family, Route::new("inbox://session/77"));
    router.register(source.clone(), inbox.clone());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    let admin_sink = sink.clone();
    let admin = std::thread::spawn(move || {
        let _hook = write_admission::inject_admission(Box::new(move |cf, _| {
            assert_eq!(cf, 1);
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(15)).unwrap();
            Ok(true)
        }));
        admin_sink.admin_inventory_resource(RouteFamily::new(1), "acme", "app", "rows")
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut payload = PayloadEncoder::new();
    payload.put_string(route);
    payload.put_u8(1);

    sink.deliver(Envelope::from_route(
        source,
        RouteAddress::new(family, Route::new("kv://inbound")),
        FrameContext::new(
            77,
            ChannelId::Pub,
            MessageType::new(112),
            Bytes::from(payload.finish()),
            family,
        ),
    ))
    .unwrap();
    let before_admin_release = inbox.receiver().recv_timeout(Duration::from_secs(2));
    release_tx.send(()).unwrap();
    let refresh = admin.join().unwrap();
    let eventual = if before_admin_release.is_err() {
        Some(
            inbox
                .receiver()
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
        )
    } else {
        None
    };

    assert!(refresh.is_ok());
    let response = before_admin_release
        .as_ref()
        .ok()
        .or(eventual.as_ref())
        .unwrap();
    assert_eq!(
        response.payload::<FrameContext>().unwrap().payload.as_ref(),
        &[0]
    );
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    let rows_present = tx
        .get(&key_layout::encode_scoped_key(&prefix, b"key"))
        .unwrap()
        .is_some();
    let inventory_present = tx
        .get(&key_layout::inventory_metadata_key("acme", "app", "rows"))
        .unwrap()
        .is_some();
    drop(tx);
    sink.stop();
    drop(sink);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
    RefreshDropOutcome {
        ack_before_release: before_admin_release.is_ok(),
        rows_present,
        inventory_present,
    }
}

#[test]
fn should_complete_drop_while_another_familys_admin_refresh_is_stalled() {
    // Arrange
    let family = 2;
    // Act
    let outcome = drop_during_stalled_refresh(family, "kv://other/*");
    // Assert
    assert!(
        outcome.ack_before_release,
        "family 2 DROP only completed after releasing family 1 admin refresh"
    );
    assert!(outcome.rows_present);
    assert!(outcome.inventory_present);
}

#[test]
fn should_wait_for_its_family_refresh_before_dropping_inventory() {
    // Arrange
    let family = 1;
    // Act
    let outcome = drop_during_stalled_refresh(family, "kv://acme/*");
    // Assert
    assert!(!outcome.ack_before_release);
    assert!(!outcome.rows_present);
    assert!(!outcome.inventory_present);
}
