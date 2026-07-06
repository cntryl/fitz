//! KV domain tier 3 system benchmarks using stress
//!
//! Concurrent realm/column family contention
//! Tests impact of state isolation and sharding on performance
//! Measures concurrent access patterns and lock contention
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via `record_completed(count)`

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::domains::kv::{KvActor, KvMessage, KvResponse, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

const SAME_FAMILY_WRITE_BATCH_COUNT: u64 = 16;
const SAME_FAMILY_PUTS_PER_BATCH: u64 = 10;

fn begin_transaction(
    actor: &mut KvActor,
    family_id: u64,
    resource: &str,
    mode: TxMode,
) -> Option<u64> {
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(family_id),
        realm: "system".to_string(),
        area: "kv".to_string(),
        resource: resource.to_string(),
        mode,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    match response {
        KvResponse::BeginOk { tx_id } => Some(tx_id),
        _ => None,
    }
}

#[stress(tier = 3)]
fn should_complete_10_puts_same_family(ctx: &mut StressContext) {
    ctx.parameter("scenario", "single_family_intensive");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter(
        "batch_size",
        format!("{SAME_FAMILY_WRITE_BATCH_COUNT}_transactions_x{SAME_FAMILY_PUTS_PER_BATCH}_puts"),
    );

    // Setup: Actor + store outside measurement
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let writes: Vec<(Bytes, Bytes)> = (0..SAME_FAMILY_PUTS_PER_BATCH)
        .map(|i| {
            (
                Bytes::from(format!("key{i}").into_bytes()),
                Bytes::from(format!("value{i}").into_bytes()),
            )
        })
        .collect();

    let iterations = ctx.measure_workload("complete_same_family_write_batches", || {
        for batch in 0..SAME_FAMILY_WRITE_BATCH_COUNT {
            let resource = format!("intensive-{batch}");
            let Some(tx_id) = begin_transaction(&mut actor, 1, &resource, TxMode::ReadWrite) else {
                continue;
            };

            for (key, value) in &writes {
                actor.handle(KvMessage::Put {
                    tx_id,
                    route_family: RouteFamily::new(1),
                    resource: resource.clone(),
                    key: key.clone(),
                    value: value.clone(),
                });
            }

            actor.handle(KvMessage::Rollback { tx_id });
        }
    });

    let completions_per_iteration =
        SAME_FAMILY_WRITE_BATCH_COUNT * (SAME_FAMILY_PUTS_PER_BATCH + 2);
    stress_config::record_completed(ctx, completions_per_iteration * iterations);
}

#[stress(tier = 3)]
fn should_complete_interleaved_puts_2_families(ctx: &mut StressContext) {
    ctx.parameter("scenario", "dual_family_concurrent");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "20_puts");

    // Setup: Actor + two column families
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);
    let tx_id1 =
        begin_transaction(&mut actor, 1, "f1", TxMode::ReadWrite).expect("begin family 1 tx");
    let tx_id2 =
        begin_transaction(&mut actor, 2, "f2", TxMode::ReadWrite).expect("begin family 2 tx");

    let iterations = ctx.measure_workload("complete_interleaved_puts_2_families", || {
        for i in 0..10 {
            actor.handle(KvMessage::Put {
                tx_id: tx_id1,
                route_family: RouteFamily::new(1),
                resource: "f1".to_string(),
                key: Bytes::from(format!("k1_{i}").into_bytes()),
                value: Bytes::from_static(b"v1"),
            });

            // Put to family 2
            actor.handle(KvMessage::Put {
                tx_id: tx_id2,
                route_family: RouteFamily::new(2),
                resource: "f2".to_string(),
                key: Bytes::from(format!("k2_{i}").into_bytes()),
                value: Bytes::from_static(b"v2"),
            });
        }
    });

    actor.handle(KvMessage::Rollback { tx_id: tx_id1 });
    actor.handle(KvMessage::Rollback { tx_id: tx_id2 });
    stress_config::record_completed(ctx, 20 * iterations); // 10 per family
}

#[stress(tier = 3)]
fn should_complete_10_puts_per_3_families(ctx: &mut StressContext) {
    ctx.parameter("scenario", "triple_family_contention");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "30_puts");

    // Setup: Actor + three column families
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);
    let txs: Vec<(u64, u64, String)> = (1..=3)
        .map(|family_id| {
            let resource = format!("f{family_id}");
            let tx_id = begin_transaction(&mut actor, family_id, &resource, TxMode::ReadWrite)
                .expect("transaction begin should succeed during setup");
            (family_id, tx_id, resource)
        })
        .collect();

    let iterations = ctx.measure_workload("complete_10_puts_per_3_families", || {
        for (family_id, tx_id, resource) in &txs {
            for i in 0..10 {
                actor.handle(KvMessage::Put {
                    tx_id: *tx_id,
                    route_family: RouteFamily::new(*family_id),
                    resource: resource.clone(),
                    key: Bytes::from(format!("k{i}").into_bytes()),
                    value: Bytes::from_static(b"v"),
                });
            }
        }
    });

    for (_, tx_id, _) in txs {
        actor.handle(KvMessage::Rollback { tx_id });
    }
    stress_config::record_completed(ctx, 30 * iterations); // 10 per family
}

#[stress(tier = 3)]
fn should_complete_mixed_read_write_families(ctx: &mut StressContext) {
    ctx.parameter("scenario", "mixed_read_write_families");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "5_gets_5_puts");

    // Setup: Actor + two column families with pre-populated data
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);

    // Populate initial data on family 1
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "system".to_string(),
        area: "kv".to_string(),
        resource: "setup".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        other => panic!("expected setup begin ok, got {other:?}"),
    };

    for i in 0..5 {
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "setup".to_string(),
            key: Bytes::from(format!("setup_k{i}").into_bytes()),
            value: Bytes::from_static(b"setup_v"),
        });
    }

    actor.handle(KvMessage::Rollback { tx_id });

    let read_tx_id =
        begin_transaction(&mut actor, 1, "read_f1", TxMode::ReadOnly).expect("begin read tx");
    let write_tx_id =
        begin_transaction(&mut actor, 2, "write_f2", TxMode::ReadWrite).expect("begin write tx");

    let iterations = ctx.measure_workload("complete_mixed_read_write_families", || {
        for i in 0..5 {
            actor.handle(KvMessage::Get {
                tx_id: read_tx_id,
                route_family: RouteFamily::new(1),
                resource: "setup".to_string(),
                key: Bytes::from(format!("setup_k{i}").into_bytes()),
            });
        }

        for i in 0..5 {
            actor.handle(KvMessage::Put {
                tx_id: write_tx_id,
                route_family: RouteFamily::new(2),
                resource: "write_f2".to_string(),
                key: Bytes::from(format!("new_k{i}").into_bytes()),
                value: Bytes::from_static(b"new_v"),
            });
        }
    });

    actor.handle(KvMessage::Rollback { tx_id: read_tx_id });
    actor.handle(KvMessage::Rollback { tx_id: write_tx_id });
    stress_config::record_completed(ctx, 10 * iterations); // 5 reads + 5 writes (no deletes in measure)
}

stress_main!();
