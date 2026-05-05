//! KV domain tier 3 system benchmarks using stress
//!
//! Concurrent realm/column family contention
//! Tests impact of state isolation and sharding on performance
//! Measures concurrent access patterns and lock contention
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via set_elements(count)

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::domains::kv::{KvActor, KvMessage, KvResponse, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

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

#[stress_test]
fn should_complete_10_puts_same_family(ctx: &mut StressContext) {
    ctx.tag("scenario", "single_family_intensive");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "10_puts");

    // Setup: Actor + store outside measurement
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let tx_id = match begin_transaction(&mut actor, 1, "intensive", TxMode::ReadWrite) {
        Some(tx_id) => tx_id,
        None => return,
    };

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for i in 0..10 {
                actor.handle(KvMessage::Put {
                    tx_id,
                    route_family: RouteFamily::new(1),
                    resource: "intensive".to_string(),
                    key: Bytes::from(format!("key{}", i).into_bytes()),
                    value: Bytes::from(format!("value{}", i).into_bytes()),
                });
            }
        },
    );

    actor.handle(KvMessage::Rollback { tx_id });
    ctx.set_elements(10 * iterations as u64);
}

#[stress_test]
fn should_complete_interleaved_puts_2_families(ctx: &mut StressContext) {
    ctx.tag("scenario", "dual_family_concurrent");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "20_puts");

    // Setup: Actor + two column families
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);
    let tx_id1 = match begin_transaction(&mut actor, 1, "f1", TxMode::ReadWrite) {
        Some(tx_id) => tx_id,
        None => return,
    };
    let tx_id2 = match begin_transaction(&mut actor, 2, "f2", TxMode::ReadWrite) {
        Some(tx_id) => tx_id,
        None => return,
    };

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for i in 0..10 {
                actor.handle(KvMessage::Put {
                    tx_id: tx_id1,
                    route_family: RouteFamily::new(1),
                    resource: "f1".to_string(),
                    key: Bytes::from(format!("k1_{}", i).into_bytes()),
                    value: Bytes::from_static(b"v1"),
                });

                // Put to family 2
                actor.handle(KvMessage::Put {
                    tx_id: tx_id2,
                    route_family: RouteFamily::new(2),
                    resource: "f2".to_string(),
                    key: Bytes::from(format!("k2_{}", i).into_bytes()),
                    value: Bytes::from_static(b"v2"),
                });
            }
        },
    );

    actor.handle(KvMessage::Rollback { tx_id: tx_id1 });
    actor.handle(KvMessage::Rollback { tx_id: tx_id2 });
    ctx.set_elements(20 * iterations as u64); // 10 per family
}

#[stress_test]
fn should_complete_10_puts_per_3_families(ctx: &mut StressContext) {
    ctx.tag("scenario", "triple_family_contention");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "30_puts");

    // Setup: Actor + three column families
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);
    let txs: Vec<(u64, u64, String)> = (1..=3)
        .map(|family_id| {
            let resource = format!("f{}", family_id);
            let tx_id = begin_transaction(&mut actor, family_id, &resource, TxMode::ReadWrite)
                .expect("transaction begin should succeed during setup");
            (family_id, tx_id, resource)
        })
        .collect();

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for (family_id, tx_id, resource) in &txs {
                for i in 0..10 {
                    actor.handle(KvMessage::Put {
                        tx_id: *tx_id,
                        route_family: RouteFamily::new(*family_id),
                        resource: resource.clone(),
                        key: Bytes::from(format!("k{}", i).into_bytes()),
                        value: Bytes::from_static(b"v"),
                    });
                }
            }
        },
    );

    for (_, tx_id, _) in txs {
        actor.handle(KvMessage::Rollback { tx_id });
    }
    ctx.set_elements(30 * iterations as u64); // 10 per family
}

#[stress_test]
fn should_complete_mixed_read_write_families(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_read_write_families");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "5_gets_5_puts");

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
        _ => return,
    };

    for i in 0..5 {
        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "setup".to_string(),
            key: Bytes::from(format!("setup_k{}", i).into_bytes()),
            value: Bytes::from_static(b"setup_v"),
        });
    }

    actor.handle(KvMessage::Rollback { tx_id });

    let read_tx_id = match begin_transaction(&mut actor, 1, "read_f1", TxMode::ReadOnly) {
        Some(tx_id) => tx_id,
        None => return,
    };
    let write_tx_id = match begin_transaction(&mut actor, 2, "write_f2", TxMode::ReadWrite) {
        Some(tx_id) => tx_id,
        None => return,
    };

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for i in 0..5 {
                actor.handle(KvMessage::Get {
                    tx_id: read_tx_id,
                    route_family: RouteFamily::new(1),
                    resource: "setup".to_string(),
                    key: Bytes::from(format!("setup_k{}", i).into_bytes()),
                });
            }

            for i in 0..5 {
                actor.handle(KvMessage::Put {
                    tx_id: write_tx_id,
                    route_family: RouteFamily::new(2),
                    resource: "write_f2".to_string(),
                    key: Bytes::from(format!("new_k{}", i).into_bytes()),
                    value: Bytes::from_static(b"new_v"),
                });
            }
        },
    );

    actor.handle(KvMessage::Rollback { tx_id: read_tx_id });
    actor.handle(KvMessage::Rollback { tx_id: write_tx_id });
    ctx.set_elements(10 * iterations as u64); // 5 reads + 5 writes (no deletes in measure)
}

stress_main!();
