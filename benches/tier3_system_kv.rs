//! KV domain tier 3 system benchmarks using stress
//!
//! Concurrent realm/column family contention
//! Tests impact of state isolation and sharding on performance
//! Measures concurrent access patterns and lock contention
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via set_elements(count)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::domains::kv::{KvActor, KvMessage, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

#[stress_test]
fn should_complete_10_puts_same_family(ctx: &mut StressContext) {
    ctx.tag("scenario", "single_family_intensive");

    // Setup: Actor + store outside measurement
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        // Begin transaction
        let response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "system".to_string(),
            area: "kv".to_string(),
            resource: "intensive".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match response {
            fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        // 10 puts on same family
        for i in 0..10 {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "intensive".to_string(),
                key: Bytes::from(format!("key{}", i).into_bytes()),
                value: Bytes::from(format!("value{}", i).into_bytes()),
            });
        }

        // Rollback
        actor.handle(KvMessage::Rollback { tx_id });
    });
    ctx.set_elements(10 * iterations as u64);
}

#[stress_test]
fn should_complete_interleaved_puts_2_families(ctx: &mut StressContext) {
    ctx.tag("scenario", "dual_family_concurrent");

    // Setup: Actor + two column families
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        // Begin on family 1
        let response1 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "system".to_string(),
            area: "kv".to_string(),
            resource: "f1".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id1 = match response1 {
            fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        // Begin on family 2
        let response2 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(2),
            realm: "system".to_string(),
            area: "kv".to_string(),
            resource: "f2".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id2 = match response2 {
            fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        // Interleaved puts: alternating between families
        for i in 0..10 {
            // Put to family 1
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

        // Rollback both
        actor.handle(KvMessage::Rollback { tx_id: tx_id1 });
        actor.handle(KvMessage::Rollback { tx_id: tx_id2 });
    });
    ctx.set_elements(20 * iterations as u64); // 10 per family
}

#[stress_test]
fn should_complete_10_puts_per_3_families(ctx: &mut StressContext) {
    ctx.tag("scenario", "triple_family_contention");

    // Setup: Actor + three column families
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        // Three families accessed sequentially
        for family_id in 1..=3 {
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(family_id),
                realm: "system".to_string(),
                area: "kv".to_string(),
                resource: format!("f{}", family_id),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            // 10 puts per family
            for i in 0..10 {
                actor.handle(KvMessage::Put {
                    tx_id,
                    route_family: RouteFamily::new(family_id),
                    resource: format!("f{}", family_id),
                    key: Bytes::from(format!("k{}", i).into_bytes()),
                    value: Bytes::from_static(b"v"),
                });
            }

            actor.handle(KvMessage::Rollback { tx_id });
        }
    });
    ctx.set_elements(30 * iterations as u64); // 10 per family
}

#[stress_test]
fn should_complete_mixed_read_write_families(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_read_write_families");

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

    // Measure: read-only on f1, write on f2
    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        // Read-only transaction on family 1
        let response1 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "system".to_string(),
            area: "kv".to_string(),
            resource: "read_f1".to_string(),
            mode: TxMode::ReadOnly,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id1 = match response1 {
            fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        // 5 reads
        for i in 0..5 {
            actor.handle(KvMessage::Get {
                tx_id: tx_id1,
                route_family: RouteFamily::new(1),
                resource: "setup".to_string(),
                key: Bytes::from(format!("setup_k{}", i).into_bytes()),
            });
        }

        actor.handle(KvMessage::Rollback { tx_id: tx_id1 });

        // Write transaction on family 2
        let response2 = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(2),
            realm: "system".to_string(),
            area: "kv".to_string(),
            resource: "write_f2".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id2 = match response2 {
            fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        // 5 writes on family 2
        for i in 0..5 {
            actor.handle(KvMessage::Put {
                tx_id: tx_id2,
                route_family: RouteFamily::new(2),
                resource: "write_f2".to_string(),
                key: Bytes::from(format!("new_k{}", i).into_bytes()),
                value: Bytes::from_static(b"new_v"),
            });
        }

        actor.handle(KvMessage::Rollback { tx_id: tx_id2 });
    });
    ctx.set_elements(10 * iterations as u64); // 5 reads + 5 writes (no deletes in measure)
}

stress_main!();
