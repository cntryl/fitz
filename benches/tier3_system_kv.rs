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

const TRIPLE_FAMILY_PUTS_PER_FAMILY: u64 = 25;

fn configure_kv_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "kv_operations");
    ctx.parameter("logical_unit", "kv_operation");
}

fn begin_transaction(
    actor: &mut KvActor,
    family_id: u64,
    resource: &str,
    mode: TxMode,
) -> Option<u64> {
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::try_from(family_id).expect("benchmark family must fit in u32"),
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
fn should_complete_10_puts_per_3_families(ctx: &mut StressContext) {
    ctx.parameter("scenario", "triple_family_contention");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter(
        "batch_size",
        format!("{}_puts", 3 * TRIPLE_FAMILY_PUTS_PER_FAMILY),
    );
    configure_kv_measurement(ctx);

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
            for i in 0..TRIPLE_FAMILY_PUTS_PER_FAMILY {
                actor.handle(KvMessage::Put {
                    tx_id: *tx_id,
                    route_family: RouteFamily::try_from(*family_id)
                        .expect("benchmark family must fit in u32"),
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
    stress_config::record_completed(ctx, 3 * TRIPLE_FAMILY_PUTS_PER_FAMILY * iterations);
}

stress_main!();
