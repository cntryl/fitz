use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::runtime::envelope::{Envelope, MessageId};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::{Duration, Instant};

stress_allocator!();

const ENVELOPE_BATCH_OPS: u64 = 256;

#[derive(Clone)]
#[allow(dead_code)]
struct TestMessage {
    value: u64,
}

fn address(route: String) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(1), Route::new(route))
}

fn record_group(ctx: &mut StressContext, payload: &str) {
    ctx.parameter("group", "hotpath_envelope");
    ctx.parameter("payload", payload);
}

#[stress(tier = 1, name = "owning_new_struct_payload")]
fn should_create_owning_new_struct_payload(ctx: &mut StressContext) {
    record_group(ctx, "struct");
    let pairs: Vec<(RouteAddress, TestMessage)> = (0_u64..4)
        .map(|i| {
            (
                address(format!("ftz://1/kv/acme/app/users{i}")),
                TestMessage { value: 42 + i },
            )
        })
        .collect();
    let mut index = 0usize;

    ctx.measure("owning_new_struct_payload", || {
        let (destination, message) = &pairs[index];
        index = (index + 1) % pairs.len();
        black_box(Envelope::new(
            black_box(destination.clone()),
            black_box(message.clone()),
        ));
    });
}

#[stress(tier = 1, name = "owning_from_route_struct_payload")]
fn should_create_owning_from_route_struct_payload(ctx: &mut StressContext) {
    record_group(ctx, "struct");
    let triples: Vec<(RouteAddress, RouteAddress, TestMessage)> = (0_u64..4)
        .map(|i| {
            (
                address(format!("ftz://1/rpc/acme/app/client{i}")),
                address(format!("ftz://1/rpc/acme/app/server{i}")),
                TestMessage { value: 100 + i },
            )
        })
        .collect();
    let mut index = 0usize;

    ctx.measure_batch(
        "owning_from_route_struct_payload",
        ENVELOPE_BATCH_OPS,
        || {
            for _ in 0..ENVELOPE_BATCH_OPS {
                let (source, destination, message) = &triples[index];
                index = (index + 1) % triples.len();
                black_box(Envelope::from_route(
                    black_box(source.clone()),
                    black_box(destination.clone()),
                    black_box(message.clone()),
                ));
            }
        },
    );
}

#[stress(tier = 1, name = "owning_new_with_deadline_struct_payload")]
fn should_create_owning_new_with_deadline_struct_payload(ctx: &mut StressContext) {
    record_group(ctx, "struct");
    let deadline = Instant::now() + Duration::from_secs(30);
    let pairs: Vec<(RouteAddress, TestMessage)> = (0_u64..4)
        .map(|i| {
            (
                address(format!("ftz://1/lease/acme/app/resource{i}")),
                TestMessage { value: 200 + i },
            )
        })
        .collect();
    let mut index = 0usize;

    ctx.measure_batch(
        "owning_new_with_deadline_struct_payload",
        ENVELOPE_BATCH_OPS,
        || {
            for _ in 0..ENVELOPE_BATCH_OPS {
                let (destination, message) = &pairs[index];
                index = (index + 1) % pairs.len();
                black_box(
                    Envelope::new(black_box(destination.clone()), black_box(message.clone()))
                        .with_deadline(black_box(deadline)),
                );
            }
        },
    );
}

#[stress(tier = 1, name = "owning_new_with_causation_struct_payload")]
fn should_create_owning_new_with_causation_struct_payload(ctx: &mut StressContext) {
    record_group(ctx, "struct");
    let parent_id = MessageId::new();
    let pairs: Vec<(RouteAddress, TestMessage)> = (0_u64..4)
        .map(|i| {
            (
                address(format!("ftz://1/notice/acme/app/events{i}")),
                TestMessage { value: 300 + i },
            )
        })
        .collect();
    let mut index = 0usize;

    ctx.measure("owning_new_with_causation_struct_payload", || {
        let (destination, message) = &pairs[index];
        index = (index + 1) % pairs.len();
        black_box(
            Envelope::new(black_box(destination.clone()), black_box(message.clone()))
                .with_causation(black_box(parent_id)),
        );
    });
}

#[stress(tier = 1, name = "owning_new_vec_payload_1_u64")]
fn should_create_owning_new_vec_payload_1_u64(ctx: &mut StressContext) {
    record_group(ctx, "vec_1_u64");
    let pool: Vec<(RouteAddress, Vec<u64>)> = (0_u64..4)
        .map(|i| {
            (
                address(format!("ftz://1/stream/acme/app/logs{i}")),
                vec![1 + i],
            )
        })
        .collect();
    let mut index = 0usize;

    ctx.measure("owning_new_vec_payload_1_u64", || {
        let (destination, message) = &pool[index];
        index = (index + 1) % pool.len();
        black_box(Envelope::new(
            black_box(destination.clone()),
            black_box(message.clone()),
        ));
    });
}

#[stress(tier = 1, name = "owning_new_vec_payload_100_u64")]
fn should_create_owning_new_vec_payload_100_u64(ctx: &mut StressContext) {
    record_group(ctx, "vec_100_u64");
    let large_message = (0..100).collect::<Vec<u64>>();
    let pool: Vec<(RouteAddress, Vec<u64>)> = (0..4)
        .map(|i| {
            (
                address(format!("ftz://1/stream/acme/app/logs_large{i}")),
                large_message.clone(),
            )
        })
        .collect();
    let mut index = 0usize;

    ctx.measure("owning_new_vec_payload_100_u64", || {
        let (destination, message) = &pool[index];
        index = (index + 1) % pool.len();
        black_box(Envelope::new(
            black_box(destination.clone()),
            black_box(message.clone()),
        ));
    });
}

stress_main!();
