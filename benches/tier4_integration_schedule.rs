//! Schedule domain tier 4 integration benchmarks
//!
//! Full protocol encoding/decoding and parsing pipeline
//! Note: Persistence tests skipped due to Midge commit() bug

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::schedule::protocol::SchedulePayload;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_encode_payload(c: &mut Criterion) {
    let payload = SchedulePayload {
        cron: "0 9 * * 1,2,3,4,5".to_string(),
        resource: "meeting".to_string(),
        operation: "send_reminder".to_string(),
    };

    let mut group = c.benchmark_group("schedule_integration_encode");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("encode_payload", |b| {
        b.iter(|| payload.clone().encode())
    });

    group.finish();
}

fn bench_roundtrip_payload(c: &mut Criterion) {
    let payload = SchedulePayload {
        cron: "*/15 * * * *".to_string(),
        resource: "cache_refresh".to_string(),
        operation: "invalidate".to_string(),
    };

    let mut group = c.benchmark_group("schedule_integration_roundtrip");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("encode_decode_roundtrip", |b| {
        b.iter(|| {
            let encoded = payload.clone().encode();
            SchedulePayload::decode(&encoded)
        })
    });

    group.finish();
}

fn bench_multiple_payloads(c: &mut Criterion) {
    let payloads = vec![
        SchedulePayload {
            cron: "0 0 * * *".to_string(),
            resource: "backup".to_string(),
            operation: "full".to_string(),
        },
        SchedulePayload {
            cron: "0 6 * * 1,2,3,4,5".to_string(),
            resource: "report".to_string(),
            operation: "generate".to_string(),
        },
        SchedulePayload {
            cron: "*/30 * * * *".to_string(),
            resource: "health_check".to_string(),
            operation: "ping".to_string(),
        },
    ];

    let mut group = c.benchmark_group("schedule_integration_multi_payload");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(3));

    group.bench_function("encode_3_payloads", |b| {
        b.iter(|| {
            for payload in &payloads {
                let _ = payload.clone().encode();
            }
        })
    });

    group.finish();
}

fn bench_large_schedule_payload(c: &mut Criterion) {
    let large_operation = "operation_with_long_name_".repeat(10);
    let large_resource = "resource_name_".repeat(10);

    let payload = SchedulePayload {
        cron: "0 9 * * *".to_string(),
        resource: large_resource,
        operation: large_operation,
    };

    let mut group = c.benchmark_group("schedule_integration_large_payload");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("encode_decode_large_payload", |b| {
        b.iter(|| {
            let encoded = payload.clone().encode();
            SchedulePayload::decode(&encoded)
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_encode_payload,
        bench_roundtrip_payload,
        bench_multiple_payloads,
        bench_large_schedule_payload
}
criterion_main!(benches);
