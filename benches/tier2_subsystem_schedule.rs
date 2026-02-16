//! Tier 2 (Subsystem) benchmarks for Schedule domain
//!
//! Measures schedule subsystem performance including:
//! - TLV encode/decode for CREATE/CANCEL/LIST messages
//! - Schedule actor + codec integration
//! - Response encoding
//!
//! Includes TLV overhead but no router/transport.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, SamplingMode};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse, ScheduleListEntry};
use fitz::protocol::schedule_codec;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::tlv_codec::TlvEncoder;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::SessionId;
use fitz::testkit::create_test_engine_with_cfs;
use bytes::Bytes;

#[path = "../benches/config.rs"]
mod config;

/// Create a test schedule actor
fn create_test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

/// Precompute routes, crons, payloads
fn precompute_data(count: usize) -> (Vec<String>, Vec<String>, Vec<Bytes>) {
    let routes = (0..count)
        .map(|i| format!("schedule://acme/jobs/task{:06}", i))
        .collect();
    
    let crons = (0..count)
        .map(|i| {
            let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
            patterns[i % patterns.len()].to_string()
        })
        .collect();
    
    let payloads = (0..count)
        .map(|i| Bytes::from(format!("payload-{:06}", i)))
        .collect();
    
    (routes, crons, payloads)
}

/// Benchmark: Encode CREATE message
fn bench_encode_create(c: &mut Criterion) {
    let (routes, crons, payloads) = precompute_data(1);
    
    let mut group = c.benchmark_group("schedule_codec_encode_create");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("encode", |b| {
        b.iter(|| {
            let mut enc = TlvEncoder::new();
            enc.put_string(black_box(&routes[0]));
            enc.put_string(black_box(&crons[0]));
            enc.put_bytes(black_box(&payloads[0]));
            let _bytes = enc.finish();
        });
    });
    
    group.finish();
}

/// Benchmark: Decode CREATE message
fn bench_decode_create(c: &mut Criterion) {
    let (routes, crons, payloads) = precompute_data(1);
    
    // Precompute encoded message
    let mut enc = TlvEncoder::new();
    enc.put_string(&routes[0]);
    enc.put_string(&crons[0]);
    enc.put_bytes(&payloads[0]);
    let encoded = enc.finish();
    
    let ctx = FrameContext {
        session_id: 1,
        channel_id: fitz::protocol::ChannelId::Control,
        msg_type: fitz::protocol::MessageType(700), // CREATE
        payload: Bytes::from(encoded.clone()),
        route_family: RouteFamily::new(1),
    };
    let route_family = RouteFamily::new(1);
    let session_id = SessionId(1);
    let subscriber = RouteAddress::new(route_family, Route::new("subscriber1"));
    
    let mut group = c.benchmark_group("schedule_codec_decode_create");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("decode", |b| {
        b.iter(|| {
            let _msg = schedule_codec::parse_request(
                black_box(&ctx),
                black_box(&encoded),
                route_family,
                session_id,
                subscriber.clone(),
            ).unwrap();
        });
    });
    
    group.finish();
}

/// Benchmark: En code CANCEL message
fn bench_encode_cancel(c: &mut Criterion) {
    let route = "schedule://acme/jobs/task001".to_string();
    
    let mut group = c.benchmark_group("schedule_codec_encode_cancel");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("encode", |b| {
        b.iter(|| {
            let mut enc = TlvEncoder::new();
            enc.put_string(black_box(&route));
            let _bytes = enc.finish();
        });
    });
    
    group.finish();
}

/// Benchmark: Encode LIST response with varying entry counts
fn bench_encode_list_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_codec_encode_list_response");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [10, 100, 1000] {
        let (routes, crons, payloads) = precompute_data(count);
        let entries: Vec<ScheduleListEntry> = (0..count)
            .map(|i| ScheduleListEntry {
                route: routes[i].clone(),
                cron: crons[i].clone(),
                payload: payloads[i].clone(),
            })
            .collect();
        
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let response = ScheduleResponse::ListDefs(entries.clone());
                let _bytes = schedule_codec::encode_response(black_box(&response));
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Full round-trip (encode request → decode → actor handle → encode response)
fn bench_full_roundtrip_create(c: &mut Criterion) {
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1);
    
    // Precompute encoded CREATE request
    let mut enc = TlvEncoder::new();
    enc.put_string(&routes[0]);
    enc.put_string(&crons[0]);
    enc.put_bytes(&payloads[0]);
    let encoded_request = enc.finish();
    
    let ctx = FrameContext {
        session_id: 1,
        channel_id: fitz::protocol::ChannelId::Control,
        msg_type: fitz::protocol::MessageType(700),
        payload: Bytes::from(encoded_request.clone()),
        route_family: RouteFamily::new(1),
    };
    let route_family = RouteFamily::new(1);
    let session_id = SessionId(1);
    let subscriber = RouteAddress::new(route_family, Route::new("subscriber1"));
    
    let mut group = c.benchmark_group("schedule_subsystem_roundtrip_create");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("roundtrip", |b| {
        b.iter(|| {
            // Decode request
            let msg = schedule_codec::parse_request(
                black_box(&ctx),
                black_box(&encoded_request),
                route_family,
                session_id,
                subscriber.clone(),
            ).unwrap();
            
            // Handle in actor
            let response = actor.handle(black_box(msg));
            
            // Encode response
            let _encoded_response = schedule_codec::encode_response(black_box(&response));
        });
    });
    
    group.finish();
}

/// Benchmark: Full roundtrip LIST (with varying schedule counts)
fn bench_full_roundtrip_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_subsystem_roundtrip_list");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [10, 100, 1000] {
        // Setup: Create schedules
        let mut actor = create_test_actor();
        let (routes, crons, payloads) = precompute_data(count);
        
        for i in 0..count {
            actor.handle(ScheduleMessage::Create {
                route: routes[i].clone(),
                cron: crons[i].clone(),
                payload: payloads[i].clone(),
            });
        }
        
        // Precompute encoded LIST request (empty payload)
        let encoded_request = Vec::new();
        
        let ctx = FrameContext {
            session_id: 1,
            channel_id: fitz::protocol::ChannelId::Control,
            msg_type: fitz::protocol::MessageType(702), // LIST
            payload: Bytes::new(),
            route_family: RouteFamily::new(1),
        };
        let route_family = RouteFamily::new(1);
        let session_id = SessionId(1);
        let subscriber = RouteAddress::new(route_family, Route::new("subscriber1"));
        
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                // Decode request
                let msg = schedule_codec::parse_request(
                    black_box(&ctx),
                    black_box(&encoded_request),
                    route_family,
                    session_id,
                    subscriber.clone(),
                ).unwrap();
                
                // Handle in actor
                let response = actor.handle(black_box(msg));
                
                // Encode response
                let _encoded_response = schedule_codec::encode_response(black_box(&response));
            });
        });
    }
    
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_encode_create,
        bench_decode_create,
        bench_encode_cancel,
        bench_encode_list_response,
        bench_full_roundtrip_create,
        bench_full_roundtrip_list,
}
criterion_main!(benches);
