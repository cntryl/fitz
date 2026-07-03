#![allow(deprecated)]
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::protocol::mux::{Mux, MuxError};
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_mux_route_reuse(c: &mut Criterion) {
    let sizes = [0usize, 16usize, 64usize];

    let mut group = c.benchmark_group("hotpath_mux");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // Setup - build a single record and reuse clones
        let mut encoder = TlvEncoder::with_capacity(256);
        let payload = vec![0u8; size];
        encoder.encode(MessageType::new(120), &payload);
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let (record, _) = decoder.decode_one(&data).unwrap();

        let mut mux = Mux::new(1024);
        group.throughput(Throughput::Elements(1));
        let name = format!("route_owning_record_clone_{size}b");
        group.bench_function(&name, |b| {
            b.iter(|| {
                let msg = mux.route(record.clone()).unwrap();
                mux.release(msg.channel);
                black_box(&msg);
            });
        });

        let mut mux2 = Mux::new(1024);
        let (mt, slice, _) = decoder.decode_one_ref(&data).unwrap();
        let name = format!("route_ref_zero_copy_{size}b");
        group.bench_function(&name, |b| {
            b.iter(|| {
                let cref = mux2.route_ref(mt, black_box(slice)).unwrap();
                mux2.release(cref.channel);
                black_box(&cref);
            });
        });
    }

    group.finish();
}

fn bench_mux_route_decode_each(c: &mut Criterion) {
    let sizes = [0usize, 16usize, 64usize];

    let mut group = c.benchmark_group("hotpath_mux");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // Setup encoder data used each iteration for a fresh decode
        let mut encoder = TlvEncoder::with_capacity(256);
        let payload = vec![0u8; size];
        encoder.encode(MessageType::new(120), &payload);
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let mut mux = Mux::new(1024);

        group.throughput(Throughput::Elements(1));
        let name = format!("decode_then_route_owning_{size}b");
        group.bench_function(&name, |b| {
            b.iter(|| {
                let (record, _) = decoder.decode_one(black_box(&data)).unwrap();
                let msg = mux.route(record).unwrap();
                mux.release(msg.channel);
                black_box(&msg);
            });
        });
    }

    group.finish();
}

fn bench_mux_release_and_backpressure(c: &mut Criterion) {
    let sizes = [0usize, 16usize, 64usize];

    let mut group = c.benchmark_group("hotpath_mux");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for &size in &sizes {
        let mut encoder = TlvEncoder::with_capacity(256);
        let payload = vec![0u8; size];
        encoder.encode(MessageType::new(120), &payload);
        let data = encoder.finish();

        let decoder = TlvDecoder::new();
        let (mt, slice, _) = decoder.decode_one_ref(&data).unwrap();

        group.bench_function(format!("release_after_route_ref_{size}b"), |b| {
            b.iter_batched(
                || {
                    let mut mux = Mux::new(1);
                    let cref = mux.route_ref(mt, slice).unwrap();
                    (mux, cref.channel)
                },
                |(mut mux, channel)| {
                    mux.release(channel);
                    black_box(mux.occupancy(channel));
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("route_ref_channel_full_{size}b"), |b| {
            b.iter_batched(
                || {
                    let mut mux = Mux::new(1);
                    mux.route_ref(mt, slice).unwrap();
                    mux
                },
                |mut mux| match mux.route_ref(mt, slice) {
                    Err(MuxError::ChannelFull(channel)) => {
                        black_box(channel);
                    }
                    _ => panic!("expected ChannelFull"),
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_mux_route_reuse, bench_mux_route_decode_each, bench_mux_release_and_backpressure
}
criterion_main!(benches);
