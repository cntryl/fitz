use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_mux_route_reuse(c: &mut Criterion) {
    let sizes = [0usize, 16usize, 64usize];

    let mut group = c.benchmark_group("hotpath_mux_route_reuse");
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
        let name = format!("route_reuse_{}B", size);
        group.bench_function(&name, |b| {
            b.iter(|| {
                // clone is cheap for Bytes; route consumes the record
                let msg = mux.route(record.clone()).unwrap();
                // release immediately to avoid backpressure
                mux.release(msg.channel);
                black_box(&msg);
            })
        });

        // Zero-copy route_ref bench
        let mut mux2 = Mux::new(1024);
        let (mt, slice, _) = decoder.decode_one_ref(&data).unwrap();
        let name = format!("route_ref_reuse_{}B", size);
        group.bench_function(&name, |b| {
            b.iter(|| {
                let cref = mux2.route_ref(mt, black_box(slice)).unwrap();
                mux2.release(cref.channel);
                black_box(&cref);
            })
        });
    }

    group.finish();
}

fn bench_mux_route_decode_each(c: &mut Criterion) {
    let sizes = [0usize, 16usize, 64usize];

    let mut group = c.benchmark_group("hotpath_mux_route_decode");
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
        let name = format!("route_decode_each_{}B", size);
        group.bench_function(&name, |b| {
            b.iter(|| {
                // decode a new record per iteration and route it
                let (record, _) = decoder.decode_one(black_box(&data)).unwrap();
                let msg = mux.route(record).unwrap();
                // release to keep capacity available
                mux.release(msg.channel);
                black_box(&msg);
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_mux_route_reuse, bench_mux_route_decode_each
}
criterion_main!(benches);
