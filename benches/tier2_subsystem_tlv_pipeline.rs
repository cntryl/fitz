use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_pipeline_iter_route_fanout(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;
    let subs = [1usize, 8usize, 64usize];

    let mut group = c.benchmark_group("subsystem_tlv_pipeline");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // build frame
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new(i as u16), &payload);
        }
        let data = encoder.finish();

        for &nsub in &subs {
            let name = format!("iter_{}B_{}subs", size, nsub);
            // mux capacity large enough to never fail
            let mut mux = Mux::new(records);
            let decoder = TlvDecoder::new();
            group.throughput(Throughput::Elements(records as u64));
            group.bench_function(&name, |b| {
                b.iter(|| {
                    // decode via iterator and route_ref with simulated fanout
                    for res in decoder.iter(black_box(&data)) {
                        let (mt, slice) = res.unwrap();
                        let cref = mux.route_ref(mt, slice).unwrap();
                        // simulate N subscribers doing a small amount of work with payload
                        for _ in 0..nsub {
                            black_box(slice);
                        }
                        mux.release(cref.channel);
                    }
                })
            });
        }
    }

    group.finish();
}

/// Decode + route using zero-copy path (decode_one_ref / route_ref) to match hot path and avoid 256x TlvRecord clone.
fn bench_pipeline_decode_into_route_fanout(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;
    let subs = [1usize, 8usize, 64usize];

    let mut group = c.benchmark_group("subsystem_tlv_pipeline");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // build frame
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new(i as u16), &payload);
        }
        let data = encoder.finish();

        for &nsub in &subs {
            let name = format!("into_{}B_{}subs", size, nsub);
            let mut mux = Mux::new(records);
            let decoder = TlvDecoder::new();
            group.throughput(Throughput::Elements(records as u64));
            group.bench_function(&name, |b| {
                b.iter(|| {
                    for res in decoder.iter(black_box(&data)) {
                        let (mt, slice) = res.unwrap();
                        let cref = mux.route_ref(mt, slice).unwrap();
                        for _ in 0..nsub {
                            black_box(slice);
                        }
                        mux.release(cref.channel);
                    }
                })
            });
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_pipeline_iter_route_fanout, bench_pipeline_decode_into_route_fanout
}
criterion_main!(benches);
