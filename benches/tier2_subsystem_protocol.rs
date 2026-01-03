use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder, TlvRecord};

#[path = "config.rs"]
mod config;

fn bench_pipeline_iter_route_fanout(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;
    let subs = [1usize, 8usize, 64usize];

    let mut group = c.benchmark_group("pipeline_iter_route_fanout");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // build frame
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new((i % 0xFF) as u16), &payload);
        }
        let data = encoder.finish();
        let decoder = TlvDecoder::new();

        for &nsub in &subs {
            let name = format!("iter_{}B_{}subs", size, nsub);
            // mux capacity large enough to never fail
            let mut mux = Mux::new(records);
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

fn bench_pipeline_decode_into_route_fanout(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;
    let subs = [1usize, 8usize, 64usize];

    let mut group = c.benchmark_group("pipeline_decode_into_route_fanout");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        // build frame
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new((i % 0xFF) as u16), &payload);
        }
        let data = encoder.finish();
        let decoder = TlvDecoder::new();

        for &nsub in &subs {
            let name = format!("into_{}B_{}subs", size, nsub);
            let mut mux = Mux::new(records);
            // preallocate vec to reuse
            let mut out: Vec<TlvRecord> = Vec::with_capacity(records);
            group.throughput(Throughput::Elements(records as u64));
            group.bench_function(&name, |b| {
                b.iter(|| {
                    out.clear();
                    decoder.decode_into(black_box(&data), &mut out).unwrap();
                    for rec in &out {
                        let mt = rec.msg_type();
                        let slice = rec.value();
                        let msg = mux.route(rec.clone()).unwrap();
                        for _ in 0..nsub {
                            black_box(slice);
                        }
                        mux.release(msg.channel);
                    }
                })
            });
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_pipeline_iter_route_fanout, bench_pipeline_decode_into_route_fanout
}
criterion_main!(benches);
