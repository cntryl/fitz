use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::notification::minimal::NotificationDomain;
use fitz::protocol::mux::Mux;
use fitz::protocol::tlv::{MessageType, TlvDecoder, TlvEncoder};

#[path = "config.rs"]
mod config;

fn bench_pipeline_notification(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let records = 256usize;
    let subs = [1usize, 8usize, 64usize];

    let mut group = c.benchmark_group("pipeline_notification");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        let mut encoder = TlvEncoder::with_capacity(1024 * 8);
        let payload = vec![0u8; size];
        for i in 0..records {
            encoder.encode(MessageType::new((i % 0xFF) as u16), &payload);
        }
        let data = encoder.finish();
        let decoder = TlvDecoder::new();

        for &nsub in &subs {
            // setup domain (matcher + fanout stub)
            let mut domain = NotificationDomain::new();
            for sub in 0..nsub {
                domain.register(1, sub);
            }

            let mut mux = Mux::new(records);

            let name = format!("notify_{}B_{}subs", size, nsub);
            group.throughput(Throughput::Elements(records as u64));
            group.bench_function(&name, |b| {
                b.iter(|| {
                    for res in decoder.iter(black_box(&data)) {
                        let (mt, slice) = res.unwrap();
                        // route and grant
                        if let Ok((_cref, _grant)) = mux.route_grant(mt, slice) {
                            // dispatch to domain (match -> fanout)
                            let _ = domain.handle(MessageType::new(1), slice);
                            // grant drops here and releases
                        } else {
                            // drop when full
                        }
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
    targets = bench_pipeline_notification
}
criterion_main!(benches);
