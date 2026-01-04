use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::notification::bench::Matcher;
use fitz::protocol::tlv::MessageType;

#[path = "config.rs"]
mod config;

fn bench_matcher_lookup(c: &mut Criterion) {
    let mut matcher = Matcher::new();
    // register many subscribers for a single msg type
    for i in 0..64usize {
        matcher.register(100, i);
    }

    let payload = vec![0u8; 64];

    let mut group = c.benchmark_group("hotpath_notification_match");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("match_into_64subs", |b| {
        b.iter(|| {
            let mut out: smallvec::SmallVec<[usize; 8]> = smallvec::SmallVec::new();
            let n = matcher.match_into(&mut out, MessageType::new(100), black_box(&payload));
            black_box(n);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_matcher_lookup
}
criterion_main!(benches);
