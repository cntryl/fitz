use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use fitz::domains::notification::minimal::NotificationDomain;
use fitz::protocol::tlv::MessageType;

#[path = "config.rs"]
mod config;

fn bench_payload_sensitivity(c: &mut Criterion) {
    let sizes = [16usize, 64usize, 256usize];
    let subs = 64usize;

    let mut domain = NotificationDomain::new();
    for sub in 0..subs { domain.register(1, sub); }

    let mut group = c.benchmark_group("payload_sensitivity");
    group.sampling_mode(SamplingMode::Flat);

    for &size in &sizes {
        let payload = vec![0u8; size];
        group.bench_with_input(format!("domain_handle_{}B", size), &payload, |b, p| {
            b.iter(|| {
                let _ = domain.handle(MessageType::new(1), black_box(p.as_slice()));
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_payload_sensitivity
}
criterion_main!(benches);