#![allow(deprecated)]
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{DeliveryError, MailboxSink};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};

#[path = "criterion_config.rs"]
mod criterion_config;

struct MessageActor;

impl Actor for MessageActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn prefill_normal_lane(mailbox: &Mailbox, address: &RouteAddress, count: usize) {
    for value in 0..count {
        mailbox
            .deliver(Envelope::new(address.clone(), value as u64))
            .expect("prefill should succeed");
    }
}

fn bench_mailbox_deliver_primary(c: &mut Criterion) {
    let empty_address = test_address(1, "/bench/mailbox/primary/empty");
    let mid_fill_address = test_address(1, "/bench/mailbox/primary/mid-fill");
    let full_address = test_address(1, "/bench/mailbox/primary/full");
    let high_priority_address = test_address(1, "/bench/mailbox/primary/high-priority");

    let mut group = c.benchmark_group("subsystem_mailbox");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("deliver_empty_primary", |b| {
        b.iter_batched(
            || {
                (
                    Mailbox::new(8),
                    Envelope::new(empty_address.clone(), 42_u64),
                )
            },
            |(mailbox, envelope)| {
                mailbox
                    .deliver(envelope)
                    .expect("deliver to empty mailbox should succeed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("deliver_mid_fill_primary", |b| {
        b.iter_batched(
            || {
                let mailbox = Mailbox::new(8);
                prefill_normal_lane(&mailbox, &mid_fill_address, 4);
                (mailbox, Envelope::new(mid_fill_address.clone(), 99_u64))
            },
            |(mailbox, envelope)| {
                mailbox
                    .deliver(envelope)
                    .expect("deliver to mid-fill mailbox should succeed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("deliver_full_primary", |b| {
        b.iter_batched(
            || {
                let mailbox = Mailbox::new(1);
                prefill_normal_lane(&mailbox, &full_address, 1);
                (mailbox, Envelope::new(full_address.clone(), 2_u64))
            },
            |(mailbox, envelope)| match mailbox.deliver(envelope) {
                Err(DeliveryError::MailboxFull { .. }) => {
                    black_box(());
                }
                other => panic!("expected MailboxFull, got {other:?}"),
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("deliver_high_priority_when_normal_lane_full_primary", |b| {
        b.iter_batched(
            || {
                let mailbox = Mailbox::new(1);
                prefill_normal_lane(&mailbox, &high_priority_address, 1);
                (mailbox, Envelope::new(high_priority_address.clone(), 7_u64))
            },
            |(mailbox, envelope)| {
                mailbox
                    .deliver_high_priority(envelope)
                    .expect("high-priority lane should remain independent from normal saturation");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_mailbox_send_smoke(c: &mut Criterion) {
    let scheduler = Scheduler::new(1);
    let actor_refs: Vec<_> = (0..32)
        .map(|i| {
            scheduler.spawn(
                MessageActor,
                test_address(1, &format!("/bench/mailbox/smoke/{i}")),
                65_536,
            )
        })
        .collect();

    let mut group = c.benchmark_group("subsystem_mailbox");
    group.sampling_mode(SamplingMode::Flat);

    group.throughput(Throughput::Elements(1));
    group.bench_function("actor_ref_send_smoke", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            actor_refs[idx % actor_refs.len()]
                .send(black_box(idx as u64))
                .expect("smoke send should stay on the success path");
            idx = (idx + 1) % actor_refs.len();
        });
    });

    group.throughput(Throughput::Elements(100));
    group.bench_function("actor_ref_send_100_smoke", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            for offset in 0..100 {
                let route_idx = (idx + offset) % actor_refs.len();
                actor_refs[route_idx]
                    .send(black_box((idx + offset) as u64))
                    .expect("smoke burst should stay on the success path");
            }
            idx = (idx + 100) % actor_refs.len();
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_mailbox_deliver_primary, bench_mailbox_send_smoke
}
criterion_main!(benches);
