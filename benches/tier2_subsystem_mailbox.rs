#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{DeliveryError, MailboxSink};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use std::hint::black_box;

const MID_FILL_BATCH_SIZE: usize = 64;
const SEND_SMOKE_BATCH_SIZE: usize = 32;
const FRESH_DELIVER_BATCH_SIZE: usize = 512;
const ERROR_DELIVER_BATCH_SIZE: usize = 128;

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

#[stress_test(tier = 2, name = "deliver_empty_primary")]
fn should_deliver_empty_primary(ctx: &mut StressContext) {
    let empty_address = test_address(1, "/bench/mailbox/primary/empty");

    let items = (0..FRESH_DELIVER_BATCH_SIZE)
        .map(|_| {
            (
                Mailbox::new(8),
                Envelope::new(empty_address.clone(), 42_u64),
            )
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, FRESH_DELIVER_BATCH_SIZE as u64, || {
        for (mailbox, envelope) in items {
            mailbox
                .deliver(envelope)
                .expect("deliver to empty mailbox should succeed");
        }
    });
}

#[stress_test(tier = 2, name = "deliver_mid_fill_64_primary")]
fn should_deliver_mid_fill_64_primary(ctx: &mut StressContext) {
    let mid_fill_address = test_address(1, "/bench/mailbox/primary/mid-fill");
    let items = (0..MID_FILL_BATCH_SIZE)
        .map(|_| {
            let mailbox = Mailbox::new(8);
            prefill_normal_lane(&mailbox, &mid_fill_address, 4);
            (mailbox, Envelope::new(mid_fill_address.clone(), 99_u64))
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, MID_FILL_BATCH_SIZE as u64, || {
        for (mailbox, envelope) in items {
            mailbox
                .deliver(envelope)
                .expect("deliver to mid-fill mailbox should succeed");
        }
    });
}

#[stress_test(tier = 2, name = "deliver_full_primary")]
fn should_deliver_full_primary(ctx: &mut StressContext) {
    let full_address = test_address(1, "/bench/mailbox/primary/full");
    let items = (0..ERROR_DELIVER_BATCH_SIZE)
        .map(|_| {
            let mailbox = Mailbox::new(1);
            prefill_normal_lane(&mailbox, &full_address, 1);
            (mailbox, Envelope::new(full_address.clone(), 2_u64))
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, ERROR_DELIVER_BATCH_SIZE as u64, || {
        for (mailbox, envelope) in items {
            match mailbox.deliver(envelope) {
                Err(DeliveryError::MailboxFull { .. }) => {
                    black_box(());
                }
                other => panic!("expected MailboxFull, got {other:?}"),
            }
        }
    });
}

#[stress_test(tier = 2, name = "deliver_high_priority_when_normal_lane_full_primary")]
fn should_deliver_high_priority_when_normal_lane_full_primary(ctx: &mut StressContext) {
    let high_priority_address = test_address(1, "/bench/mailbox/primary/high-priority");
    let items = (0..ERROR_DELIVER_BATCH_SIZE)
        .map(|_| {
            let mailbox = Mailbox::new(1);
            prefill_normal_lane(&mailbox, &high_priority_address, 1);
            (mailbox, Envelope::new(high_priority_address.clone(), 7_u64))
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, ERROR_DELIVER_BATCH_SIZE as u64, || {
        for (mailbox, envelope) in items {
            mailbox
                .deliver_high_priority(envelope)
                .expect("high-priority lane should remain independent from normal saturation");
        }
    });
}

#[stress_test(tier = 2, name = "actor_ref_send_32_smoke")]
fn should_actor_ref_send_32_smoke(ctx: &mut StressContext) {
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

    let mut idx = 0usize;
    tier2_stress::measure_iterations(ctx, SEND_SMOKE_BATCH_SIZE as u64, || {
        for offset in 0..SEND_SMOKE_BATCH_SIZE {
            let route_idx = (idx + offset) % actor_refs.len();
            actor_refs[route_idx]
                .send(black_box((idx + offset) as u64))
                .expect("smoke send should stay on the success path");
        }
        idx = (idx + SEND_SMOKE_BATCH_SIZE) % actor_refs.len();
    });
}

#[stress_test(tier = 2, name = "actor_ref_send_100_smoke")]
fn should_actor_ref_send_100_smoke(ctx: &mut StressContext) {
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

    let mut idx = 0usize;
    tier2_stress::measure_iterations(ctx, 100, || {
        for offset in 0..100 {
            let route_idx = (idx + offset) % actor_refs.len();
            actor_refs[route_idx]
                .send(black_box((idx + offset) as u64))
                .expect("smoke burst should stay on the success path");
        }
        idx = (idx + 100) % actor_refs.len();
    });
}

stress_main!();
