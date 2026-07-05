#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{DeliveryError, MailboxSink};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::hint::black_box;

const MID_FILL_BATCH_SIZE: usize = 64;
const FRESH_DELIVER_BATCH_SIZE: usize = 512;
const ERROR_DELIVER_BATCH_SIZE: usize = 128;

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

#[stress(tier = 2, name = "deliver_empty_primary")]
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

#[stress(tier = 2, name = "deliver_mid_fill_64_primary")]
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

#[stress(tier = 2, name = "deliver_full_primary")]
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

#[stress(tier = 2, name = "deliver_high_priority_when_normal_lane_full_primary")]
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

stress_main!();
