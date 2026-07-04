#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::MailboxSink;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use std::hint::black_box;
use std::sync::Arc;

const REGISTER_SINGLE_BATCH_SIZE: usize = 512;
const REGISTER_BATCH_BATCH_SIZE: usize = 128;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn make_registration_batch(
    prefix: &str,
    count: usize,
) -> (Vec<RouteAddress>, Vec<Arc<dyn MailboxSink>>) {
    let addresses = (0..count)
        .map(|i| test_address(1, &format!("{prefix}/{i}")))
        .collect();
    let sinks = (0..count)
        .map(|_| Arc::new(Mailbox::new(100)) as Arc<dyn MailboxSink>)
        .collect();
    (addresses, sinks)
}

fn register_all(scheduler: &Scheduler, addresses: &[RouteAddress], sinks: &[Arc<dyn MailboxSink>]) {
    let router = scheduler.router();
    for (address, sink) in addresses.iter().zip(sinks) {
        router.register(address.clone(), Arc::clone(sink));
    }
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "register_single_fresh_primary"
)]
fn should_register_single_fresh_primary(ctx: &mut StressContext) {
    let (single_addresses, single_sinks) = make_registration_batch("/bench/reg/single", 1);
    let schedulers = (0..REGISTER_SINGLE_BATCH_SIZE)
        .map(|_| Scheduler::new(1))
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, REGISTER_SINGLE_BATCH_SIZE as u64, || {
        for scheduler in schedulers {
            scheduler.router().register(
                black_box(single_addresses[0].clone()),
                black_box(Arc::clone(&single_sinks[0])),
            );
        }
    });
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "register_single_replace_primary"
)]
fn should_register_single_replace_primary(ctx: &mut StressContext) {
    let (single_addresses, single_sinks) = make_registration_batch("/bench/reg/single", 1);
    let schedulers = (0..REGISTER_SINGLE_BATCH_SIZE)
        .map(|_| {
            let scheduler = Scheduler::new(1);
            scheduler
                .router()
                .register(single_addresses[0].clone(), Arc::clone(&single_sinks[0]));
            scheduler
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, REGISTER_SINGLE_BATCH_SIZE as u64, || {
        for scheduler in schedulers {
            scheduler.router().register(
                black_box(single_addresses[0].clone()),
                black_box(Arc::clone(&single_sinks[0])),
            );
        }
    });
}

#[stress_test(tier = 2, mode = "fixed_duration", name = "register_64_fresh_primary")]
fn should_register_64_fresh_primary(ctx: &mut StressContext) {
    let (batch_addresses, batch_sinks) = make_registration_batch("/bench/reg/batch", 64);
    let schedulers = (0..REGISTER_BATCH_BATCH_SIZE)
        .map(|_| Scheduler::new(1))
        .collect::<Vec<_>>();

    tier2_stress::measure_once(
        ctx,
        (REGISTER_BATCH_BATCH_SIZE * batch_addresses.len()) as u64,
        || {
            for scheduler in schedulers {
                register_all(&scheduler, &batch_addresses, &batch_sinks);
            }
        },
    );
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "register_64_replace_primary"
)]
fn should_register_64_replace_primary(ctx: &mut StressContext) {
    let (batch_addresses, batch_sinks) = make_registration_batch("/bench/reg/batch", 64);
    let schedulers = (0..REGISTER_BATCH_BATCH_SIZE)
        .map(|_| {
            let scheduler = Scheduler::new(1);
            register_all(&scheduler, &batch_addresses, &batch_sinks);
            scheduler
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(
        ctx,
        (REGISTER_BATCH_BATCH_SIZE * batch_addresses.len()) as u64,
        || {
            for scheduler in schedulers {
                register_all(&scheduler, &batch_addresses, &batch_sinks);
            }
        },
    );
}

stress_main!();
