#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use std::hint::black_box;
use std::sync::Arc;

const REGISTER_BATCH_SIZE: usize = 64;
const REGISTER_BATCH_REPEAT_COUNT: usize = 512;
const REGISTER_SINGLE_REPEAT_COUNT: usize = 8_192;

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

fn register_all(router: &Router, addresses: &[RouteAddress], sinks: &[Arc<dyn MailboxSink>]) {
    for (address, sink) in addresses.iter().zip(sinks) {
        router.register(address.clone(), Arc::clone(sink));
    }
}

#[stress(tier = 2, name = "register_single_fresh_primary")]
fn should_register_single_fresh_primary(ctx: &mut StressContext) {
    let (single_addresses, single_sinks) = make_registration_batch("/bench/reg/single", 1);
    let scheduler = Scheduler::new(1);
    let router = scheduler.router();

    tier2_stress::measure_iterations(ctx, REGISTER_SINGLE_REPEAT_COUNT as u64, || {
        for _ in 0..REGISTER_SINGLE_REPEAT_COUNT {
            router.register(
                black_box(single_addresses[0].clone()),
                black_box(Arc::clone(&single_sinks[0])),
            );
            router.clear();
        }
    });
}

#[stress(tier = 2, name = "register_64_fresh_primary")]
fn should_register_64_fresh_primary(ctx: &mut StressContext) {
    let (batch_addresses, batch_sinks) =
        make_registration_batch("/bench/reg/batch", REGISTER_BATCH_SIZE);
    let scheduler = Scheduler::new(1);
    let router = scheduler.router();

    tier2_stress::measure_iterations(
        ctx,
        (REGISTER_BATCH_SIZE * REGISTER_BATCH_REPEAT_COUNT) as u64,
        || {
            for _ in 0..REGISTER_BATCH_REPEAT_COUNT {
                register_all(&router, &batch_addresses, &batch_sinks);
                router.clear();
            }
        },
    );
}

#[stress(tier = 2, name = "register_64_replace_primary")]
fn should_register_64_replace_primary(ctx: &mut StressContext) {
    let (batch_addresses, batch_sinks) =
        make_registration_batch("/bench/reg/batch", REGISTER_BATCH_SIZE);
    let scheduler = Scheduler::new(1);
    let router = scheduler.router();
    register_all(&router, &batch_addresses, &batch_sinks);

    tier2_stress::measure_iterations(
        ctx,
        (REGISTER_BATCH_SIZE * REGISTER_BATCH_REPEAT_COUNT) as u64,
        || {
            for _ in 0..REGISTER_BATCH_REPEAT_COUNT {
                register_all(&router, &batch_addresses, &batch_sinks);
            }
        },
    );
}

stress_main!();
