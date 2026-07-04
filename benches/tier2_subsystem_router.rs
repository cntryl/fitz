#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{DeliveryError, MailboxSink, RouteError, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::hint::black_box;
use std::sync::Arc;

const MAILBOX_ROUTE_BATCH_SIZE: usize = 256;
const BACKPRESSURE_BATCH_SIZE: usize = 128;

struct NoopSink;

impl MailboxSink for NoopSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Ok(())
    }

    fn deliver_high_priority(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Ok(())
    }
}

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn make_exact_router(count: usize, sink: &Arc<dyn MailboxSink>) -> (Router, Vec<RouteAddress>) {
    let router = Router::new();
    let addresses: Vec<_> = (0..count)
        .map(|i| test_address(1, &format!("rpc://acme/router/exact/{i}")))
        .collect();

    for address in &addresses {
        router.register(address.clone(), Arc::clone(sink));
    }

    (router, addresses)
}

#[stress_test(tier = 2, mode = "fixed_duration", name = "route_exact_noop_primary")]
fn should_route_exact_noop_primary(ctx: &mut StressContext) {
    let sink: Arc<dyn MailboxSink> = Arc::new(NoopSink);
    let (router, addresses) = make_exact_router(1, &sink);
    let address = addresses[0].clone();
    let mut seq = 0_u64;

    tier2_stress::measure_iterations(ctx, 1, || {
        router
            .route(Envelope::new(black_box(address.clone()), black_box(seq)))
            .expect("exact route should succeed");
        seq = seq.wrapping_add(1);
    });
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "route_domain_fallback_noop_primary"
)]
fn should_route_domain_fallback_noop_primary(ctx: &mut StressContext) {
    let router = Router::new();
    router.register_domain_pattern("rpc", Arc::new(NoopSink));
    let address = test_address(1, "rpc://acme/router/fallback/target");
    let mut seq = 0_u64;

    tier2_stress::measure_iterations(ctx, 1, || {
        router
            .route(Envelope::new(black_box(address.clone()), black_box(seq)))
            .expect("domain fallback route should succeed");
        seq = seq.wrapping_add(1);
    });
}

fn route_batch_exact(ctx: &mut StressContext, route_count: usize) {
    let sink: Arc<dyn MailboxSink> = Arc::new(NoopSink);
    let (router, addresses) = make_exact_router(route_count, &sink);
    let mut seq = 0_u64;

    tier2_stress::measure_iterations(ctx, route_count as u64, || {
        for address in &addresses {
            router
                .route(Envelope::new(address.clone(), black_box(seq)))
                .expect("batched exact route should succeed");
            seq = seq.wrapping_add(1);
        }
    });
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "route_batch_exact_16_noop_primary"
)]
fn should_route_batch_exact_16_noop_primary(ctx: &mut StressContext) {
    route_batch_exact(ctx, 16);
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "route_batch_exact_64_noop_primary"
)]
fn should_route_batch_exact_64_noop_primary(ctx: &mut StressContext) {
    route_batch_exact(ctx, 64);
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "route_batch_exact_1024_noop_primary"
)]
fn should_route_batch_exact_1024_noop_primary(ctx: &mut StressContext) {
    route_batch_exact(ctx, 1024);
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "route_exact_mailbox_256_messages_primary"
)]
fn should_route_exact_mailbox_256_messages_primary(ctx: &mut StressContext) {
    let router = Router::new();
    let address = test_address(1, "rpc://acme/router/mailbox/target");
    let mailbox = Arc::new(Mailbox::new(1));
    router.register(address.clone(), mailbox.clone());
    let mut seq = 0_u64;

    tier2_stress::measure_iterations(ctx, MAILBOX_ROUTE_BATCH_SIZE as u64, || {
        for _ in 0..MAILBOX_ROUTE_BATCH_SIZE {
            router
                .route(Envelope::new(black_box(address.clone()), black_box(seq)))
                .expect("mailbox route should succeed");
            let _ = mailbox
                .receiver()
                .try_recv()
                .expect("mailbox route should enqueue message");
            seq = seq.wrapping_add(1);
        }
    });
}

#[stress_test(
    tier = 2,
    mode = "fixed_duration",
    name = "route_exact_backpressure_mailbox_primary"
)]
fn should_route_exact_backpressure_mailbox_primary(ctx: &mut StressContext) {
    let items = (0..BACKPRESSURE_BATCH_SIZE)
        .map(|_| {
            let router = Router::new();
            let mailbox = Arc::new(Mailbox::new(1));
            let address = test_address(1, "rpc://acme/router/full/target");
            router.register(address.clone(), mailbox.clone());
            mailbox
                .deliver(Envelope::new(address.clone(), 0_u64))
                .expect("prefill should succeed");
            (router, address)
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, BACKPRESSURE_BATCH_SIZE as u64, || {
        for (router, address) in items {
            match router.route(Envelope::new(black_box(address), black_box(1_u64))) {
                Err(RouteError::DeliveryFailed(_, DeliveryError::MailboxFull { .. })) => {
                    black_box(());
                }
                other => panic!("expected MailboxFull, got {other:?}"),
            }
        }
    });
}

stress_main!();
