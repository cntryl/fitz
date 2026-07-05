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

const MAILBOX_ROUTE_BATCH_SIZE: usize = 32_768;
const BACKPRESSURE_BATCH_SIZE: usize = 128;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

#[stress_test(tier = 2, name = "route_exact_mailbox_32768_messages_primary")]
fn should_route_exact_mailbox_32768_messages_primary(ctx: &mut StressContext) {
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

#[stress_test(tier = 2, name = "route_exact_backpressure_mailbox_primary")]
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
