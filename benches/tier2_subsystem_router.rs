#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

const MAILBOX_ROUTE_BATCH_SIZE: usize = 32_768;
fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

#[stress(tier = 2, name = "route_exact_mailbox_32768_messages_primary")]
fn should_route_exact_mailbox_32768_messages_primary(ctx: &mut StressContext) {
    let router = Router::new();
    let address = test_address(1, "rpc://acme/router/mailbox/target");
    let mailbox = Arc::new(Mailbox::new(1));
    router.register(address.clone(), mailbox.clone());
    let mut seq = 0_u64;

    tier2_stress::measure_iterations(
        ctx,
        "route_exact_mailbox_32768_messages_primary",
        MAILBOX_ROUTE_BATCH_SIZE as u64,
        || {
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
        },
    );
}

stress_main!();
