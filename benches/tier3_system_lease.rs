//! Lease domain tier 3 system benchmarks using live domain sinks.
//!
//! Concurrent lease contention and route isolation measurement.
//! Tests the same FrameContext -> LeaseDomainSink path used by the live server.
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via set_elements(count)

#[macro_use]
#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_test, StressContext};
use fitz::benchkit::{
    create_bench_lease_sink, parse_lease_extend_token_response, parse_lease_token_response,
    register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;

const CLIENT_SESSION_ID: u64 = 1;

fn build_acquire_payload(route: &str, owner_id: &str, ttl_secs: u64) -> Bytes {
    let mut enc = PayloadEncoder::new();
    enc.put_string(route);
    enc.put_string(owner_id);
    enc.put_u64(ttl_secs);
    enc.put_u32(0);
    Bytes::from(enc.finish())
}

fn build_extend_payload(route: &str, owner_id: &str, token: u64, ttl_secs: u64) -> Bytes {
    let mut enc = PayloadEncoder::new();
    enc.put_string(route);
    enc.put_string(owner_id);
    enc.put_u64(token);
    enc.put_u64(ttl_secs);
    Bytes::from(enc.finish())
}

fn build_release_payload(route: &str, owner_id: &str, token: u64) -> Bytes {
    let mut enc = PayloadEncoder::new();
    enc.put_string(route);
    enc.put_string(owner_id);
    enc.put_u64(token);
    Bytes::from(enc.finish())
}

fn build_query_payload(route: &str) -> Bytes {
    let mut enc = PayloadEncoder::new();
    enc.put_string(route);
    Bytes::from(enc.finish())
}

fn setup_lease_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_lease_sink(router.clone());
    router.register_domain_pattern("lease", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    (router, family, source, inbox)
}

fn request(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        router.as_ref(),
        source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Lease,
        msg_type,
        payload,
        family,
    )
    .expect("lease route");
    let responses = inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("lease response")
}

fn acquire_token(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    route: &str,
    owner_id: &str,
) -> u64 {
    let response = request(
        router,
        family,
        source,
        inbox,
        route,
        400,
        build_acquire_payload(route, owner_id, 30),
    );
    parse_lease_token_response(response.as_ref()).expect("lease token")
}

#[stress_test]
fn should_complete_acquire_release_sequence(ctx: &mut StressContext) {
    ctx.tag("scenario", "single_route_intensive");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "acquire_release");

    let (router, family, source, inbox) = setup_lease_sink();
    let routes: Vec<String> = (0..100)
        .map(|i| format!("lease://realm/area/lock{}/acquire", i))
        .collect();

    let mut idx = 0usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let route = &routes[idx];
        let token = acquire_token(&router, family, &source, &inbox, route, "client-1");
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            402,
            build_release_payload(route, "client-1", token),
        );
        idx = (idx + 1) % routes.len();
    });
    ctx.set_elements(2 * iterations as u64);
}

#[stress_test]
fn should_complete_alternate_renew_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "dual_route_concurrent");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "single_renew");

    let (router, family, source, inbox) = setup_lease_sink();
    let route1 = "lease://realm/area1/lock1/renew";
    let route2 = "lease://realm/area2/lock2/renew";
    let mut token1 = acquire_token(&router, family, &source, &inbox, route1, "client-1");
    let mut token2 = acquire_token(&router, family, &source, &inbox, route2, "client-2");

    let mut phase = 0usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        if phase.is_multiple_of(2) {
            let response = request(
                &router,
                family,
                &source,
                &inbox,
                route1,
                401,
                build_extend_payload(route1, "client-1", token1, 30),
            );
            token1 =
                parse_lease_extend_token_response(response.as_ref()).expect("extend token route1");
        } else {
            let response = request(
                &router,
                family,
                &source,
                &inbox,
                route2,
                401,
                build_extend_payload(route2, "client-2", token2, 30),
            );
            token2 =
                parse_lease_extend_token_response(response.as_ref()).expect("extend token route2");
        }
        phase += 1;
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_round_robin_query_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "triple_route_contention");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "single_query");

    let (router, family, source, inbox) = setup_lease_sink();
    let routes = [
        "lease://realm/area1/lock1/query",
        "lease://realm/area2/lock2/query",
        "lease://realm/area3/lock3/query",
    ];
    let owners = ["client-1", "client-2", "client-3"];
    for (route, owner) in routes.iter().zip(owners.iter()) {
        let _ = acquire_token(&router, family, &source, &inbox, route, owner);
    }

    let query_payloads: Vec<Bytes> = routes
        .iter()
        .map(|route| build_query_payload(route))
        .collect();
    let mut phase = 0usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let route = routes[phase % routes.len()];
        let payload = query_payloads[phase % query_payloads.len()].clone();
        let _ = request(&router, family, &source, &inbox, route, 403, payload);
        phase += 1;
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_cycling_query_renew_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_operations_high_load");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "single_mixed_op");

    let (router, family, source, inbox) = setup_lease_sink();
    let route = "lease://realm/area/lock1/mixed";
    let query_payload = build_query_payload(route);
    let mut token = acquire_token(&router, family, &source, &inbox, route, "client-1");

    let mut phase = 0usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        match phase % 3 {
            0 | 2 => {
                let _ = request(
                    &router,
                    family,
                    &source,
                    &inbox,
                    route,
                    403,
                    query_payload.clone(),
                );
            }
            1 => {
                let response = request(
                    &router,
                    family,
                    &source,
                    &inbox,
                    route,
                    401,
                    build_extend_payload(route, "client-1", token, 30),
                );
                token = parse_lease_extend_token_response(response.as_ref()).expect("extend token");
            }
            _ => unreachable!(),
        }
        phase += 1;
    });
    ctx.set_elements(iterations as u64);
}

stress_main_with_env!();
