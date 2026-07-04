//! Lease domain tier 3 system benchmarks using live domain sinks.
//!
//! Concurrent lease contention and route isolation measurement.
//! Tests the same `FrameContext` -> `LeaseDomainSink` path used by the live server.
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via `set_elements(count)`

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    create_bench_lease_sink, parse_lease_extend_token_response, parse_lease_token_response,
    register_session_queue_sink, route_frame_to_address, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

const CLIENT_SESSION_ID: u64 = 1;
const LEASE_QUERY_CONFIRM_BATCH_SIZE: usize = 64;

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

fn lease_address(family: RouteFamily, route: &str) -> RouteAddress {
    RouteAddress::new(family, Route::from_ref(route))
}

fn send_request(
    router: &Arc<Router>,
    source: &RouteAddress,
    destination: &RouteAddress,
    msg_type: u16,
    payload: Bytes,
) {
    route_frame_to_address(
        router.as_ref(),
        source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Lease,
        msg_type,
        payload,
    )
    .expect("lease route");
}

fn drain_responses(inbox: &Arc<FrameQueueSink>, expected_count: usize) -> Vec<FrameContext> {
    let responses = inbox.drain_after_count(expected_count, Duration::from_secs(1));
    assert_eq!(
        responses.len(),
        expected_count,
        "lease benchmark should receive one response per routed request"
    );
    responses
}

fn request(
    router: &Arc<Router>,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    destination: &RouteAddress,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    send_request(router, source, destination, msg_type, payload);
    let responses = drain_responses(inbox, 1);
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("lease response")
}

fn acquire_token(
    router: &Arc<Router>,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    route: &str,
    destination: &RouteAddress,
    owner_id: &str,
) -> u64 {
    let response = request(
        router,
        source,
        inbox,
        destination,
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
    let lease_routes: Vec<(String, RouteAddress)> = (0..100)
        .map(|i| {
            let route = format!("lease://realm/area/lock{i}/acquire");
            let address = lease_address(family, &route);
            (route, address)
        })
        .collect();

    let mut idx = 0usize;
    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let (route, address) = &lease_routes[idx];
            let token = acquire_token(&router, &source, &inbox, route, address, "client-1");
            let _ = request(
                &router,
                &source,
                &inbox,
                address,
                402,
                build_release_payload(route, "client-1", token),
            );
            idx = (idx + 1) % lease_routes.len();
        },
    );
    ctx.set_elements(2 * iterations as u64);
}

#[stress_test]
fn should_complete_alternate_renew_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "dual_route_concurrent");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "single_renew");

    let (router, family, source, inbox) = setup_lease_sink();
    let renew_route_a = "lease://realm/area1/lock1/renew";
    let renew_route_b = "lease://realm/area2/lock2/renew";
    let renew_address_a = lease_address(family, renew_route_a);
    let renew_address_b = lease_address(family, renew_route_b);
    let mut token1 = acquire_token(
        &router,
        &source,
        &inbox,
        renew_route_a,
        &renew_address_a,
        "client-1",
    );
    let mut token2 = acquire_token(
        &router,
        &source,
        &inbox,
        renew_route_b,
        &renew_address_b,
        "client-2",
    );

    let mut phase = 0usize;
    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            if phase.is_multiple_of(2) {
                let response = request(
                    &router,
                    &source,
                    &inbox,
                    &renew_address_a,
                    401,
                    build_extend_payload(renew_route_a, "client-1", token1, 30),
                );
                token1 = parse_lease_extend_token_response(response.as_ref())
                    .expect("extend token route1");
            } else {
                let response = request(
                    &router,
                    &source,
                    &inbox,
                    &renew_address_b,
                    401,
                    build_extend_payload(renew_route_b, "client-2", token2, 30),
                );
                token2 = parse_lease_extend_token_response(response.as_ref())
                    .expect("extend token route2");
            }
            phase += 1;
        },
    );
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_round_robin_query_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "triple_route_contention");
    ctx.tag("measurement_scope", "routed_system");
    let batch_size_tag = format!("{LEASE_QUERY_CONFIRM_BATCH_SIZE}_queries");
    ctx.tag("batch_size", batch_size_tag.as_str());

    let (router, family, source, inbox) = setup_lease_sink();
    let query_routes = [
        "lease://realm/area1/lock1/query",
        "lease://realm/area2/lock2/query",
        "lease://realm/area3/lock3/query",
    ];
    let owners = ["client-1", "client-2", "client-3"];
    let query_addresses: Vec<RouteAddress> = query_routes
        .iter()
        .map(|route| lease_address(family, route))
        .collect();
    for ((route, address), owner) in query_routes
        .iter()
        .zip(query_addresses.iter())
        .zip(owners.iter())
    {
        let _ = acquire_token(&router, &source, &inbox, route, address, owner);
    }

    let query_payloads: Vec<Bytes> = query_routes
        .iter()
        .map(|route| build_query_payload(route))
        .collect();
    let mut phase = 0usize;
    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for _ in 0..LEASE_QUERY_CONFIRM_BATCH_SIZE {
                let route_index = phase % query_routes.len();
                let payload = query_payloads[route_index].clone();
                send_request(
                    &router,
                    &source,
                    &query_addresses[route_index],
                    403,
                    payload,
                );
                phase += 1;
            }
            drain_responses(&inbox, LEASE_QUERY_CONFIRM_BATCH_SIZE);
        },
    );
    let batch_size =
        u64::try_from(LEASE_QUERY_CONFIRM_BATCH_SIZE).expect("lease query batch size fits u64");
    ctx.set_elements(iterations as u64 * batch_size);
}

#[stress_test]
fn should_complete_cycling_query_renew_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_operations_high_load");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "single_mixed_op");

    let (router, family, source, inbox) = setup_lease_sink();
    let route = "lease://realm/area/lock1/mixed";
    let address = lease_address(family, route);
    let query_payload = build_query_payload(route);
    let mut token = acquire_token(&router, &source, &inbox, route, &address, "client-1");

    let mut phase = 0usize;
    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            match phase % 3 {
                0 | 2 => {
                    let _ = request(
                        &router,
                        &source,
                        &inbox,
                        &address,
                        403,
                        query_payload.clone(),
                    );
                }
                1 => {
                    let response = request(
                        &router,
                        &source,
                        &inbox,
                        &address,
                        401,
                        build_extend_payload(route, "client-1", token, 30),
                    );
                    token =
                        parse_lease_extend_token_response(response.as_ref()).expect("extend token");
                }
                _ => unreachable!(),
            }
            phase += 1;
        },
    );
    ctx.set_elements(iterations as u64);
}

stress_main!();
