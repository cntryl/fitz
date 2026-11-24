#[path = "tier4_notice_support.rs"]
mod tier4_notice_support;
use fitz_core::testkit::operations as tier4_support;

use crate::tier4_notice_support::{
    complete_network_publish, subscribe_network_client, with_authenticated_notice_clients,
    with_notice_clients, InProcessNoticeFixture,
};
use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions, TransportKind,
};
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{build_notice_publish, build_notice_subscribe};
use fitz::protocol::wire2::header::{decode_message as decode_wire2, encode_message, HeaderFlags};
use fitz_core::runtime::family_actor_pool::{family_shard_affinity, shard_count_for_family_count};
use fitz_e2e::testkit::transport::{build_connect_frame, generate_test_jwt, TestClient};
use futures_util::future::join_all;
use std::cell::Cell;
use std::num::NonZeroU64;
use std::time::Duration;

const NOTICE_ROUTE: &str = "notice://tier4/publish/events";
const NOTICE_PAYLOAD_SIZE: usize = 1_024;

fn dimensions(layer: LayerKind) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "notice",
        scenario: "delivery_confirmed_publish",
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: NOTICE_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "all_matching",
        client_count: 2,
        workload_mix: "publish_and_delivery",
        completed_unit: "confirmed_publish",
        gate_class: "regression_gate",
    }
}

fn tag_publish(ctx: &mut StressContext, layer: LayerKind) {
    tag_dimensions(ctx, &dimensions(layer));
    ctx.parameter("publisher_count", 1);
    ctx.parameter("subscriber_count", 1);
    ctx.parameter("completion_mode", "publisher_ack_and_subscriber_delivery");
}

#[stress(tier = 4)]
fn should_measure_direct_delivery_confirmed_publish(ctx: &mut StressContext) {
    tag_publish(ctx, LayerKind::Direct);
    let payload = vec![0xA5; NOTICE_PAYLOAD_SIZE];
    let fixture = InProcessNoticeFixture::new(NOTICE_ROUTE, NOTICE_ROUTE, &payload, 1);
    measure_operations(ctx, "direct_delivery_confirmed_publish", 1, |latencies| {
        latencies.push(fixture.complete_direct_publish());
    });
    fixture.stop();
}

#[stress(tier = 4)]
fn should_measure_encoded_delivery_confirmed_publish(ctx: &mut StressContext) {
    tag_publish(ctx, LayerKind::Encoded);
    let payload = vec![0xA5; NOTICE_PAYLOAD_SIZE];
    let fixture = InProcessNoticeFixture::new(NOTICE_ROUTE, NOTICE_ROUTE, &payload, 1);
    measure_operations(ctx, "encoded_delivery_confirmed_publish", 1, |latencies| {
        latencies.push(fixture.complete_encoded_publish());
    });
    fixture.stop();
}

fn measure_network_publish(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_publish(ctx, LayerKind::from(transport));
    let payload = vec![0xA5; NOTICE_PAYLOAD_SIZE];
    let subscribe_frame = build_notice_subscribe(NOTICE_ROUTE);
    let publish_frame = build_notice_publish(NOTICE_ROUTE, &payload);

    with_notice_clients(transport, 2, |runtime, clients| {
        let subscription_id =
            runtime.block_on(subscribe_network_client(&mut clients[0], &subscribe_frame));
        let (subscribers, publisher_slice) = clients.split_at_mut(1);
        let publisher = &mut publisher_slice[0];
        measure_operations(ctx, measurement, 1, |latencies| {
            latencies.push(runtime.block_on(complete_network_publish(
                publisher,
                subscribers,
                &publish_frame,
                &[subscription_id],
                NOTICE_ROUTE,
                &payload,
            )));
        });
    });
}

const ACTOR_PUBLISH_SESSIONS: usize = 8;
const ACTOR_PUBLISH_PAYLOAD: &[u8] = b"authenticated actor dispatch benchmark";

fn process_cpu_time() -> Duration {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` points to writable stack storage for the returned clock value.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) },
        0
    );
    Duration::new(
        u64::try_from(time.tv_sec).expect("process CPU seconds are nonnegative"),
        u32::try_from(time.tv_nsec).expect("process CPU nanoseconds fit"),
    )
}

fn acknowledged_publish_frame(route: &str) -> Vec<u8> {
    let frame = build_notice_publish(route, ACTOR_PUBLISH_PAYLOAD);
    let (header, payload) = decode_wire2(&frame, u32::MAX).expect("valid Notice publish frame");
    encode_message(
        header.message_type,
        HeaderFlags::ack_requested(),
        Some(NonZeroU64::new(1).expect("nonzero correlation")),
        payload,
    )
}

async fn connect_authenticated_clients(clients: &mut [TestClient], identities: &[String]) {
    assert_eq!(clients.len(), identities.len());
    for (client, identity) in clients.iter_mut().zip(identities) {
        let connect = build_connect_frame("", &generate_test_jwt(identity));
        client
            .send_frame(&connect)
            .await
            .expect("send authenticated Notice CONNECT");
    }
}

async fn publish_acknowledged(client: &mut TestClient, frame: &[u8]) -> Duration {
    let started = std::time::Instant::now();
    let response = client
        .request(frame, 5_000)
        .await
        .expect("acknowledged Notice publish response");
    let (header, payload) = decode_wire2(&response, u32::MAX).expect("valid Notice ACK");
    assert_eq!(header.message_type, 500, "Notice publish ACK type");
    assert_eq!(header.correlation, NonZeroU64::new(1));
    assert_eq!(payload, [0], "Notice publish accepted");
    started.elapsed()
}

fn measure_authenticated_dispatch_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    measurement: &'static str,
    collocated_family_count: usize,
    family_for_session: impl Fn(usize, usize) -> u32,
) {
    let shard_count = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let collocated_families = (0..collocated_family_count)
        .map(|index| u32::try_from(index * shard_count + 1).expect("collocated family id fits u32"))
        .collect::<Vec<_>>();
    let max_family = collocated_families
        .last()
        .copied()
        .unwrap_or(1)
        .max(u32::try_from(shard_count).expect("shard count fits u32"));
    let route_families = (1..=max_family).collect::<Vec<_>>();

    let identities = (0..ACTOR_PUBLISH_SESSIONS)
        .map(|index| format!("actor-dispatch-session-{index}"))
        .collect::<Vec<_>>();
    let mappings = identities
        .iter()
        .enumerate()
        .map(|(index, identity)| (identity.clone(), family_for_session(index, shard_count)))
        .collect::<Vec<_>>();
    let routes = identities
        .iter()
        .map(|identity| format!("notice://{identity}/actor-dispatch/events"))
        .collect::<Vec<_>>();
    let frames = routes
        .iter()
        .map(|route| acknowledged_publish_frame(route))
        .collect::<Vec<_>>();
    let active_family_count = mappings
        .iter()
        .map(|(_, family)| *family)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let runtime_shard_count = shard_count_for_family_count(route_families.len());
    assert!(mappings
        .iter()
        .all(|(_, family)| route_families.contains(family)));
    assert_eq!(
        mappings
            .iter()
            .map(|(_, family)| family_shard_affinity(
                fitz_core::runtime::routing::RouteFamily::new(*family),
                runtime_shard_count,
            ))
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "active families are collocated on one actor shard"
    );

    ctx.parameter("scenario", scenario);
    ctx.parameter("authenticated_session_count", ACTOR_PUBLISH_SESSIONS);
    ctx.parameter("provisioned_family_count", route_families.len());
    ctx.parameter("active_family_count", active_family_count);
    ctx.parameter("collocated_family_count", collocated_family_count);
    ctx.parameter("actor_shard_count", runtime_shard_count);

    with_authenticated_notice_clients(
        &route_families,
        &mappings,
        &identities,
        |runtime, clients| {
            runtime.block_on(connect_authenticated_clients(clients, &identities));
            for (client, frame) in clients.iter_mut().zip(&frames) {
                runtime.block_on(publish_acknowledged(client, frame));
            }

            let process_cpu = Cell::new(Duration::ZERO);
            let completed = measure_operations(
                ctx,
                measurement,
                ACTOR_PUBLISH_SESSIONS as u64,
                |latencies| {
                    let cpu_before = process_cpu_time();
                    let durations = runtime.block_on(join_all(
                        clients
                            .iter_mut()
                            .zip(&frames)
                            .map(|(client, frame)| publish_acknowledged(client, frame)),
                    ));
                    process_cpu.set(
                        process_cpu
                            .get()
                            .saturating_add(process_cpu_time().saturating_sub(cpu_before)),
                    );
                    latencies.extend(durations);
                },
            );
            ctx.metadata("cpu_time_source", "CLOCK_PROCESS_CPUTIME_ID");
            let cpu_metric = format!("tcp_process_cpu_{scenario}_ns_per_publish");
            ctx.record_external(&cpu_metric, process_cpu.get(), completed);
            ctx.metadata(
                "broker_completion",
                "all_eight_publish_acknowledgements_verified",
            );
        },
    );
}

#[stress(tier = 4)]
fn should_measure_tcp_delivery_confirmed_publish(ctx: &mut StressContext) {
    measure_network_publish(ctx, TransportKind::Tcp, "tcp_delivery_confirmed_publish");
}

#[stress(tier = 4)]
fn should_measure_ws_delivery_confirmed_publish(ctx: &mut StressContext) {
    measure_network_publish(
        ctx,
        TransportKind::WebSocket,
        "ws_delivery_confirmed_publish",
    );
}

#[stress(tier = 4, name = "tcp_acknowledged_actor_dispatch_one_family")]
fn should_measure_tcp_acknowledged_actor_dispatch_one_family(ctx: &mut StressContext) {
    measure_authenticated_dispatch_case(
        ctx,
        "one_family",
        "tcp_acknowledged_one_family",
        1,
        |_, _| 1,
    );
}

#[stress(tier = 4, name = "tcp_acknowledged_actor_dispatch_one_hot_among_32")]
fn should_measure_tcp_acknowledged_actor_dispatch_one_hot_among_32(ctx: &mut StressContext) {
    measure_authenticated_dispatch_case(
        ctx,
        "one_hot_among_32",
        "tcp_acknowledged_one_hot_among_32",
        32,
        |_, _| 1,
    );
}

#[stress(tier = 4, name = "tcp_acknowledged_actor_dispatch_eight_active_of_32")]
fn should_measure_tcp_acknowledged_actor_dispatch_eight_active_of_32(ctx: &mut StressContext) {
    measure_authenticated_dispatch_case(
        ctx,
        "eight_active_of_32",
        "tcp_acknowledged_eight_active_of_32",
        32,
        |index, shard_count| {
            u32::try_from(index * shard_count + 1).expect("active family id fits u32")
        },
    );
}

stress_main!();
