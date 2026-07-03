#![allow(clippy::too_many_lines)]

use super::*;
use fitz::benchkit::{
    build_queue_enqueue, build_stream_append, build_stream_begin, create_bench_store,
    extract_single_tlv_field, parse_queue_response, parse_stream_response, parse_stream_session_id,
    shared_bench_runtime,
};
use fitz::domains::queue::QueueResponse;
use fitz::domains::stream::StreamActor;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};

pub(super) fn measure_queue_latency_layers(settings: ProofSettings) -> Vec<LatencyRow> {
    vec![
        LatencyRow {
            domain: "queue",
            operation: "enqueue",
            layer: "direct_actor",
            client_count: 1,
            stats: measure_queue_direct_actor(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "queue",
            operation: "enqueue",
            layer: "routed_inproc",
            client_count: 1,
            stats: measure_queue_routed(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "queue",
            operation: "enqueue",
            layer: "tcp_loopback",
            client_count: 1,
            stats: measure_queue_tcp(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "queue",
            operation: "enqueue",
            layer: "websocket_loopback",
            client_count: 1,
            stats: measure_queue_websocket(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "queue",
            operation: "enqueue",
            layer: "websocket_multiclient",
            client_count: MULTICLIENT_COUNT,
            stats: measure_queue_websocket_multiclient(settings),
            gate_p99_under_1ms: None,
        },
    ]
    .into_iter()
    .map(|mut row| {
        row.gate_p99_under_1ms = Some(row.stats.p99_us < P99_LATENCY_GATE_US);
        row
    })
    .collect()
}

pub(super) fn measure_stream_append_latency_layers(settings: ProofSettings) -> Vec<LatencyRow> {
    vec![
        LatencyRow {
            domain: "stream",
            operation: "append",
            layer: "direct_actor",
            client_count: 1,
            stats: measure_stream_direct_actor(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "stream",
            operation: "append",
            layer: "routed_inproc",
            client_count: 1,
            stats: measure_stream_routed(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "stream",
            operation: "append",
            layer: "tcp_loopback",
            client_count: 1,
            stats: measure_stream_tcp(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "stream",
            operation: "append",
            layer: "websocket_loopback",
            client_count: 1,
            stats: measure_stream_websocket(settings),
            gate_p99_under_1ms: None,
        },
        LatencyRow {
            domain: "stream",
            operation: "append",
            layer: "websocket_multiclient",
            client_count: MULTICLIENT_COUNT,
            stats: measure_stream_websocket_multiclient(settings),
            gate_p99_under_1ms: None,
        },
    ]
}

fn assert_queue_frame_ok(frame: &[u8]) {
    let (_msg_type, status, _payload) = parse_queue_response(frame);
    assert_eq!(status, 0, "queue frame operation failed");
}

fn assert_stream_frame_ok(frame: &[u8]) {
    let (_msg_type, status, _payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "stream frame operation failed");
}

fn measure_queue_direct_actor(settings: ProofSettings) -> LatencyStats {
    let store = create_bench_store();
    let mut actor = queue_actor_on_store(store, "proof", "latency", "direct");
    measure_sequential(settings, |index| {
        let body = Bytes::from(format!("queue-direct-{index}"));
        let start = Instant::now();
        let response = actor.handle_send(body, None);
        assert!(matches!(response, QueueResponse::Sent { .. }));
        start.elapsed()
    })
}

fn measure_queue_routed(settings: ProofSettings) -> LatencyStats {
    let context = setup_routed_context(DomainKind::Queue);
    let route = "queue://proof/latency/routed/enqueue";
    let frame = build_queue_enqueue(route, QUEUE_MESSAGE_BYTES);
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    measure_sequential(settings, |_| {
        let start = Instant::now();
        let response = routed_request(&context, route, ChannelId::Sub, msg_type, payload.clone());
        assert_queue_payload_ok(response.as_ref());
        start.elapsed()
    })
}

fn measure_queue_tcp(settings: ProofSettings) -> LatencyStats {
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start proof server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect proof tcp client");
    let frame = build_queue_enqueue("queue://proof/latency/tcp/enqueue", QUEUE_MESSAGE_BYTES);

    measure_sequential(settings, |_| {
        let start = Instant::now();
        let response = runtime
            .block_on(client.request(&frame, 2_000))
            .expect("queue tcp response");
        assert_queue_frame_ok(&response);
        start.elapsed()
    })
}

fn measure_queue_websocket(settings: ProofSettings) -> LatencyStats {
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start proof server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect proof websocket client");
    let frame = build_queue_enqueue("queue://proof/latency/ws/enqueue", QUEUE_MESSAGE_BYTES);

    measure_sequential(settings, |_| {
        let start = Instant::now();
        let response = runtime
            .block_on(client.request(&frame, 2_000))
            .expect("queue websocket response");
        assert_queue_frame_ok(&response);
        start.elapsed()
    })
}

fn measure_queue_websocket_multiclient(settings: ProofSettings) -> LatencyStats {
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start proof server");
    let clients = websocket_clients(runtime, &server, MULTICLIENT_COUNT);
    let frame = Arc::new(build_queue_enqueue(
        "queue://proof/latency/ws-multiclient/enqueue",
        QUEUE_MESSAGE_BYTES,
    ));

    measure_multiclient(settings, MULTICLIENT_COUNT, |client_index| {
        let clients = clients.clone();
        let frame = frame.clone();
        async move {
            let mut client = clients[client_index].lock().await;
            let start = Instant::now();
            let response = client
                .request(frame.as_ref(), 2_000)
                .await
                .expect("queue multiclient response");
            assert_queue_frame_ok(&response);
            start.elapsed()
        }
    })
}

fn measure_stream_direct_actor(settings: ProofSettings) -> LatencyStats {
    let route_count = stream_route_pool_size(settings);
    let store = Arc::new(fitz::domains::stream::store::StreamStore::new(
        create_bench_store(),
    ));
    let mut actors = (0..route_count)
        .map(|index| {
            let mut actor = StreamActor::new(
                RouteFamily::new(FAMILY_ID),
                "proof".to_string(),
                "latency-direct".to_string(),
                format!("stream-{index}"),
                store.clone(),
            )
            .expect("create proof stream actor");
            actor
                .begin_append_session(
                    STREAM_OWNER_SESSION_ID,
                    u64::try_from(index + 1).expect("stream session id"),
                    None,
                )
                .expect("begin proof stream session");
            actor
        })
        .collect::<Vec<_>>();

    measure_sequential(settings, |index| {
        let route_index = index % route_count;
        let expected_offset = u64::try_from(index / route_count).expect("expected offset");
        let start = Instant::now();
        let assigned = actors[route_index]
            .append_to_session_with_discriminator_for_owner(
                STREAM_OWNER_SESSION_ID,
                u64::try_from(route_index + 1).expect("stream session id"),
                expected_offset,
                Bytes::from_static(STREAM_EVENT_BYTES),
                None,
                None,
            )
            .expect("append proof stream event");
        assert_eq!(assigned, expected_offset);
        start.elapsed()
    })
}

fn measure_stream_routed(settings: ProofSettings) -> LatencyStats {
    let route_count = stream_route_pool_size(settings);
    let context = setup_routed_context(DomainKind::Stream);
    let routes = proof_stream_routes("stream://proof/latency-routed", route_count);
    let session_ids = begin_routed_stream_sessions(&context, &routes);

    measure_sequential(settings, |index| {
        let route_index = index % route_count;
        let expected_offset = u64::try_from(index / route_count).expect("expected offset");
        let frame = build_stream_append(
            session_ids[route_index],
            expected_offset,
            STREAM_EVENT_BYTES,
        );
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let start = Instant::now();
        let response = routed_request(
            &context,
            &routes[route_index],
            ChannelId::Pub,
            msg_type,
            payload,
        );
        assert_stream_payload_ok(response.as_ref());
        start.elapsed()
    })
}

fn measure_stream_tcp(settings: ProofSettings) -> LatencyStats {
    let route_count = stream_route_pool_size(settings);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start proof server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect proof tcp client");
    let routes = proof_stream_routes("stream://proof/latency-tcp", route_count);
    let session_ids = routes
        .iter()
        .map(|route| {
            let response = runtime
                .block_on(client.request(&build_stream_begin(route), 2_000))
                .expect("stream tcp begin response");
            let (_msg_type, status, payload) = parse_stream_response(&response);
            assert_eq!(status, 0, "stream tcp begin failed");
            parse_stream_session_id(&payload).expect("stream tcp session id")
        })
        .collect::<Vec<_>>();

    measure_sequential(settings, |index| {
        let route_index = index % route_count;
        let expected_offset = u64::try_from(index / route_count).expect("expected offset");
        let frame = build_stream_append(
            session_ids[route_index],
            expected_offset,
            STREAM_EVENT_BYTES,
        );
        let start = Instant::now();
        let response = runtime
            .block_on(client.request(&frame, 2_000))
            .expect("stream tcp append response");
        assert_stream_frame_ok(&response);
        start.elapsed()
    })
}

fn measure_stream_websocket(settings: ProofSettings) -> LatencyStats {
    let route_count = stream_route_pool_size(settings);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start proof server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect proof websocket client");
    let routes = proof_stream_routes("stream://proof/latency-ws", route_count);
    let session_ids = routes
        .iter()
        .map(|route| {
            let response = runtime
                .block_on(client.request(&build_stream_begin(route), 2_000))
                .expect("stream websocket begin response");
            let (_msg_type, status, payload) = parse_stream_response(&response);
            assert_eq!(status, 0, "stream websocket begin failed");
            parse_stream_session_id(&payload).expect("stream websocket session id")
        })
        .collect::<Vec<_>>();

    measure_sequential(settings, |index| {
        let route_index = index % route_count;
        let expected_offset = u64::try_from(index / route_count).expect("expected offset");
        let frame = build_stream_append(
            session_ids[route_index],
            expected_offset,
            STREAM_EVENT_BYTES,
        );
        let start = Instant::now();
        let response = runtime
            .block_on(client.request(&frame, 2_000))
            .expect("stream websocket append response");
        assert_stream_frame_ok(&response);
        start.elapsed()
    })
}

fn measure_stream_websocket_multiclient(settings: ProofSettings) -> LatencyStats {
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start proof server");
    let clients = websocket_clients(runtime, &server, MULTICLIENT_COUNT);
    let session_ids = runtime.block_on(futures::future::join_all(clients.iter().enumerate().map(
        |(client_index, client)| {
            let client = client.clone();
            async move {
                let route = format!("stream://proof/latency-ws-multi/stream-{client_index}/append");
                let mut client = client.lock().await;
                let response = client
                    .request(&build_stream_begin(&route), 2_000)
                    .await
                    .expect("stream multiclient begin response");
                let (_msg_type, status, payload) = parse_stream_response(&response);
                assert_eq!(status, 0, "stream multiclient begin failed");
                parse_stream_session_id(&payload).expect("stream multiclient session id")
            }
        },
    )));
    let session_ids = Arc::new(session_ids);
    let offsets = Arc::new(
        (0..MULTICLIENT_COUNT)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    measure_multiclient(settings, MULTICLIENT_COUNT, |client_index| {
        let clients = clients.clone();
        let session_ids = session_ids.clone();
        let offsets = offsets.clone();
        async move {
            let expected_offset =
                offsets[client_index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let frame = build_stream_append(
                session_ids[client_index],
                expected_offset,
                STREAM_EVENT_BYTES,
            );
            let mut client = clients[client_index].lock().await;
            let start = Instant::now();
            let response = client
                .request(&frame, 2_000)
                .await
                .expect("stream multiclient append response");
            assert_stream_frame_ok(&response);
            start.elapsed()
        }
    })
}
