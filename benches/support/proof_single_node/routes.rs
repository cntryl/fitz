use super::{
    assert_queue_payload_ok, assert_stream_payload_ok, begin_routed_stream_sessions,
    measure_sequential, proof_stream_routes, routed_request, setup_routed_context, ChannelId,
    DomainKind, Instant, LatencyStats, ProofSettings, RouteSensitivityRow, QUEUE_MESSAGE_BYTES,
    STREAM_EVENT_BYTES,
};
use fitz::benchkit::{build_queue_enqueue, build_stream_append, extract_single_tlv_field};

pub(super) fn measure_route_sensitivity(settings: ProofSettings) -> Vec<RouteSensitivityRow> {
    let mut rows = Vec::new();
    for route_count in [10usize, 100, 10_000] {
        rows.push(RouteSensitivityRow {
            domain: "queue",
            route_count,
            stats: measure_queue_route_count(settings, route_count),
        });
        rows.push(RouteSensitivityRow {
            domain: "stream",
            route_count,
            stats: measure_stream_route_count(settings, route_count),
        });
    }
    rows
}

pub(super) fn route_count_effect_answer(rows: &[RouteSensitivityRow]) -> String {
    let queue = route_domain_has_minimal_effect(rows, "queue");
    let stream = route_domain_has_minimal_effect(rows, "stream");
    if queue && stream {
        "minimal effect".to_string()
    } else {
        "material effect observed".to_string()
    }
}

fn measure_queue_route_count(settings: ProofSettings, route_count: usize) -> LatencyStats {
    let context = setup_routed_context(DomainKind::Queue);
    let routes = (0..route_count)
        .map(|index| format!("queue://proof/routes/queue-{index}/enqueue"))
        .collect::<Vec<_>>();
    let frames = routes
        .iter()
        .map(|route| extract_single_tlv_field(&build_queue_enqueue(route, QUEUE_MESSAGE_BYTES)))
        .collect::<Vec<_>>();

    for (route, (msg_type, payload)) in routes.iter().zip(frames.iter()) {
        let response = routed_request(&context, route, ChannelId::Sub, *msg_type, payload.clone());
        assert_queue_payload_ok(response.as_ref());
    }

    measure_sequential(settings, |index| {
        let route_index = index % route_count;
        let (msg_type, payload) = &frames[route_index];
        let start = Instant::now();
        let response = routed_request(
            &context,
            &routes[route_index],
            ChannelId::Sub,
            *msg_type,
            payload.clone(),
        );
        assert_queue_payload_ok(response.as_ref());
        start.elapsed()
    })
}

fn measure_stream_route_count(settings: ProofSettings, route_count: usize) -> LatencyStats {
    let context = setup_routed_context(DomainKind::Stream);
    let routes = proof_stream_routes("stream://proof/routes", route_count);
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

fn route_domain_has_minimal_effect(rows: &[RouteSensitivityRow], domain: &str) -> bool {
    let Some(base) = rows
        .iter()
        .find(|row| row.domain == domain && row.route_count == 10)
    else {
        return false;
    };
    let Some(high) = rows
        .iter()
        .find(|row| row.domain == domain && row.route_count == 10_000)
    else {
        return false;
    };

    let p99_allowed = high.stats.p99_us <= base.stats.p99_us.saturating_mul(12) / 10
        || high.stats.p99_us.saturating_sub(base.stats.p99_us) <= 100;
    let throughput_allowed = high.stats.ops_sec >= base.stats.ops_sec * 0.85;
    p99_allowed && throughput_allowed
}
