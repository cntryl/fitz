use crate::api::admin::list::StreamInfo;
use crate::api::admin::stats;
use crate::api::admin::topology::helpers::{
    add_broker_domain_flow, counter, domain_node_id, scope_for_resource, scoped_resource,
    top_resources, topology_connection, topology_lane, topology_state,
};
use crate::api::admin::topology::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScopedResource,
    TopologyState,
};

pub(in crate::api::admin::topology) fn stream_lane(
    stats: &stats::StreamStats,
    streams: &[StreamInfo],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.notify_drops_total + stats.append_conflicts_total + stats.failure_total;
    let activity = stats.operations_per_second > 0.0
        || stats.events_total > 0
        || stats.subscriptions_active > 0
        || stats.append_sessions_active > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("streams", "Streams", stats.streams_active as f64),
        counter("events", "Events", stats.events_total as f64),
        counter(
            "append_sessions",
            "Append sessions",
            stats.append_sessions_active as f64,
        ),
        counter(
            "subscriptions",
            "Subscriptions",
            stats.subscriptions_active as f64,
        ),
        counter(
            "append_conflicts",
            "Append conflicts",
            stats.append_conflicts_total as f64,
        ),
        counter(
            "notify_drops",
            "Notify drops",
            stats.notify_drops_total as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "stream",
        &state,
        stats.operations_per_second,
        counters.clone(),
    );

    for stream in streams.iter().filter(|stream| stream.sessions_active > 0) {
        connections.push(topology_connection(
            (
                format!(
                    "stream-append:{}:{}:{}",
                    stream.realm, stream.area, stream.resource
                ),
                domain_node_id("stream"),
                format!(
                    "stream:{}:{}:{}",
                    stream.realm, stream.area, stream.resource
                ),
            ),
            TopologyConnectionKind::StreamAppendActivity,
            format!("{} / {} append activity", stream.area, stream.resource),
            TopologyState::Flowing,
            scope_for_resource(&stream.realm, &stream.area, &stream.resource, None),
            vec![
                counter("sessions", "Sessions", stream.sessions_active as f64),
                counter("offset", "Offset", stream.offset as f64),
                counter("watermark", "Watermark", stream.watermark as f64),
            ],
        ));
    }

    topology_lane(
        ("stream", "Stream"),
        state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.append_sessions_active, stats.subscriptions_active),
        top_stream_resources(streams),
    )
}

fn top_stream_resources(streams: &[StreamInfo]) -> Vec<TopologyScopedResource> {
    let resources = streams
        .iter()
        .map(|stream| {
            let counters = vec![
                counter("offset", "Offset", stream.offset as f64),
                counter("watermark", "Watermark", stream.watermark as f64),
                counter("size_bytes", "Size bytes", stream.size_bytes as f64),
                counter("sessions", "Sessions", stream.sessions_active as f64),
            ];
            scoped_resource(
                "stream",
                format!("{} / {} / {}", stream.realm, stream.area, stream.resource),
                if stream.sessions_active > 0 || stream.offset > 0 {
                    TopologyState::Flowing
                } else {
                    TopologyState::Quiet
                },
                scope_for_resource(&stream.realm, &stream.area, &stream.resource, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}
