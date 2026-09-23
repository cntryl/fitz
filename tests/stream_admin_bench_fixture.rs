use bytes::Bytes;
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit,
    create_bench_stream_admin_sink, extract_single_tlv_field, parse_stream_session_id,
    register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

fn request(
    router: &Router,
    family: RouteFamily,
    source: &RouteAddress,
    inbox: &FrameQueueSink,
    route: &str,
    frame: Vec<u8>,
) -> Bytes {
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    route_frame(
        router,
        source,
        route,
        1,
        ChannelId::Pub,
        msg_type,
        payload,
        family,
    )
    .expect("route Stream request");
    inbox
        .drain_after_count(1, Duration::from_secs(1))
        .last()
        .expect("Stream response")
        .payload
        .clone()
}

#[test]
fn should_project_commits_from_all_benchmark_families() {
    // Arrange
    let router = Arc::new(Router::new());
    let families = [RouteFamily::new(1), RouteFamily::new(2)];
    let sink = create_bench_stream_admin_sink(Arc::clone(&router), &families);
    router.register_domain_pattern("stream", sink.clone() as Arc<dyn MailboxSink>);
    for family in families {
        let (source, inbox) = register_session_queue_sink(&router, family, 1);
        let route = "stream://bench/events/orders";
        let begin = request(&router, family, &source, &inbox, route, build_stream_begin(route));
        let session_id = parse_stream_session_id(begin.as_ref()).expect("append session");
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            build_stream_append(session_id, 0, b"seed"),
        );
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            build_stream_commit(session_id, 1),
        );
    }

    // Act
    sink.refresh();
    let counts = sink.snapshot_counts();

    // Assert
    assert_eq!(counts, (2, 2));
}
