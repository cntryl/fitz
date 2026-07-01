#![allow(deprecated)]
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::{
    build_notice_subscribe, create_bench_notice_sink, extract_single_tlv_field,
    register_session_counting_sink, route_frame, CountingSink,
};
use fitz::domains::notice::sink::NoticeDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::runtime::domain_event::DomainPublishEvent;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

const PUBLISH_ROUTE: &str = "notice://realm/area/orders/create";

struct MatchPatternCase {
    label: &'static str,
    pattern: &'static str,
}

struct NoticePublishCase {
    sink: Arc<NoticeDomainSink>,
    destination: RouteAddress,
    event: DomainPublishEvent,
    subscriber_sinks: Vec<Arc<CountingSink>>,
}

impl NoticePublishCase {
    fn publish_once(&self) {
        self.sink
            .deliver(Envelope::new(self.destination.clone(), self.event.clone()))
            .expect("notice publish event");
    }

    fn assert_single_delivery_per_subscriber(&self) {
        self.publish_once();

        for subscriber_sink in &self.subscriber_sinks {
            assert_eq!(
                subscriber_sink.count(),
                1,
                "expected one notice delivery per subscriber"
            );
        }
    }

    fn reset_subscriber_counts(&self) {
        for subscriber_sink in &self.subscriber_sinks {
            subscriber_sink.reset();
        }
    }
}

fn create_publish_case(subscriber_count: usize, pattern: &str) -> NoticePublishCase {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_notice_sink(router.clone());
    router.register_domain_pattern("notice", sink.clone() as Arc<dyn MailboxSink>);

    let subscribe_frame = build_notice_subscribe(pattern);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    let mut subscriber_sinks = Vec::with_capacity(subscriber_count);

    for index in 0..subscriber_count {
        let session_id = (index + 1) as u64;
        let (subscriber_source, subscriber_sink) =
            register_session_counting_sink(&router, family, session_id);
        route_frame(
            router.as_ref(),
            &subscriber_source,
            pattern,
            session_id,
            ChannelId::Sub,
            subscribe_msg_type,
            subscribe_payload.clone(),
            family,
        )
        .expect("notice subscribe");
        subscriber_sink.reset();
        subscriber_sinks.push(subscriber_sink);
    }

    let case = NoticePublishCase {
        sink,
        destination: RouteAddress::new(family, Route::new(PUBLISH_ROUTE)),
        event: DomainPublishEvent::new(
            family,
            Route::new(PUBLISH_ROUTE),
            Bytes::from_static(b"notice fanout payload"),
        ),
        subscriber_sinks,
    };
    case.assert_single_delivery_per_subscriber();
    case.reset_subscriber_counts();
    case
}

fn bench_notice_publish_fanout(c: &mut Criterion) {
    let match_patterns = [
        MatchPatternCase {
            label: "exact_route",
            pattern: PUBLISH_ROUTE,
        },
        MatchPatternCase {
            label: "single_star",
            pattern: "notice://realm/area/orders/*",
        },
        MatchPatternCase {
            label: "double_star",
            pattern: "notice://realm/area/**",
        },
    ];

    let mut group = c.benchmark_group("subsystem_notice_publish");
    group.sampling_mode(SamplingMode::Flat);

    for match_case in match_patterns {
        for subscriber_count in [1usize, 16usize, 64usize, 256usize] {
            let case = create_publish_case(subscriber_count, match_case.pattern);
            group.throughput(Throughput::Elements(subscriber_count as u64));
            group.bench_function(
                format!(
                    "publish_{}_{}_subscribers_primary",
                    match_case.label, subscriber_count
                ),
                |b| {
                    b.iter(|| {
                        case.publish_once();
                        black_box(());
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_notice_publish_fanout
}
criterion_main!(benches);
