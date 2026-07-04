#![allow(deprecated)]
use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_notice_subscribe, create_bench_notice_sink, extract_single_tlv_field,
    register_session_counting_sink, route_frame, wait_for_counting_sinks_each_count, CountingSink,
};
use fitz::domains::notice::sink::NoticeDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::runtime::domain_event::DomainPublishEvent;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PUBLISH_ROUTE: &str = "notice://realm/area/orders/create";
const PUBLISH_REPEAT_COUNT: u64 = 256;
const PUBLISH_CHUNK_SIZE: u64 = 64;

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
        self.wait_for_deliveries_per_subscriber(1);
    }

    fn wait_for_deliveries_per_subscriber(&self, expected_per_subscriber: usize) {
        let delivery_count = wait_for_counting_sinks_each_count(
            &self.subscriber_sinks,
            expected_per_subscriber,
            Duration::from_secs(1),
        );
        assert_eq!(
            delivery_count,
            self.subscriber_sinks.len() * expected_per_subscriber,
            "expected notice delivery count per subscriber"
        );
        assert!(
            self.subscriber_sinks
                .iter()
                .all(|sink| sink.count() == expected_per_subscriber),
            "notice publish should not skip or duplicate subscriber deliveries"
        );
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
        assert_eq!(
            subscriber_sink.wait_for_count(1, Duration::from_secs(1)),
            1,
            "notice subscribe should ack before publish measurement"
        );
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

fn publish_fanout(ctx: &mut StressContext, subscriber_count: usize, pattern: &str) {
    let case = create_publish_case(subscriber_count, pattern);
    let mut remaining = PUBLISH_REPEAT_COUNT;
    let mut total = Duration::ZERO;
    while remaining > 0 {
        let chunk = remaining.min(PUBLISH_CHUNK_SIZE);
        case.reset_subscriber_counts();
        let start = Instant::now();
        for _ in 0..chunk {
            case.publish_once();
            black_box(());
        }
        let expected_per_subscriber =
            usize::try_from(chunk).expect("notice publish count fits usize");
        case.wait_for_deliveries_per_subscriber(expected_per_subscriber);
        total += start.elapsed();
        case.reset_subscriber_counts();
        remaining -= chunk;
    }
    tier2_stress::record_duration(
        ctx,
        total / u32::try_from(PUBLISH_REPEAT_COUNT).expect("publish repeat count fits u32"),
        subscriber_count as u64,
    );
}

macro_rules! notice_publish_bench {
    ($fn_name:ident, $stress_name:literal, $subscribers:expr, $pattern:expr) => {
        #[stress_test(tier = 2, mode = "fixed_duration", name = $stress_name)]
        fn $fn_name(ctx: &mut StressContext) {
            publish_fanout(ctx, $subscribers, $pattern);
        }
    };
}

notice_publish_bench!(
    should_publish_exact_route_1_subscribers_primary,
    "publish_exact_route_1_subscribers_primary",
    1,
    PUBLISH_ROUTE
);
notice_publish_bench!(
    should_publish_exact_route_16_subscribers_primary,
    "publish_exact_route_16_subscribers_primary",
    16,
    PUBLISH_ROUTE
);
notice_publish_bench!(
    should_publish_exact_route_64_subscribers_primary,
    "publish_exact_route_64_subscribers_primary",
    64,
    PUBLISH_ROUTE
);
notice_publish_bench!(
    should_publish_exact_route_256_subscribers_primary,
    "publish_exact_route_256_subscribers_primary",
    256,
    PUBLISH_ROUTE
);
notice_publish_bench!(
    should_publish_single_star_1_subscribers_primary,
    "publish_single_star_1_subscribers_primary",
    1,
    "notice://realm/area/orders/*"
);
notice_publish_bench!(
    should_publish_single_star_16_subscribers_primary,
    "publish_single_star_16_subscribers_primary",
    16,
    "notice://realm/area/orders/*"
);
notice_publish_bench!(
    should_publish_single_star_64_subscribers_primary,
    "publish_single_star_64_subscribers_primary",
    64,
    "notice://realm/area/orders/*"
);
notice_publish_bench!(
    should_publish_single_star_256_subscribers_primary,
    "publish_single_star_256_subscribers_primary",
    256,
    "notice://realm/area/orders/*"
);
notice_publish_bench!(
    should_publish_double_star_1_subscribers_primary,
    "publish_double_star_1_subscribers_primary",
    1,
    "notice://realm/area/**"
);
notice_publish_bench!(
    should_publish_double_star_16_subscribers_primary,
    "publish_double_star_16_subscribers_primary",
    16,
    "notice://realm/area/**"
);
notice_publish_bench!(
    should_publish_double_star_64_subscribers_primary,
    "publish_double_star_64_subscribers_primary",
    64,
    "notice://realm/area/**"
);
notice_publish_bench!(
    should_publish_double_star_256_subscribers_primary,
    "publish_double_star_256_subscribers_primary",
    256,
    "notice://realm/area/**"
);

stress_main!();
