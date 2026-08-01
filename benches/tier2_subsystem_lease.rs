use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::benchkit::{
    create_bench_lease_sink, register_session_counting_sink, route_frame,
    wait_for_counting_sinks_each_count, CountingSink,
};
use fitz::domains::lease::sink::LeaseDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::lease_codec::msg_type;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::domain_event::DomainPublishEvent;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WATCH_ROUTE: &str = "lease://realm/locks/primary";
const NOTIFY_REPEAT_COUNT: u64 = 32_768;
const EXACT_ROUTE_64_NOTIFY_REPEAT_COUNT: u64 = 131_072;
const NOTIFY_CHUNK_SIZE: u64 = 512;

struct PreparedLeaseNotifyCase {
    sink: Arc<LeaseDomainSink>,
    destination: RouteAddress,
    event: DomainPublishEvent,
    watcher_sinks: Vec<Arc<CountingSink>>,
}

impl PreparedLeaseNotifyCase {
    fn publish_once(&self) {
        self.sink
            .deliver(Envelope::new(self.destination.clone(), self.event.clone()))
            .expect("lease publish event");
    }

    fn publish_and_wait_for_notifications(&self) {
        self.publish_once();
        self.wait_for_notifications_per_watcher(1);
        self.reset_watcher_counts();
    }

    fn wait_for_notifications_per_watcher(&self, expected_per_watcher: usize) {
        let delivery_count = wait_for_counting_sinks_each_count(
            &self.watcher_sinks,
            expected_per_watcher,
            Duration::from_secs(1),
        );
        assert_eq!(
            delivery_count,
            self.watcher_sinks.len() * expected_per_watcher,
            "expected one lease notification per watcher"
        );
        assert!(
            self.watcher_sinks
                .iter()
                .all(|sink| sink.count() == expected_per_watcher),
            "lease notifications should not skip or duplicate watcher deliveries"
        );
    }

    fn reset_watcher_counts(&self) {
        for watcher_sink in &self.watcher_sinks {
            watcher_sink.reset();
        }
    }

    fn validate_and_reset(&self) {
        self.publish_and_wait_for_notifications();
    }
}

fn encode_lease_subscribe(route: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    Bytes::from(encoder.finish())
}

fn register_lease_watch(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    route: &str,
) {
    route_frame(
        router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Sub,
        msg_type::SUBSCRIBE,
        encode_lease_subscribe(route),
        family,
    )
    .expect("lease watch registration");
}

fn prepare_notify_case(watcher_count: usize, route: &str) -> PreparedLeaseNotifyCase {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_lease_sink(router.clone());
    router.register_domain_pattern("lease", sink.clone() as Arc<dyn MailboxSink>);

    let mut watcher_sinks = Vec::with_capacity(watcher_count);
    for index in 0..watcher_count {
        let session_id = 10_000 + index as u64;
        let (watcher_source, watcher_sink) =
            register_session_counting_sink(&router, family, session_id);
        register_lease_watch(
            &router,
            family,
            &watcher_source,
            WATCH_ROUTE,
            session_id,
            route,
        );
        assert_eq!(
            watcher_sink.wait_for_count(1, Duration::from_secs(1)),
            1,
            "lease watch registration should ack before notification measurement"
        );
        watcher_sink.reset();
        watcher_sinks.push(watcher_sink);
    }

    let case = PreparedLeaseNotifyCase {
        sink,
        destination: RouteAddress::new(family, Route::new(WATCH_ROUTE)),
        event: DomainPublishEvent::new(family, Route::new(WATCH_ROUTE), Bytes::new()),
        watcher_sinks,
    };
    case.validate_and_reset();
    case
}

fn notify_watchers(ctx: &mut StressContext, name: &str, watcher_count: usize, route: &str) {
    let case = prepare_notify_case(watcher_count, route);
    let repeat_count = notify_repeat_count(watcher_count, route);
    let mut remaining = repeat_count;
    let mut total = Duration::ZERO;
    while remaining > 0 {
        let chunk = remaining.min(NOTIFY_CHUNK_SIZE);
        case.reset_watcher_counts();
        let start = Instant::now();
        for _ in 0..chunk {
            case.publish_once();
            black_box(());
        }
        let expected_per_watcher = usize::try_from(chunk).expect("lease publish count fits usize");
        case.wait_for_notifications_per_watcher(expected_per_watcher);
        total += start.elapsed();
        case.reset_watcher_counts();
        remaining -= chunk;
    }
    tier2_stress::record_duration(
        ctx,
        name,
        total,
        repeat_count.saturating_mul(watcher_count as u64),
    );
}

fn notify_repeat_count(watcher_count: usize, route: &str) -> u64 {
    if watcher_count == 64 && route == WATCH_ROUTE {
        EXACT_ROUTE_64_NOTIFY_REPEAT_COUNT
    } else {
        NOTIFY_REPEAT_COUNT
    }
}

macro_rules! lease_notify_bench {
    ($fn_name:ident, $stress_name:literal, $watchers:expr, $route:expr) => {
        #[stress(tier = 2)]
        fn $fn_name(ctx: &mut StressContext) {
            notify_watchers(ctx, $stress_name, $watchers, $route);
        }
    };
}

lease_notify_bench!(
    should_notify_exact_route_16_watchers_primary,
    "notify_exact_route_16_watchers_primary",
    16,
    WATCH_ROUTE
);
lease_notify_bench!(
    should_notify_exact_route_64_watchers_primary,
    "notify_exact_route_64_watchers_primary",
    64,
    WATCH_ROUTE
);
lease_notify_bench!(
    should_notify_exact_route_256_watchers_primary,
    "notify_exact_route_256_watchers_primary",
    256,
    WATCH_ROUTE
);
stress_main!();
