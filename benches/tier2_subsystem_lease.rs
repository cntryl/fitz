use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress_main, stress_test, StressContext};
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
const CLIENT_SESSION_ID: u64 = 1;
const WATCH_REGISTER_BATCH_SIZE: usize = 256;
const WATCH_REGISTER_CASE_COUNT: usize = 8;
const NOTIFY_REPEAT_COUNT: u64 = 512;
const NOTIFY_CHUNK_SIZE: u64 = 64;

struct PreparedLeaseNotifyCase {
    sink: Arc<LeaseDomainSink>,
    destination: RouteAddress,
    event: DomainPublishEvent,
    watcher_sinks: Vec<Arc<CountingSink>>,
}

struct PreparedLeaseWatchRegisterCase {
    router: Arc<Router>,
    family: RouteFamily,
    watchers: Vec<(u64, RouteAddress, Arc<CountingSink>)>,
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

fn encode_lease_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

fn prepare_watch_register_case() -> PreparedLeaseWatchRegisterCase {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_lease_sink(router.clone());
    router.register_domain_pattern("lease", sink as Arc<dyn MailboxSink>);
    let watchers = (0..WATCH_REGISTER_BATCH_SIZE)
        .map(|index| {
            let session_id = CLIENT_SESSION_ID + index as u64;
            let (source, sink) = register_session_counting_sink(&router, family, session_id);
            (session_id, source, sink)
        })
        .collect();

    PreparedLeaseWatchRegisterCase {
        router,
        family,
        watchers,
    }
}

fn register_lease_watch(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    pattern: &str,
) {
    route_frame(
        router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Sub,
        msg_type::SUBSCRIBE,
        encode_lease_subscribe(pattern),
        family,
    )
    .expect("lease watch registration");
}

fn prepare_notify_case(watcher_count: usize, pattern: &str) -> PreparedLeaseNotifyCase {
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
            pattern,
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

#[stress_test(tier = 2, name = "watch_register_256_sessions_x8_cases_primary")]
fn should_watch_register_256_sessions_x8_cases_primary(ctx: &mut StressContext) {
    let cases = (0..WATCH_REGISTER_CASE_COUNT)
        .map(|_| prepare_watch_register_case())
        .collect::<Vec<_>>();
    let start = Instant::now();
    for case in &cases {
        for (session_id, source, _) in &case.watchers {
            register_lease_watch(
                &case.router,
                case.family,
                source,
                WATCH_ROUTE,
                *session_id,
                WATCH_ROUTE,
            );
        }
    }
    let duration = start.elapsed();

    for case in &cases {
        let sinks = case
            .watchers
            .iter()
            .map(|(_, _, sink)| sink.clone())
            .collect::<Vec<_>>();
        let responses = wait_for_counting_sinks_each_count(&sinks, 1, Duration::from_secs(1));
        assert_eq!(
            responses, WATCH_REGISTER_BATCH_SIZE,
            "lease watch registration should ack every watcher"
        );
    }
    for case in cases {
        case.router.clear();
    }
    tier2_stress::record_duration(
        ctx,
        duration,
        (WATCH_REGISTER_BATCH_SIZE * WATCH_REGISTER_CASE_COUNT) as u64,
    );
}

fn notify_watchers(ctx: &mut StressContext, watcher_count: usize, pattern: &str) {
    let case = prepare_notify_case(watcher_count, pattern);
    let mut remaining = NOTIFY_REPEAT_COUNT;
    let mut total = Duration::ZERO;
    while remaining > 0 {
        let chunk = remaining.min(NOTIFY_CHUNK_SIZE);
        case.reset_watcher_counts();
        let start = Instant::now();
        for _ in 0..chunk {
            case.publish_once();
        }
        let expected_per_watcher = usize::try_from(chunk).expect("lease publish count fits usize");
        case.wait_for_notifications_per_watcher(expected_per_watcher);
        total += start.elapsed();
        case.reset_watcher_counts();
        remaining -= chunk;
    }
    tier2_stress::record_duration(
        ctx,
        total / u32::try_from(NOTIFY_REPEAT_COUNT).expect("notify repeat count fits u32"),
        watcher_count as u64,
    );
}

macro_rules! lease_notify_bench {
    ($fn_name:ident, $stress_name:literal, $watchers:expr, $pattern:expr) => {
        #[stress_test(tier = 2, name = $stress_name)]
        fn $fn_name(ctx: &mut StressContext) {
            notify_watchers(ctx, $watchers, $pattern);
        }
    };
}

lease_notify_bench!(
    should_notify_exact_route_1_watchers_primary,
    "notify_exact_route_1_watchers_primary",
    1,
    WATCH_ROUTE
);
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
lease_notify_bench!(
    should_notify_single_star_1_watchers_primary,
    "notify_single_star_1_watchers_primary",
    1,
    "lease://realm/locks/*"
);
lease_notify_bench!(
    should_notify_single_star_16_watchers_primary,
    "notify_single_star_16_watchers_primary",
    16,
    "lease://realm/locks/*"
);
lease_notify_bench!(
    should_notify_single_star_64_watchers_primary,
    "notify_single_star_64_watchers_primary",
    64,
    "lease://realm/locks/*"
);
lease_notify_bench!(
    should_notify_single_star_256_watchers_primary,
    "notify_single_star_256_watchers_primary",
    256,
    "lease://realm/locks/*"
);
lease_notify_bench!(
    should_notify_double_star_1_watchers_primary,
    "notify_double_star_1_watchers_primary",
    1,
    "lease://realm/**"
);
lease_notify_bench!(
    should_notify_double_star_16_watchers_primary,
    "notify_double_star_16_watchers_primary",
    16,
    "lease://realm/**"
);
lease_notify_bench!(
    should_notify_double_star_64_watchers_primary,
    "notify_double_star_64_watchers_primary",
    64,
    "lease://realm/**"
);
lease_notify_bench!(
    should_notify_double_star_256_watchers_primary,
    "notify_double_star_256_watchers_primary",
    256,
    "lease://realm/**"
);

stress_main!();
