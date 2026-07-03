use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
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

#[path = "criterion_config.rs"]
mod criterion_config;

const WATCH_ROUTE: &str = "lease://realm/locks/primary";
const CLIENT_SESSION_ID: u64 = 1;
const WATCH_REGISTER_BATCH_SIZE: usize = 256;
const WATCH_REGISTER_CASE_COUNT: usize = 8;
const NOTIFY_REPEAT_COUNT: u64 = 64;
const NOTIFY_CHUNK_SIZE: u64 = 64;

struct LeasePatternCase {
    label: &'static str,
    pattern: &'static str,
}

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

fn bench_lease_watch_register_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_lease");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(
        (WATCH_REGISTER_BATCH_SIZE * WATCH_REGISTER_CASE_COUNT) as u64,
    ));

    group.bench_function(
        format!("watch_register_256_sessions_x{WATCH_REGISTER_CASE_COUNT}_cases_primary"),
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let cases: Vec<_> = (0..WATCH_REGISTER_CASE_COUNT)
                        .map(|_| prepare_watch_register_case())
                        .collect();
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
                    total += start.elapsed();

                    for case in cases {
                        let sinks: Vec<_> = case
                            .watchers
                            .iter()
                            .map(|(_, _, sink)| sink.clone())
                            .collect();
                        let responses =
                            wait_for_counting_sinks_each_count(&sinks, 1, Duration::from_secs(1));
                        assert_eq!(
                            responses, WATCH_REGISTER_BATCH_SIZE,
                            "lease watch registration should ack every watcher"
                        );
                    }
                }
                total
            });
        },
    );

    group.finish();
}

fn bench_lease_notify_primary(c: &mut Criterion) {
    let pattern_cases = [
        LeasePatternCase {
            label: "exact_route",
            pattern: WATCH_ROUTE,
        },
        LeasePatternCase {
            label: "single_star",
            pattern: "lease://realm/locks/*",
        },
        LeasePatternCase {
            label: "double_star",
            pattern: "lease://realm/**",
        },
    ];

    let mut group = c.benchmark_group("subsystem_lease");
    group.sampling_mode(SamplingMode::Flat);

    for pattern_case in pattern_cases {
        for watcher_count in [1usize, 16usize, 64usize, 256usize] {
            let case = prepare_notify_case(watcher_count, pattern_case.pattern);
            group.throughput(Throughput::Elements(watcher_count as u64));
            group.bench_function(
                format!(
                    "notify_{}_{}_watchers_primary",
                    pattern_case.label, watcher_count
                ),
                |b| {
                    b.iter_custom(|iters| {
                        let mut remaining = iters.saturating_mul(NOTIFY_REPEAT_COUNT);
                        let mut total = Duration::ZERO;
                        while remaining > 0 {
                            let chunk = remaining.min(NOTIFY_CHUNK_SIZE);
                            case.reset_watcher_counts();
                            let start = Instant::now();
                            for _ in 0..chunk {
                                case.publish_once();
                            }
                            total += start.elapsed();
                            let expected_per_watcher =
                                usize::try_from(chunk).expect("lease publish count fits usize");
                            case.wait_for_notifications_per_watcher(expected_per_watcher);
                            case.reset_watcher_counts();
                            remaining -= chunk;
                        }
                        total
                            / u32::try_from(NOTIFY_REPEAT_COUNT)
                                .expect("notify repeat count fits u32")
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
    targets = bench_lease_watch_register_primary, bench_lease_notify_primary
}
criterion_main!(benches);
