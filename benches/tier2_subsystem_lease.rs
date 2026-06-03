use bytes::Bytes;
use criterion::{BatchSize, Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use fitz::benchkit::{
    CountingSink, create_bench_lease_sink, register_session_counting_sink,
    register_session_queue_sink, route_frame,
};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::lease_codec::msg_type;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::domain_event::DomainPublishEvent;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

const WATCH_ROUTE: &str = "lease://realm/locks/primary";
const CLIENT_SESSION_ID: u64 = 1;

struct LeasePatternCase {
    label: &'static str,
    pattern: &'static str,
}

struct PreparedLeaseNotifyCase {
    sink: Arc<fitz::boot::domains::LeaseDomainSink>,
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

    fn validate_and_reset(&self) {
        self.publish_once();

        for watcher_sink in &self.watcher_sinks {
            assert_eq!(
                watcher_sink.count(),
                1,
                "expected one lease notification per watcher"
            );
            watcher_sink.reset();
        }
    }
}

fn encode_lease_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

fn setup_lease_request_sink() -> (
    Arc<Router>,
    RouteFamily,
    RouteAddress,
    Arc<fitz::benchkit::FrameQueueSink>,
) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_lease_sink(router.clone());
    router.register_domain_pattern("lease", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    (router, family, source, inbox)
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
    group.throughput(Throughput::Elements(1));

    group.bench_function("watch_register_primary", |b| {
        b.iter_batched(
            setup_lease_request_sink,
            |(router, family, source, inbox)| {
                register_lease_watch(
                    &router,
                    family,
                    &source,
                    WATCH_ROUTE,
                    CLIENT_SESSION_ID,
                    WATCH_ROUTE,
                );
                assert_eq!(
                    inbox.drain().len(),
                    1,
                    "lease watch registration should ack immediately"
                );
            },
            BatchSize::SmallInput,
        )
    });

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
                    b.iter(|| {
                        case.publish_once();
                    })
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
