//! Actor-level characterization, not a real cloud or transport benchmark.
use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::create_local_bench_store;
use fitz::domains::schedule::{ScheduleDeliveryMode, ScheduleMessage, ScheduleResponse};
use fitz::domains::WritePolicy;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::domain_internals::schedule::{ScheduleActor, ScheduleStore};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

const SAMPLES: u64 = 128;
const DUE_COUNT: usize = 32;
const CONTROL_ROUTE: &str = "schedule://control/jobs/target/run";

#[derive(Clone, Copy)]
enum Control {
    Create,
    List,
    Cancel,
}

impl Control {
    fn label(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::List => "list",
            Self::Cancel => "cancel",
        }
    }

    fn message(self, sample: u64) -> ScheduleMessage {
        match self {
            Self::Create => create(CONTROL_ROUTE, Bytes::from(sample.to_le_bytes().to_vec())),
            Self::List => ScheduleMessage::List {
                offset: 0,
                limit: 100,
            },
            Self::Cancel => ScheduleMessage::Cancel {
                route: CONTROL_ROUTE.to_string(),
            },
        }
    }

    fn succeeded(self, response: &ScheduleResponse) -> bool {
        matches!(
            (self, response),
            (Self::Create | Self::Cancel, ScheduleResponse::Ok)
                | (Self::List, ScheduleResponse::ListDefs { .. })
        )
    }
}

fn create(route: &str, payload: Bytes) -> ScheduleMessage {
    ScheduleMessage::Create {
        route: route.to_string(),
        cron: "* * * * *".to_string(),
        delivery_mode: ScheduleDeliveryMode::Broadcast,
        payload,
    }
}

type FamilyActor = Arc<Mutex<ScheduleActor>>;

fn actor_pair() -> ([FamilyActor; 2], tempfile::TempDir) {
    let (engine, directory) = create_local_bench_store();
    engine.create_column_family("cf_2").expect("second family");
    let store = ScheduleStore::new(engine);
    let first = Arc::new(Mutex::new(ScheduleActor::new(
        RouteFamily::new(1),
        store.clone(),
        WritePolicy::Sync,
    )));
    let second = Arc::new(Mutex::new(ScheduleActor::new(
        RouteFamily::new(2),
        store,
        WritePolicy::Sync,
    )));
    {
        let mut actor = first.lock().expect("first actor");
        for index in 0..DUE_COUNT {
            assert!(matches!(
                actor.handle(create(
                    &format!("schedule://burst/jobs/due-{index}/run"),
                    Bytes::from_static(b"due")
                )),
                ScheduleResponse::Ok
            ));
        }
    }
    ([first, second], directory)
}

fn due_worker(
    first: &FamilyActor,
    delay: Duration,
    prepare_rx: &mpsc::Receiver<()>,
    ready_tx: &mpsc::Sender<()>,
    start_rx: &mpsc::Receiver<()>,
    done_tx: &mpsc::Sender<usize>,
) {
    while prepare_rx.recv().is_ok() {
        let mut actor = first.lock().expect("due actor");
        actor.bench_prepare_scan(DUE_COUNT);
        ready_tx.send(()).expect("ready");
        start_rx.recv().expect("start burst");
        std::thread::sleep(delay);
        let claims = actor.bench_claim_due_fires();
        assert_eq!(claims.len(), DUE_COUNT);
        let delivered: Vec<_> = claims
            .into_iter()
            .map(|claim| (claim.fire_ms, claim.route))
            .collect();
        std::thread::sleep(delay);
        let (acked, _) = actor
            .bench_ack_pending_fire_claims(&delivered)
            .expect("ACK due burst");
        assert_eq!(acked, DUE_COUNT);
        assert_eq!(
            actor.bench_pending_claimed_occurrences_for_publish(),
            Vec::new()
        );
        done_tx.send(acked).expect("finished");
    }
}

fn characterize(ctx: &mut StressContext, control: Control, sibling: bool, modeled_cloud: bool) {
    let delay = if modeled_cloud {
        Duration::from_millis(5)
    } else {
        Duration::ZERO
    };
    let storage = if modeled_cloud {
        "modeled_strict_cloud_5ms"
    } else {
        "local_sync"
    };
    let family_scope = if sibling {
        "sibling_family"
    } else {
        "same_family"
    };
    ctx.parameter("domain", "schedule");
    ctx.parameter("scenario", "due_burst_control_latency");
    ctx.parameter("storage_profile", storage);
    ctx.parameter("family_scope", family_scope);
    ctx.parameter("control", control.label());
    ctx.parameter(
        "due_claims_per_burst",
        u64::try_from(DUE_COUNT).expect("due count"),
    );
    ctx.parameter("samples", SAMPLES);
    ctx.parameter("completed_unit", "control_operations");
    ctx.parameter("write_mode", "sync");
    ctx.metadata("target_class", "storage_characterization");
    ctx.metadata(
        "ownership_model",
        "one_mutex_per_family_real_schedule_actor",
    );
    ctx.metadata(
        "cloud_model",
        "5ms_added_per_claim_ack_and_control_write_not_measured_cloud",
    );
    let ([first, second], _directory) = actor_pair();
    let target = if sibling { second } else { first.clone() };
    let (prepare_tx, prepare_rx) = mpsc::channel::<()>();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (start_tx, start_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        due_worker(&first, delay, &prepare_rx, &ready_tx, &start_rx, &done_tx);
    });
    let mut total_latency = Duration::ZERO;
    let mut due_completed = 0;
    for sample in 0..SAMPLES {
        // Setup, including target creation for Cancel, is outside the timer.
        if matches!(control, Control::Cancel) {
            assert!(matches!(
                target
                    .lock()
                    .expect("target")
                    .handle(create(CONTROL_ROUTE, Bytes::new())),
                ScheduleResponse::Ok
            ));
        }
        let message = control.message(sample);
        prepare_tx.send(()).expect("prepare");
        ready_rx.recv().expect("due actor holds family lock");
        let started = Instant::now();
        start_tx.send(()).expect("release burst");
        let response = {
            let mut actor = target.lock().expect("control actor");
            if modeled_cloud && !matches!(control, Control::List) {
                std::thread::sleep(delay);
            }
            actor.handle(message)
        };
        let elapsed = started.elapsed();
        assert!(control.succeeded(&response), "control failed: {response:?}");
        ctx.record_latency(elapsed);
        total_latency += elapsed;
        due_completed += done_rx.recv().expect("complete burst");
    }
    drop(prepare_tx);
    worker.join().expect("due worker");
    assert_eq!(
        due_completed,
        DUE_COUNT * usize::try_from(SAMPLES).expect("sample count")
    );
    ctx.parameter(
        "due_claims_completed",
        u64::try_from(due_completed).expect("completed count"),
    );
    ctx.parameter(
        "due_acknowledgements_completed",
        u64::try_from(due_completed).expect("completed count"),
    );
    let _ = ctx.correctness().attempted(SAMPLES).completed(SAMPLES);
    ctx.record_external("control_latency", total_latency, SAMPLES);
    ctx.metadata("latency_quantiles", "p50,p95,p99");
}

#[stress(tier = 2)]
fn should_characterize_local_same_family_create(ctx: &mut StressContext) {
    characterize(ctx, Control::Create, false, false);
}
#[stress(tier = 2)]
fn should_characterize_local_sibling_family_create(ctx: &mut StressContext) {
    characterize(ctx, Control::Create, true, false);
}
#[stress(tier = 2)]
fn should_characterize_local_same_family_list(ctx: &mut StressContext) {
    characterize(ctx, Control::List, false, false);
}
#[stress(tier = 2)]
fn should_characterize_local_sibling_family_list(ctx: &mut StressContext) {
    characterize(ctx, Control::List, true, false);
}
#[stress(tier = 2)]
fn should_characterize_local_same_family_cancel(ctx: &mut StressContext) {
    characterize(ctx, Control::Cancel, false, false);
}
#[stress(tier = 2)]
fn should_characterize_local_sibling_family_cancel(ctx: &mut StressContext) {
    characterize(ctx, Control::Cancel, true, false);
}
#[stress(tier = 2)]
fn should_characterize_modeled_cloud_same_family_create(ctx: &mut StressContext) {
    characterize(ctx, Control::Create, false, true);
}
#[stress(tier = 2)]
fn should_characterize_modeled_cloud_sibling_family_create(ctx: &mut StressContext) {
    characterize(ctx, Control::Create, true, true);
}
#[stress(tier = 2)]
fn should_characterize_modeled_cloud_same_family_list(ctx: &mut StressContext) {
    characterize(ctx, Control::List, false, true);
}
#[stress(tier = 2)]
fn should_characterize_modeled_cloud_sibling_family_list(ctx: &mut StressContext) {
    characterize(ctx, Control::List, true, true);
}
#[stress(tier = 2)]
fn should_characterize_modeled_cloud_same_family_cancel(ctx: &mut StressContext) {
    characterize(ctx, Control::Cancel, false, true);
}
#[stress(tier = 2)]
fn should_characterize_modeled_cloud_sibling_family_cancel(ctx: &mut StressContext) {
    characterize(ctx, Control::Cancel, true, true);
}

stress_main!();
