use super::*;
use crate::domains::queue::actor::recovery_store::QueueTransactionMode;
use crate::domains::queue::metrics::{
    METRIC_ADMISSION_BUDGET_EXHAUSTED_TOTAL, METRIC_L0_ADMISSION_RETRIES_TOTAL,
};
use std::process::{Child, Command, Stdio};

const CHILD_CASE: &str = "FITZ_QUEUE_ADMISSION_METRIC_CHILD";
const CHILD_RECEIPT: &str = "FITZ_QUEUE_ADMISSION_METRIC_RECEIPT";

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Global counters need a fresh process: other Queue tests also increment them.
fn run_isolated(test_name: &str, body: impl FnOnce()) {
    let (_, module) = module_path!()
        .split_once("::")
        .expect("crate module prefix");
    let test_case = format!("{module}::{test_name}");
    if std::env::var_os(CHILD_CASE).as_deref() == Some(std::ffi::OsStr::new(&test_case)) {
        body();
        std::fs::write(
            std::env::var_os(CHILD_RECEIPT).expect("child receipt path"),
            test_case,
        )
        .expect("record completed metric assertions");
        return;
    }

    let directory = tempfile::tempdir().expect("metric child directory");
    let receipt = directory.path().join("completed");
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", &test_case, "--nocapture"])
            .env(CHILD_CASE, &test_case)
            .env(CHILD_RECEIPT, &receipt)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn isolated metric test"),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.0.try_wait().expect("poll metric child") {
            assert!(status.success(), "Queue metric child failed: {status}");
            assert_eq!(
                std::fs::read_to_string(&receipt).expect("metric assertions completed"),
                test_case,
                "the exact metric test must run in the child"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Queue metric child exceeded its 30-second deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn admission_counters() -> (u64, u64) {
    let metrics = crate::observability::metrics();
    (
        metrics.counter_get(METRIC_L0_ADMISSION_RETRIES_TOTAL),
        metrics.counter_get(METRIC_ADMISSION_BUDGET_EXHAUSTED_TOTAL),
    )
}

fn stored_rows(actor: &QueueActor) -> Vec<(Bytes, Bytes)> {
    actor
        .persistence
        .store
        .begin(actor.queue_key.family.id(), QueueTransactionMode::ReadOnly)
        .expect("read stored Queue state")
        .scan_all()
        .expect("scan stored Queue state")
}

#[test]
fn should_count_l0_admission_retry_given_rejection_before_commit() {
    run_isolated(
        "should_count_l0_admission_retry_given_rejection_before_commit",
        || {
            // Arrange
            let mut actor = actor();
            let before = admission_counters();
            QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
                "column family 1 has no free L0 slot (15/14)".into(),
            ));

            // Act
            let response = actor.handle_send(Bytes::from_static(b"work"), None);

            // Assert
            assert!(matches!(response, QueueResponse::Sent { .. }));
            assert_eq!(admission_counters(), (before.0 + 1, before.1));
            assert_eq!(actor.ready_count, 1);
        },
    );
}

#[test]
fn should_count_admission_budget_exhaustion_given_l0_rejections_past_deadline() {
    run_isolated(
        "should_count_admission_budget_exhaustion_given_l0_rejections_past_deadline",
        || {
            // Arrange
            let mut actor = actor();
            let (id, _) = send_and_reserve_single_message(&mut actor, "work");
            let stored_before = stored_rows(&actor);
            let before = admission_counters();
            let started = Instant::now();
            let mut attempts = 0;

            // Act
            let result = actor.commit_ack_with_admission_budget(Duration::from_millis(30), |txn| {
                attempts += 1;
                QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
                    "column family 1 has no free L0 slot (15/14)".into(),
                ));
                txn.delete(actor.cached_header_key(id))
                    .map_err(|error| error.to_string())
            });
            QueueStore::clear_commit_error_for_tests();

            // Assert
            assert!(result.is_err());
            assert!(attempts >= 1);
            assert!(started.elapsed() < Duration::from_secs(1));
            assert_eq!(admission_counters().1, before.1 + 1);
            assert_eq!(stored_rows(&actor), stored_before);
            assert!(actor.inflight.contains_key(&id));
        },
    );
}

#[test]
fn should_count_zero_budget_admission_timeout_without_retries_or_mutation() {
    run_isolated(
        "should_count_zero_budget_admission_timeout_without_retries_or_mutation",
        || {
            // Arrange
            let (engine, family, _) = super::super::storage_admission::storage_under_pressure();
            let mut key = unique_queue_key("zero-admission-budget");
            key.family = RouteFamily::new(family);
            let actor = QueueActor::new(
                key.family,
                key,
                engine,
                None,
                crate::utils::idempotency::default_dedup_store(),
            );
            let stored_before = stored_rows(&actor);
            let before = admission_counters();
            let mut attempts = 0;

            // Act
            let result = actor.commit_ack_with_admission_budget(Duration::ZERO, |txn| {
                attempts += 1;
                txn.put(b"pending".to_vec(), b"work".to_vec(), None)
                    .map_err(|error| error.to_string())
            });

            // Assert
            assert!(result
                .expect_err("storage admission stays closed")
                .contains("storage admission"));
            assert_eq!(attempts, 1);
            assert_eq!(admission_counters(), (before.0, before.1 + 1));
            assert_eq!(stored_rows(&actor), stored_before);
        },
    );
}

#[test]
fn should_not_count_unclassified_commit_errors_as_admission_pressure() {
    run_isolated(
        "should_not_count_unclassified_commit_errors_as_admission_pressure",
        || {
            // Arrange
            let mut actor = actor();
            let (id, token) = send_and_reserve_single_message(&mut actor, "work");
            let stored_before = stored_rows(&actor);
            let before = admission_counters();
            let errors = [
                cntryl_midge::MidgeError::Timeout("commit outcome unknown".into()),
                cntryl_midge::MidgeError::WriteConflict("record changed".into()),
                cntryl_midge::MidgeError::WriteStall("post-write durability is stalled".into()),
                cntryl_midge::MidgeError::WriteStall(
                    "column family 2 has no free L0 slot (15/14)".into(),
                ),
            ];

            // Act
            let responses: Vec<_> = errors
                .into_iter()
                .map(|error| {
                    QueueStore::fail_next_commit_for_tests(error);
                    actor.handle_ack_for_session(TEST_SESSION_ID, id, token)
                })
                .collect();

            // Assert
            assert!(responses
                .iter()
                .all(|response| matches!(response, QueueResponse::Error { .. })));
            assert_eq!(admission_counters(), before);
            assert_eq!(stored_rows(&actor), stored_before);
            assert!(actor.inflight.contains_key(&id));
        },
    );
}
