#[path = "../benches/stress_support/queue_pressure/envelope.rs"]
#[allow(dead_code)]
mod envelope;

use envelope::{Backlog, DrainEvidence, Envelope, ObservedStage, Termination};

const BACKLOGS: [Backlog; 3] = [
    Backlog::TEN_THOUSAND,
    Backlog::TWENTY_FIVE_THOUSAND,
    Backlog::HUNDRED_THOUSAND,
];
const FULL: Envelope = Envelope::FullWindow(Backlog::TEN_THOUSAND);
const FINITE: Envelope = Envelope::FiniteDrain(Backlog::TEN_THOUSAND);
const MINIMUM_PAUSE_NS: u128 = 5_000_000;

fn drained(envelope: Envelope) -> ObservedStage {
    let accepted = match envelope {
        Envelope::FullWindow(_) => envelope.rate() * Envelope::WINDOW_SECONDS,
        Envelope::FiniteDrain(_) => envelope.backlog(),
    };
    ObservedStage {
        termination: Termination::ConfiguredWindow,
        configured_window_completed: true,
        active_elapsed_ns: u128::from(Envelope::WINDOW_SECONDS) * 1_000_000_000,
        offered: accepted,
        harness_missed: 0,
        sent: accepted,
        accepted,
        rejected: 0,
        acknowledged: accepted,
        accepted_unacknowledged: 0,
        acknowledged_timing_samples: [accepted; 5],
        minimum_pause_ns: Some(MINIMUM_PAUSE_NS),
        minimum_worker_pause_ns: Some(MINIMUM_PAUSE_NS),
        drain: DrainEvidence {
            drained: true,
            empty_verified: true,
            clients_closed: true,
        },
    }
}

fn accepted(stage: &mut ObservedStage, count: u64) {
    stage.sent = count;
    stage.accepted = count;
    stage.acknowledged = count;
    stage.acknowledged_timing_samples = [count; 5];
    stage.offered = count + stage.harness_missed;
}

#[test]
fn should_accept_every_completed_window_envelope() {
    // Arrange
    let envelopes = BACKLOGS.map(Envelope::FullWindow);
    // Act
    let results = envelopes.map(|envelope| envelope.verify(&drained(envelope)));
    // Assert
    assert!(results.iter().all(Result::is_ok));
}

#[test]
fn should_accept_the_minimum_full_window_message_count() {
    // Arrange
    let mut stage = drained(FULL);
    let offered = stage.offered;
    let minimum = (offered * 99).div_ceil(100);
    accepted(&mut stage, minimum);
    stage.harness_missed = offered - minimum;
    stage.offered = offered;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_a_full_window_below_its_message_envelope() {
    // Arrange
    let mut stage = drained(FULL);
    let below_minimum = (stage.offered * 99).div_ceil(100) - 1;
    accepted(&mut stage, below_minimum);
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_accept_exactly_one_percent_missed_arrivals() {
    // Arrange
    let mut stage = drained(FULL);
    accepted(&mut stage, 9_900);
    stage.offered = 10_000;
    stage.harness_missed = 100;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_more_than_one_percent_missed_arrivals() {
    // Arrange
    let mut stage = drained(FULL);
    accepted(&mut stage, 9_899);
    stage.offered = 10_000;
    stage.harness_missed = 101;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_guard_stops_for_full_window_workloads() {
    // Arrange
    let stops = [
        Termination::BacklogGuard,
        Termination::AccountingGuard,
        Termination::EnqueueRejection,
        Termination::Failed,
    ];
    // Act
    let results = stops.map(|termination| {
        let mut stage = drained(FULL);
        stage.termination = termination;
        FULL.verify(&stage)
    });
    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_require_the_configured_window_to_complete() {
    // Arrange
    let mut stage = drained(FULL);
    stage.configured_window_completed = false;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_a_claimed_window_that_ended_early() {
    // Arrange
    let mut stage = drained(FULL);
    stage.active_elapsed_ns -= 1;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_accept_guard_drains_at_every_finite_message_envelope() {
    // Arrange
    let stops = [
        Termination::BacklogGuard,
        Termination::AccountingGuard,
        Termination::EnqueueRejection,
    ];
    // Act
    let results: Vec<_> = BACKLOGS
        .into_iter()
        .flat_map(|backlog| {
            stops.map(|termination| {
                let envelope = Envelope::FiniteDrain(backlog);
                let mut stage = drained(envelope);
                stage.termination = termination;
                stage.configured_window_completed = false;
                stage.active_elapsed_ns = 1;
                envelope.verify(&stage)
            })
        })
        .collect();
    // Assert
    assert!(results.iter().all(Result::is_ok));
}

#[test]
fn should_reject_finite_drains_below_the_declared_message_count() {
    // Arrange
    let envelopes = BACKLOGS.map(Envelope::FiniteDrain);
    // Act
    let results = envelopes.map(|envelope| {
        let mut stage = drained(envelope);
        stage.termination = Termination::BacklogGuard;
        accepted(&mut stage, envelope.backlog() - 1);
        envelope.verify(&stage)
    });
    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_reject_a_failed_finite_drain() {
    // Arrange
    let mut stage = drained(FINITE);
    stage.termination = Termination::Failed;
    // Act
    let result = FINITE.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_require_offered_arrivals_when_a_finite_window_completes() {
    // Arrange
    let mut stage = drained(FINITE);
    stage.harness_missed = 1_000;
    stage.offered += stage.harness_missed;
    // Act
    let result = FINITE.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_accept_finite_guard_drains_with_missed_arrivals() {
    // Arrange
    let mut stage = drained(FINITE);
    stage.termination = Termination::BacklogGuard;
    stage.harness_missed = 1_000;
    stage.offered += stage.harness_missed;
    // Act
    let result = FINITE.verify(&stage);
    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_unknown_enqueue_outcomes() {
    // Arrange
    let mut stage = drained(FULL);
    stage.sent += 1;
    stage.offered += 1;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_unacknowledged_accepted_messages() {
    // Arrange
    let mut stage = drained(FULL);
    stage.acknowledged -= 1;
    stage.accepted_unacknowledged = 1;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_nonzero_pending_counts_after_drain() {
    // Arrange
    let mut stage = drained(FULL);
    stage.accepted_unacknowledged = 1;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reconcile_offered_arrivals_with_dispatched_work() {
    // Arrange
    let mut stage = drained(FULL);
    stage.offered += 1;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_require_timing_evidence_for_every_acknowledged_message() {
    // Arrange
    let sample_kinds = 0..5;
    // Act
    let results: Vec<_> = sample_kinds
        .map(|index| {
            let mut stage = drained(FULL);
            stage.acknowledged_timing_samples[index] -= 1;
            FULL.verify(&stage)
        })
        .collect();
    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_require_the_minimum_observed_consumer_pause() {
    // Arrange
    let minimum = Some(MINIMUM_PAUSE_NS);
    let early = Some(MINIMUM_PAUSE_NS - 1);
    let pauses = [
        (None, minimum),
        (early, minimum),
        (minimum, None),
        (minimum, early),
    ];
    // Act
    let results = pauses.map(|(pause, worker_pause)| {
        let mut stage = drained(FULL);
        stage.minimum_pause_ns = pause;
        stage.minimum_worker_pause_ns = worker_pause;
        FULL.verify(&stage)
    });
    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_reject_an_incomplete_drain() {
    // Arrange
    let mut stage = drained(FULL);
    stage.drain.drained = false;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_require_empty_queue_verification() {
    // Arrange
    let mut stage = drained(FULL);
    stage.drain.empty_verified = false;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_client_cleanup_failure() {
    // Arrange
    let mut stage = drained(FULL);
    stage.drain.clients_closed = false;
    // Act
    let result = FULL.verify(&stage);
    // Assert
    assert!(result.is_err());
}
