use super::*;

#[test]
fn should_bound_full_connect_failure_diagnostics_per_window() {
    // Arrange
    // Full CONNECT-failure diagnostics are large (a SHA-256, a JSON
    // parse of the attacker-supplied payload, and up to six bounded claim
    // views) and are emitted for entirely unauthenticated peers, so an
    // attacker looping CONNECT must not be able to drive unbounded
    // ERROR-level log volume.
    let budget =
        crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsBudget::default();

    // Act
    let first_window = (0..50).map(|_| budget.acquire(1_000)).collect::<Vec<_>>();
    let next_window = budget.acquire(2_000);

    // Assert
    let full_count = first_window
        .iter()
        .filter(|grant| {
            matches!(
                grant,
                crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Full
            )
        })
        .count();
    assert!(
        (1..=5).contains(&full_count),
        "expected between 1 and 5 full diagnostics per window, got {full_count}"
    );
    assert!(
        matches!(
            first_window.last(),
            Some(crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Suppressed { .. })
        ),
        "failures past the window budget must be suppressed"
    );
    assert!(
        matches!(
            next_window,
            crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Full
        ),
        "a new window must allow full diagnostics again"
    );
    // The suppressed failures are still accounted for, not silently dropped.
    let suppressed = first_window
        .iter()
        .filter(|grant| matches!(grant, crate::api::runtime_ingress::session_authenticator::ConnectDiagnosticsGrant::Suppressed { .. }))
        .count();
    assert_eq!(full_count + suppressed, 50);
}

#[test]
fn should_hold_connect_diagnostics_bound_under_concurrent_window_rollover() {
    // Arrange
    // An attacker chooses the concurrency, so the per-window bound
    // must hold when many CONNECT failures land on a window boundary at once
    // - not just when they arrive one at a time.
    use crate::api::runtime_ingress::session_authenticator::{
        ConnectDiagnosticsBudget, ConnectDiagnosticsGrant,
    };

    for round in 0..200_u64 {
        let budget = Arc::new(ConnectDiagnosticsBudget::default());
        let full_grants = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(16));
        // Every thread sees the same stale-window boundary instant.
        let now_millis = 5_000 + round;

        // Act
        let handles = (0..16)
            .map(|_| {
                let budget = budget.clone();
                let full_grants = full_grants.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    if matches!(budget.acquire(now_millis), ConnectDiagnosticsGrant::Full) {
                        full_grants.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let granted = full_grants.load(Ordering::Relaxed);
        assert!(
            granted <= 5,
            "round {round}: {granted} full diagnostics granted in one window, bound is 5"
        );
    }
}
