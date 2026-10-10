use super::*;

fn report(finite: bool) -> Value {
    let accepted = if finite { 10000 } else { 7920 };
    let timing: Value = ["ack", "pause", "worker_pause", "handoff", "cycle"]
        .into_iter()
        .map(|name| {
            (
                name.to_owned(),
                json!({"distribution":{"count":accepted},"min_ns":5_000_000}),
            )
        })
        .collect();
    json!({"source_sha":"head","source_dirty":false,"comparison_label":"corrected_pacing",
        "pacing_implementation":"dedicated_sleep_worker_capacity_one_monotonic_deadline",
        "config":{"rates":[if finite { 16000 } else { 66 }],"stage_seconds":120,"producer_connections":256,"consumer_delay_ms":5,"max_backlog":10000,"max_attempts_per_stage":500_000,"drain_seconds":600},
        "status":"passed","cleanup_status":"completed","recovery_probe_passed":true,
        "stages":[{"drained":true,"empty_verified":true,"cleanup_failure":null,"termination":if finite { "backlog_safety_guard" } else { "configured_window" },"configured_window_completed":!finite,"offered":if finite { 30000 } else { 7920 },"harness_missed":0,"consumer_timing":timing,
            "accounting":{"sent":accepted,"accepted":accepted,"rejected":0,"acknowledged":accepted,"accepted_unacknowledged":0}}]})
}

fn rejects_config(key: &str, value: Value, finite: bool) {
    let mut report = report(finite);
    report["config"][key] = value;
    assert!(validate(&report, "head", 10000, finite).is_err());
}

#[test]
fn should_accept_a_complete_reconciled_full_window() {
    validate(&report(false), "head", 10000, false).unwrap();
}
#[test]
fn should_accept_the_original_finite_drain_envelope() {
    validate(&report(true), "head", 10000, true).unwrap();
}
#[test]
fn should_reject_weaker_consumer_pause() {
    rejects_config("consumer_delay_ms", json!(4), false);
}

#[test]
fn should_reject_observed_pauses_below_the_minimum() {
    // Arrange
    let reports = ["pause", "worker_pause"].map(|name| {
        let mut data = report(false);
        data["stages"][0]["consumer_timing"][name]["min_ns"] = json!(4_999_999);
        data
    });
    // Act
    let rejected = reports
        .iter()
        .all(|data| validate(data, "head", 10000, false).is_err());
    // Assert
    assert!(rejected);
}

#[test]
fn should_keep_full_window_arrivals_below_every_backlog_guard() {
    // Arrange
    let backlogs = BACKLOGS;
    // Act
    let rates = backlogs.map(offered_rate);
    // Assert
    assert_eq!(rates, [66, 166, 666]);
    assert!(backlogs
        .into_iter()
        .zip(rates)
        .all(|(backlog, rate)| rate * WINDOW_SECONDS < backlog));
}
#[test]
fn should_reject_a_longer_drain_deadline() {
    rejects_config("drain_seconds", json!(601), false);
}
#[test]
fn should_reject_fewer_producers() {
    rejects_config("producer_connections", json!(64), false);
}
#[test]
fn should_reject_a_shorter_window() {
    rejects_config("stage_seconds", json!(30), false);
}
#[test]
fn should_reject_a_changed_full_window_rate() {
    rejects_config("rates", json!([60]), false);
}
#[test]
fn should_reject_a_weaker_finite_load() {
    rejects_config("rates", json!([666]), true);
}
#[test]
fn should_reject_a_longer_finite_drain_deadline() {
    rejects_config("drain_seconds", json!(601), true);
}

fn rejects_field(key: &str, value: Value) {
    let mut data = report(false);
    data[key] = value;
    assert!(validate(&data, "head", 10000, false).is_err());
}
#[test]
fn should_require_recovery_success() {
    rejects_field("recovery_probe_passed", json!(false));
}
#[test]
fn should_require_completed_cleanup() {
    rejects_field("cleanup_status", json!("failed"));
}
#[test]
fn should_require_clean_source() {
    rejects_field("source_dirty", json!(true));
}
#[test]
fn should_require_exact_source() {
    rejects_field("source_sha", json!("other"));
}
#[test]
fn should_reject_obsolete_pacing() {
    rejects_field("comparison_label", json!("coarse_timer"));
}

fn rejects_stage(key: &str, value: Value, finite: bool) {
    let mut data = report(finite);
    data["stages"][0][key] = value;
    assert!(validate(&data, "head", 10000, finite).is_err());
}
#[test]
fn should_reject_full_window_backlog_guard() {
    rejects_stage("termination", json!("backlog_safety_guard"), false);
}
#[test]
fn should_reject_full_window_accounting_guard() {
    rejects_stage("termination", json!("accounting_safety_guard"), false);
}
#[test]
fn should_reject_full_window_broker_rejection() {
    rejects_stage("termination", json!("broker_enqueue_rejection"), false);
}
#[test]
fn should_reject_missed_full_window_arrivals() {
    rejects_stage("harness_missed", json!(80), false);
}
#[test]
fn should_reject_missed_rate_termination_in_finite_scope() {
    rejects_stage("termination", json!("offered_rate_not_met"), true);
}
#[test]
fn should_require_verified_empty_drain() {
    rejects_stage("empty_verified", json!(false), false);
}

fn rejects_count(key: &str, value: u64) {
    let mut data = report(false);
    data["stages"][0]["accounting"][key] = json!(value);
    assert!(validate(&data, "head", 10000, false).is_err());
}
#[test]
fn should_reject_unknown_enqueue_outcomes() {
    rejects_count("sent", 10002);
}
#[test]
fn should_reject_unacknowledged_work() {
    rejects_count("accepted_unacknowledged", 1);
}
#[test]
fn should_reject_inexact_ack_count() {
    rejects_count("acknowledged", 10000);
}

#[test]
fn should_reject_work_below_the_full_window_envelope() {
    // Arrange
    let mut data = report(false);
    let stage = &mut data["stages"][0];
    stage["accounting"] = json!({"sent":7920,"accepted":7840,"rejected":80,"acknowledged":7840,"accepted_unacknowledged":0});
    for timing in stage["consumer_timing"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        timing["distribution"]["count"] = json!(7840);
    }
    // Act
    let result = validate(&data, "head", 10000, false);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_work_below_the_finite_message_envelope() {
    // Arrange
    let mut data = report(true);
    let stage = &mut data["stages"][0];
    stage["accounting"] = json!({"sent":9999,"accepted":9999,"rejected":0,"acknowledged":9999,"accepted_unacknowledged":0});
    for timing in stage["consumer_timing"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        timing["distribution"]["count"] = json!(9999);
    }
    // Act
    let result = validate(&data, "head", 10000, true);
    // Assert
    assert!(result.is_err());
}

fn stage() -> Value {
    let histogram = |index: usize| {
        let mut bins = vec![0; 65];
        bins[index] = 100;
        json!({"bins":bins,"count":100,"max_ns":1_u64 << index})
    };
    json!({"accounting":{"accepted":60000},"active_elapsed_ns":120_000_000_000_u64,"drain_elapsed_ns":300_000_000_000_u64,
        "consumer_timing":{"ack":{"distribution":histogram(20)},"cycle":{"distribution":histogram(23)}}})
}

#[test]
fn should_extract_queue_comparison_metrics() {
    // Arrange
    let stage = stage();
    // Act
    let result = metrics(&stage).unwrap();
    // Assert
    assert_eq!(
        result,
        json!({"accepted_per_second":500.0,"drain_seconds":300.0,"ack_p99_ns":1_u64<<20,"cycle_p99_ns":1_u64<<23})
    );
}

fn rejects_metric(metric: &str, value: f64) {
    let before = metrics(&stage()).unwrap();
    let mut after = before.clone();
    after[metric] = json!(value);
    let failed: Vec<_> = compare(&before, &after)
        .unwrap()
        .into_iter()
        .filter(|row| row["passed"] == false)
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["metric"], metric);
}
#[test]
fn should_reject_queue_throughput_regression() {
    rejects_metric("accepted_per_second", 449.0);
}
#[test]
fn should_reject_queue_drain_regression() {
    rejects_metric("drain_seconds", 331.0);
}

#[test]
fn should_allow_only_one_second_of_near_empty_drain_jitter() {
    // Arrange
    let mut before = metrics(&stage()).unwrap();
    before["drain_seconds"] = json!(0.1);
    let mut after = before.clone();
    after["drain_seconds"] = json!(1.1);
    // Act
    let boundary = compare(&before, &after).unwrap();
    after["drain_seconds"] = json!(1.100_001);
    let exceeded = compare(&before, &after).unwrap();
    // Assert
    assert!(boundary.iter().all(|row| row["passed"] == true));
    assert!(exceeded
        .iter()
        .any(|row| row["metric"] == "drain_seconds" && row["passed"] == false));
}

#[test]
fn should_saturate_the_largest_latency_bucket_without_overflow() {
    // Arrange
    let mut bins = vec![0_u64; 65];
    bins[64] = u64::MAX;
    let distribution = json!({"bins":bins,"count":u64::MAX,"max_ns":u64::MAX});
    // Act
    let result = p99_upper_ns(&distribution).unwrap();
    // Assert
    assert_eq!(result, u64::MAX);
}
#[test]
fn should_reject_queue_ack_p99_regression() {
    rejects_metric("ack_p99_ns", 2_097_152.0);
}
#[test]
fn should_reject_queue_cycle_p99_regression() {
    rejects_metric("cycle_p99_ns", 16_777_216.0);
}

fn captures() -> Vec<Value> {
    capture_plan().into_iter().map(|(_,_,_,label)| json!({"label":label,"status":"drain_passed","metrics":metrics(&stage()).unwrap()})).collect()
}

#[test]
fn should_compare_all_six_pairs_across_both_scopes() {
    // Arrange
    let captures = captures();
    // Act
    let comparisons = comparisons(&captures).unwrap();
    // Assert
    assert_eq!(captures.len(), 12);
    assert!(accepted(&captures, &comparisons));
    assert_eq!(comparisons.as_object().unwrap().len(), 6);
}

fn rejects_scope(finite: bool) {
    let mut captures = captures();
    for capture in &mut captures {
        let label = capture["label"].as_str().unwrap();
        if label.starts_with("finite-") == finite && label.ends_with("after") {
            capture["metrics"]["drain_seconds"] = json!(331);
        }
    }
    assert!(!accepted(&captures, &comparisons(&captures).unwrap()));
}
#[test]
fn should_reject_a_full_window_pair_regression() {
    rejects_scope(false);
}
#[test]
fn should_reject_a_finite_pair_regression() {
    rejects_scope(true);
}

#[test]
fn should_reject_a_missing_comparable_capture() {
    // Arrange
    let mut captures = captures();
    captures[0]["metrics"] = Value::Null;
    // Act
    let comparisons = comparisons(&captures).unwrap();
    // Assert
    assert!(!accepted(&captures, &comparisons));
}

#[test]
fn should_reject_a_missing_required_capture() {
    // Arrange
    let mut captures = captures();
    captures.pop();
    // Act
    let result = comparisons(&captures);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_failed_captures_despite_comparable_metrics() {
    // Arrange
    let mut captures = captures();
    captures[0]["status"] = json!("failed");
    // Act
    let comparisons = comparisons(&captures).unwrap();
    // Assert
    assert!(!accepted(&captures, &comparisons));
}
