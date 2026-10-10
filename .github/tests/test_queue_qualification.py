import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("queue_qualification", Path(__file__).parents[1] / "queue_qualification.py")
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)


class QueueQualificationTests(unittest.TestCase):
    def report(self):
        return {
            "source_sha": "head", "source_dirty": False, "comparison_label": "corrected_pacing",
            "pacing_implementation": "dedicated_sleep_worker_capacity_one_monotonic_deadline",
            "config": dict(rates=[16000], stage_seconds=120, producer_connections=256,
                           consumer_delay_ms=5, max_backlog=10000, max_attempts_per_stage=500000, drain_seconds=600),
            "status": "passed", "cleanup_status": "completed", "recovery_probe_passed": True,
            "stages": [{"drained": True, "empty_verified": True, "cleanup_failure": None,
                        "termination": "backlog_safety_guard", "configured_window_completed": False,
                        "offered": 10001, "harness_missed": 0,
                        "consumer_timing": {name: {"distribution": {"count": 10001}, "min_ns": 5000000}
                                            for name in ("ack", "pause", "worker_pause", "handoff", "cycle")},
                        "accounting": dict(sent=10001, accepted=10001, rejected=0,
                                           acknowledged=10001, accepted_unacknowledged=0)}],
        }

    def test_should_accept_complete_reconciled_corrected_pacing_drain(self):
        queue.validate(self.report(), "head", 10000)

    def test_should_reject_early_termination_below_the_intended_message_envelope(self):
        report = self.report()
        report["stages"][0]["accounting"].update(sent=9999, accepted=9999, acknowledged=9999)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)

    def test_should_reject_weaker_pause_deadline_or_offered_load(self):
        for key, value in (("consumer_delay_ms", 4), ("drain_seconds", 601), ("producer_connections", 64),
                           ("stage_seconds", 30), ("rates", [8000])):
            report = self.report()
            report["config"][key] = value
            with self.assertRaises(AssertionError):
                queue.validate(report, "head", 10000)

    def test_should_reject_remaining_unknown_or_unverified_accepted_work(self):
        for key, value in (("sent", 10002), ("acknowledged", 10000), ("accepted_unacknowledged", 1)):
            report = self.report()
            report["stages"][0]["accounting"][key] = value
            with self.assertRaises(AssertionError):
                queue.validate(report, "head", 10000)

    def test_should_require_recovery_cleanup_and_exact_clean_source(self):
        for key, value in (("recovery_probe_passed", False), ("cleanup_status", "failed"),
                           ("source_dirty", True), ("source_sha", "other"), ("comparison_label", "coarse_timer")):
            report = self.report()
            report[key] = value
            with self.assertRaises(AssertionError):
                queue.validate(report, "head", 10000)

    def test_should_reject_any_observed_pause_below_the_minimum(self):
        for name in ("pause", "worker_pause"):
            report = self.report()
            report["stages"][0]["consumer_timing"][name]["min_ns"] = 4999999
            with self.assertRaises(AssertionError):
                queue.validate(report, "head", 10000)

    def test_should_accept_a_complete_guard_drain_without_qualifying_its_arrival_rate(self):
        report = self.report()
        report["stages"][0].update(offered=30000, harness_missed=19999)
        queue.validate(report, "head", 10000)
        self.assertFalse(report["stages"][0]["configured_window_completed"])

    def test_should_reject_a_full_window_that_misses_the_offered_envelope(self):
        report = self.report()
        report["stages"][0].update(termination="configured_window", configured_window_completed=True,
                                   offered=10103, harness_missed=102)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)

    def test_should_reject_the_harness_missed_offered_rate_termination(self):
        report = self.report()
        report["stages"][0].update(termination="offered_rate_not_met", configured_window_completed=False,
                                   offered=10103, harness_missed=102)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)

    def stage(self, drain_ns=300_000_000_000, ack_bin=20, cycle_bin=23):
        def histogram(index):
            bins = [0] * 65
            bins[index] = 100
            return {"bins": bins, "count": 100, "max_ns": 1 << index}
        return {"accounting": {"accepted": 60000}, "active_elapsed_ns": 120_000_000_000,
                "drain_elapsed_ns": drain_ns,
                "consumer_timing": {"ack": {"distribution": histogram(ack_bin)},
                                    "cycle": {"distribution": histogram(cycle_bin)}}}

    def test_should_extract_throughput_drain_time_and_p99_upper_bounds(self):
        metrics = queue.metrics(self.stage())
        self.assertEqual(metrics, {"accepted_per_second": 500.0, "drain_seconds": 300.0,
                                   "ack_p99_ns": 1 << 20, "cycle_p99_ns": 1 << 23})

    def test_should_pass_an_after_capture_equal_to_its_before_capture(self):
        rows = queue.compare(queue.metrics(self.stage()), queue.metrics(self.stage()))
        self.assertEqual([row["metric"] for row in rows],
                         ["accepted_per_second", "drain_seconds", "ack_p99_ns", "cycle_p99_ns"])
        self.assertTrue(all(row["passed"] for row in rows))

    def test_should_fail_each_metric_when_after_regresses_beyond_its_ratio(self):
        before = queue.metrics(self.stage())
        for metric, value in (("accepted_per_second", 449.0), ("drain_seconds", 331.0),
                              ("ack_p99_ns", 1 << 21), ("cycle_p99_ns", 1 << 24)):
            after = dict(before, **{metric: value})
            rows = {row["metric"]: row["passed"] for row in queue.compare(before, after)}
            self.assertEqual([name for name, passed in rows.items() if not passed], [metric])

    def test_should_pass_regressions_within_the_perf_loop_ratios(self):
        before = queue.metrics(self.stage())
        after = dict(before, accepted_per_second=450.0, drain_seconds=330.0)
        self.assertTrue(all(row["passed"] for row in queue.compare(before, after)))

    def test_should_tolerate_one_second_of_jitter_on_a_near_empty_drain(self):
        before = queue.metrics(self.stage(drain_ns=20_000_000))
        rows = {row["metric"]: row["passed"] for row in queue.compare(before, dict(before, drain_seconds=1.02))}
        self.assertTrue(rows["drain_seconds"])
        rows = {row["metric"]: row["passed"] for row in queue.compare(before, dict(before, drain_seconds=1.03))}
        self.assertFalse(rows["drain_seconds"])


if __name__ == "__main__":
    unittest.main()
