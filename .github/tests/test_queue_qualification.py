import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("queue_qualification", Path(__file__).parents[1] / "queue_qualification.py")
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)


class QueueQualificationTests(unittest.TestCase):
    def report(self):
        # A 10k-guard capture offers 66/s for the full 120-second window: 7,920 arrivals.
        return {
            "source_sha": "head", "source_dirty": False, "comparison_label": "corrected_pacing",
            "pacing_implementation": "dedicated_sleep_worker_capacity_one_monotonic_deadline",
            "config": dict(rates=[66], stage_seconds=120, producer_connections=256,
                           consumer_delay_ms=5, max_backlog=10000, max_attempts_per_stage=500000, drain_seconds=600),
            "status": "passed", "cleanup_status": "completed", "recovery_probe_passed": True,
            "stages": [{"drained": True, "empty_verified": True, "cleanup_failure": None,
                        "termination": "configured_window", "configured_window_completed": True,
                        "offered": 7920, "harness_missed": 0,
                        "consumer_timing": {name: {"distribution": {"count": 7920}, "min_ns": 5000000}
                                            for name in ("ack", "pause", "worker_pause", "handoff", "cycle")},
                        "accounting": dict(sent=7920, accepted=7920, rejected=0,
                                           acknowledged=7920, accepted_unacknowledged=0)}],
        }

    def test_should_accept_a_complete_reconciled_full_window_drain(self):
        queue.validate(self.report(), "head", 10000)

    def test_should_reject_accepted_work_below_the_configured_window_envelope(self):
        report = self.report()
        stage = report["stages"][0]
        stage["accounting"].update(accepted=7840, rejected=80, acknowledged=7840)
        for timing in stage["consumer_timing"].values():
            timing["distribution"]["count"] = 7840
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)

    def test_should_reject_weaker_pause_deadline_or_offered_load(self):
        for key, value in (("consumer_delay_ms", 4), ("drain_seconds", 601), ("producer_connections", 64),
                           ("stage_seconds", 30), ("rates", [16000]), ("rates", [60])):
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

    def test_should_reject_a_guard_drain_that_missed_most_offered_arrivals(self):
        report = self.report()
        report["stages"][0].update(termination="backlog_safety_guard", configured_window_completed=False,
                                   offered=30000, harness_missed=19999)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)

    def test_should_reject_every_guard_stopped_drain_even_without_missed_arrivals(self):
        for termination in ("backlog_safety_guard", "accounting_safety_guard", "broker_enqueue_rejection"):
            report = self.report()
            report["stages"][0].update(termination=termination, configured_window_completed=False)
            with self.assertRaises(AssertionError):
                queue.validate(report, "head", 10000)

    def test_should_offer_less_than_each_guard_so_a_stalled_consumer_cannot_stop_the_window(self):
        for backlog, rate in ((10000, 66), (25000, 166), (100000, 666)):
            self.assertEqual(queue.offered_rate(backlog), rate)
            self.assertLessEqual(rate * 120 * 5, backlog * 4)

    def test_should_reject_a_full_window_that_misses_the_offered_envelope(self):
        report = self.report()
        report["stages"][0].update(termination="configured_window", configured_window_completed=True,
                                   offered=7920, harness_missed=80)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)

    def test_should_reject_the_harness_missed_offered_rate_termination(self):
        report = self.report()
        report["stages"][0].update(termination="offered_rate_not_met", configured_window_completed=False,
                                   offered=7920, harness_missed=80)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)


if __name__ == "__main__":
    unittest.main()
