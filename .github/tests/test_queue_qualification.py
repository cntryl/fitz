import json
import importlib.util
from pathlib import Path
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

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

    def test_should_preserve_the_original_finite_drain_config_and_message_envelope(self):
        report = self.report()
        report["config"]["rates"] = [16000]
        stage = report["stages"][0]
        stage.update(termination="backlog_safety_guard", configured_window_completed=False,
                     offered=30000, harness_missed=19999)
        stage["accounting"].update(sent=10001, accepted=10001, acknowledged=10001)
        for timing in stage["consumer_timing"].values():
            timing["distribution"]["count"] = 10001
        queue.validate(report, "head", 10000, finite=True)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000)
        stage["accounting"].update(sent=9999, accepted=9999, acknowledged=9999)
        for timing in stage["consumer_timing"].values():
            timing["distribution"]["count"] = 9999
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000, finite=True)

    def test_should_reject_weaker_finite_drain_load_and_missed_rate_termination(self):
        report = self.report()
        report["config"]["rates"] = [16000]
        stage = report["stages"][0]
        stage["accounting"].update(sent=10000, accepted=10000, acknowledged=10000)
        for timing in stage["consumer_timing"].values():
            timing["distribution"]["count"] = 10000
        stage.update(termination="offered_rate_not_met", configured_window_completed=False)
        with self.assertRaises(AssertionError):
            queue.validate(report, "head", 10000, finite=True)
        stage["termination"] = "backlog_safety_guard"
        for key, value in (("rates", [666]), ("drain_seconds", 601), ("consumer_delay_ms", 4)):
            original = report["config"][key]
            report["config"][key] = value
            with self.assertRaises(AssertionError):
                queue.validate(report, "head", 10000, finite=True)
            report["config"][key] = original

    def test_should_require_both_finite_and_full_window_captures_for_every_envelope(self):
        campaign = queue.Campaign.__new__(queue.Campaign)
        campaign.heads = dict(before="before", after="after")
        campaign.binaries = dict(before=Path("before"), after=Path("after"))
        captures = []

        def capture(phase, backlog, label, command, *, finite=False):
            captures.append((phase, backlog, finite))
            return dict(status="drain_passed", termination="configured_window",
                        configured_window_completed=True, harness_miss_fraction=0, metrics=queue.metrics(self.stage()))

        campaign.capture = capture
        with TemporaryDirectory() as directory:
            campaign.output = Path(directory)
            with patch.dict(queue.os.environ, GITHUB_STEP_SUMMARY=str(Path(directory) / "summary")):
                campaign.measure()
        self.assertEqual(set(captures), {(phase, backlog, finite) for phase in ("before", "after")
                                        for backlog in (10000, 25000, 100000) for finite in (False, True)})
        self.assertEqual(len(captures), 12)

    def test_should_fail_when_either_required_capture_scope_fails(self):
        campaign = queue.Campaign.__new__(queue.Campaign)
        campaign.heads = dict(before="before", after="after")
        campaign.binaries = dict(before=Path("before"), after=Path("after"))
        for failed_scope in (False, True):
            def capture(phase, backlog, label, command, *, finite=False):
                return dict(status="failed" if finite == failed_scope else "drain_passed")

            campaign.capture = capture
            with TemporaryDirectory() as directory:
                campaign.output = Path(directory)
                with patch.dict(queue.os.environ, GITHUB_STEP_SUMMARY=str(Path(directory) / "summary")):
                    with self.assertRaises(AssertionError):
                        campaign.measure()

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
    def comparison_campaign(self):
        campaign = queue.Campaign.__new__(queue.Campaign)
        campaign.heads = dict(before="before", after="after")
        campaign.binaries = dict(before=Path("before"), after=Path("after"))
        return campaign

    def test_should_compare_every_pair_in_both_required_scopes(self):
        campaign = self.comparison_campaign()
        campaign.capture = lambda *args, **kwargs: dict(status="drain_passed", metrics=queue.metrics(self.stage()))
        with TemporaryDirectory() as directory:
            campaign.output = Path(directory)
            with patch.dict(queue.os.environ, GITHUB_STEP_SUMMARY=str(Path(directory) / "summary")):
                campaign.measure()
            comparisons = json.loads((Path(directory) / "comparison.json").read_text())
        self.assertEqual(set(comparisons), {f"{scope}pressure-{backlog}" for scope in ("", "finite-")
                                          for backlog in (10000, 25000, 100000)})
        self.assertTrue(all(len(rows) == 4 and all(row["passed"] for row in rows)
                            for rows in comparisons.values()))

    def test_should_fail_a_regression_in_either_scope(self):
        campaign = self.comparison_campaign()
        for regressed_scope in (False, True):
            def capture(phase, backlog, label, command, *, finite=False):
                value = queue.metrics(self.stage())
                if phase == "after" and finite == regressed_scope:
                    value["drain_seconds"] = 331
                return dict(status="drain_passed", metrics=value)

            campaign.capture = capture
            with TemporaryDirectory() as directory:
                campaign.output = Path(directory)
                with patch.dict(queue.os.environ, GITHUB_STEP_SUMMARY=str(Path(directory) / "summary")):
                    with self.assertRaises(AssertionError):
                        campaign.measure()

    def test_should_fail_when_a_passed_capture_has_no_comparable_metrics(self):
        campaign = self.comparison_campaign()
        campaign.capture = lambda *args, **kwargs: dict(status="drain_passed")
        with TemporaryDirectory() as directory:
            campaign.output = Path(directory)
            with patch.dict(queue.os.environ, GITHUB_STEP_SUMMARY=str(Path(directory) / "summary")):
                with self.assertRaises(AssertionError):
                    campaign.measure()


if __name__ == "__main__":
    unittest.main()
