import importlib.util
from pathlib import Path
import re
import unittest

spec = importlib.util.spec_from_file_location(
    "stream_performance", Path(__file__).parents[1] / "stream_performance.py"
)
perf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf)


class StreamPerformanceTests(unittest.TestCase):
    def test_should_pair_every_transport_workload_separately(self):
        selectors = [pattern for target, pattern in perf.GROUPS if target == "tier4_stream_gate"]
        self.assertEqual(len(selectors), 8)
        self.assertEqual(len(set(selectors)), 8)
        self.assertTrue(all(pattern.startswith("should_measure_") for pattern in selectors))
        source = (Path(__file__).parents[2] / "benches/tier4_stream_gate.rs").read_text()
        self.assertEqual(set(selectors), set(re.findall(r"fn (should_\w+)\(", source)))

    def test_should_alternate_source_and_control_order(self):
        self.assertEqual(perf.order(1, "left", "right"), ("left", "right"))
        self.assertEqual(perf.order(2, "left", "right"), ("right", "left"))
        self.assertEqual(perf.order(3, "before", "after"), ("before", "after"))

    def test_should_reject_either_unstable_control_even_when_all_budgets_pass(self):
        for name in ("max_event", "ws_exact_replay"):
            controls = {key: {"stable_within_five_percent": key != name} for key in perf.CONTROLS}
            self.assertEqual(perf.verdict({"gate": True}, controls), "measurement_unstable")

    def test_should_keep_stable_budget_failure_distinct_from_unstable_timing(self):
        controls = {key: {"stable_within_five_percent": True} for key in perf.CONTROLS}
        self.assertEqual(perf.verdict({"gate": False}, controls), "budget_failure")
        self.assertEqual(perf.verdict({"gate": True}, controls), "passed")

    def test_should_check_both_throughput_and_p95_control_bounds(self):
        for throughput, p95, expected in ((1, 1, True), (.94, 1, False), (1, 1.06, False)):
            rows = {
                side: [
                    {"name": name, "stats": {"mean": value}}
                    for _ in range(3)
                    for name, value in (("row", factor), ("row_latency", latency))
                ]
                for side, factor, latency in (("left", 1, 1), ("right", throughput, p95))
            }
            self.assertEqual(perf.control_result(rows, "row", "hash")["stable_within_five_percent"], expected)

    def test_should_reject_missing_control_captures(self):
        with self.assertRaises(AssertionError):
            perf.control_result({"left": [], "right": []}, "row", "hash")

    def test_should_preserve_every_original_budget(self):
        names = [pattern.removeprefix("should_measure_") for target, pattern in perf.GROUPS if target == "tier4_stream_gate"]
        names += ["disk_sync_write_max_event", "hot_resource_append_depth_100000", "disk_sync_write_64b"]
        names += ["characterization_" + str(i) for i in range(32 - len(names))]
        summaries = {
            phase: {name + suffix: [{"stats": {"mean": 100}, "quality": "noisy"}] * 3
                    for name in names for suffix in ("", "_latency")}
            for phase in ("before", "after")
        }
        records, checks = perf.compare(summaries)
        self.assertEqual(len(records), 32)
        self.assertEqual(len(checks), 8)
        self.assertTrue(all(checks.values()))
        self.assertTrue(all(row["before_quality"] == "noisy,noisy,noisy" for row in records))
        for row_name, suffix, value, check in (
            ("memory_ws_exact_replay", "", 89, "memory_ws_exact_replay"),
            ("memory_ws_exact_replay", "_latency", 111, "memory_ws_exact_replay"),
            ("disk_sync_write_max_event", "", 94, "maximum_valid_event"),
            ("disk_sync_write_max_event", "_latency", 106, "maximum_valid_event"),
            ("hot_resource_append_depth_100000", "", 89, "100k_vs_empty"),
            ("hot_resource_append_depth_100000", "_latency", 111, "100k_vs_empty"),
        ):
            key = row_name + suffix
            original = summaries["after"][key]
            summaries["after"][key] = [{"stats": {"mean": value}, "quality": "noisy"}] * 3
            self.assertFalse(perf.compare(summaries)[1][check])
            summaries["after"][key] = original


WORKFLOW = Path(__file__).parents[1] / "workflows/stream-perf.yml"
# Files that only rerun the acceptance tests; they are not comparison inputs.
TRIGGER_ONLY = {
    ".github/workflows/stream-perf.yml",
    ".github/stream_performance.py",
    ".github/tests/test_stream_performance.py",
}


def comparison_paths():
    text = WORKFLOW.read_text()
    block = re.search(r"\n    paths:\n((?:      - .+\n)+)", text).group(1)
    triggers = {line.split("- ", 1)[1].removesuffix("/**") for line in block.splitlines()}
    skip = re.search(r'git diff --quiet "\$BASE_SHA" HEAD -- (.+?);', text, re.S).group(1)
    return triggers - TRIGGER_ONLY, {spec.strip("'") for spec in skip.split() if spec != "\\"}


class StreamPerformanceWorkflowTests(unittest.TestCase):
    def test_should_keep_the_path_filter_and_in_job_skip_identical(self):
        triggers, skip = comparison_paths()
        self.assertEqual(triggers, skip)

    def test_should_compare_when_shared_stream_dependencies_change(self):
        triggers, skip = comparison_paths()
        shared = {
            "src/api", "src/dispatch", "src/runtime", "src/session",
            "src/snapshot", "src/snapshot.rs", "src/storage", "src/storage.rs",
            "src/testkit",
        }
        self.assertLessEqual(shared, triggers)
        self.assertLessEqual(shared, skip)


if __name__ == "__main__":
    unittest.main()
