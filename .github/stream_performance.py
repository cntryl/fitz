"""Same-host Stream qualification. Diagnostic builds finish before either full run."""

import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time


GROUPS = [
    ("tier4_stream_gate", "should_measure_" + row)
    for row in (
        "memory_tcp_append_open_session", "memory_ws_append_open_session",
        "memory_tcp_sync_write_lifecycle", "memory_ws_sync_write_lifecycle",
        "memory_tcp_exact_replay", "memory_ws_exact_replay",
        "local_disk_tcp_sync_write_lifecycle", "local_disk_ws_sync_write_lifecycle",
    )
] + [
    ("tier4_stream_shapes", "should_replay_"),
    ("tier4_stream_compacted", None),
    *[("tier4_stream_shapes", "should_characterize_disk_sync_write_" + size)
      for size in ("64b", "1k", "15k", "16k", "17k", "max_event")],
    ("tier4_stream_shapes", "should_characterize_hot_resource_append_after_100000_events"),
]
CONTROLS = {
    "ws_exact_replay": ("tier4_stream_gate", "should_measure_memory_ws_exact_replay", "memory_ws_exact_replay"),
    "max_event": ("tier4_stream_shapes", "should_characterize_disk_sync_write_max_event", "disk_sync_write_max_event"),
}


def order(iteration, first, second):
    return (first, second) if iteration % 2 else (second, first)


def measurement_groups():
    # History scaling compares two candidate shapes, so keep their captures
    # adjacent rather than allowing unrelated disk-size trials between them.
    empty = ("tier4_stream_shapes", "should_characterize_disk_sync_write_64b")
    hot = ("tier4_stream_shapes", "should_characterize_hot_resource_append_after_100000_events")
    indexed = list(enumerate(GROUPS))
    history = next(item for item in indexed if item[1] == hot)
    return [(item, history) if item[1] == empty else (item,)
            for item in indexed if item[1] != hot]


def mean_median(rows):
    assert len(rows) == 3, "Every row requires three captures"
    return statistics.median(row["stats"]["mean"] for row in rows)


def control_result(rows, name, binary_hash):
    value = lambda side, row_name: mean_median([row for row in rows[side] if row["name"] == row_name])
    result = {
        "binary_sha256": binary_hash,
        "throughput_ratio": value("right", name) / value("left", name),
        "p95_ratio": value("right", name + "_latency") / value("left", name + "_latency"),
    }
    result["stable_within_five_percent"] = all(
        .95 <= result[key] <= 1.05 for key in ("throughput_ratio", "p95_ratio")
    )
    return result


def verdict(checks, controls):
    assert set(controls) == set(CONTROLS), "Both timing controls are mandatory"
    if not all(control["stable_within_five_percent"] for control in controls.values()):
        return "measurement_unstable"
    return "passed" if all(checks.values()) else "budget_failure"


def compare(summaries):
    assert summaries["before"].keys() == summaries["after"].keys()
    records = []
    for name, before in sorted(summaries["before"].items()):
        if name.endswith("_latency"):
            continue
        after = summaries["after"][name]
        bt, at = mean_median(before), mean_median(after)
        bp = mean_median(summaries["before"][name + "_latency"])
        ap = mean_median(summaries["after"][name + "_latency"])
        records.append({
            "name": name, "before_ops_s": bt, "after_ops_s": at,
            "throughput_ratio": at / bt, "before_p95_us": bp / 1000,
            "after_p95_us": ap / 1000, "p95_ratio": ap / bp,
            "before_quality": ",".join(row["quality"] for row in before),
            "after_quality": ",".join(row["quality"] for row in after),
        })
    rows = {row["name"]: row for row in records}
    checks = {row["name"]: row["throughput_ratio"] >= .90 and row["p95_ratio"] <= 1.10
              for row in records if row["name"].startswith("memory_")}
    maximum = rows["disk_sync_write_max_event"]
    checks["maximum_valid_event"] = maximum["throughput_ratio"] >= .95 and maximum["p95_ratio"] <= 1.05
    hot, empty = rows["hot_resource_append_depth_100000"], rows["disk_sync_write_64b"]
    checks["100k_vs_empty"] = hot["after_ops_s"] >= .90 * empty["after_ops_s"] and hot["after_p95_us"] <= 1.10 * empty["after_p95_us"]
    assert len(records) == 32 and len(checks) == 8
    return records, checks


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, data):
    path.write_text(json.dumps(data, indent=2))


def host_snapshot():
    # Untimed snapshots preserve CPU/I/O context without polling measured work.
    paths = ("/proc/stat", "/proc/loadavg", "/proc/diskstats", "/proc/pressure/cpu", "/proc/pressure/io")
    return {path: Path(path).read_text() if Path(path).exists() else None for path in paths}


class Campaign:
    def __init__(self):
        self.root = Path.cwd()
        self.output = Path(os.environ["STREAM_OUTPUT"])
        self.output.mkdir(parents=True)
        self.sources = {"before": Path(os.environ["STREAM_BASELINE"]), "after": self.root}
        self.env = dict(os.environ, CARGO_TARGET_DIR=str(self.root / "target"))
        self.binaries = {}
        self.heads = {}

    def build(self):
        provenance = {}
        for phase, source in self.sources.items():
            assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True)
            self.heads[phase] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
            metadata = json.loads(subprocess.check_output(
                ["cargo", "metadata", "--locked", "--format-version", "1", "--features", "benchkit"], cwd=source, env=self.env
            ))
            save(self.output / (phase + "-cargo-metadata.json"), metadata)
            provenance[phase] = {
                "head": self.heads[phase], "lock_sha256": sha256(source / "Cargo.lock"),
                "dependencies": sorted((p["name"], p["version"], p["source"]) for p in metadata["packages"] if p["name"] != "fitz"),
                "fixtures": {str(p.relative_to(source)): sha256(p) for p in sorted((source / "benches").rglob("*.rs"))},
            }
        assert provenance["before"]["lock_sha256"] == provenance["after"]["lock_sha256"]
        assert provenance["before"]["dependencies"] == provenance["after"]["dependencies"]
        assert provenance["before"]["fixtures"] == provenance["after"]["fixtures"]
        provenance["production_baseline"] = os.environ["STREAM_BASE_PRODUCTION"]
        provenance["rustc"] = subprocess.check_output(["rustc", "-Vv"], text=True)
        provenance["planned_runs"] = ["qualification", "confirmation"]
        provenance["measurement_groups"] = measurement_groups()
        save(self.output / "provenance.json", provenance)
        for phase, source in self.sources.items():
            # Cargo freshness can alias packages across worktree paths. Rebuild Fitz
            # for each source, retaining dependencies and diagnostic build captures.
            subprocess.run(["cargo", "clean", "--release", "--package", "fitz"], cwd=source, env=self.env, check=True)
            for target in dict.fromkeys(target for target, _ in GROUPS):
                command = ["cargo", "bench", "--locked", "--features", "benchkit", "--bench", target, "--"]
                if target == "tier4_stream_shapes":
                    command += ["--workload", "should_replay_"]
                self.execute(phase, target, "build-" + phase + "-" + target, command, build=True)
                executables = [p for p in (self.root / "target" / "release" / "deps").glob(target + "-*") if p.is_file() and os.access(p, os.X_OK)]
                assert len(executables) == 1, executables
                archive = self.output / "binaries" / phase
                archive.mkdir(parents=True, exist_ok=True)
                self.binaries[(phase, target)] = archive / executables[0].name
                shutil.copy2(executables[0], self.binaries[(phase, target)])
        hashes = {phase: {target: sha256(self.binaries[(phase, target)]) for target, _ in GROUPS} for phase in self.sources}
        save(self.output / "binary-hashes.json", hashes)
        for target, _ in GROUPS:
            assert hashes["before"][target] != hashes["after"][target], "Source variants reused the same binary: " + target

    def execute(self, phase, target, label, command, build=False):
        source = self.sources[phase]
        destination = self.output / label
        destination.mkdir(parents=True)
        preparation_started = time.monotonic()
        if not build:
            os.sync()
        metadata = {
            "host_sync_seconds": time.monotonic() - preparation_started,
            "host_before": host_snapshot(), "head": self.heads[phase],
            "production_baseline": os.environ["STREAM_BASE_PRODUCTION"],
            "command": command, "cwd": str(source),
            "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        if not build:
            metadata["binary_sha256"] = sha256(Path(command[0]))
        save(destination / "capture.json", metadata)
        with (destination / "run.log").open("w") as log:
            result = subprocess.run(command, cwd=source, env=self.env, stdout=log, stderr=subprocess.STDOUT)
        metadata.update(exit_code=result.returncode, host_after=host_snapshot(),
                        finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        save(destination / "capture.json", metadata)
        if result.returncode:
            raise RuntimeError("Failed benchmark: " + str(destination))
        if build:
            artifact = source / "target" / "stress" / target.replace("_", "-") / "latest.json"
            shutil.copy2(artifact, destination / "initial.json")
            return None
        artifacts = list((destination / "stress").glob("*/latest.json"))
        assert len(artifacts) == 1, artifacts
        run = json.loads(artifacts[0].read_text())
        assert run["environment"]["git_commit"] == self.heads[phase]
        profile = run["environment"]["profile_config"]
        assert (profile["profile"], profile["warmup_samples"], profile["measured_samples"], profile["sample_duration"]) == ("default", 1, 5, 500000000)
        assert all(row["correctness"]["passed"] for row in run["summaries"])
        return run["summaries"]

    def capture(self, run_name, phase, target, pattern, label):
        relative = run_name + "/" + label
        command = [str(self.binaries[(phase, target)]), "--bench", "--output-dir", str(self.output / relative / "stress")]
        if pattern:
            command += ["--workload", pattern]
        print(relative + " " + target + " " + str(pattern), flush=True)
        return self.execute(phase, target, relative, command)

    def measure(self, run_name):
        summaries = {"before": {}, "after": {}}
        control_rows = {name: {"left": [], "right": []} for name in CONTROLS}
        for group in measurement_groups():
            for iteration in range(1, 4):
                # Alternate the history/empty order as well as source order.
                ordered = group if iteration % 2 else tuple(reversed(group))
                for group_index, (target, pattern) in ordered:
                    matching = [(name, row) for name, (ct, cp, row) in CONTROLS.items()
                                if (target, pattern) == (ct, cp)]
                    for phase in order(iteration, "before", "after"):
                        rows = self.capture(run_name, phase, target, pattern, f"pair-{group_index}-{iteration}-{phase}")
                        for row in rows:
                            summaries[phase].setdefault(row["name"], []).append(row)
                    if matching:
                        for side in order(iteration, "left", "right"):
                            control_rows[matching[0][0]][side].extend(self.capture(
                                run_name, "before", target, pattern,
                                f"control-{matching[0][0]}-{iteration}-{side}"))
        controls = {name: control_result(control_rows[name], row,
                                         sha256(self.binaries[("before", target)]))
                    for name, (target, _, row) in CONTROLS.items()}
        records, checks = compare(summaries)
        destination = self.output / run_name
        with (destination / "comparison.csv").open("w") as file:
            writer = csv.DictWriter(file, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        save(destination / "budgets.json", checks)
        save(destination / "control.json", controls)
        result = verdict(checks, controls)
        save(destination / "result.json", {"status": result, "head": self.heads["after"]})
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as file:
            file.write(f"\n{run_name}: **{result}**. Three-run medians, default profile, one host; p95 uses the median run mean of per-sample p95.\n\n")
            file.write("| Row | Throughput ratio | p95 ratio |\n|---|---:|---:|\n")
            for row in records:
                file.write(f"| {row['name']} | {row['throughput_ratio']:.3f} | {row['p95_ratio']:.3f} |\n")
            file.write("\nBudget observations: " + json.dumps(checks) + "\n")
            file.write("\nUnchanged-binary controls: " + json.dumps(controls) + "\n")
        return result


def main():
    campaign = Campaign()
    campaign.build()
    # Predeclared confirmation runs even when the first set has failed budgets
    # or unstable controls. There is no retry-until-green loop.
    results = {name: campaign.measure(name) for name in ("qualification", "confirmation")}
    save(campaign.output / "acceptance.json", results)
    assert all(result == "passed" for result in results.values()), results


if __name__ == "__main__":
    main()
