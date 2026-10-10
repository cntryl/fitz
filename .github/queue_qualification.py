"""Matched Queue campaigns with separate full-window and unchanged finite-drain gates."""

import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

WINDOW_SECONDS = 120


def offered_rate(backlog):
    """Offer 80% of the backlog guard over the window, so even a stalled consumer cannot trip it."""
    return backlog * 4 // (5 * WINDOW_SECONDS)


def validate(report, head, backlog, *, finite=False):
    rate = 16000 if finite else offered_rate(backlog)
    assert report["source_sha"] == head and report["source_dirty"] is False
    assert report["comparison_label"] == "corrected_pacing"
    assert report["pacing_implementation"] == "dedicated_sleep_worker_capacity_one_monotonic_deadline"
    assert report["config"] == dict(rates=[rate], stage_seconds=WINDOW_SECONDS, producer_connections=256,
                                    consumer_delay_ms=5, max_backlog=backlog, max_attempts_per_stage=500000,
                                    drain_seconds=600)
    assert report["status"] == "passed" and report["cleanup_status"] == "completed"
    assert report["recovery_probe_passed"] is True
    assert len(report["stages"]) == 1
    stage = report["stages"][0]
    assert stage["drained"] and stage["empty_verified"] and stage["cleanup_failure"] is None
    counts = stage["accounting"]
    if finite:
        # Preserve #404's original load and accepted-message drain envelope.
        # A guard drain establishes neither the offered rate nor a full window.
        assert stage["termination"] in ("configured_window", "backlog_safety_guard",
                                         "accounting_safety_guard", "broker_enqueue_rejection")
        assert counts["accepted"] >= backlog, "Early stop below intended message envelope"
        if stage["termination"] == "configured_window":
            assert stage["harness_missed"] * 100 <= stage["offered"], "Offered arrivals were not met"
    else:
        assert stage["termination"] == "configured_window" and stage["configured_window_completed"] is True, \
            "Only a completed configured window qualifies"
        assert stage["harness_missed"] * 100 <= stage["offered"], "Offered arrivals were not met"
        assert counts["accepted"] * 100 >= rate * WINDOW_SECONDS * 99, "Accepted work below the configured window envelope"
    assert counts["accepted"] == counts["acknowledged"] and counts["accepted_unacknowledged"] == 0
    assert counts["sent"] == counts["accepted"] + counts["rejected"], "Unknown ENQUEUE outcomes"
    timings = stage["consumer_timing"]
    for name in ("ack", "pause", "worker_pause", "handoff", "cycle"):
        assert timings[name]["distribution"]["count"] == counts["acknowledged"], name
    assert timings["pause"]["min_ns"] >= 5000000 and timings["worker_pause"]["min_ns"] >= 5000000


def save(path, data):
    path.write_text(json.dumps(data, indent=2))


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def host():
    paths = ("/proc/stat", "/proc/meminfo", "/proc/vmstat", "/proc/diskstats", "/proc/pressure/cpu", "/proc/pressure/io")
    return {path: Path(path).read_text() if Path(path).exists() else None for path in paths}


class Campaign:
    def __init__(self):
        self.root = Path.cwd()
        self.output = Path(os.environ["QUEUE_OUTPUT"])
        self.output.mkdir(parents=True)
        self.sources = {"before": Path(os.environ["QUEUE_BASELINE"]), "after": self.root}
        self.env = dict(os.environ, CARGO_TARGET_DIR=str(self.root / "target"),
                        STRESS_SUITE="queue-pressure",
                        FITZ_QUEUE_PRESSURE_STAGE_SECS=str(WINDOW_SECONDS),
                        FITZ_QUEUE_PRESSURE_PRODUCERS="256", FITZ_QUEUE_PRESSURE_CONSUMER_DELAY_MS="5",
                        FITZ_QUEUE_PRESSURE_MAX_ATTEMPTS="500000", FITZ_QUEUE_PRESSURE_DRAIN_SECS="600")
        self.heads = {}
        self.binaries = {}

    def build(self):
        provenance = {}
        for phase, source in self.sources.items():
            assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True)
            self.heads[phase] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
            resolution = json.loads(subprocess.check_output(
                ["cargo", "metadata", "--locked", "--format-version", "1", "--features", "benchkit,stress-soak"],
                cwd=source, env=self.env))
            save(self.output / (phase + "-cargo-metadata.json"), resolution)
            provenance[phase] = {
                "head": self.heads[phase], "lock_sha256": sha256(source / "Cargo.lock"),
                "dependencies": sorted((p["name"], p["version"], p["source"]) for p in resolution["packages"] if p["name"] != "fitz"),
                "fixtures": {str(p.relative_to(source)): sha256(p) for p in sorted((source / "benches").rglob("*.rs"))},
                "runtime_sources": {str(p.relative_to(source)): sha256(p) for p in sorted((source / "src").rglob("*.rs"))},
            }
        for key in ("lock_sha256", "dependencies", "fixtures"):
            assert provenance["before"][key] == provenance["after"][key], key
        provenance["identical_instrumented_runtime_sources"] = provenance["before"]["runtime_sources"] == provenance["after"]["runtime_sources"]
        provenance["production_baseline"] = os.environ["QUEUE_BASE_PRODUCTION"]
        provenance["diagnostic_only_commit"] = os.environ["QUEUE_TIMING_COMMIT"]
        provenance["rustc"] = subprocess.check_output(["rustc", "-Vv"], text=True)
        save(self.output / "provenance.json", provenance)
        for phase, source in self.sources.items():
            # Rebuild each source while retaining dependencies. The initial 1k
            # campaign is diagnostic and preserves build provenance, not acceptance.
            subprocess.run(["cargo", "clean", "--release", "--package", "fitz"], cwd=source, env=self.env, check=True)
            self.capture(phase, 1000, "build-" + phase, ["cargo", "bench", "--locked", "--bench", "queue_pressure",
                                                        "--features", "benchkit,stress-soak", "--"], build=True)
            executables = [p for p in (self.root / "target/release/deps").glob("queue_pressure-*") if p.is_file() and os.access(p, os.X_OK)]
            assert len(executables) == 1, executables
            archive = self.output / "binaries" / phase
            archive.mkdir(parents=True)
            self.binaries[phase] = archive / executables[0].name
            shutil.copy2(executables[0], self.binaries[phase])
        save(self.output / "binary-hashes.json", {phase: sha256(binary) for phase, binary in self.binaries.items()})

    def capture(self, phase, backlog, label, command, build=False, *, finite=False):
        source = self.sources[phase]
        destination = self.output / label
        destination.mkdir(parents=True)
        physical = source / "target/fitz-stress/queue-pressure"
        prior = set(physical.glob("*.json"))
        env = dict(self.env, FITZ_QUEUE_PRESSURE_MAX_BACKLOG=str(backlog),
                   FITZ_QUEUE_PRESSURE_RATES=str(16000 if finite else offered_rate(backlog)))
        command = command + ["--output-dir", str(destination / "stress")]
        metadata = {"head": self.heads[phase], "command": command, "cwd": str(source),
                    "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "host_before": host()}
        if not build:
            metadata["binary_sha256"] = sha256(self.binaries[phase])
        save(destination / "capture.json", metadata)
        with (destination / "run.log").open("w") as log:
            result = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
        metadata.update(exit_code=result.returncode, host_after=host(),
                        finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        reports = sorted(set(physical.glob("*.json")) - prior)
        for path in reports:
            shutil.copy2(path, destination / path.name)
        metadata["reports"] = [path.name for path in reports]
        try:
            assert result.returncode == 0, "Workload failed; raw accounting retained"
            assert len(reports) == 1, "Exactly one owned campaign is required"
            report = json.loads(reports[0].read_text())
            assert len(report["stages"]) == 1, "Exactly one pressure stage is required"
            stage = report["stages"][0]
            metadata.update(qualification_scope="finite_accepted_message_drain" if finite else "completed_window_drain",
                            termination=stage["termination"],
                            configured_window_completed=stage["configured_window_completed"],
                            offered=stage["offered"], harness_missed=stage["harness_missed"],
                            harness_miss_fraction=stage["harness_missed"] / stage["offered"] if stage["offered"] else None)
            if not build:
                validate(report, self.heads[phase], backlog, finite=finite)
            metadata["status"] = "diagnostic_build" if build else "drain_passed"
        except (AssertionError, KeyError, TypeError, ValueError) as error:
            metadata.update(status="failed", error=str(error))
        save(destination / "capture.json", metadata)
        if build:
            assert metadata["status"] == "diagnostic_build", metadata
        return metadata

    def measure(self):
        results = []
        for finite in (False, True):
            for index, backlog in enumerate((10000, 25000, 100000)):
                for phase in (("before", "after") if index % 2 == 0 else ("after", "before")):
                    label = f"{'finite-' if finite else ''}pressure-{backlog}-{phase}"
                    print(label, flush=True)
                    metadata = self.capture(phase, backlog, label, [str(self.binaries[phase]), "--bench"], finite=finite)
                    results.append({"label": label, "head": self.heads[phase], "status": metadata["status"],
                                    "termination": metadata.get("termination"),
                                    "window_completed": metadata.get("configured_window_completed"),
                                    "harness_miss_fraction": metadata.get("harness_miss_fraction")})
                    save(self.output / "acceptance.json", results)
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as file:
            file.write("Matched corrected-pacing drains. All six full-window captures and all six unchanged finite-envelope captures must pass. Each full-window capture must complete 120 seconds with at most 1% missed arrivals.\n\n")
            file.write("| Capture | Qualification | Termination | Full active window | Missed arrivals |\n|---|---|---|---|---|\n")
            for result in results:
                file.write(f"| {result['label']} | {result['status']} | {result['termination']} | {result['window_completed']} | {result['harness_miss_fraction']} |\n")
            file.write("\nGuard and broker-rejection stops fail full-window qualification. A finite-envelope guard drain qualifies only its verified accepted-message count under the original 16,000/s load and 600-second drain deadline. Resource-floor survival and published-dependency qualification remain separate.\n")
        assert all(result["status"] == "drain_passed" for result in results), results


if __name__ == "__main__":
    campaign = Campaign()
    campaign.build()
    campaign.measure()
