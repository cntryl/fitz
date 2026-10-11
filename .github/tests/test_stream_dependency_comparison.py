import importlib.util
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location(
    "stream_dependency_performance", Path(__file__).parents[1] / "stream_performance.py"
)
perf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf)


class StreamDependencyComparisonTests(unittest.TestCase):
    @contextmanager
    def prepared_baseline(self, change_benchmarks=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "repo"
            repo.mkdir()
            def git(*args):
                return subprocess.check_output(["git", "-c", "commit.gpgsign=false", *args],
                                               cwd=repo, text=True).strip()
            git("init", "--quiet")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            git("config", "commit.gpgsign", "true")
            git("config", "gpg.program", "/usr/bin/false")
            (repo / "src").mkdir()
            (repo / "src/lib.rs").write_text("pub fn unchanged() {}\n")
            (repo / "src/boundary_tests.rs").write_text("old test fixture\n")
            (repo / "benches").mkdir()
            (repo / "benches/fixture.rs").write_text("fn main() {}\n")
            original_manifest = '[dependencies]\nrmcp = "3.5.0"\n'
            original_lock = 'name = "rmcp"\nversion = "3.5.0"\n'
            (repo / "Cargo.toml").write_text(original_manifest)
            (repo / "Cargo.lock").write_text(original_lock)
            git("add", ".")
            git("commit", "--quiet", "-m", "baseline")
            base = git("rev-parse", "HEAD")
            (repo / "Cargo.toml").write_text(original_manifest.replace("3.5.0", "3.5.1"))
            (repo / "Cargo.lock").write_text(original_lock.replace("3.5.0", "3.5.1"))
            (repo / "src/boundary_tests.rs").write_text("new test fixture\n")
            if change_benchmarks:
                (repo / "benches/fixture.rs").write_text("fn main() { /* new fixture */ }\n")
            git("commit", "--quiet", "-am", "dependency update")
            workflow = (Path(__file__).parents[1] / "workflows/stream-perf.yml").read_text()
            step = workflow.split("      - name: Prepare identical fixtures on the baseline\n", 1)[1]
            script = textwrap.dedent(step.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
            runner = root / "runner"
            runner.mkdir()
            github_env = root / "github-env"
            subprocess.run(["bash", "-e", "-c", script], cwd=repo,
                           env=dict(os.environ, BASE_SHA=base, RUNNER_TEMP=str(runner),
                                    GITHUB_ENV=str(github_env)),
                           capture_output=True, text=True, check=True)
            baseline = runner / "fitz-stream-baseline"
            yield repo, baseline, github_env, original_manifest, original_lock

    def test_should_keep_original_dependencies_when_benchmark_fixtures_are_unchanged(self):
        with self.prepared_baseline() as (_, baseline, github_env, original_manifest, original_lock):
            self.assertEqual((baseline / "Cargo.toml").read_text(), original_manifest)
            self.assertEqual((baseline / "Cargo.lock").read_text(), original_lock)
            self.assertIn("STREAM_COMPARISON_KIND=dependencies", github_env.read_text())
            self.assertEqual((baseline / "src/boundary_tests.rs").read_text(), "old test fixture\n")

    def test_should_normalize_changed_benchmarks_and_dependencies_without_replacing_baseline_source(self):
        with self.prepared_baseline(change_benchmarks=True) as (repo, baseline, github_env, _, _):
            for filename in ("Cargo.toml", "Cargo.lock", "benches/fixture.rs"):
                self.assertEqual((baseline / filename).read_bytes(), (repo / filename).read_bytes())
            self.assertIn("STREAM_COMPARISON_KIND=source", github_env.read_text())
            self.assertEqual((baseline / "src/lib.rs").read_text(), "pub fn unchanged() {}\n")
            self.assertEqual((baseline / "src/boundary_tests.rs").read_text(), "old test fixture\n")

    def test_should_record_binary_equivalence_without_claiming_qualification_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            campaign = SimpleNamespace(output=root, heads={"after": "candidate"},
                                       binary_equivalent=True, build=Mock(),
                                       measure=Mock(side_effect=AssertionError("identical artifacts need no timing comparison")))
            with patch.object(perf, "Campaign", return_value=campaign), patch.dict(
                    os.environ, GITHUB_STEP_SUMMARY=str(root / "summary.md")):
                perf.main()
            campaign.build.assert_called_once_with()
            campaign.measure.assert_not_called()
            result = json.loads((root / "acceptance.json").read_text())
            self.assertEqual(result["status"], "binary_equivalent")
            self.assertIn("No qualification or confirmation timing samples", (root / "summary.md").read_text())
            self.assertIn("diagnostic build captures are retained", (root / "summary.md").read_text())


    def variants(self):
        before = {"fixtures": {"benches/gate.rs": "fixture"},
                  "manifest_sha256": "old-manifest", "lock_sha256": "old-lock",
                  "dependencies": [("rmcp", "3.5.0", "registry")],
                  "compiled_library_sha256": {".rlib": "library", ".rmeta": "metadata"},
                  "compiled_source_inputs": {"src/lib.rs": "unchanged"}}
        after = {**before, "manifest_sha256": "new-manifest", "lock_sha256": "new-lock",
                 "dependencies": [("rmcp", "3.5.1", "registry")]}
        hashes = {phase: {target: "a" * 64 for target, _ in perf.GROUPS}
                  for phase in ("before", "after")}
        return {"before": before, "after": after}, hashes

    def test_should_accept_independently_built_equivalent_dependency_artifacts(self):
        provenance, hashes = self.variants()
        self.assertTrue(perf.binary_equivalence("dependencies", provenance, hashes))

    def test_should_measure_changed_compiled_source_with_equal_executables(self):
        provenance, hashes = self.variants()
        provenance["after"]["compiled_source_inputs"] = {"src/lib.rs": "changed"}
        self.assertFalse(perf.binary_equivalence("dependencies", provenance, hashes))

    def test_should_measure_source_comparison_with_equal_executables(self):
        provenance, hashes = self.variants()
        for field in ("manifest_sha256", "lock_sha256", "dependencies"):
            provenance["after"][field] = provenance["before"][field]
        self.assertFalse(perf.binary_equivalence("source", provenance, hashes))

    def test_should_reject_normalized_dependency_comparison(self):
        provenance, hashes = self.variants()
        for field in ("manifest_sha256", "lock_sha256"):
            provenance["after"][field] = provenance["before"][field]
        with self.assertRaisesRegex(AssertionError, "normalized"):
            perf.binary_equivalence("dependencies", provenance, hashes)

    def test_should_reject_replaced_original_production_cargo_inputs_before_compilation(self):
        for replaced in ("Cargo.toml", "Cargo.lock"):
            with self.subTest(filename=replaced), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                sources = {phase: root / phase for phase in ("before", "after")}
                for phase, source in sources.items():
                    source.mkdir()
                    (source / "benches").mkdir()
                    (source / "benches/gate.rs").write_text("same fixture")
                    for filename in ("Cargo.toml", "Cargo.lock"):
                        original = "production " + filename
                        (source / filename).write_text(original if phase == "before" else "candidate " + filename)
                (sources["before"] / replaced).write_text("replaced original")
                def output(command, cwd=None, **_):
                    if command[:3] == ["git", "status", "--porcelain"]:
                        return ""
                    if command[:3] == ["git", "rev-parse", "HEAD"]:
                        return cwd.name
                    if command[:2] == ["cargo", "metadata"]:
                        return json.dumps({"resolve": {"root": "fitz"}, "packages": []}).encode()
                    if command[:2] == ["git", "show"]:
                        return ("production " + command[2].split(":")[1]).encode()
                    raise AssertionError("Unexpected command: " + repr(command))
                with patch.object(perf.Path, "cwd", return_value=sources["after"]), patch.dict(
                        os.environ, STREAM_BASELINE=str(sources["before"]), STREAM_OUTPUT=str(root / "output"),
                        STREAM_BASE_PRODUCTION="production", STREAM_COMPARISON_KIND="dependencies"), patch.object(
                        perf.subprocess, "check_output", side_effect=output), patch.object(perf.subprocess, "run") as run:
                    with self.assertRaisesRegex(AssertionError, "Baseline production dependencies were replaced"):
                        perf.Campaign().build()
                    run.assert_not_called()

    def test_should_reject_mismatched_benchmark_fixtures(self):
        provenance, hashes = self.variants()
        provenance["after"]["fixtures"] = {"benches/gate.rs": "changed"}
        with self.assertRaises(AssertionError):
            perf.binary_equivalence("dependencies", provenance, hashes)

    def test_should_reject_missing_benchmark_artifact(self):
        provenance, hashes = self.variants()
        hashes["after"].pop(next(iter(hashes["after"])))
        with self.assertRaises(AssertionError):
            perf.binary_equivalence("dependencies", provenance, hashes)

    def test_should_measure_when_any_dependency_benchmark_binary_differs(self):
        provenance, hashes = self.variants()
        hashes["after"][next(iter(hashes["after"]))] = "b" * 64
        self.assertFalse(perf.binary_equivalence("dependencies", provenance, hashes))

    def test_should_measure_when_any_dependency_library_artifact_differs(self):
        for suffix in (".rlib", ".rmeta"):
            with self.subTest(suffix=suffix):
                provenance, hashes = self.variants()
                provenance["after"]["compiled_library_sha256"] = {
                    **provenance["after"]["compiled_library_sha256"], suffix: "changed"}
                self.assertFalse(perf.binary_equivalence("dependencies", provenance, hashes))

    def test_should_reject_missing_library_artifact_provenance(self):
        provenance, hashes = self.variants()
        provenance["after"]["compiled_library_sha256"] = {}
        with self.assertRaisesRegex(AssertionError, "library artifact"):
            perf.binary_equivalence("dependencies", provenance, hashes)

    def test_should_measure_changed_source_and_dependencies_with_distinct_binaries(self):
        provenance, hashes = self.variants()
        provenance["after"]["compiled_source_inputs"] = {"src/lib.rs": "changed"}
        hashes["after"] = {target: "b" * 64 for target in hashes["after"]}
        self.assertFalse(perf.binary_equivalence("dependencies", provenance, hashes))

    def test_should_keep_both_predeclared_timing_runs_for_changed_artifacts(self):
        for qualification in ("passed", "budget_failure", "measurement_unstable"):
            with self.subTest(qualification=qualification), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                campaign = SimpleNamespace(output=root, heads={"after": "candidate"},
                                           binary_equivalent=False, build=Mock(),
                                           measure=Mock(side_effect=[qualification, "passed"]))
                with patch.object(perf, "Campaign", return_value=campaign):
                    if qualification == "passed":
                        perf.main()
                    else:
                        with self.assertRaisesRegex(AssertionError, qualification):
                            perf.main()
                self.assertEqual([call.args[0] for call in campaign.measure.call_args_list],
                                 ["qualification", "confirmation"])
                self.assertEqual(json.loads((root / "acceptance.json").read_text()),
                                 {"qualification": qualification, "confirmation": "passed"})

    def test_should_require_fresh_benchmark_from_the_expected_source(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "gate.rs"
            log = Path(directory) / "build.log"
            row = {"reason": "compiler-artifact", "package_id": "fitz-package",
                   "target": {"kind": ["bench"], "name": "gate", "src_path": str(source)},
                   "fresh": False}
            log.write_text(json.dumps(row) + "\n")
            self.assertEqual(perf.fresh_compiler_artifact(log, "fitz-package", "bench", source, "gate"), row)
            row["fresh"] = True
            log.write_text(json.dumps(row) + "\n")
            with self.assertRaisesRegex(AssertionError, "cached bench"):
                perf.fresh_compiler_artifact(log, "fitz-package", "bench", source, "gate")
            row["fresh"] = False
            log.write_text(json.dumps(row) + "\n")
            with self.assertRaises(AssertionError):
                perf.fresh_compiler_artifact(log, "fitz-package", "bench", source.with_name("wrong.rs"), "gate")

    def test_should_read_cargo_primary_library_dep_info(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "src").mkdir()
            (source / "src/lib.rs").write_text("pub fn value() {}\n")
            library = source / "libfitz.rlib"
            library.touch()
            (source / "libfitz.d").write_text(str(library) + ": src/lib.rs\n")
            row = {"reason": "compiler-artifact", "package_id": "fitz-package",
                   "target": {"kind": ["lib"], "src_path": str(source / "src/lib.rs")},
                   "fresh": False, "filenames": [str(library)]}
            log = source / "build.log"
            log.write_text(json.dumps(row) + "\n")
            self.assertEqual(set(perf.compiled_source_inputs(source, log, "fitz-package")["hashes"]),
                             {"src/lib.rs"})

    def test_should_hash_every_emitted_library_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "src").mkdir()
            (source / "src/lib.rs").write_text("pub fn value() {}\n")
            libraries = [source / ("libfitz" + suffix) for suffix in (".rlib", ".rmeta")]
            for library in libraries:
                library.write_text(library.suffix)
            (source / "libfitz.d").write_text(str(libraries[0]) + ": src/lib.rs\n")
            row = {"reason": "compiler-artifact", "package_id": "fitz-package",
                   "target": {"kind": ["lib"], "src_path": str(source / "src/lib.rs")},
                   "fresh": False, "filenames": [str(path) for path in libraries]}
            log = source / "build.log"
            log.write_text(json.dumps(row) + "\n")
            self.assertEqual(perf.compiled_source_inputs(source, log, "fitz-package")["library_sha256"],
                             {path.suffix: perf.sha256(path) for path in libraries})

    def test_should_require_fresh_correct_library_and_record_actual_source_inputs(self):
        with tempfile.TemporaryDirectory(prefix="stream inputs ") as directory:
            source = Path(directory)
            (source / "src").mkdir()
            (source / "src/lib.rs").write_text("pub fn unchanged() {}\n")
            (source / "src/value with spaces.rs").write_text("pub const VALUE: u8 = 1;\n")
            (source / "src/boundary_tests.rs").write_text("test-only fixture changes\n")
            library = source / "libfitz-example.rlib"
            library.touch()
            dep_info = source / "fitz-example.d"
            dep_info.write_text(str(dep_info) + ": src/lib.rs src/value\\ with\\ spaces.rs\n")
            row = {"reason": "compiler-artifact", "package_id": "fitz-package",
                   "target": {"kind": ["lib"], "src_path": str(source / "src/lib.rs")},
                   "fresh": False, "filenames": [str(library)]}
            log = source / "build.log"
            log.write_text(json.dumps(row) + "\n")
            witness = perf.compiled_source_inputs(source, log, "fitz-package")
            self.assertEqual(set(witness["hashes"]), {"src/lib.rs", "src/value with spaces.rs"})
            self.assertEqual(witness["library_sha256"], {".rlib": perf.sha256(library)})
            self.assertNotIn("src/boundary_tests.rs", witness["hashes"])
            row["fresh"] = True
            log.write_text(json.dumps(row) + "\n")
            with self.assertRaisesRegex(AssertionError, "cached library"):
                perf.compiled_source_inputs(source, log, "fitz-package")
            with self.assertRaisesRegex(AssertionError, "exact library"):
                perf.compiled_source_inputs(source, log, "wrong-package")


class StreamSourceArtifactComparisonTests(unittest.TestCase):
    @contextmanager
    def rebuilt_campaign(self, all_equal=False, alias=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = {phase: root / phase for phase in ("before", "after")}
            targets = list(dict.fromkeys(target for target, _ in perf.GROUPS))
            for phase, source in sources.items():
                (source / "src").mkdir(parents=True)
                value = 0 if all_equal or phase == "before" else 1
                (source / "src/lib.rs").write_text(f"pub const VALUE: u8 = {value};\n")
                (source / "benches").mkdir()
                for target in targets:
                    (source / "benches" / (target + ".rs")).write_text("fn main() {}\n")
                for filename in ("Cargo.toml", "Cargo.lock"):
                    (source / filename).write_text("unchanged " + filename)

            def output(command, cwd=None, **_):
                if command[:3] == ["git", "status", "--porcelain"]:
                    return ""
                if command[:3] == ["git", "rev-parse", "HEAD"]:
                    return cwd.name + "-head"
                if command[:2] == ["cargo", "metadata"]:
                    return json.dumps({"resolve": {"root": "fitz-" + cwd.name},
                                       "packages": []}).encode()
                if command == ["rustc", "-Vv"]:
                    return "fixture toolchain"
                raise AssertionError("Unexpected command: " + repr(command))

            def execute(phase, target, label, command, build=False):
                self.assertTrue(build)
                source = sources[phase]
                library = source / ("libfitz-" + phase + ".rlib")
                metadata = library.with_suffix(".rmeta")
                library.write_text("same library" if all_equal else phase + " library")
                metadata.write_text("same metadata" if all_equal else phase + " metadata")
                library.with_suffix(".d").write_text(str(library) + ": src/lib.rs\n")
                executable = source / target
                executable.write_text(phase if target == targets[0] and not all_equal else "same")
                executable.chmod(0o700)
                library_source = source / "src/lib.rs"
                benchmark_source = source / "benches" / (target + ".rs")
                if phase == "after" and alias == "wrong_library_source":
                    library_source = sources["before"] / "src/lib.rs"
                if phase == "after" and alias == "wrong_benchmark_source":
                    benchmark_source = sources["before"] / "benches" / (target + ".rs")
                rows = [
                    {"reason": "compiler-artifact", "package_id": "fitz-" + phase,
                     "target": {"kind": ["lib"], "src_path": str(library_source)},
                     "fresh": phase == "after" and alias == "cached_library",
                     "filenames": [str(library), str(metadata)]},
                    {"reason": "compiler-artifact", "package_id": "fitz-" + phase,
                     "target": {"kind": ["bench"], "name": target,
                                "src_path": str(benchmark_source)},
                     "fresh": phase == "after" and alias == "cached_benchmark",
                     "executable": str(executable)},
                ]
                destination = campaign.output / label
                destination.mkdir()
                (destination / "run.log").write_text("\n".join(json.dumps(row) for row in rows))

            with patch.object(perf.Path, "cwd", return_value=sources["after"]), patch.dict(
                    os.environ, STREAM_BASELINE=str(sources["before"]),
                    STREAM_OUTPUT=str(root / "output"), STREAM_BASE_PRODUCTION="production",
                    STREAM_COMPARISON_KIND="source"), patch.object(
                        perf.subprocess, "check_output", side_effect=output), patch.object(
                        perf.subprocess, "run") as clean:
                campaign = perf.Campaign()
                with patch.object(campaign, "execute", side_effect=execute):
                    yield campaign, clean

    def test_should_time_source_change_with_mixed_fresh_executable_hashes(self):
        with self.rebuilt_campaign() as (campaign, clean), patch.object(
                perf, "Campaign", return_value=campaign), patch.object(
                    campaign, "measure", return_value="passed") as measure:
            perf.main()
            hashes = json.loads((campaign.output / "binary-hashes.json").read_text())
            comparisons = [hashes["before"][target] == hashes["after"][target]
                           for target in hashes["before"]]
            self.assertIn(True, comparisons)
            self.assertIn(False, comparisons)
            self.assertFalse(campaign.binary_equivalent)
            self.assertEqual(clean.call_count, 2)
            self.assertEqual([call.args[0] for call in measure.call_args_list],
                             ["qualification", "confirmation"])

    def test_should_time_fully_equal_fresh_artifacts_in_source_mode(self):
        with self.rebuilt_campaign(all_equal=True) as (campaign, _), patch.object(
                perf, "Campaign", return_value=campaign), patch.object(
                    campaign, "measure", return_value="passed") as measure:
            perf.main()
            provenance = json.loads((campaign.output / "provenance.json").read_text())
            self.assertEqual(provenance["before"]["compiled_source_inputs"],
                             provenance["after"]["compiled_source_inputs"])
            self.assertEqual(provenance["before"]["compiled_library_sha256"],
                             provenance["after"]["compiled_library_sha256"])
            hashes = json.loads((campaign.output / "binary-hashes.json").read_text())
            self.assertEqual(hashes["before"], hashes["after"])
            self.assertFalse(campaign.binary_equivalent)
            self.assertEqual(provenance["evidence_mode"], "timing_comparison")
            self.assertEqual([call.args[0] for call in measure.call_args_list],
                             ["qualification", "confirmation"])

    def test_should_reject_cached_or_wrong_source_compiler_artifacts(self):
        for alias in ("cached_library", "cached_benchmark",
                      "wrong_library_source", "wrong_benchmark_source"):
            with self.subTest(alias=alias), self.rebuilt_campaign(alias=alias) as (campaign, _):
                with self.assertRaises(AssertionError):
                    campaign.build()
                self.assertFalse((campaign.output / "binary-hashes.json").exists())


if __name__ == "__main__":
    unittest.main()
