#!/usr/bin/env python3
"""
Produce CSV and Markdown summaries for Criterion benchmarks and stress tests,
plus a rubric-aware performance scorecard from config/perf_targets.json.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import csv
import json


REPO_ROOT = Path(__file__).resolve().parents[1]
CRITERION_ROOT = REPO_ROOT / "target" / "criterion"
STRESS_ROOT = REPO_ROOT / "target" / "stress"
TARGET_ROOT = REPO_ROOT / "target"
PERF_TARGETS_PATH = REPO_ROOT / "config" / "perf_targets.json"

OUT_CSV = CRITERION_ROOT / "benchmark_summary.csv"
OUT_MD = TARGET_ROOT / "bench_summary.md"
STRESS_CSV = STRESS_ROOT / "stress_summary.csv"
PERF_SCORECARD_JSON = TARGET_ROOT / "perf_scorecard.json"
PERF_SCORECARD_MD = TARGET_ROOT / "perf_scorecard.md"

SCOREBOARD_SPECS = [
    {
        "id": "engine_core",
        "title": "Engine Core",
        "target_class": "engine_core",
        "budget_group": None,
        "product_surface": True,
    },
    {
        "id": "service_budget/direct_api",
        "title": "Service Budget: Direct API",
        "target_class": "service_budget",
        "budget_group": "direct_api",
        "product_surface": True,
    },
    {
        "id": "service_budget/transport",
        "title": "Service Budget: Transport",
        "target_class": "service_budget",
        "budget_group": "transport",
        "product_surface": True,
    },
    {
        "id": "service_budget/contention",
        "title": "Service Budget: Contention",
        "target_class": "service_budget",
        "budget_group": "contention",
        "product_surface": True,
    },
    {
        "id": "internal_explainer",
        "title": "Internal Explainers",
        "target_class": "internal_explainer",
        "budget_group": None,
        "product_surface": False,
    },
]


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def format_optional_float(value, places: int = 6) -> str:
    if value is None:
        return "NA"
    return f"{value:.{places}f}"


def format_optional_percent(value, places: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}%"


def format_optional_scalar(value) -> str:
    if value is None:
        return "n/a"
    return str(value)


def format_duration_ns(duration_ns: float) -> str:
    if duration_ns >= 1e6:
        return f"{duration_ns / 1e6:.2f}ms"
    if duration_ns >= 1e3:
        return f"{duration_ns / 1e3:.2f}us"
    return f"{duration_ns:.0f}ns"


def suite_sort_key(name: str):
    if name.startswith("tier1_"):
        return (0, name)
    if name.startswith("tier2_"):
        return (1, name)
    return (2, name)


def collect_criterion_entries():
    entries = []
    if not CRITERION_ROOT.exists():
        return entries

    for estimates_path in CRITERION_ROOT.rglob("new/estimates.json"):
        if not estimates_path.exists():
            continue
        try:
            data = load_json(estimates_path)
        except Exception as exc:  # pragma: no cover
            print(f"skipping {estimates_path} (read error): {exc}")
            continue

        benchmark = str(estimates_path.relative_to(CRITERION_ROOT).parent.parent)
        if "schedule_system_scan_and_fire" in benchmark:
            continue

        mean = data.get("mean", {}).get("point_estimate")
        if mean is None:
            continue

        ci = data.get("mean", {}).get("confidence_interval", {})
        std_dev = data.get("std_dev", {}).get("point_estimate")
        rel_stddev = None
        if std_dev is not None and mean != 0:
            rel_stddev = std_dev / mean

        entries.append(
            {
                "benchmark": benchmark,
                "suite": benchmark.replace("\\", "/").split("/")[0],
                "mean": mean,
                "mean_ci_lower": ci.get("lower_bound"),
                "mean_ci_upper": ci.get("upper_bound"),
                "std_dev": std_dev,
                "rel_stddev": rel_stddev,
                "high_variance": rel_stddev is not None and rel_stddev > 0.10,
                "mean_us": mean / 1e3,
                "mean_ms": mean / 1e6,
                "file": str(estimates_path),
            }
        )

    return entries


def write_criterion_csv(entries):
    if not CRITERION_ROOT.exists():
        return

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "benchmark",
                "mean",
                "mean_ci_lower",
                "mean_ci_upper",
                "std_dev",
                "rel_stddev",
                "high_variance",
                "mean_us(assume_ns)",
                "mean_ms(assume_ns)",
                "file",
            ]
        )
        for entry in sorted(entries, key=lambda item: item["mean"]):
            writer.writerow(
                [
                    entry["benchmark"],
                    format_optional_float(entry["mean"]),
                    format_optional_float(entry["mean_ci_lower"]),
                    format_optional_float(entry["mean_ci_upper"]),
                    format_optional_float(entry["std_dev"]),
                    format_optional_float(entry["rel_stddev"]),
                    str(entry["high_variance"]),
                    format_optional_float(entry["mean_us"]),
                    format_optional_float(entry["mean_ms"]),
                    entry["file"],
                ]
            )


def collect_stress_entries():
    entries = []
    if not STRESS_ROOT.exists():
        return entries

    for suite_dir in sorted(STRESS_ROOT.glob("*/")):
        if not suite_dir.is_dir():
            continue

        latest_json = suite_dir / "latest.json"
        if not latest_json.exists():
            continue

        try:
            data = load_json(latest_json)
        except Exception as exc:  # pragma: no cover
            print(f"skipping {latest_json} (read error): {exc}")
            continue

        suite = data.get("suite", suite_dir.name)
        for result in data.get("results", []):
            duration = result.get("duration")
            elements = result.get("elements")
            if duration is None or elements in (None, 0):
                continue

            all_runs = result.get("all_runs") or [duration]
            tags = result.get("tags", {})
            scenario = tags.get("scenario", "unknown")
            layer = tags.get("layer")
            throughput_ops_per_ns = elements / duration if duration > 0 else 0.0
            per_op_ns = duration / elements if elements > 0 else 0.0

            if len(all_runs) > 1:
                average_run = sum(all_runs) / len(all_runs)
                variance = sum((run - average_run) ** 2 for run in all_runs) / len(all_runs)
                stddev_runs = variance ** 0.5
                rel_stddev_runs = stddev_runs / average_run if average_run > 0 else 0.0
            else:
                stddev_runs = 0.0
                rel_stddev_runs = 0.0

            entries.append(
                {
                    "suite": suite,
                    "name": result.get("name", ""),
                    "scenario": scenario,
                    "layer": layer,
                    "duration_ns": duration,
                    "duration_us": duration / 1e3,
                    "duration_ms": duration / 1e6,
                    "elements": elements,
                    "per_op_ns": per_op_ns,
                    "per_op_us": per_op_ns / 1e3,
                    "throughput_ops_per_ns": throughput_ops_per_ns,
                    "throughput_ops_per_us": throughput_ops_per_ns * 1e3,
                    "throughput_ops_per_ms": throughput_ops_per_ns * 1e6,
                    "throughput_ops_per_s": throughput_ops_per_ns * 1e9,
                    "num_runs": len(all_runs),
                    "stddev_runs": stddev_runs,
                    "rel_stddev_runs": rel_stddev_runs,
                    "file": str(latest_json),
                }
            )

    return entries


def write_stress_csv(entries):
    if not STRESS_ROOT.exists():
        return

    STRESS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with STRESS_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "suite",
                "name",
                "scenario",
                "layer",
                "duration_ms",
                "elements",
                "per_op_us",
                "throughput_ops_per_s",
                "runs",
                "stddev_runs_ns",
                "rel_stddev_runs",
                "file",
            ]
        )
        for entry in sorted(entries, key=lambda item: item["throughput_ops_per_s"], reverse=True):
            writer.writerow(
                [
                    entry["suite"],
                    entry["name"],
                    entry["scenario"],
                    entry["layer"] or "",
                    f"{entry['duration_ms']:.2f}",
                    entry["elements"],
                    f"{entry['per_op_us']:.6f}",
                    f"{entry['throughput_ops_per_s']:.2f}",
                    entry["num_runs"],
                    f"{entry['stddev_runs']:.2f}",
                    f"{entry['rel_stddev_runs']:.6f}" if entry["rel_stddev_runs"] else "NA",
                    entry["file"],
                ]
            )


def write_bench_summary(criterion_entries, stress_entries):
    sorted_by_mean = sorted(criterion_entries, key=lambda item: item["mean"])
    fastest = sorted_by_mean[:10]
    slowest = sorted_by_mean[-10:][::-1]
    high_variance = [entry for entry in criterion_entries if entry["high_variance"]]

    criterion_suites = defaultdict(list)
    for entry in criterion_entries:
        criterion_suites[entry["suite"]].append(entry)

    stress_suites = defaultdict(list)
    for entry in stress_entries:
        stress_suites[entry["suite"]].append(entry)

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    with OUT_MD.open("w", encoding="utf-8") as handle:
        handle.write("# Benchmark & Stress Test Summary\n\n")
        handle.write("Generated from Criterion benchmarks and stress tests.\n\n")

        handle.write("# Criterion Benchmarks\n\n")
        handle.write("Note: mean_us / mean_ms assume raw numbers are in nanoseconds.\n\n")

        handle.write("## Top 10 fastest (by mean)\n\n")
        handle.write("| rank | benchmark | mean | mean_ms | mean_us | std_dev | rel_stddev |\n")
        handle.write("|---:|---|---:|---:|---:|---:|---:|\n")
        for index, entry in enumerate(fastest, 1):
            handle.write(
                f"| {index} | {entry['benchmark']} | {entry['mean']:.6f} | {entry['mean_ms']:.6f} | "
                f"{entry['mean_us']:.6f} | {format_optional_float(entry['std_dev'])} | "
                f"{format_optional_float(entry['rel_stddev'])} |\n"
            )

        handle.write("\n## Top 10 slowest (by mean)\n\n")
        handle.write("| rank | benchmark | mean | mean_ms | mean_us | std_dev | rel_stddev |\n")
        handle.write("|---:|---|---:|---:|---:|---:|---:|\n")
        for index, entry in enumerate(slowest, 1):
            handle.write(
                f"| {index} | {entry['benchmark']} | {entry['mean']:.6f} | {entry['mean_ms']:.6f} | "
                f"{entry['mean_us']:.6f} | {format_optional_float(entry['std_dev'])} | "
                f"{format_optional_float(entry['rel_stddev'])} |\n"
            )

        handle.write("\n## High variance benchmarks (rel_stddev > 0.10)\n\n")
        if not high_variance:
            handle.write("None detected.\n")
        else:
            handle.write("| benchmark | mean | std_dev | rel_stddev |\n")
            handle.write("|---|---:|---:|---:|\n")
            for entry in sorted(high_variance, key=lambda item: item["rel_stddev"] or 0.0, reverse=True):
                handle.write(
                    f"| {entry['benchmark']} | {entry['mean']:.6f} | {format_optional_float(entry['std_dev'])} | "
                    f"{format_optional_float(entry['rel_stddev'])} |\n"
                )

        handle.write("\n## Per-Suite Results (Criterion)\n\n")
        for suite_name in sorted(criterion_suites, key=suite_sort_key):
            suite_entries = sorted(criterion_suites[suite_name], key=lambda item: item["mean"])
            total_mean_ns = sum(entry["mean"] for entry in suite_entries)
            count = len(suite_entries)
            average_ns = total_mean_ns / count if count else 0.0

            handle.write(f"### {suite_name}\n\n")
            handle.write(
                f"**Benchmarks**: {count} | **Avg mean**: {average_ns / 1e3:.3f} us "
                f"(total {total_mean_ns / 1e6:.2f} ms)\n\n"
            )
            handle.write("| benchmark | mean_ns | mean_us | mean_ms | std_dev | rel_stddev |\n")
            handle.write("|---|---:|---:|---:|---:|---:|\n")
            for entry in suite_entries:
                benchmark_short = entry["benchmark"].replace("\\", "/").split("/", 1)[-1]
                handle.write(
                    f"| {benchmark_short} | {entry['mean']:.2f} | {entry['mean_us']:.4f} | "
                    f"{entry['mean_ms']:.6f} | {format_optional_float(entry['std_dev'], 4)} | "
                    f"{format_optional_float(entry['rel_stddev'], 4)} |\n"
                )
            handle.write("\n")

        handle.write("\n# Stress Tests\n\n")
        if not stress_entries:
            handle.write("No stress test results found.\n")
            return

        handle.write("Ordered by throughput (highest first).\n\n")
        handle.write("## Per-Suite Results (Stress)\n\n")
        for suite_name in sorted(stress_suites):
            suite_entries = stress_suites[suite_name]
            total_duration = sum(entry["duration_ns"] for entry in suite_entries)
            total_elements = sum(entry["elements"] for entry in suite_entries)
            total_throughput = (total_elements / total_duration) * 1e9 if total_duration > 0 else 0.0
            has_layer = any(entry.get("layer") for entry in suite_entries)

            handle.write(f"### {suite_name}\n\n")
            handle.write(
                f"**Total**: {total_elements} ops in {format_duration_ns(total_duration)} = "
                f"{total_throughput:.0f} ops/sec\n\n"
            )
            if has_layer:
                handle.write("| scenario | layer | ops | duration | per_op_us | ops/sec |\n")
                handle.write("|---|---|---:|---:|---:|---:|\n")
                for entry in sorted(suite_entries, key=lambda item: item["throughput_ops_per_s"], reverse=True):
                    handle.write(
                        f"| {entry['scenario']} | {entry['layer'] or 'n/a'} | {entry['elements']} | "
                        f"{format_duration_ns(entry['duration_ns'])} | {entry['per_op_us']:.3f} | "
                        f"{entry['throughput_ops_per_s']:.0f} |\n"
                    )
            else:
                handle.write("| scenario | ops | duration | per_op_us | ops/sec |\n")
                handle.write("|---|---:|---:|---:|---:|\n")
                for entry in sorted(suite_entries, key=lambda item: item["throughput_ops_per_s"], reverse=True):
                    handle.write(
                        f"| {entry['scenario']} | {entry['elements']} | "
                        f"{format_duration_ns(entry['duration_ns'])} | {entry['per_op_us']:.3f} | "
                        f"{entry['throughput_ops_per_s']:.0f} |\n"
                    )
            handle.write("\n")


def build_target_name(target_key: str, target: dict) -> str:
    if target.get("kind") == "criterion":
        return target_key.split("criterion:", 1)[1]

    parts = [target.get("suite", "unknown"), target.get("scenario", "unknown")]
    if target.get("layer"):
        parts.append(target["layer"])
    return " / ".join(parts)


def get_target_measurement(target_key: str, target: dict, criterion_entries, stress_entries):
    if target.get("kind") == "criterion":
        for entry in criterion_entries:
            if f"criterion:{entry['benchmark']}" == target_key:
                return {
                    "current_mean_us": entry["mean_us"],
                    "rel_stddev": entry["rel_stddev"],
                    "source": entry["file"],
                }
        return None

    for entry in stress_entries:
        key = f"stress:{entry['suite']}|{entry['scenario']}"
        if entry.get("layer"):
            key = f"{key}|{entry['layer']}"
        if key == target_key:
            return {
                "current_mean_us": entry["per_op_us"],
                "rel_stddev": entry["rel_stddev_runs"],
                "source": entry["file"],
                "throughput_ops_per_s": entry["throughput_ops_per_s"],
            }
    return None


def gap_pct(current_mean_us, target_mean_us):
    if current_mean_us is None or target_mean_us in (None, 0):
        return None
    return ((current_mean_us - target_mean_us) / target_mean_us) * 100.0


def test_target_actionable(target: dict, measurement) -> bool:
    gating = target.get("gating")
    if gating == "hard":
        return True
    if gating == "variance_gated":
        if not measurement:
            return False
        max_rel_stddev = target.get("max_rel_stddev")
        rel_stddev = measurement.get("rel_stddev")
        if max_rel_stddev is None or rel_stddev is None:
            return False
        return rel_stddev <= max_rel_stddev
    return False


def build_target_results(criterion_entries, stress_entries):
    perf_targets = load_json(PERF_TARGETS_PATH)
    target_results = []

    for target_key in sorted(perf_targets.get("targets", {})):
        target = perf_targets["targets"][target_key]
        measurement = get_target_measurement(target_key, target, criterion_entries, stress_entries)
        current_mean_us = measurement.get("current_mean_us") if measurement else None
        rel_stddev = measurement.get("rel_stddev") if measurement else None
        actionable = test_target_actionable(target, measurement)
        operational_gap = gap_pct(current_mean_us, target.get("operational_target"))
        stretch_gap = gap_pct(current_mean_us, target.get("stretch_target"))
        meets_operational = (
            current_mean_us is not None
            and target.get("operational_target") is not None
            and current_mean_us <= target["operational_target"]
        )
        meets_stretch = (
            current_mean_us is not None
            and target.get("stretch_target") is not None
            and current_mean_us <= target["stretch_target"]
        )

        if target.get("gating") == "informational":
            status = "informational"
        elif not measurement:
            status = "missing_measurement"
        elif target.get("gating") == "variance_gated" and not actionable:
            status = "variance_blocked"
        elif meets_operational:
            status = "operational_met"
        elif meets_stretch:
            status = "stretch_met"
        else:
            status = "operational_miss"

        target_results.append(
            {
                "target_key": target_key,
                "name": build_target_name(target_key, target),
                "kind": target.get("kind"),
                "domain": target.get("domain", "internal"),
                "suite": target.get("suite"),
                "scenario": target.get("scenario"),
                "layer": target.get("layer"),
                "target_class": target.get("target_class"),
                "budget_group": target.get("budget_group"),
                "gating": target.get("gating"),
                "max_rel_stddev": target.get("max_rel_stddev"),
                "actionable": actionable,
                "measured": measurement is not None,
                "status": status,
                "current_mean_us": current_mean_us,
                "rel_stddev": rel_stddev,
                "throughput_ops_per_s": measurement.get("throughput_ops_per_s") if measurement else None,
                "source": measurement.get("source") if measurement else None,
                "operational_target": target.get("operational_target"),
                "stretch_target": target.get("stretch_target"),
                "operational_gap_pct": operational_gap,
                "stretch_gap_pct": stretch_gap,
                "meets_operational": meets_operational,
                "meets_stretch": meets_stretch,
                "note": target.get("note"),
            }
        )

    return perf_targets, target_results


def scoreboard_rows(target_results, target_class: str, budget_group):
    rows = []
    for row in target_results:
        if row["target_class"] != target_class:
            continue
        if budget_group is not None and row.get("budget_group") != budget_group:
            continue
        if budget_group is None and row.get("budget_group") not in (None, ""):
            continue
        rows.append(row)
    return rows


def select_worst_miss(rows):
    misses = [
        row
        for row in rows
        if row["actionable"] and row["measured"] and not row["meets_operational"] and row["operational_gap_pct"] is not None
    ]
    if not misses:
        return None

    misses.sort(
        key=lambda row: (
            row["operational_gap_pct"],
            row["stretch_gap_pct"] if row["stretch_gap_pct"] is not None else float("-inf"),
            row["current_mean_us"] if row["current_mean_us"] is not None else float("-inf"),
            row["target_key"],
        ),
        reverse=True,
    )
    return misses[0]


def build_scoreboard_summary(spec: dict, rows):
    actionable_rows = [row for row in rows if row["actionable"]]
    total_actionable = len(actionable_rows)
    operational_met = sum(1 for row in actionable_rows if row["meets_operational"])
    stretch_met = sum(1 for row in actionable_rows if row["meets_stretch"])
    missing_measurements = sum(1 for row in actionable_rows if not row["measured"])
    worst_miss_row = select_worst_miss(rows)

    return {
        "id": spec["id"],
        "title": spec["title"],
        "target_class": spec["target_class"],
        "budget_group": spec["budget_group"],
        "total_targets": len(rows),
        "total_actionable": total_actionable,
        "missing_measurements": missing_measurements,
        "operational_met": operational_met,
        "stretch_met": stretch_met,
        "operational_attainment_pct": (operational_met / total_actionable * 100.0) if total_actionable else None,
        "stretch_attainment_pct": (stretch_met / total_actionable * 100.0) if total_actionable else None,
        "worst_gap_pct": worst_miss_row["operational_gap_pct"] if worst_miss_row else None,
        "worst_miss": worst_miss_row,
        "pass": (operational_met == total_actionable) if total_actionable else True,
    }


def build_product_summary(scoreboards, target_results):
    product_ids = [spec["id"] for spec in SCOREBOARD_SPECS if spec["product_surface"]]
    product_rows = [row for row in target_results if row["actionable"] and row["target_class"] in {"engine_core", "service_budget"}]
    operational_met = sum(1 for row in product_rows if row["meets_operational"])
    stretch_met = sum(1 for row in product_rows if row["meets_stretch"])
    worst_miss_row = select_worst_miss(product_rows)

    engine_core = scoreboards["engine_core"]
    direct_api = scoreboards["service_budget/direct_api"]
    transport = scoreboards["service_budget/transport"]
    contention = scoreboards["service_budget/contention"]

    total_actionable = len(product_rows)
    return {
        "scoreboard_ids": product_ids,
        "total_actionable": total_actionable,
        "missing_measurements": sum(1 for row in product_rows if not row["measured"]),
        "operational_met": operational_met,
        "stretch_met": stretch_met,
        "operational_attainment_pct": (operational_met / total_actionable * 100.0) if total_actionable else None,
        "stretch_attainment_pct": (stretch_met / total_actionable * 100.0) if total_actionable else None,
        "worst_gap_pct": worst_miss_row["operational_gap_pct"] if worst_miss_row else None,
        "worst_miss": worst_miss_row,
        "engine_core_pass": engine_core["pass"],
        "service_budget": {
            "direct_api_pass": direct_api["pass"],
            "transport_pass": transport["pass"],
            "contention_pass": contention["pass"],
        },
        "product_pass": engine_core["pass"] and direct_api["pass"] and transport["pass"] and contention["pass"],
    }


def write_perf_scorecard_md(scorecard):
    lines = [
        "# Performance Scorecard",
        "",
        "Generated from current benchmark outputs and config/perf_targets.json.",
        "",
        "Informational targets are excluded from attainment percentages. Variance-gated targets only count when their current rel_stddev is at or below max_rel_stddev.",
        "",
        "## Product Summary",
        "",
        f"- engine_core_pass: `{str(scorecard['product_summary']['engine_core_pass']).lower()}`",
        f"- service_budget.direct_api_pass: `{str(scorecard['product_summary']['service_budget']['direct_api_pass']).lower()}`",
        f"- service_budget.transport_pass: `{str(scorecard['product_summary']['service_budget']['transport_pass']).lower()}`",
        f"- service_budget.contention_pass: `{str(scorecard['product_summary']['service_budget']['contention_pass']).lower()}`",
        f"- product_pass: `{str(scorecard['product_summary']['product_pass']).lower()}`",
        "",
        "| scoreboard | class | budget group | total actionable | missing | operational met | stretch met | operational attainment | stretch attainment | pass | worst gap pct | worst miss |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---|",
    ]

    for spec in SCOREBOARD_SPECS:
        board = scorecard["scoreboards"][spec["id"]]
        worst_name = board["worst_miss"]["name"] if board["worst_miss"] else "none"
        lines.append(
            f"| {board['title']} | {board['target_class']} | {board['budget_group'] or 'n/a'} | "
            f"{board['total_actionable']} | {board['missing_measurements']} | {board['operational_met']} | "
            f"{board['stretch_met']} | {format_optional_percent(board['operational_attainment_pct'])} | "
            f"{format_optional_percent(board['stretch_attainment_pct'])} | "
            f"`{str(board['pass']).lower()}` | {format_optional_percent(board['worst_gap_pct'])} | {worst_name} |"
        )

    product = scorecard["product_summary"]
    product_worst = product["worst_miss"]["name"] if product["worst_miss"] else "none"
    lines.extend(
        [
            "",
            "| summary | total actionable | missing | operational met | stretch met | operational attainment | stretch attainment | worst gap pct | worst miss |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
            f"| product surface | {product['total_actionable']} | {product['missing_measurements']} | {product['operational_met']} | {product['stretch_met']} | "
            f"{format_optional_percent(product['operational_attainment_pct'])} | {format_optional_percent(product['stretch_attainment_pct'])} | "
            f"{format_optional_percent(product['worst_gap_pct'])} | {product_worst} |",
            "",
            "## Internal Explainers",
            "",
            "Internal explainers are advisory. They never flip `product_pass`, but they stay visible to explain product-surface movement.",
            "",
        ]
    )

    internal = scorecard["scoreboards"]["internal_explainer"]
    if internal["worst_miss"]:
        internal_worst = internal["worst_miss"]
        lines.extend(
            [
                f"- Worst advisory miss: `{internal_worst['name']}`",
                f"- Operational gap: `{format_optional_percent(internal_worst['operational_gap_pct'])}`",
                f"- Current mean: `{format_optional_scalar(round(internal_worst['current_mean_us'], 6) if internal_worst['current_mean_us'] is not None else None)} us`",
                "",
            ]
        )
    else:
        lines.extend(["- No actionable advisory misses.", ""])

    visibility_rows = scorecard["visibility_only"]["targets"]
    lines.extend(
        [
            "## Visibility Only",
            "",
            "These targets stay visible in reports but are excluded from attainment rollups and hotspot selection.",
            "",
        ]
    )

    if not visibility_rows:
        lines.append("None.")
    else:
        lines.extend(
            [
                "| target | class | budget group | gating | note |",
                "|---|---|---|---|---|",
            ]
        )
        for row in visibility_rows:
            lines.append(
                f"| {row['name']} | {row['target_class']} | {row.get('budget_group') or 'n/a'} | {row['gating']} | {row.get('note') or ''} |"
            )

    PERF_SCORECARD_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_perf_scorecard(target_results, perf_targets):
    scoreboards = {}
    for spec in SCOREBOARD_SPECS:
        rows = scoreboard_rows(target_results, spec["target_class"], spec["budget_group"])
        scoreboards[spec["id"]] = build_scoreboard_summary(spec, rows)

    visibility_only = [row for row in target_results if row["gating"] == "informational"]
    variance_blocked = [row for row in target_results if row["status"] == "variance_blocked"]

    scorecard = {
        "version": perf_targets.get("version"),
        "baseline": perf_targets.get("baseline", {}),
        "generated_files": {
            "criterion_csv": str(OUT_CSV),
            "stress_csv": str(STRESS_CSV),
            "benchmark_summary_md": str(OUT_MD),
        },
        "scoreboards": scoreboards,
        "product_summary": build_product_summary(scoreboards, target_results),
        "internal_explainer": scoreboards["internal_explainer"],
        "visibility_only": {
            "count": len(visibility_only),
            "targets": visibility_only,
        },
        "variance_blocked": {
            "count": len(variance_blocked),
            "targets": variance_blocked,
        },
        "target_results": target_results,
    }

    PERF_SCORECARD_JSON.write_text(json.dumps(scorecard, indent=2) + "\n", encoding="utf-8")
    write_perf_scorecard_md(scorecard)


def main():
    criterion_entries = collect_criterion_entries()
    stress_entries = collect_stress_entries()

    write_criterion_csv(criterion_entries)
    write_stress_csv(stress_entries)
    write_bench_summary(criterion_entries, stress_entries)

    if PERF_TARGETS_PATH.exists():
        perf_targets, target_results = build_target_results(criterion_entries, stress_entries)
        write_perf_scorecard(target_results, perf_targets)

    if CRITERION_ROOT.exists():
        print(f"Wrote {OUT_CSV} (criterion) with {len(criterion_entries)} entries.")
    if STRESS_ROOT.exists():
        print(f"Wrote {STRESS_CSV} (stress) with {len(stress_entries)} entries.")
    print(f"Wrote {OUT_MD} (unified summary).")
    if PERF_TARGETS_PATH.exists():
        print(f"Wrote {PERF_SCORECARD_JSON} (rubric scorecard).")
        print(f"Wrote {PERF_SCORECARD_MD} (rubric scorecard markdown).")


if __name__ == "__main__":
    main()
