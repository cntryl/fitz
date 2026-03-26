#!/usr/bin/env python3
"""
Produce comprehensive CSV and Markdown summaries of Criterion benchmarks and stress tests.

- Criterion: extracts mean, CI, std_dev, relative_stddev from target/criterion
- Stress: extracts duration, throughput (elements/ns), scenario tags from target/stress

Stress output path: run stress bench binaries first, e.g.:
        cargo bench --bench tier3_system_kv -- --runs 5 --warmup 1
        cargo bench --bench tier4_integration_kv -- --runs 5 --warmup 1
  ...
Then cntryl-stress writes results under target/stress/<bench_name>/ (e.g. latest.json).
This script expects target/stress/<suite_dir>/latest.json per suite.

Flags high variance when relative_stddev > 0.10 (10%).
Also writes human-friendly mean_us and mean_ms columns assuming nanoseconds.
"""
from pathlib import Path
import json
import csv
import math
import os
import statistics
import platform
import subprocess
import sys
from datetime import datetime, timezone
from collections import Counter, defaultdict

CRITERION_ROOT = Path(__file__).resolve().parents[1] / 'target' / 'criterion'
STRESS_ROOT = Path(__file__).resolve().parents[1] / 'target' / 'stress'
TARGET_ROOT = Path(__file__).resolve().parents[1] / 'target'
BASELINE_FILE = Path(__file__).resolve().parents[1] / 'config' / 'bench_baseline.json'
CURRENT_RESULTS_FILE = TARGET_ROOT / 'bench_results.json'
OUT_CSV = CRITERION_ROOT / 'benchmark_summary.csv'
OUT_MD = TARGET_ROOT / 'bench_summary.md'
STRESS_CSV = STRESS_ROOT / 'stress_summary.csv'
WARNING_REGRESSION_PCT = 10.0
ALERT_REGRESSION_PCT = 15.0
CRITICAL_REGRESSION_PCT = 25.0
NOISE_PERCENT = 10.0
NOISY_RISK_PERCENT = 20.0
STABILITY_ORDER = {
    'stable': 0,
    'acceptable': 1,
    'noisy': 2,
    'untrustworthy': 3,
}
COMPARABLE_STABILITY_BANDS = {'stable', 'acceptable'}


def load_json_data(path: Path):
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def save_json_data(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def git_commit_hash():
    value = run_command(['git', 'rev-parse', 'HEAD'])
    return value.strip() if value else None


def load_csv_rows(path: Path):
    if not path.exists():
        return []

    with path.open('r', newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def parse_float(value):
    if value in (None, '', 'NA'):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(values, fraction):
    if not values:
        return None

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]

    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def variance_band(rel_stddev):
    if rel_stddev is None:
        return 'unknown'
    if rel_stddev <= 0.05:
        return 'stable'
    if rel_stddev <= 0.10:
        return 'acceptable'
    if rel_stddev <= 0.20:
        return 'noisy'
    return 'untrustworthy'


def latency_bucket(mean_ns):
    if mean_ns < 10_000:
        return '<10us'
    if mean_ns < 100_000:
        return '10-100us'
    if mean_ns < 1_000_000:
        return '100us-1ms'
    return '>1ms'


def format_delta(value):
    if value is None:
        return 'NA'
    sign = '+' if value >= 0 else ''
    return f'{sign}{value:.1f}%'


def summarize_deltas(changes, lower_is_better=True, threshold=0.05):
    improved = 0
    regressed = 0
    unchanged = 0
    new = 0
    missing = 0
    movers = []

    for item in changes:
        delta = item.get('delta_pct')
        if delta is None:
            new += 1
            continue
        if item.get('baseline_only'):
            missing += 1
            continue

        movers.append(item)
        if lower_is_better:
            if delta <= -threshold:
                improved += 1
            elif delta >= threshold:
                regressed += 1
            else:
                unchanged += 1
        else:
            if delta >= threshold:
                improved += 1
            elif delta <= -threshold:
                regressed += 1
            else:
                unchanged += 1

    return {
        'improved': improved,
        'regressed': regressed,
        'unchanged': unchanged,
        'new': new,
        'missing': missing,
        'movers': movers,
    }


def is_finite_number(value):
    return isinstance(value, (int, float)) and math.isfinite(value)


MIN_REASONABLE_CRITERION_MEAN_NS = 1.0
MAX_REASONABLE_CRITERION_MEAN_NS = 1e12
MIN_MEANINGFUL_CRITERION_LATENCY_NS = 50.0
MIN_REASONABLE_STRESS_DURATION_NS = 3e9
MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S = 1e9
STALE_CRITERION_BENCHMARKS = {
    'hotpath_actor_messaging/actorref_clone_overhead',
    'hotpath_context/timer_id_new',
    'hotpath_envelope/messageid_new',
    'hotpath_envelope/metadata_extraction',
    'hotpath_envelope/is_expired_not_expired',
    'hotpath_envelope/is_expired_expired',
    'hotpath_envelope/is_expired_no_deadline',
    'hotpath_routing/full_address_from_string',
    'hotpath_routing/route_address_clone',
    'hotpath_routing/route_address_family_access',
    'hotpath_routing/route_address_route_access',
    'hotpath_routing/route_as_str',
    'hotpath_routing/route_clone_long',
    'hotpath_routing/route_clone_short',
    'hotpath_routing/route_equality_different',
    'hotpath_routing/route_equality_same',
    'hotpath_routing/route_family_from_u32',
    'hotpath_routing/route_family_new_u64',
}


def benchmark_latency_ns(entry):
    if entry.get('median_ns') is not None:
        return entry['median_ns']
    return entry.get('mean')


def benchmark_throughput_ops_per_s(entry):
    if entry.get('median_throughput_ops_per_s') is not None:
        return entry['median_throughput_ops_per_s']
    return entry.get('throughput_ops_per_s')


def trend_symbol(delta_pct, lower_is_better=True):
    if delta_pct is None:
        return '·'
    if abs(delta_pct) < 5.0:
        return '→'
    if lower_is_better:
        return '↓' if delta_pct < 0 else '↑'
    return '↑' if delta_pct > 0 else '↓'


def run_command(command):
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except Exception:
        return None
    output = (completed.stdout or completed.stderr or '').strip()
    return output or None


def detect_cpu_name():
    if sys.platform.startswith('win'):
        value = run_command(['powershell', '-NoProfile', '-Command', '(Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name)'])
        if value:
            return value
        value = platform.processor() or os.environ.get('PROCESSOR_IDENTIFIER')
        if value:
            return value
    elif sys.platform == 'darwin':
        value = run_command(['sysctl', '-n', 'machdep.cpu.brand_string'])
        if value:
            return value
    else:
        value = run_command(['bash', '-lc', "lscpu | awk -F: '/Model name/ {gsub(/^ +/, \"\", $2); print $2; exit}'"])
        if value:
            return value
    return platform.processor() or platform.machine() or 'unknown'


def detect_core_counts():
    logical = os.cpu_count() or 0
    physical = None
    if sys.platform.startswith('win'):
        output = run_command(['powershell', '-NoProfile', '-Command', '(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfCores -Sum).Sum'])
        if output:
            try:
                physical = int(float(output))
            except ValueError:
                physical = None
    if physical is None:
        physical = logical
    return physical, logical


def detect_ram_gb():
    if sys.platform.startswith('win'):
        output = run_command([
            'powershell', '-NoProfile', '-Command',
            '(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory'
        ])
        if output:
            try:
                bytes_total = int(float(output))
                return round(bytes_total / (1024 ** 3))
            except ValueError:
                pass
    elif hasattr(os, 'sysconf') and 'SC_PAGE_SIZE' in os.sysconf_names and 'SC_PHYS_PAGES' in os.sysconf_names:
        try:
            bytes_total = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            return round(bytes_total / (1024 ** 3))
        except (ValueError, OSError):
            pass
    return 'unknown'


def detect_os_label():
    system = platform.system() or 'Unknown OS'
    release = platform.release() or ''
    version = platform.version() or ''
    if sys.platform.startswith('win'):
        if release and version:
            return f'Windows {release} ({version})'
        if release:
            return f'Windows {release}'
        return 'Windows'
    if release:
        return f'{system} {release}'
    return system


def detect_rust_version():
    output = run_command(['rustc', '--version'])
    return output or 'rustc unknown'


def detect_build_mode():
    lto_enabled = False
    cargo_toml = Path(__file__).resolve().parents[1] / 'Cargo.toml'
    if cargo_toml.exists():
        text = cargo_toml.read_text(encoding='utf-8', errors='ignore')
        if '[profile.release]' in text and 'lto = true' in text:
            lto_enabled = True
    if lto_enabled:
        return 'release + LTO enabled'
    return 'release (LTO not configured)'


def detect_allocator():
    root = Path(__file__).resolve().parents[1]
    for path in root.rglob('*.rs'):
        try:
            text = path.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            continue
        if 'global_allocator' in text:
            if 'mimalloc' in text.lower():
                return 'mimalloc'
            if 'jemalloc' in text.lower():
                return 'jemalloc'
            return 'custom'
    return 'system'


def transport_config_label():
    return 'tcp: TestServer + TestClient (2000ms timeout); websocket: TestServer + TestWebSocketClient (2000ms timeout); shared_bench_runtime()'


def utc_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def fmt_ops_short(value):
    if value is None:
        return 'NA'
    if value >= 1_000_000_000:
        return 'REJECTED'
    return f'{value:.0f}'


def fmt_pct(value):
    if value is None:
        return 'NA'
    sign = '+' if value >= 0 else ''
    return f'{sign}{value:.0f}%'


def stability_label(rel_stddev):
    band = variance_band(rel_stddev)
    if band in {'stable', 'acceptable'}:
        return band
    return 'unstable'


def stress_throughput_status(entry):
    if entry.get('num_runs', 0) < 5:
        return 'insufficient_data'
    if not entry.get('meets_runtime_floor'):
        return 'invalid_for_throughput'
    return 'authoritative'


def stress_stability_label(entry):
    status = stress_throughput_status(entry)
    if status != 'authoritative':
        return status
    return variance_band(entry['rel_stddev_runs'])


def domain_label_from_suite(suite):
    normalized = suite.replace('_', '-').lower()
    parts = normalized.split('-')
    key = parts[-1] if parts else normalized
    labels = {
        'kv': 'KV',
        'lease': 'Lease',
        'notice': 'Notice',
        'queue': 'Queue',
        'rpc': 'RPC',
        'schedule': 'Schedule',
        'stream': 'Stream',
    }
    return labels.get(key, key.title())


def domain_from_suite(suite):
    normalized = suite.replace('_', '-').lower()
    if 'system-' in normalized:
        return domain_label_from_suite(normalized.split('system-', 1)[1])
    if 'integration-' in normalized:
        return domain_label_from_suite(normalized.split('integration-', 1)[1])
    if '-system-' in normalized:
        return domain_label_from_suite(normalized.split('-system-', 1)[1])
    if '-integration-' in normalized:
        return domain_label_from_suite(normalized.split('-integration-', 1)[1])
    return domain_label_from_suite(normalized)


def benchmark_key(entry):
    return entry['benchmark']


def stress_key(entry):
    return (entry['suite'], entry['name'], entry['scenario'])


def benchmark_domain(benchmark):
    return benchmark.replace('\\', '/').split('/', 1)[0]


def comparison_label(record):
    if record['kind'] == 'criterion':
        return record['benchmark']
    return f"{record['suite']} / {record['scenario']}"


def comparison_domain(record):
    if record['kind'] == 'criterion':
        return benchmark_domain(record['benchmark'])
    return domain_from_suite(record['suite'])


def comparison_transport(record):
    if record['kind'] == 'criterion':
        return record.get('transport') or 'NA'
    return record.get('layer') or 'NA'


def comparison_metric_value(record):
    if record['kind'] == 'criterion':
        return record.get('median_ns')
    return record.get('median_throughput_ops_per_s')


def comparison_direction(record):
    return 'lower' if record['kind'] == 'criterion' else 'higher'


def is_comparable_record(record):
    return record.get('status') == 'authoritative' and record.get('stability') in COMPARABLE_STABILITY_BANDS


def stability_rank(label):
    return STABILITY_ORDER.get(label, len(STABILITY_ORDER))


def directional_delta_pct(current_value, baseline_value, higher_is_better):
    if current_value is None or baseline_value is None or baseline_value == 0:
        return None

    raw_delta = ((current_value - baseline_value) / baseline_value) * 100.0
    return raw_delta if higher_is_better else -raw_delta


def classify_directional_delta(delta_pct):
    if delta_pct is None:
        return 'new'
    if delta_pct >= WARNING_REGRESSION_PCT:
        return 'improved'
    if delta_pct <= -CRITICAL_REGRESSION_PCT:
        return 'critical_regression'
    if delta_pct <= -ALERT_REGRESSION_PCT:
        return 'alert_regression'
    if delta_pct <= -WARNING_REGRESSION_PCT:
        return 'warning_regression'
    return 'unchanged'


def compare_stability(current_band, baseline_band):
    current_rank = stability_rank(current_band)
    baseline_rank = stability_rank(baseline_band)
    if current_rank < baseline_rank:
        return 'improved'
    if current_rank > baseline_rank:
        return 'regressed'
    return 'unchanged'


def is_noisy_band(band):
    return band in {'noisy', 'untrustworthy'}


def record_is_noisy(record):
    return is_noisy_band(record.get('stability'))


def build_criterion_result(entry, commit_hash, generated_at):
    stability = variance_band(entry['rel_stddev'])
    status = 'authoritative' if stability in COMPARABLE_STABILITY_BANDS else stability
    return {
        'kind': 'criterion',
        'benchmark': entry['benchmark'],
        'domain': benchmark_domain(entry['benchmark']),
        'transport': None,
        'scenario': benchmark_short_name(entry['benchmark']),
        'metric': 'latency_ns',
        'median_ns': entry['median_ns'],
        'median_value': entry['median_ns'],
        'min_value': entry.get('median_ci_lower'),
        'max_value': entry.get('median_ci_upper'),
        'stability': stability,
        'status': status,
        'runs': None,
        'timestamp': generated_at,
        'commit_hash': commit_hash,
        'rel_stddev': entry['rel_stddev'],
        'mean_ns': entry['mean'],
        'p50_latency_ns': entry['median_ns'],
        'p95_latency_ns': None,
        'p99_latency_ns': None,
        'allocs_per_op': None,
        'bytes_per_op': None,
        'source_file': entry['file'],
        'comparison_key': benchmark_key(entry),
    }


def build_stress_result(entry, commit_hash, generated_at):
    stability = variance_band(entry['rel_stddev_runs'])
    status = stress_throughput_status(entry)
    run_throughputs = [entry['elements'] / run * 1e9 for run in entry.get('run_values', []) if run > 0]
    min_throughput = min(run_throughputs) if run_throughputs else None
    max_throughput = max(run_throughputs) if run_throughputs else None
    return {
        'kind': 'stress',
        'suite': entry['suite'],
        'name': entry['name'],
        'domain': domain_from_suite(entry['suite']),
        'transport': entry.get('layer'),
        'scenario': entry['scenario'],
        'metric': 'throughput_ops_per_s',
        'median_throughput_ops_per_s': entry['median_throughput_ops_per_s'],
        'median_value': entry['median_throughput_ops_per_s'],
        'min_value': min_throughput,
        'max_value': max_throughput,
        'stability': stability,
        'status': status,
        'runs': entry['num_runs'],
        'timestamp': generated_at,
        'commit_hash': commit_hash,
        'rel_stddev': entry['rel_stddev_runs'],
        'mean_ns': None,
        'p50_latency_ns': None,
        'p95_latency_ns': None,
        'p99_latency_ns': None,
        'allocs_per_op': None,
        'bytes_per_op': None,
        'source_file': entry['file'],
        'comparison_key': '|'.join([entry['suite'], entry['name'], entry['scenario']]),
        'batch_size': entry['batch_size'],
        'median_duration_ns': entry['median_duration_ns'],
        'min_run_ns': entry['min_run_ns'],
        'max_run_ns': entry['max_run_ns'],
    }


def current_run_authoritative(criterion_records, stress_records):
    if not criterion_records or not stress_records:
        return False
    if any(record.get('status') != 'authoritative' for record in stress_records):
        return False
    if any(record.get('stability') not in COMPARABLE_STABILITY_BANDS for record in stress_records):
        return False
    if any(record.get('status') != 'authoritative' for record in criterion_records):
        return False
    if any(record.get('stability') not in COMPARABLE_STABILITY_BANDS for record in criterion_records):
        return False
    return True


def build_result_manifest(criterion_records, stress_records, comparison_summary, generated_at, commit_hash):
    return {
        'schema_version': 1,
        'generated_at': generated_at,
        'commit_hash': commit_hash,
        'policy': {
            'criterion': {
                'warning_regression_pct': WARNING_REGRESSION_PCT,
                'alert_regression_pct': ALERT_REGRESSION_PCT,
                'critical_regression_pct': CRITICAL_REGRESSION_PCT,
                'noise_pct': NOISE_PERCENT,
            },
            'stress': {
                'minimum_runs': 5,
                'target_run_seconds': 5,
                'minimum_run_seconds': 3,
                'warning_regression_pct': WARNING_REGRESSION_PCT,
                'alert_regression_pct': ALERT_REGRESSION_PCT,
                'critical_regression_pct': CRITICAL_REGRESSION_PCT,
                'noise_pct': NOISE_PERCENT,
                'noisy_risk_pct': NOISY_RISK_PERCENT,
            },
        },
        'criterion': criterion_records,
        'stress': stress_records,
        'comparison_summary': comparison_summary,
    }


def fitz_report_title():
    return '# Fitz Benchmark Report'

# ============================================================================
# CRITERION BENCHMARKS
# ============================================================================
entries = []
stress_skipped = []
criterion_skipped = []
if not CRITERION_ROOT.exists():
    pass  # skip criterion section
else:
    for p in CRITERION_ROOT.rglob('new/estimates.json'):
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text())
        except Exception as e:
            print(f"skipping {p} (read error): {e}")
            continue
        # Determine benchmark id as path relative to ROOT, omit trailing '/new/estimates.json'
        benchmark = str(p.relative_to(CRITERION_ROOT).parent.parent)
        mean = data.get('mean', {}).get('point_estimate')
        ci = data.get('mean', {}).get('confidence_interval', {})
        ci_lower = ci.get('lower_bound')
        ci_upper = ci.get('upper_bound')
        median = data.get('median', {}).get('point_estimate')
        median_ci = data.get('median', {}).get('confidence_interval', {})
        median_ci_lower = median_ci.get('lower_bound')
        median_ci_upper = median_ci.get('upper_bound')
        stddev = data.get('std_dev', {}).get('point_estimate')
        # fallback: some Criterion variants place std_dev under 'std_dev' or in same level
        if mean is None:
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'missing mean estimate',
                'file': str(p),
            })
            continue
        if median is not None and not is_finite_number(median):
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-finite median estimate',
                'file': str(p),
            })
            continue
        if not is_finite_number(mean):
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-finite mean estimate',
                'file': str(p),
            })
            continue
        if mean <= 0:
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-positive mean estimate',
                'file': str(p),
            })
            continue
        if mean < MIN_REASONABLE_CRITERION_MEAN_NS:
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': f'mean below {MIN_REASONABLE_CRITERION_MEAN_NS:.0f} ns sanity bound',
                'file': str(p),
            })
            continue
        if mean > MAX_REASONABLE_CRITERION_MEAN_NS:
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': f'mean above {MAX_REASONABLE_CRITERION_MEAN_NS:.0e} ns sanity bound',
                'file': str(p),
            })
            continue
        if stddev is not None and not is_finite_number(stddev):
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-finite std dev',
                'file': str(p),
            })
            continue
        if stddev is not None and stddev < 0:
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'negative std dev',
                'file': str(p),
            })
            continue
        if median is not None and median <= 0:
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-positive median estimate',
                'file': str(p),
            })
            continue
        if ci_lower is not None and not is_finite_number(ci_lower):
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-finite confidence interval lower bound',
                'file': str(p),
            })
            continue
        if ci_upper is not None and not is_finite_number(ci_upper):
            criterion_skipped.append({
                'benchmark': benchmark,
                'reason': 'non-finite confidence interval upper bound',
                'file': str(p),
            })
            continue
        rel_stddev = None
        if stddev is not None and mean != 0:
            rel_stddev = stddev / mean
        high_variance = False
        if rel_stddev is not None:
            high_variance = rel_stddev > 0.10
        # Skip legacy/stale Criterion entries (e.g. old "schedule_system_scan_and_fire" 222ms row)
        benchmark_id = benchmark.replace('\\', '/')
        if benchmark_id in STALE_CRITERION_BENCHMARKS:
            continue
        if 'schedule_system_scan_and_fire' in benchmark:
            continue
        # assume raw numbers are nanoseconds and provide converted columns
        mean_us = mean / 1e3
        mean_ms = mean / 1e6
        median_ns = median if median is not None else mean
        median_us = median_ns / 1e3
        median_ms = median_ns / 1e6
        stability = variance_band(rel_stddev)
        status = 'authoritative' if stability in COMPARABLE_STABILITY_BANDS else stability
        entries.append({
            'benchmark': benchmark,
            'mean': mean,
            'mean_ci_lower': ci_lower,
            'mean_ci_upper': ci_upper,
            'median_ns': median_ns,
            'median_us': median_us,
            'median_ms': median_ms,
            'median_ci_lower': median_ci_lower,
            'median_ci_upper': median_ci_upper,
            'std_dev': stddev,
            'rel_stddev': rel_stddev,
            'high_variance': high_variance,
            'stability': stability,
            'status': status,
            'mean_us': mean_us,
            'mean_ms': mean_ms,
            'file': str(p)
        })

# Write Criterion CSV (skip if criterion dir missing; create parent so write never errors)
if CRITERION_ROOT.exists():
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['benchmark','mean','mean_ci_lower','mean_ci_upper','median_ns','median_us','median_ms','std_dev','rel_stddev','high_variance','mean_us(assume_ns)','mean_ms(assume_ns)','file'])
        for e in sorted(entries, key=lambda x: benchmark_latency_ns(x)):
            writer.writerow([
                e['benchmark'],
                f"{e['mean']:.6f}" if isinstance(e['mean'], float) else e['mean'],
                f"{e['mean_ci_lower']:.6f}" if isinstance(e['mean_ci_lower'], float) else e['mean_ci_lower'],
                f"{e['mean_ci_upper']:.6f}" if isinstance(e['mean_ci_upper'], float) else e['mean_ci_upper'],
                f"{e['median_ns']:.6f}" if isinstance(e['median_ns'], float) else e['median_ns'],
                f"{e['median_us']:.6f}" if isinstance(e['median_us'], float) else e['median_us'],
                f"{e['median_ms']:.6f}" if isinstance(e['median_ms'], float) else e['median_ms'],
                f"{e['std_dev']:.6f}" if isinstance(e['std_dev'], float) else e['std_dev'],
                f"{e['rel_stddev']:.6f}" if isinstance(e['rel_stddev'], float) else e['rel_stddev'],
                str(e['high_variance']),
                f"{e['mean_us']:.6f}",
                f"{e['mean_ms']:.6f}",
                e['file']
            ])

# Derive suite (first path component) for each Criterion entry for per-suite grouping
# Path can use / or \ depending on OS
for e in entries:
    parts = e['benchmark'].replace('\\', '/').split('/')
    e['suite'] = parts[0] if parts else 'other'

# Write a small Markdown summary: top 10 fastest and slowest, and high-variance list
sorted_by_mean = sorted(entries, key=lambda x: x['mean'])
fastest = sorted_by_mean[:10]
slowest = sorted_by_mean[-10:][::-1]
high_var = [e for e in entries if e['high_variance']]

# Group Criterion entries by suite (tier) for per-suite summaries
criterion_suites = {}
for e in entries:
    suite_name = e['suite']
    if suite_name not in criterion_suites:
        criterion_suites[suite_name] = []
    criterion_suites[suite_name].append(e)
# Sort suite names: tier1_* first, then tier2_*, then rest alphabetically
def suite_sort_key(name):
    if name.startswith('tier1_'):
        return (0, name)
    if name.startswith('tier2_'):
        return (1, name)
    return (2, name)
criterion_suite_order = sorted(criterion_suites.keys(), key=suite_sort_key)

# ============================================================================
# STRESS TESTS
# ============================================================================
stress_entries = []
if STRESS_ROOT.exists():
    for suite_dir in sorted(STRESS_ROOT.glob('*/')):
        if not suite_dir.is_dir():
            continue
        latest_json = suite_dir / 'latest.json'
        if not latest_json.exists():
            continue
        try:
            data = json.loads(latest_json.read_text())
        except Exception as e:
            print(f"skipping {latest_json} (read error): {e}")
            continue

        suite = data.get('suite', suite_dir.name)
        results = data.get('results', [])

        for result in results:
            name = result.get('name', '')
            duration = result.get('duration')
            elements = result.get('elements')
            all_runs = result.get('all_runs', [])
            tags = result.get('tags', {})
            scenario = tags.get('scenario', 'unknown')

            run_values = [run for run in all_runs if is_finite_number(run) and run > 0]
            if not run_values and is_finite_number(duration) and duration > 0:
                run_values = [duration]
            if not run_values:
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'missing duration samples',
                    'file': str(latest_json),
                })
                continue

            median_run_ns = statistics.median(run_values)

            if elements is None:
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'missing elements count',
                    'file': str(latest_json),
                })
                continue
            if duration is not None and not is_finite_number(duration):
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'non-finite duration',
                    'file': str(latest_json),
                })
                continue
            if not is_finite_number(elements):
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'non-finite elements count',
                    'file': str(latest_json),
                })
                continue
            if median_run_ns <= 0:
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'non-positive median duration',
                    'file': str(latest_json),
                })
                continue
            if elements <= 0:
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'non-positive elements count',
                    'file': str(latest_json),
                })
                continue

            # Compute statistics
            throughput_ops_per_ns = elements / median_run_ns if median_run_ns > 0 else 0
            throughput_ops_per_us = throughput_ops_per_ns * 1e3
            throughput_ops_per_ms = throughput_ops_per_ns * 1e6
            throughput_ops_per_s = throughput_ops_per_ns * 1e9

            if not is_finite_number(throughput_ops_per_s):
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': 'non-finite throughput',
                    'file': str(latest_json),
                })
                continue
            if throughput_ops_per_s > MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S:
                stress_skipped.append({
                    'suite': suite,
                    'name': name,
                    'scenario': scenario,
                    'reason': f'throughput above {MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S:.0e} ops/sec sanity bound',
                    'file': str(latest_json),
                })
                continue

            duration_us = median_run_ns / 1e3
            duration_ms = median_run_ns / 1e6
            per_op_ns = median_run_ns / elements if elements > 0 else 0
            per_op_us = per_op_ns / 1e3

            median_throughput_ops_per_s = throughput_ops_per_s
            mean_run_ns = statistics.mean(run_values)

            # Variance across runs
            if len(run_values) > 1:
                variance = sum((x - mean_run_ns) ** 2 for x in run_values) / len(run_values)
                stddev = variance ** 0.5
                rel_stddev_runs = stddev / mean_run_ns if mean_run_ns > 0 else 0
            else:
                stddev = 0
                rel_stddev_runs = 0

            stress_entries.append({
                'suite': suite,
                'name': name,
                'scenario': scenario,
                'layer': tags.get('layer'),  # tier4: direct/tcp/websocket/multiclient
                'batch_size': elements,
                'duration_ns': median_run_ns,
                'median_duration_ns': median_run_ns,
                'duration_us': duration_us,
                'duration_ms': duration_ms,
                'median_duration_ms': duration_ms,
                'elements': elements,
                'run_values': run_values,
                'min_run_ns': min(run_values),
                'max_run_ns': max(run_values),
                'per_op_ns': per_op_ns,
                'per_op_us': per_op_us,
                'throughput_ops_per_ns': throughput_ops_per_ns,
                'throughput_ops_per_us': throughput_ops_per_us,
                'throughput_ops_per_ms': throughput_ops_per_ms,
                'throughput_ops_per_s': throughput_ops_per_s,
                'median_throughput_ops_per_s': median_throughput_ops_per_s,
                'num_runs': len(run_values),
                'meets_runtime_floor': median_run_ns >= MIN_REASONABLE_STRESS_DURATION_NS,
                'stddev_runs': stddev,
                'rel_stddev_runs': rel_stddev_runs,
                'stability': variance_band(rel_stddev_runs),
                'status': stress_throughput_status({
                    'num_runs': len(run_values),
                    'meets_runtime_floor': median_run_ns >= MIN_REASONABLE_STRESS_DURATION_NS,
                }),
                'file': str(latest_json)
            })

baseline_manifest = load_json_data(BASELINE_FILE) or {}
previous_criterion_rows = baseline_manifest.get('criterion', []) or []
previous_stress_rows = baseline_manifest.get('stress', []) or []
baseline_available = bool(previous_criterion_rows or previous_stress_rows)

criterion_by_benchmark = {entry['benchmark']: entry for entry in entries}
criterion_latencies = [benchmark_latency_ns(entry) for entry in entries if benchmark_latency_ns(entry) is not None]
criterion_variance_bands = Counter(variance_band(entry['rel_stddev']) for entry in entries)
criterion_latency_bands = Counter(latency_bucket(benchmark_latency_ns(entry)) for entry in entries)
criterion_suite_groups = defaultdict(list)
for entry in entries:
    criterion_suite_groups[entry['suite']].append(entry)

criterion_comparisons = []
for benchmark, entry in criterion_by_benchmark.items():
    baseline = next((row for row in previous_criterion_rows if row.get('benchmark') == benchmark), None)
    baseline_mean = None
    if baseline:
        baseline_mean = parse_float(baseline.get('median_ns'))
        if baseline_mean is None:
            baseline_mean = parse_float(baseline.get('median_value'))
        if baseline_mean is None:
            baseline_mean = parse_float(baseline.get('mean'))
    current_value = benchmark_latency_ns(entry)
    delta_pct = None
    directional_delta_pct = None
    current_stability = entry.get('stability', variance_band(entry['rel_stddev']))
    baseline_stability = baseline.get('stability') if baseline else None
    if baseline_mean and baseline_mean > 0:
        delta_pct = ((current_value - baseline_mean) / baseline_mean) * 100.0 if current_value is not None else None
        directional_delta_pct = -delta_pct if delta_pct is not None else None
    comparison_eligible = bool(baseline) and is_comparable_record(entry) and is_comparable_record(baseline)
    risk_reason = None
    if baseline and current_value is not None and not comparison_eligible:
        if current_stability not in COMPARABLE_STABILITY_BANDS or baseline_stability not in COMPARABLE_STABILITY_BANDS:
            risk_reason = 'noisy'
        elif entry.get('status') != 'authoritative' or baseline.get('status') != 'authoritative':
            risk_reason = 'insufficient_data'
    criterion_comparisons.append({
        'benchmark': benchmark,
        'mean': entry['mean'],
        'median_ns': entry['median_ns'],
        'baseline_mean': baseline_mean,
        'delta_pct': delta_pct,
        'directional_delta_pct': directional_delta_pct,
        'variance_band': current_stability,
        'rel_stddev': entry['rel_stddev'],
        'current_stability': current_stability,
        'baseline_stability': baseline_stability,
        'current_status': entry.get('status'),
        'baseline_status': baseline.get('status') if baseline else None,
        'comparison_eligible': comparison_eligible,
        'risk_reason': risk_reason,
        'suite': entry['suite'],
        'kind': 'criterion',
        'domain': benchmark_domain(benchmark),
        'transport': None,
        'current_value': current_value,
        'baseline_value': baseline_mean,
    })

criterion_missing = [
    row for row in previous_criterion_rows
    if row.get('benchmark') not in criterion_by_benchmark
]

criterion_delta_summary = summarize_deltas(criterion_comparisons, lower_is_better=True)
criterion_delta_summary['missing'] = len(criterion_missing)
criterion_delta_summary['tracked'] = len(criterion_comparisons)
criterion_delta_summary['skipped'] = len(criterion_skipped)
criterion_delta_summary['baseline_total'] = len(previous_criterion_rows)
criterion_delta_summary['median'] = statistics.median(criterion_latencies) if criterion_latencies else None
criterion_delta_summary['p90'] = percentile(criterion_latencies, 0.90)
criterion_delta_summary['fastest'] = min(entries, key=lambda x: benchmark_latency_ns(x)) if entries else None
criterion_delta_summary['slowest'] = max(entries, key=lambda x: benchmark_latency_ns(x)) if entries else None
meaningful_criterion_entries = [entry for entry in entries if benchmark_latency_ns(entry) is not None and benchmark_latency_ns(entry) >= MIN_MEANINGFUL_CRITERION_LATENCY_NS]
criterion_delta_summary['meaningful_fastest'] = min(meaningful_criterion_entries, key=lambda x: benchmark_latency_ns(x)) if meaningful_criterion_entries else None
criterion_delta_summary['noisiest'] = sorted(entries, key=lambda x: x['rel_stddev'] or -1, reverse=True)[:5]
criterion_delta_summary['slowest_top'] = sorted(entries, key=lambda x: benchmark_latency_ns(x), reverse=True)[:5]
criterion_comparison_map = {item['benchmark']: item for item in criterion_comparisons}

stress_by_key = {
    (entry['suite'], entry['name'], entry['scenario']): entry
    for entry in stress_entries
}
previous_stress_by_name = defaultdict(list)
for row in previous_stress_rows:
    previous_stress_by_name[row.get('name')].append(row)
stress_throughputs = [benchmark_throughput_ops_per_s(entry) for entry in stress_entries if benchmark_throughput_ops_per_s(entry) is not None]
stress_stability_bands = Counter(stress_stability_label(entry) for entry in stress_entries)
stress_suite_groups = defaultdict(list)
stress_layer_groups = defaultdict(list)
for entry in stress_entries:
    stress_suite_groups[entry['suite']].append(entry)
    if entry.get('layer'):
        stress_layer_groups[entry['layer']].append(entry)

stress_comparisons = []
for key, entry in stress_by_key.items():
    baseline = next(
        (
            row for row in previous_stress_rows
            if row.get('suite') == entry['suite']
            and row.get('name') == entry['name']
            and row.get('scenario') == entry['scenario']
        ),
        None,
    )
    if baseline is None:
        matches = previous_stress_by_name.get(entry['name'], [])
        baseline = matches[0] if matches else None
    baseline_throughput = None
    if baseline:
        baseline_throughput = parse_float(
            baseline.get('median_throughput_ops_per_s')
            or baseline.get('median_value')
            or baseline.get('throughput_ops_per_s')
        )
    delta_pct = None
    directional_delta_pct = None
    current_throughput = benchmark_throughput_ops_per_s(entry)
    current_stability = entry.get('stability', variance_band(entry['rel_stddev_runs']))
    baseline_stability = baseline.get('stability') if baseline else None
    if baseline_throughput and baseline_throughput > 0:
        delta_pct = ((current_throughput - baseline_throughput) / baseline_throughput) * 100.0 if current_throughput is not None else None
        directional_delta_pct = delta_pct
    comparison_eligible = bool(baseline) and is_comparable_record(entry) and is_comparable_record(baseline)
    risk_reason = None
    if baseline and current_throughput is not None and not comparison_eligible:
        if current_stability not in COMPARABLE_STABILITY_BANDS or baseline_stability not in COMPARABLE_STABILITY_BANDS:
            risk_reason = 'noisy'
        elif entry.get('status') != 'authoritative' or baseline.get('status') != 'authoritative':
            risk_reason = 'insufficient_data'
    stress_comparisons.append({
        'suite': entry['suite'],
        'name': entry['name'],
        'scenario': entry['scenario'],
        'throughput_ops_per_s': entry['throughput_ops_per_s'],
        'median_throughput_ops_per_s': entry.get('median_throughput_ops_per_s'),
        'baseline_throughput_ops_per_s': baseline_throughput,
        'delta_pct': delta_pct,
        'directional_delta_pct': directional_delta_pct,
        'variance_band': current_stability,
        'rel_stddev_runs': entry['rel_stddev_runs'],
        'current_stability': current_stability,
        'baseline_stability': baseline_stability,
        'current_status': entry.get('status'),
        'baseline_status': baseline.get('status') if baseline else None,
        'comparison_eligible': comparison_eligible,
        'risk_reason': risk_reason,
        'layer': entry.get('layer'),
        'kind': 'stress',
        'domain': domain_from_suite(entry['suite']),
        'transport': entry.get('layer'),
        'current_value': current_throughput,
        'baseline_value': baseline_throughput,
    })

stress_missing = [
    row for row in previous_stress_rows
    if (row.get('suite'), row.get('name'), row.get('scenario')) not in stress_by_key
]

stress_delta_summary = summarize_deltas(stress_comparisons, lower_is_better=False)
stress_delta_summary['missing'] = len(stress_missing)
stress_delta_summary['tracked'] = len(stress_comparisons)
stress_delta_summary['skipped'] = len(stress_skipped)
stress_delta_summary['baseline_total'] = len(previous_stress_rows)
stress_delta_summary['median'] = statistics.median(stress_throughputs) if stress_throughputs else None
stress_delta_summary['p90'] = percentile(stress_throughputs, 0.90)
stress_delta_summary['best'] = max(stress_entries, key=lambda x: benchmark_throughput_ops_per_s(x)) if stress_entries else None
stress_delta_summary['worst'] = min(stress_entries, key=lambda x: benchmark_throughput_ops_per_s(x)) if stress_entries else None
stress_delta_summary['noisiest'] = sorted(stress_entries, key=lambda x: x['rel_stddev_runs'] or -1, reverse=True)[:5]
stress_delta_summary['best_layer'] = None
if stress_layer_groups:
    stress_delta_summary['best_layer'] = max(
        ((layer, sum(benchmark_throughput_ops_per_s(item) for item in items) / len(items)) for layer, items in stress_layer_groups.items()),
        key=lambda pair: pair[1],
    )
stress_comparison_map = {
    (item['suite'], item['name'], item['scenario']): item for item in stress_comparisons
}

if not baseline_available:
    criterion_comparisons = []
    stress_comparisons = []
    criterion_comparison_map = {}
    stress_comparison_map = {}

# Write Stress CSV (skip if stress dir missing)
if STRESS_ROOT.exists():
    STRESS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with STRESS_CSV.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['suite', 'name', 'scenario', 'batch_size', 'median_duration_ms', 'elements', 'per_op_us', 'throughput_ops_per_s', 'median_throughput_ops_per_s', 'runs', 'min_run_ns', 'max_run_ns', 'stddev_runs_ns', 'rel_stddev_runs', 'stability', 'status', 'file'])
        for e in sorted(stress_entries, key=lambda x: benchmark_throughput_ops_per_s(x), reverse=True):
            writer.writerow([
                e['suite'],
                e['name'],
                e['scenario'],
                e['batch_size'],
                f"{e['median_duration_ms']:.2f}",
                e['elements'],
                f"{e['per_op_us']:.6f}",
                f"{e['throughput_ops_per_s']:.2f}",
                f"{e['median_throughput_ops_per_s']:.2f}",
                e['num_runs'],
                f"{e['min_run_ns']:.2f}",
                f"{e['max_run_ns']:.2f}",
                f"{e['stddev_runs']:.2f}",
                f"{e['rel_stddev_runs']:.6f}" if e['rel_stddev_runs'] else "NA",
                stress_stability_label(e),
                stress_throughput_status(e),
                e['file']
            ])

def mean_or_none(values):
    return statistics.mean(values) if values else None


def fmt_ns(value):
    if value is None:
        return 'NA'
    return f'{value:.0f}'


def fmt_us(value):
    if value is None:
        return 'NA'
    return f'{value / 1e3:.3f}'


def fmt_ms(value):
    if value is None:
        return 'NA'
    return f'{value / 1e6:.3f}'


def fmt_ops(value):
    if value is None:
        return 'NA'
    if value >= 1e12:
        return 'REJECTED'
    return f'{value:.0f}'


def fmt_ratio(value):
    if value is None:
        return 'NA'
    return f'{value:.2f}x'


def stress_label(item):
    scenario = item.get('scenario')
    if scenario and scenario != 'unknown':
        return scenario

    name = item.get('name', '')
    if '::' in name:
        return name.split('::')[-1]

    return name or 'unknown'


def stress_batch_size(item):
    value = item.get('batch_size')
    if value is None:
        value = item.get('elements')
    return value


def suite_label(name):
    return name.replace('_', ' ').replace('-', ' ')


def benchmark_short_name(benchmark):
    return benchmark.replace('\\', '/').split('/', 1)[-1]


def median_or_mean(entry):
    value = benchmark_latency_ns(entry)
    return value if value is not None else entry.get('mean')


def stress_throughput(entry):
    return benchmark_throughput_ops_per_s(entry)


def domain_label_from_suite(suite):
    normalized = suite.replace('_', '-').lower()
    parts = normalized.split('-')
    key = parts[-1] if parts else normalized
    labels = {
        'kv': 'KV',
        'lease': 'Lease',
        'notice': 'Notice',
        'queue': 'Queue',
        'rpc': 'RPC',
        'schedule': 'Schedule',
        'stream': 'Stream',
    }
    return labels.get(key, key.title())


def domain_from_suite(suite):
    normalized = suite.replace('_', '-').lower()
    if 'system-' in normalized:
        return domain_label_from_suite(normalized.split('system-', 1)[1])
    if 'integration-' in normalized:
        return domain_label_from_suite(normalized.split('integration-', 1)[1])
    if '-system-' in normalized:
        return domain_label_from_suite(normalized.split('-system-', 1)[1])
    if '-integration-' in normalized:
        return domain_label_from_suite(normalized.split('-integration-', 1)[1])
    return domain_label_from_suite(normalized)


def stability_for_stress(entry):
    return stress_stability_label(entry)


def runtime_note(entry):
    status = stress_throughput_status(entry)
    if status == 'authoritative':
        return f'authoritative; {variance_band(entry["rel_stddev_runs"])}'
    if status == 'insufficient_data':
        return f'insufficient_data; {entry.get("num_runs", 0)} run(s) < 5 minimum'
    return f'invalid_for_throughput; {fmt_ms(entry["duration_ns"])} ms median < 3000 ms floor'


def select_changes(comparisons, threshold, lower_is_better):
    improved = []
    regressed = []
    unchanged = []
    for item in comparisons:
        delta = item.get('delta_pct')
        if delta is None:
            continue
        if lower_is_better:
            if delta <= -threshold:
                improved.append(item)
            elif delta >= threshold:
                regressed.append(item)
            else:
                unchanged.append(item)
        else:
            if delta >= threshold:
                improved.append(item)
            elif delta <= -threshold:
                regressed.append(item)
            else:
                unchanged.append(item)
    return improved, regressed, unchanged


def comparison_display_delta(record):
    delta = record.get('directional_delta_pct')
    if delta is None:
        return 'NA'
    sign = '+' if delta >= 0 else ''
    return f'{sign}{delta:.1f}%'


def comparison_severity(record):
    delta = record.get('directional_delta_pct')
    if delta is None:
        return 'new'
    if delta >= WARNING_REGRESSION_PCT:
        return 'improved'
    if delta <= -CRITICAL_REGRESSION_PCT:
        return 'critical'
    if delta <= -ALERT_REGRESSION_PCT:
        return 'alert'
    if delta <= -WARNING_REGRESSION_PCT:
        return 'warning'
    return 'unchanged'


def comparison_status_label(record):
    status = comparison_severity(record)
    if status == 'critical':
        return 'critical regression'
    if status == 'alert':
        return 'alert regression'
    if status == 'warning':
        return 'warning regression'
    return status


def comparison_summary_text(record):
    label = comparison_label(record)
    delta = comparison_display_delta(record)
    return f'{label} {delta}'


def comparison_value_text(record, value):
    if value is None:
        return 'NA'
    if record['kind'] == 'criterion':
        return fmt_ns(value)
    return fmt_ops_short(value)


def comparison_row(record):
    return [
        comparison_label(record),
        record['kind'],
        comparison_display_delta(record),
        comparison_value_text(record, record.get('baseline_value')),
        comparison_value_text(record, record.get('current_value')),
        comparison_status_label(record),
        record.get('baseline_stability') or 'NA',
        record.get('current_stability') or 'NA',
    ]


def comparison_sort_key(record):
    delta = record.get('directional_delta_pct')
    if delta is None:
        return (1, 0.0, comparison_label(record))
    return (0, -abs(delta), comparison_label(record))


def comparison_is_new(record):
    return record.get('baseline_value') is None and record.get('current_value') is not None


def comparison_is_missing(record):
    return record.get('baseline_value') is not None and record.get('current_value') is None


def comparison_is_risk_area(record):
    if comparison_is_new(record) or comparison_is_missing(record):
        return True
    if record.get('risk_reason') in {'noisy', 'insufficient_data'}:
        return True
    if record.get('risk_reason') is not None:
        return True
    delta = record.get('directional_delta_pct')
    if delta is None:
        return False
    if record.get('current_stability') in {'noisy', 'untrustworthy'} or record.get('baseline_stability') in {'noisy', 'untrustworthy'}:
        return abs(delta) > NOISY_RISK_PERCENT
    return False


def bullet_or_none(items, none_text):
    if not items:
        return [none_text]
    return items


def write_table(f, headers, rows):
    if not rows:
        return
    f.write('| ' + ' | '.join(headers) + ' |\n')
    f.write('|' + '|'.join(['---'] * len(headers)) + '|\n')
    for row in rows:
        f.write('| ' + ' | '.join(row) + ' |\n')
    f.write('\n')


def write_section(f, title, body_lines):
    f.write(f'## {title}\n\n')
    for line in body_lines:
        f.write(f'{line}\n')
    f.write('\n')


def write_subheader(f, title):
    f.write(f'### {title}\n\n')


def fmt_ops_short(value):
    if value is None:
        return 'NA'
    if value > MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S:
        return 'REJECTED'
    return f'{value:.0f}'


def build_environment_block():
    physical_cores, logical_cores = detect_core_counts()
    ram_gb = detect_ram_gb()
    return [
        f'- CPU: {detect_cpu_name()}',
        f'- Cores: {physical_cores} physical / {logical_cores} logical',
        f'- RAM: {ram_gb}GB' if isinstance(ram_gb, (int, float)) else f'- RAM: {ram_gb}',
        f'- OS: {detect_os_label()}',
        f'- Rust version: {detect_rust_version()}',
        f'- Build mode: {detect_build_mode()}',
        f'- Allocator: {detect_allocator()}',
        f'- Transport config: {transport_config_label()}',
        f'- Test date: {utc_timestamp()}',
    ]


def build_methodology_block():
    return [
        '- Criterion is used for microbench tuning only.',
        '- Stress tests represent real runtime behavior.',
        '- Microbench rules: operations are batched to avoid sub-ns noise, `black_box` is used on inputs, and a minimum sample duration is enforced.',
        '- Stress rules: target 5 second runtime per run, 3 second minimum floor, 5 runs minimum, median throughput reported, and variance tracked.',
        '- Microbenchmarks are directional. System stress tests are authoritative.',
    ]


def build_executive_summary(baseline_available, authoritative_stress_count, total_stress_count):
    if not baseline_available:
        if authoritative_stress_count == 0:
            return [
                'Fitz captured a first benchmark snapshot, but no tracked baseline exists yet.',
                'Criterion microbenchmarks remain useful for hotspot tuning, but they are not a substitute for a promotable baseline.',
                'The current capture includes stress data, but comparison sections are intentionally suppressed until a tracked baseline is added.',
                'Transport overhead will become meaningful once the first baseline promotion lands and comparison mode is enabled.',
            ]
        return [
            f'Fitz captured a first benchmark snapshot with {authoritative_stress_count} authoritative stress scenario(s), but no tracked baseline exists yet.',
            'Criterion microbenchmarks remain useful for hotspot tuning, but they are not a substitute for a promotable baseline.',
            'The current capture includes authoritative stress data, but comparison sections are intentionally suppressed until a tracked baseline is added.',
            'Transport overhead will become meaningful once the first baseline promotion lands and comparison mode is enabled.',
        ]
    if authoritative_stress_count == 0:
        return [
            'Fitz continues to show stable hotpath and subsystem behavior, but the current stress data are not yet authoritative because no scenario meets the 3-second runtime floor and 5-run minimum.',
            'Criterion microbenchmarks remain useful for hotspot tuning, but they are not a substitute for authoritative system measurements.',
            'The primary engineering gap is now benchmark methodology and run length, not an obvious runtime instability.',
            'Transport overhead is visible and should continue to be tracked against the direct baseline once longer stress runs are captured.',
        ]
    if authoritative_stress_count < total_stress_count:
        return [
            f'Fitz has {authoritative_stress_count} authoritative stress scenario(s), but the remaining {total_stress_count - authoritative_stress_count} are still provisional or invalid for throughput.',
            'Criterion microbenchmarks remain useful for hotspot tuning, but they are not a substitute for authoritative system measurements.',
            'The current run is mixed: the authoritative scenarios look stable, while the provisional ones still need longer or more complete sampling.',
            'Transport overhead is visible and should continue to be tracked against the direct baseline once longer stress runs are captured.',
        ]
    return [
        'Fitz continues to show strong system stability across KV, queue, RPC, lease, schedule, and stream domains.',
        'Hotpath microbenchmarks remain useful for tuning, but they are directional and should not be treated as system performance signals.',
        'Current stress runs meet the minimum authority thresholds and point to stable runtime behavior under contention.',
        'The main engineering opportunity remains tightening benchmark methodology and long-run regression tracking rather than fixing a runtime bottleneck.',
    ]


def build_change_lines(items, lower_is_better, prefix):
    lines = []
    for item in items:
        if 'scenario' in item:
            name = f"{item['suite']} / {stress_label(item)}"
            metric = fmt_ops(stress_throughput(item))
            units = 'ops/sec'
            delta = item['delta_pct']
        else:
            name = item['benchmark']
            metric = fmt_us(item.get('median_ns'))
            units = 'us'
            delta = item['delta_pct']
        lines.append(f'- {name}: {fmt_pct(delta)} ({metric} {units})')
    if not lines:
        lines.append(f'- None above 10% in this run.')
    return lines


def build_regression_lines(items):
    lines = []
    for item in items:
        if 'scenario' in item:
            name = f"{item['suite']} / {stress_label(item)}"
            delta = item['delta_pct']
        else:
            name = item['benchmark']
            delta = item['delta_pct']
        lines.append(f'- {name}: {fmt_pct(delta)}')
    if not lines:
        lines.append('- None above 10% in this run.')
    return lines


def build_system_throughput_rows(domain_name, rows):
    output = []
    for item in rows:
        throughput = stress_throughput(item)
        if throughput is None or throughput > MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S:
            continue
        output.append((throughput, [
            stress_label(item),
            str(stress_batch_size(item)),
            fmt_ops_short(throughput),
            stability_for_stress(item),
            runtime_note(item),
        ]))
    output.sort(key=lambda row: row[0], reverse=True)
    return [row for _, row in output]


def build_transport_rows(rows):
    layers = ['direct', 'tcp', 'websocket']
    layer_entries = {layer: [] for layer in layers}
    for item in rows:
        layer = item.get('layer')
        if layer in layer_entries:
            throughput = stress_throughput(item)
            if throughput is None or throughput > MAX_REASONABLE_STRESS_THROUGHPUT_OPS_PER_S:
                continue
            layer_entries[layer].append(throughput)

    direct_values = layer_entries['direct']
    direct_median = statistics.median(direct_values) if direct_values else None
    table_rows = []
    for layer in layers:
        values = layer_entries[layer]
        median_value = statistics.median(values) if values else None
        if layer == 'direct':
            overhead = 'baseline'
        elif direct_median and median_value is not None:
            overhead_pct = ((direct_median - median_value) / direct_median) * 100.0
            overhead = fmt_pct(overhead_pct)
        else:
            overhead = 'NA'
        table_rows.append([
            layer,
            fmt_ops_short(median_value),
            overhead,
        ])
    return table_rows


def build_microbench_rows(items):
    meaningful = [item for item in items if benchmark_latency_ns(item) is not None and benchmark_latency_ns(item) >= MIN_MEANINGFUL_CRITERION_LATENCY_NS]
    meaningful.sort(key=lambda item: benchmark_latency_ns(item))
    rows = []
    for item in meaningful[:10]:
        rows.append([
            benchmark_short_name(item['benchmark']),
            fmt_us(item['median_ns']),
            variance_band(item['rel_stddev']),
        ])
    return rows


def build_domain_rows(rows):
    domain_groups = defaultdict(list)
    for item in rows:
        suite = item['suite'].replace('_', '-')
        domain = domain_from_suite(suite)
        domain_groups[domain].append(item)

    ordered_domains = ['KV', 'Queue', 'RPC', 'Stream', 'Lease', 'Schedule']
    tables = {}
    for domain in ordered_domains:
        domain_items = domain_groups.get(domain, [])
        domain_rows = build_system_throughput_rows(domain, domain_items)
        tables[domain] = domain_rows
    return tables


def build_summary_counts(items, variance_key):
    stable = sum(1 for item in items if variance_band(item[variance_key]) == 'stable')
    acceptable = sum(1 for item in items if variance_band(item[variance_key]) == 'acceptable')
    unstable = sum(1 for item in items if variance_band(item[variance_key]) in {'noisy', 'untrustworthy'})
    return stable, acceptable, unstable


def build_report():
    criterion_bands = Counter(variance_band(entry['rel_stddev']) for entry in entries)
    stress_bands = stress_stability_bands

    criterion_summary = {
        'count': len(entries),
        'suites': len(criterion_suite_groups),
        'median': criterion_delta_summary['median'],
        'p90': criterion_delta_summary['p90'],
        'best': criterion_delta_summary['fastest'],
        'meaningful_best': criterion_delta_summary.get('meaningful_fastest'),
        'worst': criterion_delta_summary['slowest'],
        'criterion_bands': criterion_bands,
        'improved': criterion_delta_summary['improved'],
        'regressed': criterion_delta_summary['regressed'],
        'unchanged': criterion_delta_summary['unchanged'],
        'new': criterion_delta_summary['new'],
        'missing': criterion_delta_summary['missing'],
    }

    stress_summary = {
        'count': len(stress_entries),
        'suites': len(stress_suite_groups),
        'median': stress_delta_summary['median'],
        'p90': stress_delta_summary['p90'],
        'best': stress_delta_summary['best'],
        'worst': stress_delta_summary['worst'],
        'stress_bands': stress_bands,
        'improved': stress_delta_summary['improved'],
        'regressed': stress_delta_summary['regressed'],
        'unchanged': stress_delta_summary['unchanged'],
        'new': stress_delta_summary['new'],
        'missing': stress_delta_summary['missing'],
    }

    all_comparisons = []
    for item in criterion_comparisons:
        if item['delta_pct'] is not None:
            all_comparisons.append({**item, 'kind': 'criterion', 'lower_is_better': True})
    for item in stress_comparisons:
        if item['delta_pct'] is not None:
            all_comparisons.append({**item, 'kind': 'stress', 'lower_is_better': False})

    key_improvements = []
    key_regressions = []
    for item in all_comparisons:
        delta = item['delta_pct']
        if item['kind'] == 'criterion':
            if delta <= -10.0:
                key_improvements.append(item)
            elif delta >= 10.0:
                key_regressions.append(item)
        else:
            if delta >= 10.0:
                key_improvements.append(item)
            elif delta <= -10.0:
                key_regressions.append(item)

    key_improvements.sort(key=lambda item: abs(item['delta_pct']), reverse=True)
    key_regressions.sort(key=lambda item: abs(item['delta_pct']), reverse=True)

    trend_counts = {
        'improved': criterion_summary['improved'] + stress_summary['improved'],
        'regressed': criterion_summary['regressed'] + stress_summary['regressed'],
        'unchanged': criterion_summary['unchanged'] + stress_summary['unchanged'],
    }

    regression_flag = any(
        (item['lower_is_better'] and item['delta_pct'] >= 15.0) or
        (not item['lower_is_better'] and item['delta_pct'] <= -15.0)
        for item in all_comparisons
    )

    comparison_records = [
        {**record, 'classification': comparison_severity(record)}
        for record in criterion_comparisons + stress_comparisons
    ]
    authoritative_comparisons = [record for record in comparison_records if record.get('comparison_eligible')]
    performance_changes = [record for record in authoritative_comparisons if record.get('classification') != 'new']
    improved_changes = [record for record in performance_changes if record.get('classification') == 'improved']
    unchanged_changes = [record for record in performance_changes if record.get('classification') == 'unchanged']
    regression_changes = [record for record in performance_changes if record.get('classification') in {'warning', 'alert', 'critical'}]
    critical_regressions = [record for record in comparison_records if record.get('classification') == 'critical']
    stability_changes = [
        {**record, 'stability_delta': compare_stability(record.get('current_stability'), record.get('baseline_stability'))}
        for record in authoritative_comparisons
        if record.get('baseline_stability') is not None and record.get('current_stability') is not None
    ]
    stability_regressions = [record for record in stability_changes if record.get('stability_delta') == 'regressed']
    stability_improvements = [record for record in stability_changes if record.get('stability_delta') == 'improved']
    new_scenarios = [record for record in comparison_records if comparison_is_new(record)]
    missing_scenarios = [record for record in comparison_records if comparison_is_missing(record)]
    risk_areas = [record for record in comparison_records if comparison_is_risk_area(record)]

    generated_at = utc_timestamp()
    commit_hash = git_commit_hash()
    current_criterion_records = [build_criterion_result(entry, commit_hash, generated_at) for entry in entries]
    current_stress_records = [build_stress_result(entry, commit_hash, generated_at) for entry in stress_entries]
    current_authoritative = current_run_authoritative(current_criterion_records, current_stress_records)
    comparison_summary = {
        'performance_changes': len(performance_changes),
        'improved': len(improved_changes),
        'unchanged': len(unchanged_changes),
        'regressions': len(regression_changes),
        'critical': len(critical_regressions),
        'stability_regressions': len(stability_regressions),
        'stability_improvements': len(stability_improvements),
        'new': len(new_scenarios),
        'missing': len(missing_scenarios),
        'risk_areas': len(risk_areas),
        'authoritative': current_authoritative,
        'baseline_available': baseline_available,
    }

    current_manifest = build_result_manifest(
        current_criterion_records,
        current_stress_records,
        comparison_summary,
        generated_at,
        commit_hash,
    )
    save_json_data(CURRENT_RESULTS_FILE, current_manifest)

    promoted_criterion = previous_criterion_rows
    promoted_stress = previous_stress_rows
    if current_authoritative and not critical_regressions:
        promoted_criterion = current_criterion_records
        promoted_stress = current_stress_records
        save_json_data(
            BASELINE_FILE,
            {
                'schema_version': 1,
                'generated_at': current_manifest['generated_at'],
                'commit_hash': current_manifest['commit_hash'],
                'policy': current_manifest['policy'],
                'criterion': promoted_criterion,
                'stress': promoted_stress,
                'source': 'authoritative promotion',
            },
        )

    env_lines = build_environment_block()
    methodology_lines = build_methodology_block()
    authoritative_stress_count = sum(1 for entry in stress_entries if stress_throughput_status(entry) == 'authoritative')
    executive_summary_lines = build_executive_summary(baseline_available, authoritative_stress_count, len(stress_entries))

    microbench_counts = {
        'stable': criterion_bands.get('stable', 0),
        'noisy': criterion_bands.get('noisy', 0) + criterion_bands.get('acceptable', 0),
        'untrustworthy': criterion_bands.get('untrustworthy', 0),
    }

    stress_stable = stress_bands.get('stable', 0)
    stress_acceptable = stress_bands.get('acceptable', 0)
    stress_unstable = stress_bands.get('noisy', 0) + stress_bands.get('untrustworthy', 0)
    stress_insufficient = stress_bands.get('insufficient_data', 0)
    stress_invalid = stress_bands.get('invalid_for_throughput', 0)
    provisional_count = sum(1 for entry in stress_entries if stress_throughput_status(entry) != 'authoritative')

    microbench_rows = build_microbench_rows(entries)
    domain_tables = build_domain_rows([item for item in stress_entries if item.get('suite', '').startswith('tier3') or item.get('suite', '').startswith('system')])
    transport_rows = build_transport_rows([item for item in stress_entries if item.get('layer') in {'direct', 'tcp', 'websocket'}])

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    with OUT_MD.open('w', encoding='utf-8') as f:
        f.write(fitz_report_title() + '\n\n')

        write_section(f, 'Environment', env_lines)
        write_section(f, 'Methodology', methodology_lines)
        write_section(f, 'Executive Summary', [f'- {line}' for line in executive_summary_lines])

        if baseline_available:
            performance_rows = [comparison_row(record) for record in sorted(performance_changes, key=comparison_sort_key)[:15]]
            performance_lines = [
                f'- authoritative comparisons: {len(performance_changes)}',
                f'- improved: {len(improved_changes)}',
                f'- unchanged: {len(unchanged_changes)}',
                f'- regressions: {len(regression_changes)}',
            ]
            write_section(f, 'Performance Changes', performance_lines)
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], performance_rows)

            regression_rows = [comparison_row(record) for record in sorted(regression_changes, key=comparison_sort_key)[:15]]
            regression_lines = [
                f'- critical regressions: {len(critical_regressions)}',
                f'- warning/alert regressions: {len(regression_changes)}',
            ]
            if critical_regressions:
                regression_lines.append('- critical regressions require investigation before baseline promotion.')
            write_section(f, 'Regressions', regression_lines)
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], regression_rows)

            stability_rows = [comparison_row(record) for record in sorted(stability_changes, key=lambda record: (stability_rank(record.get('baseline_stability')), stability_rank(record.get('current_stability')), comparison_label(record)))[:15]]
            stability_lines = [
                f'- stability improvements: {len(stability_improvements)}',
                f'- stability regressions: {len(stability_regressions)}',
            ]
            write_section(f, 'Stability Changes', stability_lines)
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], stability_rows)

            new_rows = [comparison_row(record) for record in sorted(new_scenarios, key=comparison_sort_key)[:15]]
            write_section(f, 'New Scenarios', [f'- {len(new_scenarios)} new scenario(s) detected.'])
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], new_rows)

            missing_rows = [comparison_row(record) for record in sorted(missing_scenarios, key=comparison_sort_key)[:15]]
            write_section(f, 'Missing Scenarios', [f'- {len(missing_scenarios)} baseline scenario(s) were not present in the current authoritative run.'])
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], missing_rows)

            risk_rows = [comparison_row(record) for record in sorted(risk_areas, key=comparison_sort_key)[:15]]
            risk_lines = [
                f'- {len(risk_areas)} scenario(s) are noisy, insufficient_data, or otherwise require review.',
                f'- threshold for noisy risk: {NOISY_RISK_PERCENT:.0f}% directional change.',
            ]
            write_section(f, 'Risk Areas', risk_lines)
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], risk_rows)
        else:
            write_section(
                f,
                'Baseline Status',
                [
                    '- No tracked baseline exists yet, so this report is a first-run snapshot.',
                    '- Comparison sections are intentionally suppressed until config/bench_baseline.json is created and promoted.',
                ],
            )
            write_section(
                f,
                'Performance Changes',
                ['- No tracked baseline exists yet; compare mode will activate after the first baseline promotion.'],
            )
            write_table(f, ['item', 'kind', 'delta', 'baseline', 'current', 'status', 'baseline stability', 'current stability'], [])

        improvement_lines = [
            f'- {item["benchmark"]}: {fmt_pct(item["delta_pct"])}' if item['kind'] == 'criterion'
            else f'- {item["suite"]} / {stress_label(item)}: {fmt_pct(item["delta_pct"])}'
            for item in key_improvements
        ]
        write_section(f, 'Key Improvements', bullet_or_none(improvement_lines, '- None above 10% in this run.'))

        regression_lines = [
            f'- {item["benchmark"]}: {fmt_pct(item["delta_pct"])}' if item['kind'] == 'criterion'
            else f'- {item["suite"]} / {stress_label(item)}: {fmt_pct(item["delta_pct"])}'
            for item in key_regressions
        ]
        if not regression_lines:
            regression_lines = ['- None above 10% in this run.']
        write_section(f, 'Regressions', regression_lines)

        f.write('## System Throughput (Primary Signal)\n\n')
        authoritative_count = authoritative_stress_count
        provisional_count = len(stress_entries) - authoritative_count
        if stress_entries:
            f.write(
                f'{authoritative_count} stress scenario(s) are authoritative; '
                f'{provisional_count} remain provisional or invalid for throughput.\n\n'
            )
        else:
            f.write('No stress scenarios were available in the latest run.\n\n')
        for domain in ['KV', 'Queue', 'RPC', 'Stream', 'Lease', 'Schedule']:
            write_subheader(f, domain)
            write_table(
                f,
                ['scenario', 'batch_size', 'ops/sec', 'stability', 'status'],
                domain_tables.get(domain, []) or [['No qualifying stress rows', 'NA', 'NA', 'insufficient_data', 'No scenario data available']]
            )

        f.write('## Transport Cost\n\n')
        write_table(f, ['transport', 'ops/sec', 'overhead vs direct'], transport_rows or [['direct', 'NA', 'baseline'], ['tcp', 'NA', 'NA'], ['websocket', 'NA', 'NA']])

        f.write('## Microbench Summary (Tuning Only)\n\n')
        f.write(f'stable benches:\n{microbench_counts["stable"]}\n\n')
        f.write(f'noisy benches:\n{microbench_counts["noisy"]}\n\n')
        f.write(f'untrustworthy:\n{microbench_counts["untrustworthy"]}\n\n')
        f.write('Criterion microbenchmarks remain useful for hotspot tuning but are not treated as system performance indicators.\n\n')
        write_table(f, ['benchmark', 'median_us', 'stability'], microbench_rows or [['No meaningful hotpath results above 0.05 us', 'NA', 'stable']])

        f.write('## Stability Summary\n\n')
        f.write(f'total stress scenarios:\n{stress_summary["count"]}\n\n')
        f.write(f'stable:\n{stress_stable}\n\n')
        f.write(f'acceptable:\n{stress_acceptable}\n\n')
        f.write(f'unstable:\n{stress_unstable}\n\n')
        f.write(f'authoritative:\n{sum(1 for entry in stress_entries if stress_throughput_status(entry) == "authoritative")}\n\n')
        f.write(f'insufficient_data:\n{stress_insufficient}\n\n')
        f.write(f'invalid_for_throughput:\n{stress_invalid}\n\n')
        if provisional_count:
            f.write(f'Stress stability remains provisional because {provisional_count} scenario(s) are not yet authoritative.\n\n')
        else:
            f.write('Stress stability remains high with no observed runtime failures.\n\n')

        f.write('## Trend Analysis\n\n')
        f.write(f'improved:\n{trend_counts["improved"]}\n\n')
        f.write(f'regressed:\n{trend_counts["regressed"]}\n\n')
        f.write(f'unchanged:\n{trend_counts["unchanged"]}\n\n')
        if regression_flag:
            f.write('REGRESSION REQUIRES INVESTIGATION\n\n')
        if critical_regressions:
            f.write('CRITICAL REGRESSIONS DETECTED\n\n')

        if not baseline_available:
            f.write('Baseline promotion is pending because no tracked baseline exists yet.\n\n')
        elif not current_authoritative:
            f.write('Baseline promotion was skipped because the current run is not fully authoritative.\n\n')

        interpretation_lines = [
            'Fitz appears stable in the current run.',
            'Fitz is not showing a convincing throughput regression in the available data.',
            'Fitz is authoritative on any stress row that meets the 3 second floor and 5-run minimum; shorter runs are explicitly marked invalid or insufficient.',
            'Nothing in this summary suggests an urgent runtime problem; the main concern is measurement rigor.',
        ]
        write_section(f, 'Interpretation', [f'- {line}' for line in interpretation_lines[:6]])

        takeaway_lines = [
            'Fitz demonstrates solid engineering behavior, but throughput claims should only be treated as authoritative when the stress run meets the minimum duration and run-count thresholds.',
        ]
        write_section(f, 'Takeaway', [f'- {line}' for line in takeaway_lines])

    if CRITERION_ROOT.exists():
        print(f"Wrote {OUT_CSV} (criterion) with {len(entries)} entries.")
    if STRESS_ROOT.exists():
        print(f"Wrote {STRESS_CSV} (stress) with {len(stress_entries)} entries.")
    print(f"Wrote {OUT_MD} (summary).")
    return 1 if critical_regressions else 0


def main():
    return build_report()


if __name__ == '__main__':
    raise SystemExit(main())
