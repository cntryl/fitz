# Performance Loop Runner

Use the PowerShell runner at `scripts/run_perf_loop.ps1` to capture a full local optimization cycle with stable artifacts under `target/perf-loops/`.

## Commands

Initial pass:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_perf_loop.ps1 -CycleLabel rpc-pass-01
```

After making the optimization change, resume from the verification half of the same cycle:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_perf_loop.ps1 -CycleLabel rpc-pass-01 -ResumeFromOptimize
```

Dry-run orchestration without invoking cargo or python:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_perf_loop.ps1 -CycleLabel smoke-check -DryRun
```

Optional benchmark filter:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_perf_loop.ps1 -CycleLabel queue-pass-01 -BenchFilter tier3_system_queue
```

## Output

Each cycle writes to `target/perf-loops/<cycle-label>/`:

- `manifest.json`: phase status, commands, snapshot paths, selected optimization target, comparison result
- `logs/`: raw command output for each test, bench, and summary phase
- `snapshots/baseline/`: first benchmark summary, perf scorecard, and copied benchmark artifacts
- `snapshots/verification/`: second benchmark summary, perf scorecard, and copied benchmark artifacts after optimization
- `comparison.json`: before/after improvement summary once verification is complete

## Selection Rules

The runner records an optimization checkpoint after the first test and benchmark pass. It selects one candidate deterministically using:

- [config/perf_targets.json](../../config/perf_targets.json) as the source of truth
- target-bucket order: `engine_core`, then `service_budget/direct_api`, then `service_budget/transport`, then `service_budget/contention`, then `internal_explainer`
- percent over the `operational_target` inside each bucket
- percent over the `stretch_target` as the next tie-breaker inside the bucket
- higher current `mean_us` as the final tie-breaker inside the bucket

The runner does not mutate source code during optimization. It records the target, then expects the engineer to make the code change before running the resume step.
