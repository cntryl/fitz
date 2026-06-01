[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$CycleLabel,
    [string]$BenchFilter = "",
    [switch]$ResumeFromOptimize,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$cycleRoot = Join-Path "target/perf-loops" $CycleLabel
$phase = if ($ResumeFromOptimize) { "verification" } else { "baseline" }
$snapshotRoot = Join-Path $cycleRoot "snapshots/$phase"
$logRoot = Join-Path $cycleRoot "logs"

function Show-Or-Run([string]$label, [string]$command) {
    Write-Host "[$label] $command"
    if ($DryRun) {
        return
    }
    New-Item -ItemType Directory -Force $logRoot | Out-Null
    $logPath = Join-Path $logRoot "$phase-$label.log"
    Invoke-Expression "$command 2>&1" | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $command"
    }
}

$refresh = "powershell -ExecutionPolicy Bypass -File .\scripts\refresh-benchmarks.ps1 -Tiers tier3,tier4 -StressRuns 5 -StressWarmup 1"
if ($BenchFilter) {
    $refresh += " -BenchNames $BenchFilter"
}

Show-Or-Run "tests" "cargo test --workspace"
Show-Or-Run "benchmarks" $refresh
Show-Or-Run "trust" "powershell -ExecutionPolicy Bypass -File .\scripts\verify-benchmark-trust.ps1 -RequireResults -MinimumStressSamples 5"

if ($DryRun) {
    Write-Host "[$phase] would snapshot target/bench_results.json and target/bench_summary.md under $snapshotRoot"
    exit 0
}

New-Item -ItemType Directory -Force $snapshotRoot | Out-Null
Copy-Item target/bench_results.json $snapshotRoot -Force
Copy-Item target/bench_summary.md $snapshotRoot -Force

$manifest = [ordered]@{
    cycle_label = $CycleLabel
    phase = $phase
    bench_filter = $BenchFilter
    snapshot = $snapshotRoot
    completed_at = (Get-Date).ToUniversalTime().ToString("o")
}
$manifest | ConvertTo-Json | Set-Content (Join-Path $cycleRoot "manifest.json")

if (-not $ResumeFromOptimize) {
    Write-Host "Baseline captured. Make the focused optimization, then rerun with -ResumeFromOptimize."
    exit 0
}

$comparison = [ordered]@{
    cycle_label = $CycleLabel
    baseline = (Join-Path $cycleRoot "snapshots/baseline/bench_results.json")
    verification = (Join-Path $cycleRoot "snapshots/verification/bench_results.json")
    completed_at = (Get-Date).ToUniversalTime().ToString("o")
}
$comparison | ConvertTo-Json | Set-Content (Join-Path $cycleRoot "comparison.json")
Write-Host "Verification captured at $snapshotRoot"
