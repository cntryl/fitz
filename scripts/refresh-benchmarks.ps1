[CmdletBinding()]
param(
    [string[]]$Tiers = @("tier1", "tier2", "tier3", "tier4"),
    [string[]]$BenchNames = @(),
    [int]$StressRuns = 5,
    [int]$StressWarmup = 1,
    [switch]$SkipBuild,
    [switch]$SkipSummary,
    [switch]$FreezeBaseline,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$Tiers = @($Tiers | ForEach-Object { $_ -split "," } | Where-Object { $_ })
$BenchNames = @($BenchNames | ForEach-Object { $_ -split "," } | Where-Object { $_ })

function Invoke-CommandLine([string]$command) {
    Write-Host "> $command"
    if (-not $DryRun) {
        Invoke-Expression $command
        if ($LASTEXITCODE -ne 0) {
            throw "Command failed with exit code ${LASTEXITCODE}: $command"
        }
    }
}

function Remove-GeneratedDirectory([string]$relativePath) {
    $targetRoot = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "target"))
    $fullPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $relativePath))
    $targetPrefix = $targetRoot + [System.IO.Path]::DirectorySeparatorChar
    if (-not $fullPath.StartsWith($targetPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove generated directory outside target/: $fullPath"
    }
    if (Test-Path -LiteralPath $fullPath) {
        Remove-Item -LiteralPath $fullPath -Recurse -Force
    }
}

$manifest = Get-Content Cargo.toml -Raw
$allBenchNames = @([regex]::Matches($manifest, '(?m)^name = "(tier[1-4]_[^"]+)"$') |
    ForEach-Object { $_.Groups[1].Value } |
    Sort-Object -Unique)
$selected = @($allBenchNames | Where-Object {
    $name = $_
    ($Tiers | Where-Object { $name.StartsWith("${_}_") }).Count -gt 0
})
if ($BenchNames.Count -gt 0) {
    $selected = @($selected | Where-Object { $BenchNames -contains $_ })
}
if ($selected.Count -eq 0) {
    throw "No benchmark targets selected."
}

Write-Host "Selected benchmarks: $($selected -join ', ')"
if (-not $DryRun) {
    if ($selected | Where-Object { $_ -like "tier1_*" -or $_ -like "tier2_*" }) {
        Remove-GeneratedDirectory "target/criterion"
    }
    foreach ($bench in @($selected | Where-Object { $_ -like "tier3_*" -or $_ -like "tier4_*" })) {
        Remove-GeneratedDirectory (Join-Path "target/stress" $bench.Replace("_", "-"))
        Remove-GeneratedDirectory (Join-Path "target/stress" $bench)
    }
}

if (-not $SkipBuild) {
    Invoke-CommandLine "cargo bench --no-run"
}

foreach ($bench in $selected) {
    if ($bench -like "tier3_*" -or $bench -like "tier4_*") {
        Invoke-CommandLine "cargo bench --bench $bench -- --runs $StressRuns --warmup $StressWarmup"
    } else {
        Invoke-CommandLine "cargo bench --bench $bench"
    }
}

if (-not $SkipSummary) {
    Invoke-CommandLine 'cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"'
}

$stressSelected = @($selected | Where-Object { $_ -like "tier3_*" -or $_ -like "tier4_*" }).Count -gt 0
if ($stressSelected -and -not $SkipSummary) {
    Invoke-CommandLine "powershell -ExecutionPolicy Bypass -File .\scripts\verify-benchmark-trust.ps1 -RequireResults -MinimumStressSamples $StressRuns"
}

if ($FreezeBaseline) {
    if ($SkipSummary) {
        throw "-FreezeBaseline cannot be combined with -SkipSummary."
    }
    Invoke-CommandLine "powershell -ExecutionPolicy Bypass -File .\scripts\verify-benchmark-trust.ps1 -RequireResults -MinimumStressSamples 5"
    if (-not $DryRun) {
        Copy-Item target/bench_results.json config/bench_baseline.json -Force
    }
    Invoke-CommandLine "powershell -ExecutionPolicy Bypass -File .\scripts\verify-benchmark-trust.ps1 -CheckFrozenBaseline -MinimumStressSamples 5"
    Invoke-CommandLine 'cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"'
}
