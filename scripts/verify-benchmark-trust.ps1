[CmdletBinding()]
param(
    [string]$TargetsPath = "config/perf_targets.json",
    [string]$ResultsPath = "target/bench_results.json",
    [string]$BaselinePath = "config/bench_baseline.json",
    [int]$MinimumStressSamples = 5,
    [switch]$RequireResults,
    [switch]$CheckFrozenBaseline
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$violations = [System.Collections.Generic.List[string]]::new()

function Add-Violation([string]$message) {
    $script:violations.Add($message)
}

function Read-Json([string]$path) {
    if (-not (Test-Path $path)) {
        throw "Missing required JSON file: $path"
    }
    Get-Content $path -Raw | ConvertFrom-Json
}

function Stress-Target-Id($target) {
    $id = "stress:$($target.suite)|$($target.scenario)"
    if ($target.layer) {
        $id += "|$($target.layer)"
    }
    $id
}

function Matching-Stress-Rows($records, $target) {
    @($records | Where-Object {
        $_.adapter -eq "stress" -and
        $_.suite -eq $target.suite -and
        $_.scenario -eq $target.scenario -and
        ((-not $target.layer) -or $_.tags.layer -eq $target.layer)
    })
}

$config = Read-Json $TargetsPath
$stressTargets = @($config.targets.PSObject.Properties | Where-Object {
    $_.Value.kind -eq "stress"
})

foreach ($property in $stressTargets) {
    $target = $property.Value
    $expectedId = Stress-Target-Id $target
    if ($property.Name -ne $expectedId) {
        Add-Violation "orphaned target id '$($property.Name)'; expected '$expectedId'"
    }

    $sourcePath = Join-Path "benches" "$($target.suite.Replace('-', '_')).rs"
    if (-not (Test-Path $sourcePath)) {
        Add-Violation "target '$expectedId' has no benchmark source '$sourcePath'"
        continue
    }

    $source = Get-Content $sourcePath -Raw
    if (-not $source.Contains("""$($target.scenario)""")) {
        Add-Violation "target '$expectedId' references scenario '$($target.scenario)' not emitted by '$sourcePath'"
    }
    if ($target.layer -and -not $source.Contains("""$($target.layer)""")) {
        Add-Violation "target '$expectedId' references layer '$($target.layer)' not emitted by '$sourcePath'"
    }
}

if ($RequireResults) {
    $results = Read-Json $ResultsPath
    foreach ($property in $stressTargets) {
        $target = $property.Value
        $id = Stress-Target-Id $target
        $rows = Matching-Stress-Rows $results.records $target
        if ($rows.Count -eq 0) {
            Add-Violation "missing target scenario in results: '$id'"
            continue
        }

        foreach ($row in $rows) {
            if ($row.status -in @("insufficient_data", "invalid", "missing", "untrustworthy")) {
                Add-Violation "invalid stress run '$($row.id)' has status '$($row.status)'"
            }
            if ($null -eq $row.samples -or [int]$row.samples -lt $MinimumStressSamples) {
                Add-Violation "invalid stress run '$($row.id)' has $($row.samples) samples; require $MinimumStressSamples"
            }
        }
    }
}

if ($CheckFrozenBaseline) {
    $baseline = Read-Json $BaselinePath
    foreach ($row in @($baseline.records | Where-Object { $_.adapter -eq "stress" })) {
        if ($row.status -in @("insufficient_data", "invalid", "missing", "untrustworthy")) {
            Add-Violation "frozen baseline row '$($row.id)' has status '$($row.status)'"
        }
        if ($null -eq $row.samples -or [int]$row.samples -lt $MinimumStressSamples) {
            Add-Violation "frozen baseline row '$($row.id)' has $($row.samples) samples; require $MinimumStressSamples"
        }
    }
}

if ($violations.Count -gt 0) {
    $violations | ForEach-Object { Write-Host "ERROR: $_" -ForegroundColor Red }
    exit 1
}

Write-Host "Benchmark trust verification passed for $($stressTargets.Count) stress targets."
