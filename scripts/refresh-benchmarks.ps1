[CmdletBinding()]
param(
    [ValidateSet('tier1', 'tier2', 'tier3', 'tier4')]
    [string[]]$Tiers = @('tier1', 'tier2', 'tier3', 'tier4'),
    [string[]]$BenchNames = @(),
    [int]$StressRuns = 5,
    [int]$StressWarmup = 1,
    [switch]$SkipBuild,
    [switch]$SkipSummary
)

$ErrorActionPreference = 'Stop'

if ($StressRuns -lt 1) {
    throw 'StressRuns must be at least 1.'
}

if ($StressWarmup -lt 0) {
    throw 'StressWarmup cannot be negative.'
}

$repoRoot = Split-Path -Parent $PSScriptRoot

function Get-BenchTargetsForTier {
    param(
        [string]$Tier,
        [string[]]$SelectedBenchNames
    )

    $targets = Get-ChildItem -Path (Join-Path $repoRoot 'benches') -Filter "$Tier*.rs" |
        Sort-Object Name |
        ForEach-Object { $_.BaseName }

    if ($SelectedBenchNames.Count -gt 0) {
        $targets = $targets | Where-Object { $SelectedBenchNames -contains $_ }
    }

    return @($targets)
}

function Invoke-CargoBench {
    param(
        [string]$BenchName,
        [string[]]$ExtraArgs = @()
    )

    Write-Host "==> cargo bench --bench $BenchName $($ExtraArgs -join ' ')"
    & cargo bench --bench $BenchName @ExtraArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo bench failed for $BenchName"
    }
}

Push-Location $repoRoot

try {
    Get-Command cargo -ErrorAction Stop | Out-Null
    if (-not $SkipSummary) {
        Get-Command cntryl-tools -ErrorAction Stop | Out-Null
    }

    $tierTargets = [ordered]@{}
    foreach ($tier in $Tiers) {
        $targets = Get-BenchTargetsForTier -Tier $tier -SelectedBenchNames $BenchNames
        if ($targets.Count -eq 0) {
            throw "No benchmarks matched tier '$tier'."
        }
        $tierTargets[$tier] = $targets
    }

    if (-not $SkipBuild) {
        foreach ($targets in $tierTargets.Values) {
            foreach ($benchName in $targets) {
                Invoke-CargoBench -BenchName $benchName -ExtraArgs @('--no-run')
            }
        }
    }

    foreach ($entry in $tierTargets.GetEnumerator()) {
        $tier = $entry.Key
        foreach ($benchName in $entry.Value) {
            if ($tier -in @('tier3', 'tier4')) {
                Invoke-CargoBench -BenchName $benchName -ExtraArgs @('--', '--runs', $StressRuns, '--warmup', $StressWarmup)
            } else {
                Invoke-CargoBench -BenchName $benchName
            }
        }
    }

    if (-not $SkipSummary) {
        Write-Host '==> cntryl-tools summarize-benchmarks --product-name Fitz --report-title Fitz Benchmark Report'
        & cntryl-tools summarize-benchmarks --product-name Fitz --report-title 'Fitz Benchmark Report'
        if ($LASTEXITCODE -ne 0) {
            throw 'cntryl-tools summarize-benchmarks failed.'
        }
    }
}
finally {
    Pop-Location
}