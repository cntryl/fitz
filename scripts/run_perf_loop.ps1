param(
    [Parameter(Mandatory = $true)]
    [string]$CycleLabel,

    [string]$BenchFilter,

    [switch]$ResumeFromOptimize,

    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$TargetRoot = Join-Path $RepoRoot "target"
$LoopRoot = Join-Path $TargetRoot "perf-loops"
$CycleRoot = Join-Path $LoopRoot $CycleLabel
$LogsRoot = Join-Path $CycleRoot "logs"
$SnapshotsRoot = Join-Path $CycleRoot "snapshots"
$ManifestPath = Join-Path $CycleRoot "manifest.json"
$ComparisonPath = Join-Path $CycleRoot "comparison.json"
$PerfTargetsPath = Join-Path $RepoRoot "config\perf_targets.json"

$SelectionBucketOrder = @{
    "engine_core" = 0
    "service_budget/direct_api" = 1
    "service_budget/transport" = 2
    "service_budget/contention" = 3
    "internal_explainer" = 4
}

function New-Directory {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Get-TimestampUtc {
    return (Get-Date).ToUniversalTime().ToString("o")
}

function Write-Status {
    param([string]$Message)
    Write-Host "[perf-loop] $Message"
}

function Load-Manifest {
    if (Test-Path $ManifestPath) {
        return ConvertTo-Hashtable (Get-Content $ManifestPath -Raw | ConvertFrom-Json)
    }

    return @{
        cycle_label = $CycleLabel
        created_at_utc = Get-TimestampUtc
        repo_root = $RepoRoot
        dry_run = [bool]$DryRun
        bench_filter = if ($BenchFilter) { $BenchFilter } else { $null }
        phases = @{}
        snapshots = @{}
        optimization_target = $null
        comparison = $null
    }
}

function ConvertTo-Hashtable {
    param([Parameter(ValueFromPipeline = $true)]$InputObject)

    if ($null -eq $InputObject) {
        return $null
    }

    if ($InputObject -is [string] -or $InputObject.GetType().IsPrimitive -or $InputObject -is [decimal] -or $InputObject -is [datetime]) {
        return $InputObject
    }

    if ($InputObject -is [System.Collections.IDictionary]) {
        $copy = @{}
        foreach ($key in $InputObject.Keys) {
            $copy[$key] = ConvertTo-Hashtable $InputObject[$key]
        }
        return $copy
    }

    if ($InputObject -is [System.Collections.IEnumerable]) {
        $items = @()
        foreach ($item in $InputObject) {
            $items += ,(ConvertTo-Hashtable $item)
        }
        return $items
    }

    if ($InputObject.PSObject -and @($InputObject.PSObject.Properties).Count -gt 0) {
        $result = @{}
        foreach ($property in $InputObject.PSObject.Properties) {
            $result[$property.Name] = ConvertTo-Hashtable $property.Value
        }
        return $result
    }

    return $InputObject
}

function Save-Manifest {
    param([hashtable]$Manifest)
    $json = $Manifest | ConvertTo-Json -Depth 8
    Set-Content -Path $ManifestPath -Value $json
}

function Load-PerformanceTargets {
    if (-not (Test-Path $PerfTargetsPath)) {
        throw "Performance target file not found at $PerfTargetsPath."
    }

    return ConvertTo-Hashtable (Get-Content $PerfTargetsPath -Raw | ConvertFrom-Json)
}

function Test-MapKey {
    param(
        $Map,
        [string]$Key
    )

    if ($null -eq $Map) {
        return $false
    }

    if ($Map -is [System.Collections.IDictionary]) {
        return $Map.Contains($Key)
    }

    return $null -ne $Map.PSObject.Properties[$Key]
}

function Get-MapValue {
    param(
        $Map,
        [string]$Key
    )

    if ($Map -is [System.Collections.IDictionary]) {
        return $Map[$Key]
    }

    return $Map.PSObject.Properties[$Key].Value
}

function Start-Phase {
    param(
        [hashtable]$Manifest,
        [string]$PhaseName,
        [string]$LogPath,
        [string]$CommandText
    )

    $Manifest.phases[$PhaseName] = @{
        status = "running"
        started_at_utc = Get-TimestampUtc
        completed_at_utc = $null
        exit_code = $null
        log_path = $LogPath
        command = $CommandText
    }
    Save-Manifest $Manifest
}

function Complete-Phase {
    param(
        [hashtable]$Manifest,
        [string]$PhaseName,
        [int]$ExitCode,
        [string]$Status
    )

    $Manifest.phases[$PhaseName].completed_at_utc = Get-TimestampUtc
    $Manifest.phases[$PhaseName].exit_code = $ExitCode
    $Manifest.phases[$PhaseName].status = $Status
    Save-Manifest $Manifest
}

function Invoke-LoggedCommand {
    param(
        [hashtable]$Manifest,
        [string]$PhaseName,
        [string]$LogName,
        [string]$CommandText
    )

    $logPath = Join-Path $LogsRoot $LogName
    Start-Phase -Manifest $Manifest -PhaseName $PhaseName -LogPath $logPath -CommandText $CommandText

    if ($DryRun) {
        Write-Status "DRY RUN $PhaseName -> $CommandText"
        Set-Content -Path $logPath -Value "DRY RUN $(Get-TimestampUtc)`r`n$CommandText`r`n"
        Complete-Phase -Manifest $Manifest -PhaseName $PhaseName -ExitCode 0 -Status "dry_run"
        return 0
    }

    Write-Status "$PhaseName -> $CommandText"
    & cmd.exe /d /c "$CommandText 2>&1" | Tee-Object -FilePath $logPath
    $exitCode = if ($null -ne $LASTEXITCODE) { [int]$LASTEXITCODE } else { 0 }
    $status = if ($exitCode -eq 0) { "passed" } else { "failed" }
    Complete-Phase -Manifest $Manifest -PhaseName $PhaseName -ExitCode $exitCode -Status $status
    return $exitCode
}

function Get-BenchCommandText {
    if ([string]::IsNullOrWhiteSpace($BenchFilter)) {
        return "cargo bench --no-fail-fast"
    }

    return "cargo bench --no-fail-fast -- $BenchFilter"
}

function Get-PythonCommand {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $python) {
        return $python.Source
    }

    throw "python is required to run scripts\benchmark_summary.py but was not found on PATH."
}

function Copy-IfExists {
    param(
        [string]$SourcePath,
        [string]$DestinationPath
    )

    if (Test-Path $SourcePath) {
        Copy-Item -Path $SourcePath -Destination $DestinationPath -Force
        return $true
    }

    return $false
}

function Copy-DirectoryIfExists {
    param(
        [string]$SourcePath,
        [string]$DestinationPath
    )

    if (Test-Path $SourcePath) {
        if (Test-Path $DestinationPath) {
            Remove-Item -Path $DestinationPath -Recurse -Force
        }
        Copy-Item -Path $SourcePath -Destination $DestinationPath -Recurse -Force
        return $true
    }

    return $false
}

function Save-BenchSnapshot {
    param(
        [hashtable]$Manifest,
        [string]$SnapshotName
    )

    $snapshotRoot = Join-Path $SnapshotsRoot $SnapshotName
    New-Directory $snapshotRoot

    $paths = @{
        summary_md = Join-Path $snapshotRoot "bench_summary.md"
        perf_scorecard_json = Join-Path $snapshotRoot "perf_scorecard.json"
        perf_scorecard_md = Join-Path $snapshotRoot "perf_scorecard.md"
        criterion_csv = Join-Path $snapshotRoot "criterion_benchmark_summary.csv"
        stress_csv = Join-Path $snapshotRoot "stress_summary.csv"
        criterion_dir = Join-Path $snapshotRoot "criterion"
        stress_dir = Join-Path $snapshotRoot "stress"
    }

    $copied = @{
        summary_md = Copy-IfExists -SourcePath (Join-Path $TargetRoot "bench_summary.md") -DestinationPath $paths.summary_md
        perf_scorecard_json = Copy-IfExists -SourcePath (Join-Path $TargetRoot "perf_scorecard.json") -DestinationPath $paths.perf_scorecard_json
        perf_scorecard_md = Copy-IfExists -SourcePath (Join-Path $TargetRoot "perf_scorecard.md") -DestinationPath $paths.perf_scorecard_md
        criterion_csv = Copy-IfExists -SourcePath (Join-Path $TargetRoot "criterion\benchmark_summary.csv") -DestinationPath $paths.criterion_csv
        stress_csv = Copy-IfExists -SourcePath (Join-Path $TargetRoot "stress\stress_summary.csv") -DestinationPath $paths.stress_csv
        criterion_dir = Copy-DirectoryIfExists -SourcePath (Join-Path $TargetRoot "criterion") -DestinationPath $paths.criterion_dir
        stress_dir = Copy-DirectoryIfExists -SourcePath (Join-Path $TargetRoot "stress") -DestinationPath $paths.stress_dir
    }

    $Manifest.snapshots[$SnapshotName] = @{
        created_at_utc = Get-TimestampUtc
        root = $snapshotRoot
        files = $paths
        copied = $copied
    }
    Save-Manifest $Manifest
}

function Import-CriterionRows {
    param([string]$CsvPath)

    if ([string]::IsNullOrWhiteSpace($CsvPath) -or -not (Test-Path $CsvPath)) {
        return @()
    }

    return @(Import-Csv $CsvPath | ForEach-Object {
        [pscustomobject]@{
            key = $_.benchmark
            kind = "criterion"
            name = $_.benchmark
            mean = [double]$_.mean
            rel_stddev = if ($_.rel_stddev -and $_.rel_stddev -ne "NA") { [double]$_.rel_stddev } else { 0.0 }
            source = $CsvPath
        }
    })
}

function Import-StressRows {
    param([string]$CsvPath)

    if ([string]::IsNullOrWhiteSpace($CsvPath) -or -not (Test-Path $CsvPath)) {
        return @()
    }

    return @(Import-Csv $CsvPath | ForEach-Object {
        $layer = if ($_.PSObject.Properties.Name -contains "layer") { $_.layer } else { $null }
        $keyParts = @($_.suite, $_.scenario)
        if (-not [string]::IsNullOrWhiteSpace($layer)) {
            $keyParts += $layer
        }

        [pscustomobject]@{
            key = ($keyParts -join "|")
            kind = "stress"
            name = "$($_.suite) / $($_.scenario)" + $(if ($layer) { " / $layer" } else { "" })
            suite = $_.suite
            scenario = $_.scenario
            layer = $layer
            per_op_us = [double]$_.per_op_us
            throughput_ops_per_s = [double]$_.throughput_ops_per_s
            rel_stddev = if ($_.rel_stddev_runs -and $_.rel_stddev_runs -ne "NA") { [double]$_.rel_stddev_runs } else { 0.0 }
            source = $CsvPath
        }
    })
}

function Get-SelectionBucketId {
    param($Target)

    $targetClass = if (Test-MapKey -Map $Target -Key "target_class" -and $null -ne (Get-MapValue -Map $Target -Key "target_class")) { [string](Get-MapValue -Map $Target -Key "target_class") } else { "" }
    $budgetGroup = if (Test-MapKey -Map $Target -Key "budget_group" -and $null -ne (Get-MapValue -Map $Target -Key "budget_group")) { [string](Get-MapValue -Map $Target -Key "budget_group") } else { "" }

    switch ($targetClass) {
        "engine_core" {
            return "engine_core"
        }
        "internal_explainer" {
            return "internal_explainer"
        }
        "service_budget" {
            switch ($budgetGroup) {
                "direct_api" { return "service_budget/direct_api" }
                "transport" { return "service_budget/transport" }
                "contention" { return "service_budget/contention" }
            }
        }
    }

    return "other"
}

function Get-SelectionBucketRank {
    param([string]$BucketId)

    if ($SelectionBucketOrder.ContainsKey($BucketId)) {
        return [int]$SelectionBucketOrder[$BucketId]
    }

    return 999
}

function Get-TargetKey {
    param(
        [string]$Kind,
        $Row
    )

    if ($Kind -eq "criterion") {
        return "criterion:$($Row.key)"
    }

    if ($Kind -eq "stress") {
        return "stress:$($Row.key)"
    }

    throw "Unsupported target kind: $Kind"
}

function Get-TargetMapValue {
    param(
        $PerformanceTargets,
        [string]$TargetKey
    )

    if (-not (Test-MapKey -Map $PerformanceTargets.targets -Key $TargetKey)) {
        return $null
    }

    return Get-MapValue -Map $PerformanceTargets.targets -Key $TargetKey
}

function Get-CurrentMeanUs {
    param(
        [string]$Kind,
        $Row
    )

    if ($Kind -eq "criterion") {
        return ([double]$Row.mean) / 1000.0
    }

    if ($Kind -eq "stress") {
        return [double]$Row.per_op_us
    }

    throw "Unsupported metric kind: $Kind"
}

function Get-TargetGapPct {
    param(
        [double]$CurrentMeanUs,
        $TargetMeanUs
    )

    if ($null -eq $TargetMeanUs) {
        return $null
    }

    $target = [double]$TargetMeanUs
    if ($target -le 0) {
        return $null
    }

    return (($CurrentMeanUs - $target) / $target) * 100.0
}

function Test-TargetIsActionable {
    param(
        $Target,
        [double]$RelStddev
    )

    $gating = [string]$Target.gating
    switch ($gating) {
        "hard" {
            return $true
        }
        "variance_gated" {
            if ($null -eq $Target.max_rel_stddev) {
                return $false
            }
            return $RelStddev -le [double]$Target.max_rel_stddev
        }
        default {
            return $false
        }
    }
}

function Select-OptimizationTarget {
    param(
        $PerformanceTargets,
        [string]$CurrentCriterionCsv,
        [string]$CurrentStressCsv,
        [string]$PreviousCriterionCsv,
        [string]$PreviousStressCsv
    )

    $currentCriterion = Import-CriterionRows $CurrentCriterionCsv
    $currentStress = Import-StressRows $CurrentStressCsv
    $previousCriterion = @{}
    $previousStress = @{}

    foreach ($row in (Import-CriterionRows $PreviousCriterionCsv)) {
        $previousCriterion[$row.key] = $row
    }
    foreach ($row in (Import-StressRows $PreviousStressCsv)) {
        $previousStress[$row.key] = $row
    }

    $candidates = @()

    foreach ($row in $currentCriterion) {
        $targetKey = Get-TargetKey -Kind "criterion" -Row $row
        $target = Get-TargetMapValue -PerformanceTargets $PerformanceTargets -TargetKey $targetKey
        if ($null -eq $target) {
            continue
        }

        if (-not (Test-TargetIsActionable -Target $target -RelStddev ([double]$row.rel_stddev))) {
            continue
        }

        $currentMeanUs = Get-CurrentMeanUs -Kind "criterion" -Row $row
        $operationalGapPct = Get-TargetGapPct -CurrentMeanUs $currentMeanUs -TargetMeanUs $target.operational_target
        $stretchGapPct = Get-TargetGapPct -CurrentMeanUs $currentMeanUs -TargetMeanUs $target.stretch_target
        $selectedTier = $null
        $selectedTargetUs = $null
        $selectedGapPct = $null
        $selectionBucket = Get-SelectionBucketId $target
        $selectionBucketRank = Get-SelectionBucketRank $selectionBucket

        if ($null -ne $operationalGapPct -and $operationalGapPct -gt 0) {
            $selectedTier = "operational"
            $selectedTargetUs = [double]$target.operational_target
            $selectedGapPct = $operationalGapPct
        }
        elseif ($null -ne $stretchGapPct -and $stretchGapPct -gt 0) {
            $selectedTier = "stretch"
            $selectedTargetUs = [double]$target.stretch_target
            $selectedGapPct = $stretchGapPct
        }
        else {
            continue
        }

        $previousMeanUs = $null
        $regressionPct = $null
        if ($previousCriterion.ContainsKey($row.key)) {
            $previousMeanUs = ([double]$previousCriterion[$row.key].mean) / 1000.0
            if ($previousMeanUs -gt 0) {
                $regressionPct = (($currentMeanUs - $previousMeanUs) / $previousMeanUs) * 100.0
            }
        }

        $candidates += [pscustomobject]@{
            key = $row.key
            target_key = $targetKey
            name = $row.name
            kind = $row.kind
            domain = [string]$target.domain
            target_class = [string]$target.target_class
            budget_group = if (Test-MapKey -Map $target -Key "budget_group" -and $null -ne (Get-MapValue -Map $target -Key "budget_group")) { [string](Get-MapValue -Map $target -Key "budget_group") } else { $null }
            selection_bucket = $selectionBucket
            selection_bucket_rank = $selectionBucketRank
            gating = $target.gating
            target_tier = $selectedTier
            target_mean_us = $selectedTargetUs
            current_mean_us = [Math]::Round($currentMeanUs, 6)
            operational_gap_pct = if ($null -ne $operationalGapPct) { [Math]::Round($operationalGapPct, 3) } else { $null }
            gap_pct = [Math]::Round($selectedGapPct, 3)
            stretch_gap_pct = if ($null -ne $stretchGapPct) { [Math]::Round($stretchGapPct, 3) } else { $null }
            rel_stddev = [Math]::Round([double]$row.rel_stddev, 6)
            previous_mean_us = if ($null -ne $previousMeanUs) { [Math]::Round($previousMeanUs, 6) } else { $null }
            regression_pct = if ($null -ne $regressionPct) { [Math]::Round($regressionPct, 3) } else { $null }
            reason = "criterion mean $(('{0:N3}' -f $currentMeanUs)) us is $(('{0:N1}' -f $selectedGapPct))% over $selectedTier target $(('{0:N3}' -f $selectedTargetUs)) us; rel_stddev $(('{0:P2}' -f $row.rel_stddev))"
        }
    }

    foreach ($row in $currentStress) {
        $targetKey = Get-TargetKey -Kind "stress" -Row $row
        $target = Get-TargetMapValue -PerformanceTargets $PerformanceTargets -TargetKey $targetKey
        if ($null -eq $target) {
            continue
        }

        if (-not (Test-TargetIsActionable -Target $target -RelStddev ([double]$row.rel_stddev))) {
            continue
        }

        $currentMeanUs = Get-CurrentMeanUs -Kind "stress" -Row $row
        $operationalGapPct = Get-TargetGapPct -CurrentMeanUs $currentMeanUs -TargetMeanUs $target.operational_target
        $stretchGapPct = Get-TargetGapPct -CurrentMeanUs $currentMeanUs -TargetMeanUs $target.stretch_target
        $selectedTier = $null
        $selectedTargetUs = $null
        $selectedGapPct = $null
        $selectionBucket = Get-SelectionBucketId $target
        $selectionBucketRank = Get-SelectionBucketRank $selectionBucket

        if ($null -ne $operationalGapPct -and $operationalGapPct -gt 0) {
            $selectedTier = "operational"
            $selectedTargetUs = [double]$target.operational_target
            $selectedGapPct = $operationalGapPct
        }
        elseif ($null -ne $stretchGapPct -and $stretchGapPct -gt 0) {
            $selectedTier = "stretch"
            $selectedTargetUs = [double]$target.stretch_target
            $selectedGapPct = $stretchGapPct
        }
        else {
            continue
        }

        $previousMeanUs = $null
        $regressionPct = $null
        if ($previousStress.ContainsKey($row.key)) {
            $previousMeanUs = [double]$previousStress[$row.key].per_op_us
            if ($previousMeanUs -gt 0) {
                $regressionPct = (($currentMeanUs - $previousMeanUs) / $previousMeanUs) * 100.0
            }
        }

        $candidates += [pscustomobject]@{
            key = $row.key
            target_key = $targetKey
            name = $row.name
            kind = $row.kind
            domain = [string]$target.domain
            target_class = [string]$target.target_class
            budget_group = if (Test-MapKey -Map $target -Key "budget_group" -and $null -ne (Get-MapValue -Map $target -Key "budget_group")) { [string](Get-MapValue -Map $target -Key "budget_group") } else { $null }
            selection_bucket = $selectionBucket
            selection_bucket_rank = $selectionBucketRank
            gating = $target.gating
            target_tier = $selectedTier
            target_mean_us = $selectedTargetUs
            current_mean_us = [Math]::Round($currentMeanUs, 6)
            operational_gap_pct = if ($null -ne $operationalGapPct) { [Math]::Round($operationalGapPct, 3) } else { $null }
            gap_pct = [Math]::Round($selectedGapPct, 3)
            stretch_gap_pct = if ($null -ne $stretchGapPct) { [Math]::Round($stretchGapPct, 3) } else { $null }
            throughput_ops_per_s = [Math]::Round([double]$row.throughput_ops_per_s, 3)
            rel_stddev = [Math]::Round([double]$row.rel_stddev, 6)
            previous_mean_us = if ($null -ne $previousMeanUs) { [Math]::Round($previousMeanUs, 6) } else { $null }
            regression_pct = if ($null -ne $regressionPct) { [Math]::Round($regressionPct, 3) } else { $null }
            reason = "stress mean $(('{0:N3}' -f $currentMeanUs)) us is $(('{0:N1}' -f $selectedGapPct))% over $selectedTier target $(('{0:N3}' -f $selectedTargetUs)) us; throughput $(('{0:N0}' -f $row.throughput_ops_per_s)) ops/sec"
        }
    }

    if ($candidates.Count -eq 0) {
        return $null
    }

    return $candidates |
        Sort-Object -Property `
            @{ Expression = { $_.selection_bucket_rank }; Descending = $false }, `
            @{ Expression = { if ($null -ne $_.operational_gap_pct) { $_.operational_gap_pct } else { [double]::NegativeInfinity } }; Descending = $true }, `
            @{ Expression = { if ($null -ne $_.stretch_gap_pct) { $_.stretch_gap_pct } else { [double]::NegativeInfinity } }; Descending = $true }, `
            @{ Expression = { $_.current_mean_us }; Descending = $true }, `
            @{ Expression = { $_.name }; Descending = $false } |
        Select-Object -First 1
}

function Compare-BenchSnapshots {
    param(
        [string]$BaselineCriterionCsv,
        [string]$BaselineStressCsv,
        [string]$CurrentCriterionCsv,
        [string]$CurrentStressCsv
    )

    $baselineCriterion = @{}
    $baselineStress = @{}
    foreach ($row in (Import-CriterionRows $BaselineCriterionCsv)) { $baselineCriterion[$row.key] = $row }
    foreach ($row in (Import-StressRows $BaselineStressCsv)) { $baselineStress[$row.key] = $row }

    $criterionChanges = @()
    foreach ($row in (Import-CriterionRows $CurrentCriterionCsv)) {
        if (-not $baselineCriterion.ContainsKey($row.key)) { continue }
        $before = [double]$baselineCriterion[$row.key].mean
        if ($before -le 0) { continue }
        $deltaPct = (($before - $row.mean) / $before) * 100.0
        $criterionChanges += [pscustomobject]@{
            key = $row.key
            name = $row.name
            delta_pct = [Math]::Round($deltaPct, 3)
            better = $deltaPct -gt 1.0
        }
    }

    $stressChanges = @()
    foreach ($row in (Import-StressRows $CurrentStressCsv)) {
        if (-not $baselineStress.ContainsKey($row.key)) { continue }
        $before = [double]$baselineStress[$row.key].throughput_ops_per_s
        if ($before -le 0) { continue }
        $deltaPct = (($row.throughput_ops_per_s - $before) / $before) * 100.0
        $stressChanges += [pscustomobject]@{
            key = $row.key
            name = $row.name
            delta_pct = [Math]::Round($deltaPct, 3)
            better = $deltaPct -gt 1.0
        }
    }

    $allChanges = @($criterionChanges + $stressChanges)
    $improvements = @($allChanges | Where-Object { $_.better })
    $regressions = @($allChanges | Where-Object { $_.delta_pct -lt -1.0 })

    $bestImprovement = $improvements | Sort-Object -Property @{Expression = "delta_pct"; Descending = $true } | Select-Object -First 1
    $worstRegression = $regressions | Sort-Object -Property delta_pct | Select-Object -First 1

    return @{
        generated_at_utc = Get-TimestampUtc
        improvement_count = $improvements.Count
        regression_count = $regressions.Count
        best_improvement = $bestImprovement
        worst_regression = $worstRegression
        stop_condition_met = ($improvements.Count -gt 0)
    }
}

function Get-LatestCompletedCycleSnapshot {
    param([string]$SnapshotName)

    if (-not (Test-Path $LoopRoot)) {
        return $null
    }

    $candidates = Get-ChildItem -Path $LoopRoot -Directory |
        Where-Object { $_.Name -ne $CycleLabel } |
        Sort-Object LastWriteTimeUtc -Descending

    foreach ($dir in $candidates) {
        $candidateManifestPath = Join-Path $dir.FullName "manifest.json"
        if (-not (Test-Path $candidateManifestPath)) {
            continue
        }

        $candidateManifest = ConvertTo-Hashtable (Get-Content $candidateManifestPath -Raw | ConvertFrom-Json)
        if (Test-MapKey -Map $candidateManifest.snapshots -Key $SnapshotName) {
            return Get-MapValue -Map $candidateManifest.snapshots -Key $SnapshotName
        }
    }

    return $null
}

New-Directory $TargetRoot
New-Directory $LoopRoot
New-Directory $CycleRoot
New-Directory $LogsRoot
New-Directory $SnapshotsRoot

$manifest = Load-Manifest
Save-Manifest $manifest
$performanceTargets = Load-PerformanceTargets

Push-Location $RepoRoot
try {
    if (-not $ResumeFromOptimize) {
        $testExit = Invoke-LoggedCommand -Manifest $manifest -PhaseName "test_initial" -LogName "test_initial.log" -CommandText "cargo test --all"
        if ($testExit -ne 0) {
            throw "Initial test pass failed. See $($manifest.phases.test_initial.log_path)."
        }

        $benchExit = Invoke-LoggedCommand -Manifest $manifest -PhaseName "bench_initial" -LogName "bench_initial.log" -CommandText (Get-BenchCommandText)
        if ($benchExit -ne 0) {
            throw "Initial benchmark pass failed. See $($manifest.phases.bench_initial.log_path)."
        }

        $python = Get-PythonCommand
        $summaryExit = Invoke-LoggedCommand -Manifest $manifest -PhaseName "summary_initial" -LogName "summary_initial.log" -CommandText "`"$python`" scripts\benchmark_summary.py"
        if ($summaryExit -ne 0) {
            throw "Initial benchmark summary generation failed. See $($manifest.phases.summary_initial.log_path)."
        }

        Save-BenchSnapshot -Manifest $manifest -SnapshotName "baseline"
        $baseline = $manifest.snapshots.baseline
        $previousCycleBaseline = Get-LatestCompletedCycleSnapshot -SnapshotName "verification"
        $target = Select-OptimizationTarget `
            -PerformanceTargets $performanceTargets `
            -CurrentCriterionCsv $baseline.files.criterion_csv `
            -CurrentStressCsv $baseline.files.stress_csv `
            -PreviousCriterionCsv $(if ($previousCycleBaseline) { $previousCycleBaseline.files.criterion_csv } else { $null }) `
            -PreviousStressCsv $(if ($previousCycleBaseline) { $previousCycleBaseline.files.stress_csv } else { $null })

        $manifest.optimization_target = if ($null -ne $target) {
            @{
                selected_at_utc = Get-TimestampUtc
                candidate = $target
                source_snapshot = "baseline"
            }
        } else {
            @{
                selected_at_utc = Get-TimestampUtc
                candidate = $null
                source_snapshot = "baseline"
            }
        }
        Save-Manifest $manifest

        Write-Status "Optimization checkpoint recorded. Make code changes, then rerun with -ResumeFromOptimize."
        return
    }

    if (-not (Test-MapKey -Map $manifest.snapshots -Key "baseline")) {
        throw "Cannot resume from optimize without an existing baseline snapshot in $CycleRoot."
    }

    $testExit = Invoke-LoggedCommand -Manifest $manifest -PhaseName "test_verification" -LogName "test_verification.log" -CommandText "cargo test --all"
    if ($testExit -ne 0) {
        throw "Verification test pass failed. See $($manifest.phases.test_verification.log_path)."
    }

    $benchExit = Invoke-LoggedCommand -Manifest $manifest -PhaseName "bench_verification" -LogName "bench_verification.log" -CommandText (Get-BenchCommandText)
    if ($benchExit -ne 0) {
        throw "Verification benchmark pass failed. See $($manifest.phases.bench_verification.log_path)."
    }

    $python = Get-PythonCommand
    $summaryExit = Invoke-LoggedCommand -Manifest $manifest -PhaseName "summary_verification" -LogName "summary_verification.log" -CommandText "`"$python`" scripts\benchmark_summary.py"
    if ($summaryExit -ne 0) {
        throw "Verification benchmark summary generation failed. See $($manifest.phases.summary_verification.log_path)."
    }

    Save-BenchSnapshot -Manifest $manifest -SnapshotName "verification"

    $comparison = Compare-BenchSnapshots `
        -BaselineCriterionCsv $manifest.snapshots.baseline.files.criterion_csv `
        -BaselineStressCsv $manifest.snapshots.baseline.files.stress_csv `
        -CurrentCriterionCsv $manifest.snapshots.verification.files.criterion_csv `
        -CurrentStressCsv $manifest.snapshots.verification.files.stress_csv

    $manifest.comparison = $comparison
    Save-Manifest $manifest
    ($comparison | ConvertTo-Json -Depth 6) | Set-Content -Path $ComparisonPath

    Write-Status "Verification complete. stop_condition_met=$($comparison.stop_condition_met)"
}
finally {
    Pop-Location
}
