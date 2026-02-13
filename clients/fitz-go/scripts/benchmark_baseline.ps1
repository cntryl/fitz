# Capture baseline benchmarks for fitz-go client (PowerShell)

$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "=== Capturing Baseline Benchmarks ===" -ForegroundColor Cyan
Write-Host ""

# Run all benchmarks with memory stats
Write-Host "Running benchmarks (this may take a few minutes)..." -ForegroundColor Yellow
go test -bench=. -benchmem -benchtime=3s ./internal/... 2>&1 | Tee-Object -FilePath .benchmark_baseline.txt

Write-Host ""
Write-Host "Baseline captured to .benchmark_baseline.txt" -ForegroundColor Green
Write-Host ""

# Extract summary
Write-Host "=== Summary ===" -ForegroundColor Cyan
Select-String "Benchmark" .benchmark_baseline.txt | Select-Object -First 20

Write-Host ""
Write-Host "Full results in: .benchmark_baseline.txt" -ForegroundColor Green
Write-Host "Use this as comparison for future optimizations" -ForegroundColor Yellow
