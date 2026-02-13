#!/usr/bin/env bash
# Capture baseline benchmarks for fitz-go client

set -e

cd "$(dirname "$0")/.."

echo "=== Capturing Baseline Benchmarks ==="
echo ""

# Run all benchmarks with memory stats
echo "Running benchmarks (this may take a few minutes)..."
go test -bench=. -benchmem -benchtime=3s ./internal/... > .benchmark_baseline.txt 2>&1

echo ""
echo "Baseline captured to .benchmark_baseline.txt"
echo ""

# Extract summary
echo "=== Summary ==="
grep "Benchmark" .benchmark_baseline.txt | head -20

echo ""
echo "Full results in: .benchmark_baseline.txt"
echo "Use this as comparison for future optimizations"
