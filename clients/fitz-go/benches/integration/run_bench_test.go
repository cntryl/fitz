package integration

import (
	"testing"
)

// TestRunKvBenchmarks runs the KV benchmarks
func TestRunKvBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running KV Transaction Benchmarks ===")

	// Run 1 client scenario
	t.Log("\n--- 1 client, 10B payload ---")
	runKvTransactionBench(&testing.B{}, 1, 10)

	t.Log("\n--- 10 clients, 1KB payload ---")
	runKvTransactionBench(&testing.B{}, 10, 1024)

	t.Log("\n--- 50 clients, 10KB payload ---")
	runKvTransactionBench(&testing.B{}, 50, 10240)
}

// TestRunKvPutBenchmarks runs KV PUT benchmarks
func TestRunKvPutBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running KV Put Benchmarks ===")

	t.Log("\n--- 1 client, 10B payload ---")
	runKvPutBench(&testing.B{}, 1, 10)

	t.Log("\n--- 10 clients, 1KB payload ---")
	runKvPutBench(&testing.B{}, 10, 1024)
}

// TestRunNoticeBenchmarks runs the Notice benchmarks
func TestRunNoticeBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running Notice Publish Benchmarks ===")

	t.Log("\n--- 1 client, 10B payload ---")
	runNoticePublishBench(&testing.B{}, 1, 10)

	t.Log("\n--- 10 clients, 1KB payload ---")
	runNoticePublishBench(&testing.B{}, 10, 1024)
}

// TestRunQueueBenchmarks runs the Queue benchmarks
func TestRunQueueBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running Queue Enqueue Benchmarks ===")

	t.Log("\n--- 1 client, 10B payload ---")
	runQueueEnqueueBench(&testing.B{}, 1, 10)

	t.Log("\n--- 10 clients, 1KB payload ---")
	runQueueEnqueueBench(&testing.B{}, 10, 1024)
}

// TestRunLeaseContentionBenchmarks runs the Lease contention benchmarks
func TestRunLeaseContentionBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running Lease Acquire Contention Benchmarks ===")

	t.Log("\n--- 1 client ---")
	runLeaseAcquireBench(&testing.B{}, 1)

	t.Log("\n--- 10 clients ---")
	runLeaseAcquireBench(&testing.B{}, 10)
}

// TestRunLeaseThroughputBenchmarks runs the Lease throughput benchmarks
func TestRunLeaseThroughputBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running Lease Acquire Throughput Benchmarks (1 RTT) ===")

	t.Log("\n--- 1 client ---")
	runLeaseAcquireThroughputBench(&testing.B{}, 1)

	t.Log("\n--- 10 clients ---")
	runLeaseAcquireThroughputBench(&testing.B{}, 10)

	t.Log("\n--- 50 clients ---")
	runLeaseAcquireThroughputBench(&testing.B{}, 50)
}

// TestRunKvPutInTransactionBenchmarks runs KV PUT single-operation benchmarks
func TestRunKvPutInTransactionBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running KV PutInTransaction Benchmarks (1 RTT) ===")

	t.Log("\n--- 1 client, 10B payload ---")
	runKvPutInTransactionBench(&testing.B{}, 1, 10)

	t.Log("\n--- 10 clients, 1KB payload ---")
	runKvPutInTransactionBench(&testing.B{}, 10, 1024)

	t.Log("\n--- 50 clients, 10KB payload ---")
	runKvPutInTransactionBench(&testing.B{}, 50, 10240)
}

// TestRunKvGetInTransactionBenchmarks runs KV GET single-operation benchmarks
func TestRunKvGetInTransactionBenchmarks(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skip("Fitz broker not available at localhost:4091")
	}

	t.Log("=== Running KV GetInTransaction Benchmarks (1 RTT) ===")

	t.Log("\n--- 1 client, 10B payload ---")
	runKvGetInTransactionBench(&testing.B{}, 1, 10)

	t.Log("\n--- 10 clients, 1KB payload ---")
	runKvGetInTransactionBench(&testing.B{}, 10, 1024)

	t.Log("\n--- 50 clients, 10KB payload ---")
	runKvGetInTransactionBench(&testing.B{}, 50, 10240)
}
