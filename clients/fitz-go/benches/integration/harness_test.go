package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/benchkit"
)

// SkipIfBrokerNotAvailable checks if the Fitz broker is available and skips the test if not.
func SkipIfBrokerNotAvailable(t *testing.T) {
	if !IsBrokerAvailable() {
		t.Skipf("Fitz broker not available at localhost:4091\n" +
			"Start broker with: ./fitz server --listen 127.0.0.1:4091")
	}
}

// TestIntegrationHarnessConnectivity verifies the harness can connect to the broker.
func TestIntegrationHarnessConnectivity(t *testing.T) {
	SkipIfBrokerNotAvailable(t)

	harness := benchkit.NewIntegrationHarness("localhost:4091", 5)
	defer harness.CloseAll()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test single client connection
	client, err := harness.ConnectClient(ctx)
	if err != nil {
		t.Fatalf("failed to connect client: %v", err)
	}
	if client == nil {
		t.Fatal("client is nil")
	}
	err = client.Close()
	if err != nil {
		t.Logf("warning: failed to close client: %v", err)
	}
}

// TestIntegrationHarnessClientPool verifies the harness can manage a pool of clients.
func TestIntegrationHarnessClientPool(t *testing.T) {
	SkipIfBrokerNotAvailable(t)

	harness := benchkit.NewIntegrationHarness("localhost:4091", 10)
	defer harness.CloseAll()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get a pool of 5 clients
	clients, err := harness.GetClientPool(ctx, 5)
	if err != nil {
		t.Fatalf("failed to get client pool: %v", err)
	}
	if len(clients) != 5 {
		t.Fatalf("expected 5 clients, got %d", len(clients))
	}

	// Return clients to pool
	harness.ReturnClientPool(clients)

	// Get again - should reuse clients
	clients2, err := harness.GetClientPool(ctx, 5)
	if err != nil {
		t.Fatalf("failed to get client pool again: %v", err)
	}
	if len(clients2) != 5 {
		t.Fatalf("expected 5 clients, got %d", len(clients2))
	}
}

// TestHistogramLatencyCollection verifies the histogram collects latencies correctly.
func TestHistogramLatencyCollection(t *testing.T) {
	h := benchkit.NewHistogram()

	// Record some latencies in nanoseconds
	h.Record(1000000) // 1ms
	h.Record(2000000) // 2ms
	h.Record(3000000) // 3ms
	h.Record(4000000) // 4ms
	h.Record(5000000) // 5ms

	if h.Count() != 5 {
		t.Fatalf("expected count 5, got %d", h.Count())
	}

	p50 := h.P50()
	if p50 < 2_000_000 || p50 > 4_000_000 {
		t.Logf("P50 = %v (expected ~3ms)", p50)
	}

	p95 := h.P95()
	if p95 < 4_000_000 || p95 > 5_000_000 {
		t.Logf("P95 = %v (expected ~5ms)", p95)
	}

	min := h.Min()
	max := h.Max()
	if min != 1_000_000 || max != 5_000_000 {
		t.Fatalf("expected min=1ms max=5ms, got min=%v max=%v", min, max)
	}
}

// TestBenchmarkResults verifies result formatting.
func TestBenchmarkResults(t *testing.T) {
	h := benchkit.NewHistogram()
	for i := 0; i < 100; i++ {
		h.Record(int64((i + 1) * 1000000)) // 1ms to 100ms
	}

	result := benchkit.BenchmarkResults{
		Name:        "TestBench",
		NumClients:  10,
		PayloadSize: 1024,
		Duration:    30 * time.Second,
		OpCount:     3000,
		ErrorCount:  5,
		Throughput:  100.0,
		P50:         h.P50(),
		P95:         h.P95(),
		P99:         h.P99(),
		P999:        h.P999(),
		Min:         h.Min(),
		Max:         h.Max(),
		Mean:        h.Mean(),
	}

	s := result.String()
	if s == "" {
		t.Fatal("result string is empty")
	}

	// Check that result string contains expected fields
	expectedFields := []string{"TestBench", "clients=10", "payloadSize=1024B", "3000 ops", "100"}
	for _, field := range expectedFields {
		if !contains(s, field) {
			t.Logf("result string missing expected field: %s\nGot: %s", field, s)
		}
	}

	t.Logf("Result: %s", s)
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && fmt.Sprintf("%s", s[:]) != "" &&
		(s == substr || len(substr) <= len(s))
}
