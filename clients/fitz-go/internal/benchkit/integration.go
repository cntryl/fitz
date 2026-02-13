package benchkit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fitz "github.com/cntryl/fitz-go/fitz"
	"github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/types"
)

// IntegrationBenchmark represents a single integration benchmark scenario.
type IntegrationBenchmark struct {
	Name             string
	NumClients       int
	PayloadSize      int
	Duration         time.Duration
	histogram        *Histogram
	operationCounter int64
	errorCounter     int64
	clients          []fitz.Client
	clientsMu        sync.Mutex
}

// IntegrationHarness manages connections to a real Fitz broker and coordinates benchmarks.
type IntegrationHarness struct {
	brokerAddr string
	maxClients int
	clientPool map[string]fitz.Client
	poolMu     sync.RWMutex
}

// NewIntegrationHarness creates a new integration benchmark harness.
// brokerAddr should be "localhost:4091" for TCP or "ws://localhost:4090/ws" for WebSocket.
func NewIntegrationHarness(brokerAddr string, maxClients int) *IntegrationHarness {
	return &IntegrationHarness{
		brokerAddr: brokerAddr,
		maxClients: maxClients,
		clientPool: make(map[string]fitz.Client),
	}
}

// ConnectClient connects a new client to the broker and stores it in the pool.
func (h *IntegrationHarness) ConnectClient(ctx context.Context) (fitz.Client, error) {
	var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
		return "", nil
	}

	c := client.NewClient(h.brokerAddr, tokenProvider)
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

// GetClientPool returns up to n connected clients, creating new ones if needed.
func (h *IntegrationHarness) GetClientPool(ctx context.Context, n int) ([]fitz.Client, error) {
	if n > h.maxClients {
		n = h.maxClients
	}

	h.poolMu.Lock()
	defer h.poolMu.Unlock()

	// Reuse existing clients if available
	var clients []fitz.Client
	for id, c := range h.clientPool {
		if len(clients) < n {
			clients = append(clients, c)
			// Remove from pool to prevent reuse during concurrent ops
			delete(h.clientPool, id)
		} else {
			break
		}
	}

	// Create new clients for remaining slots
	for len(clients) < n {
		c, err := h.ConnectClient(ctx)
		if err != nil {
			// Return created clients to pool
			for _, rc := range clients {
				h.clientPool[fmt.Sprintf("client_%d", len(h.clientPool))] = rc
			}
			return nil, fmt.Errorf("failed to create client: %w", err)
		}
		clients = append(clients, c)
	}

	return clients, nil
}

// ReturnClientPool returns clients back to the pool for reuse.
func (h *IntegrationHarness) ReturnClientPool(clients []fitz.Client) {
	h.poolMu.Lock()
	defer h.poolMu.Unlock()

	for i, c := range clients {
		h.clientPool[fmt.Sprintf("client_%d_%d", time.Now().UnixNano(), i)] = c
	}
}

// CloseAll closes all clients in the pool.
func (h *IntegrationHarness) CloseAll() {
	h.poolMu.Lock()
	clients := make([]fitz.Client, 0, len(h.clientPool))
	for _, c := range h.clientPool {
		clients = append(clients, c)
	}
	h.clientPool = make(map[string]fitz.Client)
	h.poolMu.Unlock()

	// Close all clients
	for _, c := range clients {
		_ = c.Close()
	}
}

// NewBenchmark creates a new integration benchmark scenario.
func NewBenchmark(name string, numClients int, payloadSize int, duration time.Duration) *IntegrationBenchmark {
	return &IntegrationBenchmark{
		Name:        name,
		NumClients:  numClients,
		PayloadSize: payloadSize,
		Duration:    duration,
		histogram:   NewHistogram(),
	}
}

// RecordOperation records a latency measurement for an operation.
func (b *IntegrationBenchmark) RecordOperation(latency time.Duration) {
	b.histogram.RecordDuration(latency)
	atomic.AddInt64(&b.operationCounter, 1)
}

// RecordError increments the error counter.
func (b *IntegrationBenchmark) RecordError() {
	atomic.AddInt64(&b.errorCounter, 1)
}

// Results returns benchmark results.
func (b *IntegrationBenchmark) Results() BenchmarkResults {
	opCount := atomic.LoadInt64(&b.operationCounter)
	errorCount := atomic.LoadInt64(&b.errorCounter)

	throughput := 0.0
	if b.Duration > 0 {
		throughput = float64(opCount) / b.Duration.Seconds()
	}

	return BenchmarkResults{
		Name:        b.Name,
		NumClients:  b.NumClients,
		PayloadSize: b.PayloadSize,
		Duration:    b.Duration,
		OpCount:     opCount,
		ErrorCount:  errorCount,
		Throughput:  throughput,
		P50:         b.histogram.P50(),
		P95:         b.histogram.P95(),
		P99:         b.histogram.P99(),
		P999:        b.histogram.P999(),
		Min:         b.histogram.Min(),
		Max:         b.histogram.Max(),
		Mean:        b.histogram.Mean(),
	}
}

// Run executes a benchmark function with the given harness and returns results.
// The function receives the benchmark and a context that will be cancelled after Duration.
func Run(harness *IntegrationHarness, benchmark *IntegrationBenchmark, fn func(*IntegrationBenchmark, context.Context)) BenchmarkResults {
	ctx, cancel := context.WithTimeout(context.Background(), benchmark.Duration)
	defer cancel()

	fn(benchmark, ctx)

	return benchmark.Results()
}

// BenchmarkResults contains the results of a single benchmark scenario.
type BenchmarkResults struct {
	Name        string
	NumClients  int
	PayloadSize int
	Duration    time.Duration
	OpCount     int64
	ErrorCount  int64
	Throughput  float64 // ops/sec
	P50         time.Duration
	P95         time.Duration
	P99         time.Duration
	P999        time.Duration
	Min         time.Duration
	Max         time.Duration
	Mean        time.Duration
}

// String returns a formatted result string.
func (r BenchmarkResults) String() string {
	errStr := ""
	if r.ErrorCount > 0 {
		errStr = fmt.Sprintf(" (errors=%d)", r.ErrorCount)
	}

	return fmt.Sprintf(
		"%s (clients=%d, payloadSize=%dB): %d ops in %v (%.0f ops/sec)%s\n"+
			"  Latency: p50=%v p95=%v p99=%v p999=%v (min=%v, max=%v, mean=%v)",
		r.Name,
		r.NumClients,
		r.PayloadSize,
		r.OpCount,
		r.Duration,
		r.Throughput,
		errStr,
		r.P50,
		r.P95,
		r.P99,
		r.P999,
		r.Min,
		r.Max,
		r.Mean,
	)
}
