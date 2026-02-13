package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/benchkit"
	"github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/domains/lease"
)

// BenchmarkLeaseAcquireContention benchmarks distributed lease acquisition under contention.
// Multiple clients compete for the same lease to test serialization behavior.
// Expected: 10-100 ops/sec (serialized lock acquisition with contention waits).
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkLeaseAcquireContention(b *testing.B) {
	if !IsBrokerAvailable() {
		b.Skipf("Fitz broker not available at localhost:4091")
	}

	scenarios := []struct {
		name       string
		numClients int
	}{
		{"1client", 1},
		{"10clients", 10},
		{"50clients", 50},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runLeaseAcquireBench(b, scenario.numClients)
		})
	}
}

func runLeaseAcquireBench(b *testing.B, numClients int) {
	// Create clients
	clients := make([]lease.Client, numClients)
	fitzClients := make([]*client.Client, numClients)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < numClients; i++ {
		var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
			return "", nil
		}
		fc := client.NewClient("localhost:4091", tokenProvider)
		err := fc.Connect(ctx)
		if err != nil {
			b.Fatalf("failed to connect: %v", err)
		}
		fitzClients[i] = fc
		clients[i] = fc.Lease()
	}

	defer func() {
		for _, fc := range fitzClients {
			_ = fc.Close()
		}
	}()

	histogram := benchkit.NewHistogram()
	operationCount := atomic.Int64{}
	errorCount := atomic.Int64{}
	done := atomic.Bool{}

	// Run benchmark with concurrent clients
	var wg sync.WaitGroup
	benchDuration := 10 * time.Second
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			leaseClient := clients[clientIdx]
			route := fmt.Sprintf("lease://realm%d/app/locks", (clientIdx/10)%10) // Contention: groups of clients compete for same lease

			for !done.Load() {
				opStart := time.Now()
				_, err := leaseClient.Acquire(context.Background(), route, 30)
				elapsed := time.Since(opStart)

				histogram.RecordDuration(elapsed)
				operationCount.Add(1)

				if err != nil {
					errorCount.Add(1)
				}
			}
		}(i)
	}

	// Wait for benchmark duration
	time.Sleep(benchDuration)
	done.Store(true)
	wg.Wait()

	actualDuration := time.Since(start)

	// Report results
	results := &benchkit.BenchmarkResults{
		Name:       fmt.Sprintf("LeaseAcquire_%dclients", numClients),
		NumClients: numClients,
		Duration:   actualDuration,
		OpCount:    operationCount.Load(),
		ErrorCount: errorCount.Load(),
		Throughput: float64(operationCount.Load()) / actualDuration.Seconds(),
		P50:        time.Duration(histogram.P50()),
		P95:        time.Duration(histogram.P95()),
		P99:        time.Duration(histogram.P99()),
		P999:       time.Duration(histogram.P999()),
		Min:        time.Duration(histogram.Min()),
		Max:        time.Duration(histogram.Max()),
		Mean:       time.Duration(histogram.Mean()),
	}

	fmt.Printf("\n%s\n", results.String())
}

// BenchmarkLeaseAcquireThroughput benchmarks lease acquisition throughput without contention.
// Each client acquires a unique lease (1 network round trip per operation).
// This matches Fitz server tier 1 benchmark methodology.
// Expected: 1000-2000 ops/sec at 1 RTT × 500µs = 500µs-1ms per op.
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkLeaseAcquireThroughput(b *testing.B) {
	if !IsBrokerAvailable() {
		b.Skipf("Fitz broker not available at localhost:4091")
	}

	scenarios := []struct {
		name       string
		numClients int
	}{
		{"1client", 1},
		{"10clients", 10},
		{"50clients", 50},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runLeaseAcquireThroughputBench(b, scenario.numClients)
		})
	}
}

func runLeaseAcquireThroughputBench(b *testing.B, numClients int) {
	// Create clients
	clients := make([]lease.Client, numClients)
	fitzClients := make([]*client.Client, numClients)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < numClients; i++ {
		var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
			return "", nil
		}
		fc := client.NewClient("localhost:4091", tokenProvider)
		err := fc.Connect(ctx)
		if err != nil {
			b.Fatalf("failed to connect: %v", err)
		}
		fitzClients[i] = fc
		clients[i] = fc.Lease()
	}

	defer func() {
		for _, fc := range fitzClients {
			_ = fc.Close()
		}
	}()

	histogram := benchkit.NewHistogram()
	operationCount := atomic.Int64{}
	errorCount := atomic.Int64{}
	done := atomic.Bool{}

	// Warmup: exclude cold starts
	warmupOps := 100
	for i := 0; i < warmupOps; i++ {
		route := fmt.Sprintf("lease://realm0/app/warmup_%d", i)
		_, _ = clients[i%numClients].Acquire(context.Background(), route, 30)
	}

	// Run benchmark with concurrent clients
	var wg sync.WaitGroup
	benchDuration := 10 * time.Second
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			leaseClient := clients[clientIdx]
			leaseCounter := 0

			for !done.Load() {
				// Each client acquires unique lease (no contention)
				route := fmt.Sprintf("lease://realm%d/app/lock_%d_%d", clientIdx%10, clientIdx, leaseCounter)
				leaseCounter++

				opStart := time.Now()
				_, err := leaseClient.Acquire(context.Background(), route, 30)
				elapsed := time.Since(opStart)

				histogram.RecordDuration(elapsed)
				operationCount.Add(1)

				if err != nil {
					errorCount.Add(1)
				}
			}
		}(i)
	}

	// Wait for benchmark duration
	time.Sleep(benchDuration)
	done.Store(true)
	wg.Wait()

	actualDuration := time.Since(start)

	// Report results
	results := &benchkit.BenchmarkResults{
		Name:       fmt.Sprintf("LeaseAcquireThroughput_%dclients", numClients),
		NumClients: numClients,
		Duration:   actualDuration,
		OpCount:    operationCount.Load(),
		ErrorCount: errorCount.Load(),
		Throughput: float64(operationCount.Load()) / actualDuration.Seconds(),
		P50:        time.Duration(histogram.P50()),
		P95:        time.Duration(histogram.P95()),
		P99:        time.Duration(histogram.P99()),
		P999:       time.Duration(histogram.P999()),
		Min:        time.Duration(histogram.Min()),
		Max:        time.Duration(histogram.Max()),
		Mean:       time.Duration(histogram.Mean()),
	}

	fmt.Printf("\n%s\n", results.String())
}
