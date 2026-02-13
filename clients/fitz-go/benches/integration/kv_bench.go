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
	"github.com/cntryl/fitz-go/internal/domains/kv"
)

// BenchmarkKvTransactionWorkflow benchmarks complete KV transaction workflows.
// Tests Begin → Get → Put → Commit sequences (4 network round trips).
// Measures end-to-end workflow latency, not individual operation throughput.
// Expected: 100-500 ops/sec at 4 RTTs × 500µs = 2-4ms per workflow.
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkKvTransactionWorkflow(b *testing.B) {
	if !IsBrokerAvailable() {
		b.Skipf("Fitz broker not available at localhost:4091")
	}

	scenarios := []struct {
		name        string
		numClients  int
		payloadSize int
	}{
		{"1client_10B", 1, 10},
		{"10clients_1KB", 10, 1024},
		{"50clients_10KB", 50, 10240},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runKvTransactionBench(b, scenario.numClients, scenario.payloadSize)
		})
	}
}

func runKvTransactionBench(b *testing.B, numClients, payloadSize int) {
	// Create clients
	clients := make([]kv.Client, numClients)
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
		clients[i] = fc.KV()
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
			kvClient := clients[clientIdx]
			// Each client gets unique route to avoid concurrency conflicts
			route := fmt.Sprintf("kv://realm%d/app/bench_%d", clientIdx, clientIdx)
			payload := GeneratePayload(payloadSize)
			key := []byte(fmt.Sprintf("key_%d", clientIdx))

			for !done.Load() {
				opStart := time.Now()
				err := runKvTransaction(context.Background(), kvClient, route, key, payload)
				elapsed := time.Since(opStart)

				histogram.RecordDuration(elapsed)
				operationCount.Add(1)

				if err != nil {
					errorCount.Add(1)
					// Debug: print first error only
					if errorCount.Load() == 1 {
						fmt.Printf("First KV transaction error: %v\n", err)
					}
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
		Name:        fmt.Sprintf("KvTransaction_%dclients", numClients),
		NumClients:  numClients,
		PayloadSize: payloadSize,
		Duration:    actualDuration,
		OpCount:     operationCount.Load(),
		ErrorCount:  errorCount.Load(),
		Throughput:  float64(operationCount.Load()) / actualDuration.Seconds(),
		P50:         time.Duration(histogram.P50()),
		P95:         time.Duration(histogram.P95()),
		P99:         time.Duration(histogram.P99()),
		P999:        time.Duration(histogram.P999()),
		Min:         time.Duration(histogram.Min()),
		Max:         time.Duration(histogram.Max()),
		Mean:        time.Duration(histogram.Mean()),
	}

	fmt.Printf("\n%s\n", results.String())
}

func runKvTransaction(ctx context.Context, kvClient kv.Client, route string, key []byte, payload []byte) error {
	// Begin transaction
	tx, err := kvClient.Begin(ctx, route)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	// Get (will be not found on first runs - that's OK)
	_, found, err := tx.Get(ctx, key)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("get: %w", err)
	}
	// Not finding the key is fine for this workflow benchmark
	_ = found

	// Put
	err = tx.Put(ctx, key, payload)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("put: %w", err)
	}

	// Commit
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// BenchmarkKvPutWorkflow benchmarks PUT workflow including transaction management.
// Tests Begin → Put → Commit sequences (3 network round trips).
// Measures workflow latency, not raw Put throughput.
// Expected: 100-500 ops/sec at 3 RTTs.
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkKvPutWorkflow(b *testing.B) {
	if !IsBrokerAvailable() {
		b.Skipf("Fitz broker not available at localhost:4091")
	}

	scenarios := []struct {
		name        string
		numClients  int
		payloadSize int
	}{
		{"1client_10B", 1, 10},
		{"10clients_1KB", 10, 1024},
		{"50clients_10KB", 50, 10240},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runKvPutBench(b, scenario.numClients, scenario.payloadSize)
		})
	}
}

func runKvPutBench(b *testing.B, numClients, payloadSize int) {
	// Create clients
	clients := make([]kv.Client, numClients)
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
		clients[i] = fc.KV()
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
			kvClient := clients[clientIdx]
			// Each client gets unique route to avoid concurrency conflicts
			route := fmt.Sprintf("kv://realm%d/app/bench_%d", clientIdx, clientIdx)
			payload := GeneratePayload(payloadSize)
			key := []byte(fmt.Sprintf("key_%d_%d", clientIdx, time.Now().UnixNano()))

			for !done.Load() {
				opStart := time.Now()
				err := runKvPut(context.Background(), kvClient, route, key, payload)
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
		Name:        fmt.Sprintf("KvPut_%dclients_%dB", numClients, payloadSize),
		NumClients:  numClients,
		PayloadSize: payloadSize,
		Duration:    actualDuration,
		OpCount:     operationCount.Load(),
		ErrorCount:  errorCount.Load(),
		Throughput:  float64(operationCount.Load()) / actualDuration.Seconds(),
		P50:         time.Duration(histogram.P50()),
		P95:         time.Duration(histogram.P95()),
		P99:         time.Duration(histogram.P99()),
		P999:        time.Duration(histogram.P999()),
		Min:         time.Duration(histogram.Min()),
		Max:         time.Duration(histogram.Max()),
		Mean:        time.Duration(histogram.Mean()),
	}

	fmt.Printf("\n%s\n", results.String())
}

func runKvPut(ctx context.Context, kvClient kv.Client, route string, key []byte, payload []byte) error {
	// Begin transaction
	tx, err := kvClient.Begin(ctx, route)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	// Put
	err = tx.Put(ctx, key, payload)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("put: %w", err)
	}

	// Commit
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// BenchmarkKvPutInTransaction benchmarks raw PUT operation throughput.
// Measures only Put() within an existing transaction (1 network round trip).
// This matches Fitz server tier 1 benchmark methodology.
// Expected: 1000-2000 ops/sec at 1 RTT × 500µs = 500µs-1ms per op.
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkKvPutInTransaction(b *testing.B) {
	if !IsBrokerAvailable() {
		b.Skipf("Fitz broker not available at localhost:4091")
	}

	scenarios := []struct {
		name        string
		numClients  int
		payloadSize int
	}{
		{"1client_10B", 1, 10},
		{"10clients_1KB", 10, 1024},
		{"50clients_10KB", 50, 10240},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runKvPutInTransactionBench(b, scenario.numClients, scenario.payloadSize)
		})
	}
}

func runKvPutInTransactionBench(b *testing.B, numClients, payloadSize int) {
	// Create clients
	clients := make([]kv.Client, numClients)
	fitzClients := make([]*client.Client, numClients)

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()

	for i := 0; i < numClients; i++ {
		var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
			return "", nil
		}
		fc := client.NewClient("localhost:4091", tokenProvider)
		err := fc.Connect(connectCtx)
		if err != nil {
			b.Fatalf("failed to connect: %v", err)
		}
		fitzClients[i] = fc
		clients[i] = fc.KV()
	}

	defer func() {
		for _, fc := range fitzClients {
			_ = fc.Close()
		}
	}()

	// Create transactions for each client with long-lived context
	transactions := make([]kv.Tx, numClients)
	for i := 0; i < numClients; i++ {
		// Each client gets unique route to avoid concurrency conflicts
		route := fmt.Sprintf("kv://realm%d/app/bench_%d", i, i)
		tx, err := clients[i].Begin(context.Background(), route)
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}
		transactions[i] = tx
	}

	defer func() {
		for _, tx := range transactions {
			_ = tx.Rollback(context.Background())
		}
	}()

	histogram := benchkit.NewHistogram()
	operationCount := atomic.Int64{}
	errorCount := atomic.Int64{}
	done := atomic.Bool{}

	// Warmup: exclude cold starts
	warmupOps := 100
	for i := 0; i < numClients && i < warmupOps; i++ {
		key := []byte(fmt.Sprintf("warmup_%d", i))
		payload := GeneratePayload(payloadSize)
		_ = transactions[i%numClients].Put(context.Background(), key, payload)
	}

	// Run benchmark with concurrent clients
	var wg sync.WaitGroup
	benchDuration := 10 * time.Second
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			tx := transactions[clientIdx]
			payload := GeneratePayload(payloadSize)
			keyCounter := 0

			for !done.Load() {
				key := []byte(fmt.Sprintf("key_%d_%d", clientIdx, keyCounter))
				keyCounter++

				opStart := time.Now()
				err := tx.Put(context.Background(), key, payload)
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
		Name:        fmt.Sprintf("KvPutInTransaction_%dclients_%dB", numClients, payloadSize),
		NumClients:  numClients,
		PayloadSize: payloadSize,
		Duration:    actualDuration,
		OpCount:     operationCount.Load(),
		ErrorCount:  errorCount.Load(),
		Throughput:  float64(operationCount.Load()) / actualDuration.Seconds(),
		P50:         time.Duration(histogram.P50()),
		P95:         time.Duration(histogram.P95()),
		P99:         time.Duration(histogram.P99()),
		P999:        time.Duration(histogram.P999()),
		Min:         time.Duration(histogram.Min()),
		Max:         time.Duration(histogram.Max()),
		Mean:        time.Duration(histogram.Mean()),
	}

	fmt.Printf("\n%s\n", results.String())
}

// BenchmarkKvGetInTransaction benchmarks raw GET operation throughput.
// Measures only Get() within an existing transaction (1 network round trip).
// This matches Fitz server tier 1 benchmark methodology.
// Expected: 1000-2000 ops/sec at 1 RTT × 500µs = 500µs-1ms per op.
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkKvGetInTransaction(b *testing.B) {
	if !IsBrokerAvailable() {
		b.Skipf("Fitz broker not available at localhost:4091")
	}

	scenarios := []struct {
		name        string
		numClients  int
		payloadSize int
	}{
		{"1client_10B", 1, 10},
		{"10clients_1KB", 10, 1024},
		{"50clients_10KB", 50, 10240},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runKvGetInTransactionBench(b, scenario.numClients, scenario.payloadSize)
		})
	}
}

func runKvGetInTransactionBench(b *testing.B, numClients, payloadSize int) {
	// Create clients
	clients := make([]kv.Client, numClients)
	fitzClients := make([]*client.Client, numClients)

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connectCancel()

	for i := 0; i < numClients; i++ {
		var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
			return "", nil
		}
		fc := client.NewClient("localhost:4091", tokenProvider)
		err := fc.Connect(connectCtx)
		if err != nil {
			b.Fatalf("failed to connect: %v", err)
		}
		fitzClients[i] = fc
		clients[i] = fc.KV()
	}

	defer func() {
		for _, fc := range fitzClients {
			_ = fc.Close()
		}
	}()

	// Create transactions and pre-populate keys with long-lived context
	transactions := make([]kv.Tx, numClients)
	for i := 0; i < numClients; i++ {
		// Each client gets unique route to avoid concurrency conflicts
		route := fmt.Sprintf("kv://realm%d/app/bench_%d", i, i)
		tx, err := clients[i].Begin(context.Background(), route)
		if err != nil {
			b.Fatalf("failed to begin transaction: %v", err)
		}
		transactions[i] = tx

		// Pre-populate keys for reading
		for j := 0; j < 100; j++ {
			key := []byte(fmt.Sprintf("key_%d_%d", i, j))
			payload := GeneratePayload(payloadSize)
			err = tx.Put(context.Background(), key, payload)
			if err != nil {
				b.Fatalf("failed to pre-populate key: %v", err)
			}
		}
	}

	defer func() {
		for _, tx := range transactions {
			_ = tx.Rollback(context.Background())
		}
	}()

	histogram := benchkit.NewHistogram()
	operationCount := atomic.Int64{}
	errorCount := atomic.Int64{}
	done := atomic.Bool{}

	// Warmup: exclude cold starts
	warmupOps := 100
	for i := 0; i < warmupOps; i++ {
		key := []byte(fmt.Sprintf("key_%d_%d", i%numClients, i%100))
		_, _, _ = transactions[i%numClients].Get(context.Background(), key)
	}

	// Run benchmark with concurrent clients
	var wg sync.WaitGroup
	benchDuration := 10 * time.Second
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			tx := transactions[clientIdx]
			keyCounter := 0

			for !done.Load() {
				key := []byte(fmt.Sprintf("key_%d_%d", clientIdx, keyCounter%100))
				keyCounter++

				opStart := time.Now()
				_, _, err := tx.Get(context.Background(), key)
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
		Name:        fmt.Sprintf("KvGetInTransaction_%dclients_%dB", numClients, payloadSize),
		NumClients:  numClients,
		PayloadSize: payloadSize,
		Duration:    actualDuration,
		OpCount:     operationCount.Load(),
		ErrorCount:  errorCount.Load(),
		Throughput:  float64(operationCount.Load()) / actualDuration.Seconds(),
		P50:         time.Duration(histogram.P50()),
		P95:         time.Duration(histogram.P95()),
		P99:         time.Duration(histogram.P99()),
		P999:        time.Duration(histogram.P999()),
		Min:         time.Duration(histogram.Min()),
		Max:         time.Duration(histogram.Max()),
		Mean:        time.Duration(histogram.Mean()),
	}

	fmt.Printf("\n%s\n", results.String())
}
