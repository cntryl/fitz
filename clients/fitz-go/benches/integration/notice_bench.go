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
	"github.com/cntryl/fitz-go/internal/domains/notice"
)

// BenchmarkNoticePublish benchmarks pub/sub messaging fire-and-forget send latency.
// Measures only encode and send time (0 network round trips - no response wait).
// This is NOT comparable to request-response operations (KV, Queue, Lease).
// Expected: 30,000+ ops/sec (fire-and-forget send with no blocking).
// Requires real Fitz broker running at localhost:4091 (TCP).
func BenchmarkNoticePublish(b *testing.B) {
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
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			runNoticePublishBench(b, scenario.numClients, scenario.payloadSize)
		})
	}
}

func runNoticePublishBench(b *testing.B, numClients, payloadSize int) {
	// Create clients
	clients := make([]notice.Client, numClients)
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
		clients[i] = fc.Notice()
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
		route := fmt.Sprintf("notice://realm0/app/warmup")
		payload := GeneratePayload(payloadSize)
		_ = clients[i%numClients].Publish(context.Background(), route, payload)
	}

	// Run benchmark with concurrent clients
	var wg sync.WaitGroup
	benchDuration := 10 * time.Second
	start := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			noticeClient := clients[clientIdx]
			route := fmt.Sprintf("notice://realm%d/app/events", clientIdx%10)
			payload := GeneratePayload(payloadSize)

			for !done.Load() {
				opStart := time.Now()
				err := noticeClient.Publish(context.Background(), route, payload)
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
		Name:        fmt.Sprintf("NoticePublish_%dclients", numClients),
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
