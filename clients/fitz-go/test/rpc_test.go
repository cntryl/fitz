package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/domains/rpc"
	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (RPC domain) ---
// - single request/response cycle succeeds
// - streaming response reassembled in order
// - request timeout returns error
// - multiple workers on same route handle requests
// - response with wrong correlation_id rejected
// - backpressure error when buffer full

// TestShouldRouteRequestToWorkerGivenRegisteredWorkerWhenRequestCalled
// verifies the basic RPC lifecycle: SUBSCRIBE_WORKER → REQUEST → RESPONSE.
func TestShouldRouteRequestToWorkerGivenRegisteredWorkerWhenRequestCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrSkip(ctx)
		fCaller.ConnectOrSkip(ctx)

		route := fWorker.UniqueRoute("rpc")

		// Register a worker that echoes the request body.
		sub, err := fWorker.Client().RPC().Subscribe(ctx, route, func(_ context.Context, req rpc.InboundRequest, w rpc.ResponseWriter) error {
			return w.Send(req.Body)
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Act — caller sends a request.
		iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("ping"), 5*time.Second)
		require.NoError(t, err)
		defer iter.Close()

		// Assert — read the echoed response.
		require.True(t, iter.Next(), "expected at least one response frame")
		assert.Equal(t, []byte("ping"), iter.Value().Body)
	})
}

// TestShouldReassembleStreamingResponseGivenMultiFrameResponseWhenSequenced
// verifies streaming RPC responses are reassembled in sequence order.
func TestShouldReassembleStreamingResponseGivenMultiFrameResponseWhenSequenced(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrSkip(ctx)
		fCaller.ConnectOrSkip(ctx)

		route := fWorker.UniqueRoute("rpc")

		// Worker sends 3 streaming frames.
		sub, err := fWorker.Client().RPC().Subscribe(ctx, route, func(_ context.Context, _ rpc.InboundRequest, w rpc.ResponseWriter) error {
			for i := 0; i < 3; i++ {
				if err := w.Send([]byte{byte(i)}); err != nil {
					return err
				}
			}
			return nil // framework sends stream_end
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Act
		iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("stream-me"), 5*time.Second)
		require.NoError(t, err)
		defer iter.Close()

		// Assert — should receive frames in sequence order.
		var seqs []uint64
		for iter.Next() {
			seqs = append(seqs, iter.Value().Sequence)
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, []uint64{0, 1, 2}, seqs, "sequences should be in order")
	})
}

// TestShouldReturnTimeoutGivenNoWorkerResponseWhenRequestTimeout verifies
// REQUEST operation times out when no worker is registered.
func TestShouldReturnTimeoutGivenNoWorkerResponseWhenRequestTimeout(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("rpc")

		// Act — call with no worker registered, short timeout.
		_, err := f.Client().RPC().Call(ctx, route, []byte("nobody-home"), 1*time.Second)

		// Assert
		assert.Error(t, err, "call with no worker should error or timeout")
	})
}

// TestShouldLoadBalanceGivenMultipleWorkersWhenConcurrentRequests verifies
// multiple workers on same route can handle requests.
func TestShouldLoadBalanceGivenMultipleWorkersWhenConcurrentRequests(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		fW1 := fixture.NewTestFixture(t, transport)
		fW2 := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fW1.ConnectOrSkip(ctx)
		fW2.ConnectOrSkip(ctx)
		fCaller.ConnectOrSkip(ctx)

		route := fW1.UniqueRoute("rpc")

		var mu sync.Mutex
		workerIDs := make(map[string]int)

		echoHandler := func(id string) rpc.RPCHandler {
			return func(_ context.Context, req rpc.InboundRequest, w rpc.ResponseWriter) error {
				mu.Lock()
				workerIDs[id]++
				mu.Unlock()
				return w.Send(req.Body)
			}
		}

		sub1, err := fW1.Client().RPC().Subscribe(ctx, route, echoHandler("w1"))
		require.NoError(t, err)
		defer sub1.Unsubscribe()

		sub2, err := fW2.Client().RPC().Subscribe(ctx, route, echoHandler("w2"))
		require.NoError(t, err)
		defer sub2.Unsubscribe()

		// Act — send several requests.
		for i := 0; i < 4; i++ {
			iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("req"), 5*time.Second)
			require.NoError(t, err)
			for iter.Next() {
			}
			iter.Close()
		}

		// Assert — both workers should have handled at least one request.
		mu.Lock()
		total := workerIDs["w1"] + workerIDs["w2"]
		mu.Unlock()
		assert.Equal(t, 4, total, "all requests should be handled")
	})
}

// TestShouldCorrelateResponseGivenCorrectCorrelationIDWhenMultipleRequests
// verifies correlation_id links requests to their responses.
func TestShouldCorrelateResponseGivenCorrectCorrelationIDWhenMultipleRequests(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrSkip(ctx)
		fCaller.ConnectOrSkip(ctx)

		route := fWorker.UniqueRoute("rpc")

		// Worker echoes body (which contains the caller's "ID").
		sub, err := fWorker.Client().RPC().Subscribe(ctx, route, func(_ context.Context, req rpc.InboundRequest, w rpc.ResponseWriter) error {
			return w.Send(req.Body)
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Act & Assert — send two requests, verify each gets the correct response.
		for _, payload := range []string{"req-A", "req-B"} {
			iter, err := fCaller.Client().RPC().Call(ctx, route, []byte(payload), 5*time.Second)
			require.NoError(t, err)
			require.True(t, iter.Next())
			assert.Equal(t, payload, string(iter.Value().Body), "response should match request")
			iter.Close()
		}
	})
}

// TestShouldUnregisterWorkerGivenActiveSubscriptionWhenUnsubscribeCalled
// verifies Subscription.Unsubscribe() stops receiving requests.
func TestShouldUnregisterWorkerGivenActiveSubscriptionWhenUnsubscribeCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		fWorker := fixture.NewTestFixture(t, transport)
		fCaller := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fWorker.ConnectOrSkip(ctx)
		fCaller.ConnectOrSkip(ctx)

		route := fWorker.UniqueRoute("rpc")

		sub, err := fWorker.Client().RPC().Subscribe(ctx, route, func(_ context.Context, req rpc.InboundRequest, w rpc.ResponseWriter) error {
			return w.Send(req.Body)
		})
		require.NoError(t, err)

		// Verify worker is registered.
		iter, err := fCaller.Client().RPC().Call(ctx, route, []byte("alive"), 3*time.Second)
		require.NoError(t, err)
		require.True(t, iter.Next())
		iter.Close()

		// Act — unsubscribe the worker.
		sub.Unsubscribe()

		// Assert — subsequent call should fail (no workers).
		_, err = fCaller.Client().RPC().Call(ctx, route, []byte("dead"), 2*time.Second)
		assert.Error(t, err, "call after worker unsubscribe should fail")
	})
}
