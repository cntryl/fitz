package connection_test

import (
	"sync"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
)

// TestShouldAllocateCorrelationID tests that each request gets a unique ID.
func TestShouldAllocateCorrelationID(t *testing.T) {
	t.Run("incremental IDs", func(t *testing.T) {
		mux := connection.NewMultiplexer()
		defer mux.Close()

		for i := 0; i < 3; i++ {
			ch := make(chan []byte, 1)
			mux.RegisterRequest(uint16(100+i), ch, nil)
			// IDs are assigned during registration, but we can verify registration succeeds
		}

		metrics := mux.Metrics()
		assert.Equal(t, int64(3), metrics.RequestsInFlight)
	})
}

// TestShouldRouteResponseByCorrelationID tests that responses go to correct channels.
func TestShouldRouteResponseByCorrelationID(t *testing.T) {
	t.Run("matching ID receives response", func(t *testing.T) {
		mux := connection.NewMultiplexer()
		defer mux.Close()

		ch1 := make(chan []byte, 1)
		ch2 := make(chan []byte, 1)

		mux.RegisterRequest(100, ch1, nil)
		mux.RegisterRequest(200, ch2, nil)

		mux.Dispatch(100, []byte("response1"))
		mux.Dispatch(200, []byte("response2"))

		assert.Equal(t, []byte("response1"), <-ch1)
		assert.Equal(t, []byte("response2"), <-ch2)
	})

	t.Run("unregistered ID discards response", func(t *testing.T) {
		mux := connection.NewMultiplexer()
		defer mux.Close()

		ch := make(chan []byte, 1)
		mux.RegisterRequest(100, ch, nil)

		// Dispatch to unregistered ID (should not panic or block)
		mux.Dispatch(999, []byte("orphaned"))

		select {
		case <-ch:
			t.Fatal("response should not be received on ch")
		case <-time.After(50 * time.Millisecond):
			// Expected - timeout waiting for unregistered dispatch
		}
	})
}

// TestShouldUnblockWaiterOnResponse tests that waiting goroutines are unblocked.
func TestShouldUnblockWaiterOnResponse(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	ch := make(chan []byte, 1)
	done := make(chan bool, 1)

	mux.RegisterRequest(100, ch, nil)

	// Goroutine waiting for response
	go func() {
		select {
		case <-ch:
			done <- true
		case <-time.After(1 * time.Second):
			done <- false
		}
	}()

	time.Sleep(50 * time.Millisecond)
	mux.Dispatch(100, []byte("response"))

	success := <-done
	assert.True(t, success)
}

// TestShouldHandleConcurrentRequests tests multiple concurrent requests.
func TestShouldHandleConcurrentRequests(t *testing.T) {
	t.Run("10 concurrent requests", func(t *testing.T) {
		mux := connection.NewMultiplexer()
		defer mux.Close()

		numRequests := 10
		responses := make([]chan []byte, numRequests)
		for i := 0; i < numRequests; i++ {
			ch := make(chan []byte, 1)
			responses[i] = ch
			mux.RegisterRequest(uint16(100+i), ch, nil)
		}

		// Dispatch responses
		for i := 0; i < numRequests; i++ {
			mux.Dispatch(uint16(100+i), []byte("response"+string(rune(i))))
		}

		// Collect all responses
		for i := 0; i < numRequests; i++ {
			resp := <-responses[i]
			assert.NotNil(t, resp)
		}

		metrics := mux.Metrics()
		assert.Equal(t, int64(0), metrics.RequestsInFlight) // All dispatched and consumed
	})

	t.Run("100 concurrent requests", func(t *testing.T) {
		mux := connection.NewMultiplexer()
		defer mux.Close()

		numRequests := 100
		responses := make([]chan []byte, numRequests)
		for i := 0; i < numRequests; i++ {
			ch := make(chan []byte, 1)
			responses[i] = ch
			mux.RegisterRequest(uint16(i), ch, nil)
		}

		// Dispatch responses concurrently
		var wg sync.WaitGroup
		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				mux.Dispatch(uint16(idx), []byte("resp"))
			}(i)
		}
		wg.Wait()

		// Collect all responses
		for i := 0; i < numRequests; i++ {
			resp := <-responses[i]
			assert.NotNil(t, resp)
		}
	})
}

// TestShouldMaintainFIFOOrder tests that responses are delivered in FIFO order.
func TestShouldMaintainFIFOOrder(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Register multiple requests for same message type (FIFO queue)
	ch1 := make(chan []byte, 1)
	ch2 := make(chan []byte, 1)
	ch3 := make(chan []byte, 1)

	mux.RegisterRequest(100, ch1, nil)
	mux.RegisterRequest(100, ch2, nil)
	mux.RegisterRequest(100, ch3, nil)

	// Dispatch in order
	mux.Dispatch(100, []byte("first"))
	mux.Dispatch(100, []byte("second"))
	mux.Dispatch(100, []byte("third"))

	// Verify FIFO order
	assert.Equal(t, []byte("first"), <-ch1)
	assert.Equal(t, []byte("second"), <-ch2)
	assert.Equal(t, []byte("third"), <-ch3)
}

// TestShouldCloseGracefully tests multiplexer close.
func TestShouldCloseGracefully(t *testing.T) {
	mux := connection.NewMultiplexer()
	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, nil)

	// Close should complete without hang
	mux.Close()

	// Verify no panics on subsequent close
	mux.Close()
}

// TestShouldReportMetrics tests metrics accuracy.
func TestShouldReportMetrics(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Initially empty
	metrics := mux.Metrics()
	assert.Zero(t, metrics.RequestsInFlight)
	assert.Zero(t, metrics.RequestsTotal)

	// Register one request
	ch := make(chan []byte, 1)
	mux.RegisterRequest(100, ch, nil)

	metrics = mux.Metrics()
	assert.Equal(t, int64(1), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)

	// Dispatch response
	mux.Dispatch(100, []byte("resp"))

	// Consume response
	<-ch

	metrics = mux.Metrics()
	assert.Equal(t, int64(0), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)
}

// Benchmarks

func BenchmarkRegisterRequest(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i%1000), ch, nil)
	}
}

func BenchmarkMuxDispatchResponse(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Pre-register 1000 requests
	for i := 0; i < 1000; i++ {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i), ch, nil)
	}

	payload := []byte("test response payload")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mux.Dispatch(uint16(i%1000), payload)
	}
}

func BenchmarkConcurrentDispatch(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Pre-register 1000 requests
	for i := 0; i < 1000; i++ {
		ch := make(chan []byte, 1)
		mux.RegisterRequest(uint16(i), ch, nil)
	}

	payload := []byte("test response")

	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mux.Dispatch(uint16(idx%1000), payload)
		}(i)
	}
	wg.Wait()
}
