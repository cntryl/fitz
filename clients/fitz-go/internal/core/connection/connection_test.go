package connection_test

import (
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/testkit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock transport is now provided by testkit.MockTransport from internal/testkit package

// TestShouldCreateConnectionGivenValidConfig tests basic connection creation.
func TestShouldCreateConnectionGivenValidConfig(t *testing.T) {
	// Arrange
	transport := &testkit.MockTransport{}
	cfg := connection.DefaultConfig()

	// Act
	conn := connection.New(transport, cfg)

	// Assert
	require.NotNil(t, conn)
}

// TestShouldParseStandardResponseGivenSuccessStatus tests success response parsing.
func TestShouldParseStandardResponseGivenSuccessStatus(t *testing.T) {
	// Arrange - Success response: [status=0][remaining data]
	payload := []byte{0x00, 0x01, 0x02, 0x03}

	// Act
	success, remaining, err := connection.ParseStandardResponse(payload)

	// Assert
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, remaining)
}

// TestShouldParseStandardResponseGivenErrorStatus tests error response parsing.
func TestShouldParseStandardResponseGivenErrorStatus(t *testing.T) {
	// Arrange - Error response: [status=1][u32 BE len][error message]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU8(buf, 1) // Error status
	connection.WriteString(buf, "test error message")
	payload := buf.Bytes()

	// Act
	success, _, err := connection.ParseStandardResponse(payload)

	// Assert
	require.Error(t, err)
	assert.False(t, success)
	assert.Contains(t, err.Error(), "test error message")
}

// TestShouldRejectParseGivenEmptyPayload tests edge case of empty payload.
func TestShouldRejectParseGivenEmptyPayload(t *testing.T) {
	_, _, err := connection.ParseStandardResponse([]byte{})

	require.Error(t, err)
}

// TestShouldEncodeDecodeU32BE tests U32BE encoding/decoding.
func TestShouldEncodeDecodeU32BE(t *testing.T) {
	t.Run("value 0", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		connection.WriteU32BE(buf, 0)
		actual, _, err := connection.ReadU32BE(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, uint32(0), actual)
	})

	t.Run("value max uint32", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		connection.WriteU32BE(buf, 0xFFFFFFFF)
		actual, _, err := connection.ReadU32BE(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, uint32(0xFFFFFFFF), actual)
	})
}

// TestShouldEncodeDecodeU64BE tests U64BE encoding/decoding.
func TestShouldEncodeDecodeU64BE(t *testing.T) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	expectedValue := uint64(0x123456789ABCDEF0)

	connection.WriteU64BE(buf, expectedValue)
	actual, _, err := connection.ReadU64BE(buf.Bytes(), 0)

	require.NoError(t, err)
	assert.Equal(t, expectedValue, actual)
}

// TestShouldEncodeDecodeU8 tests U8 encoding/decoding.
func TestShouldEncodeDecodeU8(t *testing.T) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU8(buf, 42)
	actual, _, err := connection.ReadU8(buf.Bytes(), 0)

	require.NoError(t, err)
	assert.Equal(t, uint8(42), actual)
}

// TestShouldEncodeDecodeString tests string encoding/decoding.
func TestShouldEncodeDecodeString(t *testing.T) {
	t.Run("simple ASCII", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		expectedString := "hello world"

		connection.WriteString(buf, expectedString)
		actual, _, err := connection.ReadString(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, expectedString, actual)
	})

	t.Run("UTF-8 with special characters", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		expectedString := "test string with special chars: ñ 测试"

		connection.WriteString(buf, expectedString)
		actual, _, err := connection.ReadString(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, expectedString, actual)
	})

	t.Run("empty string", func(t *testing.T) {
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		connection.WriteString(buf, "")
		actual, _, err := connection.ReadString(buf.Bytes(), 0)

		require.NoError(t, err)
		assert.Equal(t, "", actual)
	})
}

// TestShouldMatchResponsesInFIFOOrder tests multiplexer FIFO ordering.
func TestShouldMatchResponsesInFIFOOrder(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Register 3 requests for same MessageType
	resp1 := make(chan []byte, 1)
	resp2 := make(chan []byte, 1)
	resp3 := make(chan []byte, 1)

	mux.RegisterRequest(100, resp1, nil)
	mux.RegisterRequest(100, resp2, nil)
	mux.RegisterRequest(100, resp3, nil)

	// Act - Dispatch responses
	mux.Dispatch(100, []byte("response_1"))
	mux.Dispatch(100, []byte("response_2"))
	mux.Dispatch(100, []byte("response_3"))

	// Assert - Verify FIFO order
	assert.Equal(t, []byte("response_1"), <-resp1)
	assert.Equal(t, []byte("response_2"), <-resp2)
	assert.Equal(t, []byte("response_3"), <-resp3)
}

// TestShouldReturnMetricsGivenMultiplexer tests metrics collection.
func TestShouldReturnMetricsGivenMultiplexer(t *testing.T) {
	// Arrange
	mux := connection.NewMultiplexer()
	defer mux.Close()

	respChan := make(chan []byte, 1)
	mux.RegisterRequest(100, respChan, nil)

	// Act
	metrics := mux.Metrics()

	// Assert
	assert.Equal(t, int64(1), metrics.RequestsInFlight)
	assert.Equal(t, uint64(1), metrics.RequestsTotal)
}

// TestShouldDispatchToCorrectChannel tests response routing.
func TestShouldDispatchToCorrectChannel(t *testing.T) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	resp := make(chan []byte, 1)
	mux.RegisterRequest(100, resp, nil)

	mux.Dispatch(100, []byte("test response"))

	select {
	case data := <-resp:
		assert.Equal(t, []byte("test response"), data)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("response not received")
	}
}

// Benchmarks

func BenchmarkEncodeDecodeU32BE(b *testing.B) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		connection.WriteU32BE(buf, 0x12345678)
		_, _, _ = connection.ReadU32BE(buf.Bytes(), 0)
	}
}

func BenchmarkEncodeDecodeU64BE(b *testing.B) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		connection.WriteU64BE(buf, 0x123456789ABCDEF0)
		_, _, _ = connection.ReadU64BE(buf.Bytes(), 0)
	}
}

func BenchmarkEncodeDecodeString(b *testing.B) {
	b.Run("small string", func(b *testing.B) {
		testString := "hello world"
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			connection.WriteString(buf, testString)
			_, _, _ = connection.ReadString(buf.Bytes(), 0)
		}
	})

	b.Run("large string", func(b *testing.B) {
		testString := "this is a much longer test string that might be used in real KV operations"
		buf := connection.GetBuffer()
		defer connection.PutBuffer(buf)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			connection.WriteString(buf, testString)
			_, _, _ = connection.ReadString(buf.Bytes(), 0)
		}
	})
}

func BenchmarkDispatchResponse(b *testing.B) {
	mux := connection.NewMultiplexer()
	defer mux.Close()

	// Pre-register channels to avoid registration overhead in benchmark
	channels := make([]chan []byte, 100)
	for i := 0; i < 100; i++ {
		ch := make(chan []byte, 1)
		channels[i] = ch
		mux.RegisterRequest(uint16(100+i), ch, nil)
	}

	payload := []byte("test response")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mux.Dispatch(uint16(100+i%100), payload)
	}
}

func BenchmarkParseStandardResponseSuccess(b *testing.B) {
	// [status=0][1K remaining]
	remaining := make([]byte, 1024)
	payload := make([]byte, 1+len(remaining))
	payload[0] = 0
	copy(payload[1:], remaining)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}

func BenchmarkParseStandardResponseError(b *testing.B) {
	// [status=1][u32 len][error message]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU8(buf, 1)
	connection.WriteString(buf, "test error message")
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}

func BenchmarkGetPutBuffer(b *testing.B) {
	// Warm up the pool
	for i := 0; i < 10; i++ {
		buf := connection.GetBuffer()
		connection.PutBuffer(buf)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := connection.GetBuffer()
		connection.PutBuffer(buf)
	}
}
