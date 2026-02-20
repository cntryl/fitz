package rpc

import (
	"encoding/binary"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldMapRPCError tests error message mapping.
func TestShouldMapRPCError(t *testing.T) {
	t.Run("map no workers error", func(t *testing.T) {
		// Arrange
		errMsg := "no workers available for request"

		// Act
		mapped := mapRPCError(errMsg)

		// Assert
		assert.Equal(t, ErrNoWorkers, mapped)
	})

	t.Run("map no workers case insensitive", func(t *testing.T) {
		// Arrange
		errMsg := "NO WORKERS AVAILABLE"

		// Act
		mapped := mapRPCError(errMsg)

		// Assert
		assert.Equal(t, ErrNoWorkers, mapped)
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := "unexpected error condition"

		// Act
		mapped := mapRPCError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped.Error())
	})

	t.Run("empty error message", func(t *testing.T) {
		// Arrange
		errMsg := ""

		// Act
		mapped := mapRPCError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
	})

	t.Run("timeout related error", func(t *testing.T) {
		// Arrange
		errMsg := "rpc operation timed out"

		// Act
		mapped := mapRPCError(errMsg)

		// Assert
		// Should return wrapped as unknown error
		assert.Equal(t, "rpc operation timed out", mapped.Error())
	})
}

// TestShouldDefineRPCOpcodes tests that RPC opcodes are properly defined.
func TestShouldDefineRPCOpcodes(t *testing.T) {
	t.Run("subscribe worker opcode", func(t *testing.T) {
		assert.Equal(t, uint16(300), RPCSubscribeWorker)
	})

	t.Run("unsubscribe worker opcode", func(t *testing.T) {
		assert.Equal(t, uint16(301), RPCUnsubscribeWorker)
	})

	t.Run("request opcode", func(t *testing.T) {
		assert.Equal(t, uint16(302), RPCRequest)
	})

	t.Run("response opcode", func(t *testing.T) {
		assert.Equal(t, uint16(303), RPCResponse)
	})

	t.Run("ack opcode", func(t *testing.T) {
		assert.Equal(t, uint16(304), RPCAck)
	})

	t.Run("opcodes are sequential", func(t *testing.T) {
		// Verify opcodes follow expected numbering
		assert.Equal(t, RPCSubscribeWorker+1, RPCUnsubscribeWorker)
		assert.Equal(t, RPCUnsubscribeWorker+1, RPCRequest)
		assert.Equal(t, RPCRequest+1, RPCResponse)
		assert.Equal(t, RPCResponse+1, RPCAck)
	})

	t.Run("all opcodes in 300 range", func(t *testing.T) {
		// All RPC opcodes should be in the 300-304 range per CLIENT_SPEC.md
		assert.GreaterOrEqual(t, RPCSubscribeWorker, uint16(300))
		assert.LessOrEqual(t, RPCAck, uint16(304))
	})
}

// TestShouldDefineRPCErrors tests that RPC error variables are defined.
func TestShouldDefineRPCErrors(t *testing.T) {
	t.Run("no workers error defined", func(t *testing.T) {
		assert.NotNil(t, ErrNoWorkers)
		assert.Equal(t, "no workers available", ErrNoWorkers.Error())
	})

	t.Run("rpc timeout error defined", func(t *testing.T) {
		assert.NotNil(t, ErrRPCTimeout)
		assert.Equal(t, "rpc timeout", ErrRPCTimeout.Error())
	})
}

// TestShouldEncodeRPCSubscribeWorker tests RPC SUBSCRIBE_WORKER encoding.
func TestShouldEncodeRPCSubscribeWorker(t *testing.T) {
	t.Run("valid worker route", func(t *testing.T) {
		// Arrange
		route := "rpc://acme/jobs/process"

		// Act
		payload, err := EncodeRPCSubscribeWorker(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
		// Verify route length prefix
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})

	t.Run("empty route", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeRPCSubscribeWorker("")

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})

	t.Run("complex route", func(t *testing.T) {
		// Arrange
		route := "rpc://org.example.com/services/worker-pool/v2"

		// Act
		payload, err := EncodeRPCSubscribeWorker(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})
}

// TestShouldEncodeRPCUnsubscribeWorker tests RPC UNSUBSCRIBE_WORKER encoding.
func TestShouldEncodeRPCUnsubscribeWorker(t *testing.T) {
	t.Run("valid worker route", func(t *testing.T) {
		// Arrange
		route := "rpc://acme/tasks/handler"

		// Act
		payload, err := EncodeRPCUnsubscribeWorker(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
	})

	t.Run("unsubscribe empty route", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeRPCUnsubscribeWorker("")

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})
}

// TestShouldEncodeRPCRequest tests RPC REQUEST encoding.
func TestShouldEncodeRPCRequest(t *testing.T) {
	t.Run("valid request with all fields", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		for i := range correlationID {
			correlationID[i] = byte(i)
		}
		route := "rpc://acme/calculate"
		replyRoute := "rpc://acme/responses"
		body := []byte("test body")

		// Act
		payload, err := EncodeRPCRequest(correlationID, route, replyRoute, body)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 20) // correlation_id(4+16) + minimal route/body
	})

	t.Run("request with empty reply route", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		route := "rpc://test/endpoint"

		// Act
		payload, err := EncodeRPCRequest(correlationID, route, "", []byte("data"))

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})

	t.Run("request with empty body", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		route := "rpc://test"

		// Act
		payload, err := EncodeRPCRequest(correlationID, route, "", []byte{})

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})

	t.Run("request with large body", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		route := "rpc://large"
		body := make([]byte, 10000)

		// Act
		payload, err := EncodeRPCRequest(correlationID, route, "", body)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 10000)
	})
}

// TestShouldEncodeRPCResponse tests RPC RESPONSE encoding.
func TestShouldEncodeRPCResponse(t *testing.T) {
	t.Run("valid response not stream end", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		correlationID[0] = 0xAB
		sequence := uint64(42)
		body := []byte("response data")

		// Act
		payload, err := EncodeRPCResponse(correlationID, sequence, body, false)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 20)
		// Last byte should be 0 (stream_end = false)
		assert.Equal(t, byte(0), payload[len(payload)-1])
	})

	t.Run("response with stream end", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		sequence := uint64(100)
		body := []byte("final response")

		// Act
		payload, err := EncodeRPCResponse(correlationID, sequence, body, true)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Last byte should be 1 (stream_end = true)
		assert.Equal(t, byte(1), payload[len(payload)-1])
	})

	t.Run("response with empty body", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		sequence := uint64(0)

		// Act
		payload, err := EncodeRPCResponse(correlationID, sequence, []byte{}, false)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})

	t.Run("response with nil body", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		sequence := uint64(5)

		// Act
		payload, err := EncodeRPCResponse(correlationID, sequence, nil, true)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Should encode empty body
	})

	t.Run("response with max sequence", func(t *testing.T) {
		// Arrange
		var correlationID [16]byte
		sequence := uint64(0xFFFFFFFFFFFFFFFF)

		// Act
		payload, err := EncodeRPCResponse(correlationID, sequence, []byte("data"), false)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})
}

// Benchmarks

func BenchmarkEncodeRPCSubscribeWorker(b *testing.B) {
	b.Run("short route", func(b *testing.B) {
		route := "rpc://a/b"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeRPCSubscribeWorker(route)
		}
	})

	b.Run("long route", func(b *testing.B) {
		route := "rpc://organization.example.com/services/worker-pool/specialized-handler/v2"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeRPCSubscribeWorker(route)
		}
	})
}

func BenchmarkEncodeRPCUnsubscribeWorker(b *testing.B) {
	route := "rpc://acme/workers/handler"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeRPCUnsubscribeWorker(route)
	}
}

func BenchmarkEncodeRPCRequest(b *testing.B) {
	b.Run("small body", func(b *testing.B) {
		var correlationID [16]byte
		route := "rpc://acme/calculate"
		body := []byte("test data")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeRPCRequest(correlationID, route, "", body)
		}
	})

	b.Run("large body", func(b *testing.B) {
		var correlationID [16]byte
		route := "rpc://acme/process"
		body := make([]byte, 10000)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeRPCRequest(correlationID, route, "", body)
		}
	})
}

func BenchmarkEncodeRPCResponse(b *testing.B) {
	b.Run("not stream end", func(b *testing.B) {
		var correlationID [16]byte
		body := []byte("response data here")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeRPCResponse(correlationID, 42, body, false)
		}
	})

	b.Run("stream end", func(b *testing.B) {
		var correlationID [16]byte
		body := []byte("final response")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeRPCResponse(correlationID, 100, body, true)
		}
	})
}

func BenchmarkParseRpcAckResponse(b *testing.B) {
	// [status=0] (ack only)
	payload := []byte{0}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}
