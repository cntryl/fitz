package queue

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldEncodeEnqueueWithoutDelay tests ENQUEUE encoding without delay.
func TestShouldEncodeEnqueueWithoutDelay(t *testing.T) {
	t.Run("simple message", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/tasks"
		body := []byte("task data")
		delaySeconds := uint64(0)

		// Act
		payload := EncodeEnqueue(route, body, delaySeconds)

		// Assert
		require.NotNil(t, payload)
		require.Greater(t, len(payload), len(route)+len(body)+8)

		// Verify structure: [route_len][route][body_len][body][has_delay]
		offset := 0
		routeLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		assert.Equal(t, uint32(len(route)), routeLen)

		offset += 4
		actualRoute := string(payload[offset : offset+int(routeLen)])
		assert.Equal(t, route, actualRoute)

		offset += int(routeLen)
		bodyLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		assert.Equal(t, uint32(len(body)), bodyLen)

		offset += 4
		actualBody := payload[offset : offset+int(bodyLen)]
		assert.Equal(t, body, actualBody)

		offset += int(bodyLen)
		hasDelay := payload[offset]
		assert.Equal(t, uint8(0), hasDelay)
	})

	t.Run("empty body", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/events"
		body := []byte{}

		// Act
		payload := EncodeEnqueue(route, body, 0)

		// Assert
		require.NotNil(t, payload)
		// Minimum payload size: route_len(4) + route + body_len(4) + has_delay(1)
		minExpectedSize := 4 + len(route) + 4 + 1
		require.GreaterOrEqual(t, len(payload), minExpectedSize)
	})

	t.Run("large body", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/tasks"
		body := make([]byte, 65536) // 64KB
		for i := range body {
			body[i] = byte((i + 42) % 256)
		}

		// Act
		payload := EncodeEnqueue(route, body, 0)

		// Assert
		require.NotNil(t, payload)
		require.Greater(t, len(payload), len(body))
	})
}

// TestShouldEncodeEnqueueWithDelay tests ENQUEUE encoding with delay.
func TestShouldEncodeEnqueueWithDelay(t *testing.T) {
	t.Run("with delay seconds", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/tasks"
		body := []byte("delayed task")
		delaySeconds := uint64(3600) // 1 hour

		// Act
		payload := EncodeEnqueue(route, body, delaySeconds)

		// Assert
		require.NotNil(t, payload)
		// With delay, payload should be larger: route_len(4) + route + body_len(4) + body + has_delay(1) + delay_seconds(8)
		minSize := 4 + len(route) + 4 + len(body) + 1 + 8
		require.GreaterOrEqual(t, len(payload), minSize)
	})

	t.Run("max delay", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/tasks"
		body := []byte("data")
		maxDelay := uint64(0xFFFFFFFFFFFFFFFF)

		// Act
		payload := EncodeEnqueue(route, body, maxDelay)

		// Assert
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 8) // Must have delay bytes
	})
}

// TestShouldEncodeReserveWithBatchAndWait tests RESERVE encoding with all optional fields.
func TestShouldEncodeReserveWithBatchAndWait(t *testing.T) {
	t.Run("full reserve request", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/tasks"
		leaseSeconds := uint64(30)
		batchSize := uint32(10)
		waitSeconds := uint64(5)

		// Act
		payload := EncodeReserve(route, leaseSeconds, batchSize, waitSeconds)

		// Assert
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 0)

		// Verify it contains all required fields
		offset := 0
		routeLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		assert.Equal(t, uint32(len(route)), routeLen)

		offset += 4
		offset += int(routeLen)
		actualLease := binary.BigEndian.Uint64(payload[offset : offset+8])
		assert.Equal(t, leaseSeconds, actualLease)
	})

	t.Run("reserve without batch or wait", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/items"
		leaseSeconds := uint64(60)

		// Act
		payload := EncodeReserve(route, leaseSeconds, 0, 0)

		// Assert
		require.NotNil(t, payload)
	})

	t.Run("reserve with zero lease", func(t *testing.T) {
		// Arrange
		route := "queue://acme/app/work"
		leaseSeconds := uint64(0)

		// Act
		payload := EncodeReserve(route, leaseSeconds, 1, 0)

		// Assert
		require.NotNil(t, payload)
	})
}

// TestShouldParseQueueResponse tests response parsing.
func TestShouldParseQueueResponse(t *testing.T) {
	t.Run("success response", func(t *testing.T) {
		// Arrange
		payload := []byte{0x00, 0x01, 0x02, 0x03} // status=0 + data

		// Act
		success, data, err := parseQueueResponse(payload)

		// Assert
		require.NoError(t, err)
		assert.True(t, success)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, data)
	})

	t.Run("error with code - invalid token", func(t *testing.T) {
		// Arrange
		payload := []byte{0x01, queueErrCodeInvalidToken}

		// Act
		success, _, err := parseQueueResponse(payload)

		// Assert
		assert.False(t, success)
		assert.Equal(t, ErrInvalidToken, err)
	})

	t.Run("error with code - lease expired", func(t *testing.T) {
		// Arrange
		payload := []byte{0x01, queueErrCodeLeaseExpired}

		// Act
		success, _, err := parseQueueResponse(payload)

		// Assert
		assert.False(t, success)
		assert.Equal(t, ErrLeaseExpiredQ, err)
	})

	t.Run("error with code - not found", func(t *testing.T) {
		// Arrange
		payload := []byte{0x01, queueErrCodeNotFound}

		// Act
		success, _, err := parseQueueResponse(payload)

		// Assert
		assert.False(t, success)
		assert.Equal(t, ErrMessageNotFound, err)
	})

	t.Run("error with code - queue not found", func(t *testing.T) {
		// Arrange
		payload := []byte{0x01, queueErrCodeQueueNotFound}

		// Act
		success, _, err := parseQueueResponse(payload)

		// Assert
		assert.False(t, success)
		assert.Equal(t, ErrQueueNotFound, err)
	})

	t.Run("error with message", func(t *testing.T) {
		// Arrange — string error path is mapped to sentinels
		errMsg := "queue is full"
		msgBytes := []byte(errMsg)
		payload := make([]byte, 5+len(msgBytes))
		payload[0] = 0x01
		binary.BigEndian.PutUint32(payload[1:5], uint32(len(msgBytes)))
		copy(payload[5:], msgBytes)

		// Act
		success, _, err := parseQueueResponse(payload)

		// Assert
		assert.False(t, success)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrQueueFull)
	})

	t.Run("error response too short", func(t *testing.T) {
		// Arrange
		payload := []byte{}

		// Act
		success, _, err := parseQueueResponse(payload)

		// Assert
		assert.False(t, success)
		require.Error(t, err)
	})
}

// TestShouldMapQueueError tests error message mapping.
func TestShouldMapQueueError(t *testing.T) {
	t.Run("map invalid token", func(t *testing.T) {
		// Arrange
		errMsg := "invalid token provided"

		// Act
		mapped := mapQueueError(errMsg)

		// Assert
		assert.Equal(t, ErrInvalidToken, mapped)
	})

	t.Run("map lease expired", func(t *testing.T) {
		// Arrange
		errMsg := "lease has expired"

		// Act
		mapped := mapQueueError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseExpiredQ, mapped)
	})

	t.Run("map message not found", func(t *testing.T) {
		// Arrange
		errMsg := "message not found"

		// Act
		mapped := mapQueueError(errMsg)

		// Assert
		assert.Equal(t, ErrMessageNotFound, mapped)
	})

	t.Run("map queue not found", func(t *testing.T) {
		// Arrange
		errMsg := "queue not found in realm"

		// Act
		mapped := mapQueueError(errMsg)

		// Assert
		assert.Equal(t, ErrQueueNotFound, mapped)
	})

	t.Run("map queue full", func(t *testing.T) {
		// Arrange
		errMsg := "queue is full"

		// Act
		mapped := mapQueueError(errMsg)

		// Assert
		assert.Equal(t, ErrQueueFull, mapped)
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := "unknown error condition"

		// Act
		mapped := mapQueueError(errMsg)

		// Assert
		require.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped.Error())
	})
}

// Benchmarks

func BenchmarkEncodeEnqueue(b *testing.B) {
	b.Run("small message", func(b *testing.B) {
		route := "queue://acme/app/tasks"
		body := []byte("task data")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeEnqueue(route, body, 0)
		}
	})

	b.Run("large message", func(b *testing.B) {
		route := "queue://acme/app/tasks"
		body := make([]byte, 65536)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeEnqueue(route, body, 0)
		}
	})

	b.Run("with delay", func(b *testing.B) {
		route := "queue://acme/app/tasks"
		body := []byte("delayed task")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeEnqueue(route, body, 3600)
		}
	})
}

func BenchmarkEncodeReserve(b *testing.B) {
	b.Run("full reserve", func(b *testing.B) {
		route := "queue://acme/app/tasks"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeReserve(route, 30, 10, 5)
		}
	})

	b.Run("minimal reserve", func(b *testing.B) {
		route := "queue://acme/app/tasks"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeReserve(route, 30, 0, 0)
		}
	})
}

func BenchmarkParseQueueResponse(b *testing.B) {
	successPayload := []byte{0x00, 0x01, 0x02, 0x03}
	errorPayload := []byte{0x01, queueErrCodeInvalidToken}

	b.Run("success response", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = parseQueueResponse(successPayload)
		}
	})

	b.Run("error response", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = parseQueueResponse(errorPayload)
		}
	})
}

// TestShouldEncodeExtendRequest tests EXTEND operation encoding.
func TestShouldEncodeExtendRequest(t *testing.T) {
	t.Run("valid extend parameters", func(t *testing.T) {
		// Arrange
		route := "queue://acme/jobs"
		messageID := uint64(12345)
		leaseToken := uint64(0xABCDEF123456)
		leaseSeconds := uint64(60)

		// Act
		payload, err := EncodeExtend(route, messageID, leaseToken, leaseSeconds)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
	})

	t.Run("zero message id", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeExtend("route", 0, 999, 30)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})

	t.Run("max lease seconds", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeExtend("route", 1, 2, 0xFFFFFFFFFFFFFFFF)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})
}

// TestShouldEncodeCompleteRequest tests COMPLETE operation encoding.
func TestShouldEncodeCompleteRequest(t *testing.T) {
	t.Run("valid complete parameters", func(t *testing.T) {
		// Arrange
		route := "queue://acme/tasks"
		messageID := uint64(67890)
		leaseToken := uint64(0xFEDCBA987654)

		// Act
		payload, err := EncodeComplete(route, messageID, leaseToken)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
	})

	t.Run("empty route", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeComplete("", 123, 456)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Empty route is valid (encoded as 0-length string)
	})

	t.Run("zero token", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeComplete("queue://test", 100, 0)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Zero token encodes successfully (validation is server-side)
	})
}

// Benchmarks for new encoding functions

func BenchmarkEncodeExtend(b *testing.B) {
	b.Run("standard", func(b *testing.B) {
		route := "queue://acme/jobs/processing"
		messageID := uint64(12345)
		token := uint64(0xABCDEF123456)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeExtend(route, messageID, token, 30)
		}
	})
}

func BenchmarkEncodeComplete(b *testing.B) {
	b.Run("standard", func(b *testing.B) {
		route := "queue://acme/tasks/completed"
		messageID := uint64(67890)
		token := uint64(0xFEDCBA987654)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeComplete(route, messageID, token)
		}
	})
}
