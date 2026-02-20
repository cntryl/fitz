package stream

import (
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldMapStreamError tests error message mapping.
func TestShouldMapStreamError(t *testing.T) {
	t.Run("map stream not found error", func(t *testing.T) {
		// Arrange
		errMsg := "stream not found"

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamNotFound, mapped)
	})

	t.Run("map stream not found case insensitive", func(t *testing.T) {
		// Arrange
		errMsg := "Stream NOT FOUND in realm"

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamNotFound, mapped)
	})

	t.Run("map stream conflict error", func(t *testing.T) {
		// Arrange
		errMsg := "stream conflict detected"

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamConflict, mapped)
	})

	t.Run("map stream conflict case insensitive", func(t *testing.T) {
		// Arrange
		errMsg := "CONFLICT writing to stream"

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.Equal(t, ErrStreamConflict, mapped)
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := "unexpected stream condition"

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped.Error())
	})

	t.Run("empty error message", func(t *testing.T) {
		// Arrange
		errMsg := ""

		// Act
		mapped := mapStreamError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
	})
}

// TestShouldDefineStreamOpcodes tests that Stream opcodes are properly defined.
func TestShouldDefineStreamOpcodes(t *testing.T) {
	t.Run("begin opcode", func(t *testing.T) {
		assert.Equal(t, uint16(600), StreamBegin)
	})

	t.Run("append opcode", func(t *testing.T) {
		assert.Equal(t, uint16(601), StreamAppend)
	})

	t.Run("commit opcode", func(t *testing.T) {
		assert.Equal(t, uint16(602), StreamCommit)
	})

	t.Run("rollback opcode", func(t *testing.T) {
		assert.Equal(t, uint16(603), StreamRollback)
	})

	t.Run("read opcode", func(t *testing.T) {
		assert.Equal(t, uint16(604), StreamRead)
	})

	t.Run("last opcode", func(t *testing.T) {
		assert.Equal(t, uint16(605), StreamLast)
	})

	t.Run("get metadata opcode", func(t *testing.T) {
		assert.Equal(t, uint16(606), StreamGetMetadata)
	})

	t.Run("subscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(607), StreamSubscribe)
	})

	t.Run("unsubscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(608), StreamUnsubscribe)
	})

	t.Run("notify opcode server only", func(t *testing.T) {
		assert.Equal(t, uint16(609), StreamNotify)
	})

	t.Run("opcodes are sequential", func(t *testing.T) {
		assert.Equal(t, StreamBegin+1, StreamAppend)
		assert.Equal(t, StreamAppend+1, StreamCommit)
		assert.Equal(t, StreamCommit+1, StreamRollback)
		assert.Equal(t, StreamRollback+1, StreamRead)
		assert.Equal(t, StreamRead+1, StreamLast)
		assert.Equal(t, StreamLast+1, StreamGetMetadata)
		assert.Equal(t, StreamGetMetadata+1, StreamSubscribe)
		assert.Equal(t, StreamSubscribe+1, StreamUnsubscribe)
		assert.Equal(t, StreamUnsubscribe+1, StreamNotify)
	})

	t.Run("all opcodes in 600 range", func(t *testing.T) {
		assert.GreaterOrEqual(t, StreamBegin, uint16(600))
		assert.LessOrEqual(t, StreamNotify, uint16(609))
	})
}

// TestShouldDefineStreamErrors tests that Stream error variables are defined.
func TestShouldDefineStreamErrors(t *testing.T) {
	t.Run("stream not found error", func(t *testing.T) {
		assert.NotNil(t, ErrStreamNotFound)
		assert.Equal(t, "stream not found", ErrStreamNotFound.Error())
	})

	t.Run("stream conflict error", func(t *testing.T) {
		assert.NotNil(t, ErrStreamConflict)
		assert.Equal(t, "stream conflict", ErrStreamConflict.Error())
	})

	t.Run("stream read error", func(t *testing.T) {
		assert.NotNil(t, ErrStreamReadError)
		assert.Equal(t, "stream read error", ErrStreamReadError.Error())
	})
}

// TestShouldDefineStreamTransactionModes tests transaction mode constants.
func TestShouldDefineStreamTransactionModes(t *testing.T) {
	t.Run("stream has begin operation", func(t *testing.T) {
		// Stream should support transaction-like operations
		assert.Greater(t, StreamBegin, uint16(0))
	})

	t.Run("stream has append operation", func(t *testing.T) {
		assert.Greater(t, StreamAppend, uint16(0))
	})

	t.Run("stream has commit operation", func(t *testing.T) {
		assert.Greater(t, StreamCommit, uint16(0))
	})

	t.Run("stream has rollback operation", func(t *testing.T) {
		assert.Greater(t, StreamRollback, uint16(0))
	})
}

// TestShouldEncodeStreamBegin tests STREAM BEGIN encoding.
func TestShouldEncodeStreamBegin(t *testing.T) {
	t.Run("without ingest metadata", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		expectedOffset := uint64(42)

		// Act
		payload, err := EncodeStreamBegin(route, expectedOffset, nil)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
		offset = newOffset
		assert.Equal(t, route, string(payload[offset:offset+int(routeLen)]))
		offset += int(routeLen)
		actualExpected, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, expectedOffset, actualExpected)
		offset = newOffset
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(0), payload[offset])
	})

	t.Run("with ingest metadata", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		expectedOffset := uint64(7)
		metadata := []byte("meta")

		// Act
		payload, err := EncodeStreamBegin(route, expectedOffset, metadata)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		_, newOffset, err = connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(1), payload[offset])
		offset++
		meta, _, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, metadata, meta)
	})
}

// TestShouldEncodeStreamAppend tests STREAM APPEND encoding.
func TestShouldEncodeStreamAppend(t *testing.T) {
	t.Run("without metadata", func(t *testing.T) {
		// Arrange
		sessionID := uint64(123)
		body := []byte("payload")

		// Act
		payload, err := EncodeStreamAppend(sessionID, body, nil)

		// Assert
		require.NoError(t, err)
		offset := 0
		actualSessionID, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
		offset = newOffset
		actualBody, newOffset, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, body, actualBody)
		offset = newOffset
		require.Less(t, offset, len(payload))
		assert.Equal(t, byte(0), payload[offset])
	})

	t.Run("with metadata", func(t *testing.T) {
		// Arrange
		sessionID := uint64(456)
		body := []byte("payload")
		metadata := []byte("meta")

		// Act
		payload, err := EncodeStreamAppend(sessionID, body, metadata)

		// Assert
		require.NoError(t, err)
		offset := 0
		_, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		_, newOffset, err = connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		assert.Equal(t, byte(1), payload[offset])
		offset++
		actualMetadata, _, err := connection.ReadBytes(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, metadata, actualMetadata)
	})
}

// TestShouldEncodeStreamCommit tests STREAM COMMIT encoding.
func TestShouldEncodeStreamCommit(t *testing.T) {
	t.Run("buffered mode", func(t *testing.T) {
		// Arrange
		sessionID := uint64(999)
		mode := uint8(0)

		// Act
		payload, err := EncodeStreamCommit(sessionID, mode)

		// Assert
		require.NoError(t, err)
		actualSessionID, offset, err := connection.ReadU64BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
		require.Less(t, offset, len(payload))
		assert.Equal(t, mode, payload[offset])
	})
}

// TestShouldEncodeStreamRollback tests STREAM ROLLBACK encoding.
func TestShouldEncodeStreamRollback(t *testing.T) {
	t.Run("valid rollback", func(t *testing.T) {
		// Arrange
		sessionID := uint64(456)

		// Act
		payload, err := EncodeStreamRollback(sessionID)

		// Assert
		require.NoError(t, err)
		actualSessionID, _, err := connection.ReadU64BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, sessionID, actualSessionID)
	})
}

// TestShouldEncodeStreamRead tests STREAM READ encoding.
func TestShouldEncodeStreamRead(t *testing.T) {
	t.Run("without max bytes", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		fromOffset := uint64(10)
		limit := uint64(100)

		// Act
		payload, err := EncodeStreamRead(route, fromOffset, limit, nil)

		// Assert
		require.NoError(t, err)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		actualFrom, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, fromOffset, actualFrom)
		offset = newOffset
		actualLimit, newOffset, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, limit, actualLimit)
		offset = newOffset
		assert.Equal(t, byte(0), payload[offset])
	})

	t.Run("with max bytes", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"
		fromOffset := uint64(1)
		limit := uint64(10)
		maxBytes := uint64(4096)

		// Act
		payload, err := EncodeStreamRead(route, fromOffset, limit, &maxBytes)

		// Assert
		require.NoError(t, err)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		_, newOffset, err = connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		_, newOffset, err = connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset
		assert.Equal(t, byte(1), payload[offset])
		offset++
		actualMaxBytes, _, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, maxBytes, actualMaxBytes)
	})
}

// TestShouldEncodeStreamLast tests STREAM LAST encoding.
func TestShouldEncodeStreamLast(t *testing.T) {
	t.Run("valid route", func(t *testing.T) {
		// Arrange
		route := "stream://acme/logs/app"

		// Act
		payload, err := EncodeStreamLast(route)

		// Assert
		require.NoError(t, err)
		routeLen, _, err := connection.ReadU32BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldEncodeStreamGetMetadata tests STREAM GET_METADATA encoding.
func TestShouldEncodeStreamGetMetadata(t *testing.T) {
	t.Run("valid route", func(t *testing.T) {
		// Arrange
		route := "stream://acme/metadata"

		// Act
		payload, err := EncodeStreamGetMetadata(route)

		// Assert
		require.NoError(t, err)
		routeLen, _, err := connection.ReadU32BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldEncodeStreamSubscribe tests STREAM SUBSCRIBE encoding.
func TestShouldEncodeStreamSubscribe(t *testing.T) {
	t.Run("valid route and offset", func(t *testing.T) {
		// Arrange
		route := "stream://acme/sub"
		fromOffset := uint64(99)

		// Act
		payload, err := EncodeStreamSubscribe(route, fromOffset)

		// Assert
		require.NoError(t, err)
		offset := 0
		routeLen, newOffset, err := connection.ReadU32BE(payload, offset)
		require.NoError(t, err)
		offset = newOffset + int(routeLen)
		actualOffset, _, err := connection.ReadU64BE(payload, offset)
		require.NoError(t, err)
		assert.Equal(t, fromOffset, actualOffset)
	})
}

// TestShouldEncodeStreamUnsubscribe tests STREAM UNSUBSCRIBE encoding.
func TestShouldEncodeStreamUnsubscribe(t *testing.T) {
	t.Run("valid route", func(t *testing.T) {
		// Arrange
		route := "stream://acme/unsub"

		// Act
		payload, err := EncodeStreamUnsubscribe(route)

		// Assert
		require.NoError(t, err)
		routeLen, _, err := connection.ReadU32BE(payload, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// Benchmarks

func BenchmarkEncodeStreamBegin(b *testing.B) {
	b.Run("without metadata", func(b *testing.B) {
		route := "stream://acme/logs/app"
		expectedOffset := uint64(1)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeStreamBegin(route, expectedOffset, nil)
		}
	})

	b.Run("with metadata", func(b *testing.B) {
		route := "stream://acme/logs/app"
		expectedOffset := uint64(1)
		metadata := []byte("meta")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeStreamBegin(route, expectedOffset, metadata)
		}
	})
}

func BenchmarkEncodeStreamAppend(b *testing.B) {
	b.Run("without metadata", func(b *testing.B) {
		sessionID := uint64(10)
		body := []byte("payload")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeStreamAppend(sessionID, body, nil)
		}
	})

	b.Run("with metadata", func(b *testing.B) {
		sessionID := uint64(10)
		body := []byte("payload")
		metadata := []byte("meta")

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeStreamAppend(sessionID, body, metadata)
		}
	})
}

func BenchmarkEncodeStreamCommit(b *testing.B) {
	sessionID := uint64(10)
	mode := uint8(0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeStreamCommit(sessionID, mode)
	}
}

func BenchmarkEncodeStreamRollback(b *testing.B) {
	sessionID := uint64(10)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeStreamRollback(sessionID)
	}
}

func BenchmarkEncodeStreamRead(b *testing.B) {
	b.Run("without max bytes", func(b *testing.B) {
		route := "stream://acme/logs/app"
		fromOffset := uint64(1)
		limit := uint64(10)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeStreamRead(route, fromOffset, limit, nil)
		}
	})

	b.Run("with max bytes", func(b *testing.B) {
		route := "stream://acme/logs/app"
		fromOffset := uint64(1)
		limit := uint64(10)
		maxBytes := uint64(4096)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeStreamRead(route, fromOffset, limit, &maxBytes)
		}
	})
}

func BenchmarkEncodeStreamLast(b *testing.B) {
	route := "stream://acme/logs/app"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeStreamLast(route)
	}
}

func BenchmarkEncodeStreamGetMetadata(b *testing.B) {
	route := "stream://acme/logs/app"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeStreamGetMetadata(route)
	}
}

func BenchmarkEncodeStreamSubscribe(b *testing.B) {
	route := "stream://acme/logs/app"
	fromOffset := uint64(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeStreamSubscribe(route, fromOffset)
	}
}

func BenchmarkEncodeStreamUnsubscribe(b *testing.B) {
	route := "stream://acme/logs/app"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeStreamUnsubscribe(route)
	}
}

func BenchmarkParseStreamReadResponse(b *testing.B) {
	// data blob: [u32 count=1][u64 offset][u32 body_len][body]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU32BE(buf, 1)
	connection.WriteU64BE(buf, 1)
	connection.WriteBytes(buf, []byte("record1"))
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseReadResponse(payload)
	}
}

func BenchmarkParseStreamLastResponse(b *testing.B) {
	// single record: [u64 offset][u32 body_len][body]
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteU64BE(buf, 1)
	connection.WriteBytes(buf, []byte("last"))
	payload := make([]byte, buf.Len())
	copy(payload, buf.Bytes())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parseRecord(payload, 0)
	}
}
