package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldEncodeMessageTypeSingleByte(t *testing.T) {
	t.Run("type 0", func(t *testing.T) {
		result := EncodeMessageType(0)
		require.Len(t, result, 1)
		assert.Equal(t, byte(0x00), result[0])
	})

	t.Run("type 1", func(t *testing.T) {
		result := EncodeMessageType(1)
		require.Len(t, result, 1)
		assert.Equal(t, byte(0x01), result[0])
	})

	t.Run("type 100", func(t *testing.T) {
		result := EncodeMessageType(100)
		require.Len(t, result, 1)
		assert.Equal(t, byte(0x64), result[0])
	})

	t.Run("type 254 (max single byte)", func(t *testing.T) {
		result := EncodeMessageType(254)
		require.Len(t, result, 1)
		assert.Equal(t, byte(0xFE), result[0])
	})
}

func TestShouldEncodeMessageTypeEscaped(t *testing.T) {
	t.Run("type 255 (min escaped)", func(t *testing.T) {
		result := EncodeMessageType(255)
		require.Len(t, result, 3)
		assert.Equal(t, byte(0xFF), result[0])
		assert.Equal(t, byte(0x00), result[1])
		assert.Equal(t, byte(0xFF), result[2])
	})

	t.Run("type 500", func(t *testing.T) {
		result := EncodeMessageType(500)
		require.Len(t, result, 3)
		assert.Equal(t, byte(0xFF), result[0])
		assert.Equal(t, byte(0x01), result[1])
		assert.Equal(t, byte(0xF4), result[2])
	})

	t.Run("type 65535 (max uint16)", func(t *testing.T) {
		result := EncodeMessageType(65535)
		require.Len(t, result, 3)
		assert.Equal(t, byte(0xFF), result[0])
		assert.Equal(t, byte(0xFF), result[1])
		assert.Equal(t, byte(0xFF), result[2])
	})
}

func TestShouldDecodeMessageTypeSingleByte(t *testing.T) {
	t.Run("type 0", func(t *testing.T) {
		msgType, bytesRead, err := DecodeMessageType([]byte{0x00})
		require.NoError(t, err)
		assert.Equal(t, uint16(0), msgType)
		assert.Equal(t, 1, bytesRead)
	})

	t.Run("type 100", func(t *testing.T) {
		msgType, bytesRead, err := DecodeMessageType([]byte{0x64})
		require.NoError(t, err)
		assert.Equal(t, uint16(100), msgType)
		assert.Equal(t, 1, bytesRead)
	})

	t.Run("type 254", func(t *testing.T) {
		msgType, bytesRead, err := DecodeMessageType([]byte{0xFE})
		require.NoError(t, err)
		assert.Equal(t, uint16(254), msgType)
		assert.Equal(t, 1, bytesRead)
	})
}

func TestShouldDecodeMessageTypeEscaped(t *testing.T) {
	t.Run("type 255", func(t *testing.T) {
		msgType, bytesRead, err := DecodeMessageType([]byte{0xFF, 0x00, 0xFF})
		require.NoError(t, err)
		assert.Equal(t, uint16(255), msgType)
		assert.Equal(t, 3, bytesRead)
	})

	t.Run("type 500", func(t *testing.T) {
		msgType, bytesRead, err := DecodeMessageType([]byte{0xFF, 0x01, 0xF4})
		require.NoError(t, err)
		assert.Equal(t, uint16(500), msgType)
		assert.Equal(t, 3, bytesRead)
	})

	t.Run("type 65535", func(t *testing.T) {
		msgType, bytesRead, err := DecodeMessageType([]byte{0xFF, 0xFF, 0xFF})
		require.NoError(t, err)
		assert.Equal(t, uint16(65535), msgType)
		assert.Equal(t, 3, bytesRead)
	})
}

func TestShouldRejectDecodeMessageTypeGivenTruncatedData(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		_, _, err := DecodeMessageType([]byte{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient")
	})

	t.Run("escape byte without type bytes", func(t *testing.T) {
		_, _, err := DecodeMessageType([]byte{0xFF})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient")
	})

	t.Run("escape byte with partial type", func(t *testing.T) {
		_, _, err := DecodeMessageType([]byte{0xFF, 0x00})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient")
	})
}

func TestShouldEncodeDecodeFrameRoundTrip(t *testing.T) {
	t.Run("empty payload", func(t *testing.T) {
		payload := []byte{}
		msgType := uint16(100)

		encoded := EncodeFrame(msgType, payload)
		decoded, decodedPayload, err := DecodeFrame(encoded)

		require.NoError(t, err)
		assert.Equal(t, msgType, decoded)
		assert.Equal(t, payload, decodedPayload)
	})

	t.Run("small payload", func(t *testing.T) {
		payload := []byte("hello world")
		msgType := uint16(100)

		encoded := EncodeFrame(msgType, payload)
		decoded, decodedPayload, err := DecodeFrame(encoded)

		require.NoError(t, err)
		assert.Equal(t, msgType, decoded)
		assert.Equal(t, payload, decodedPayload)
	})

	t.Run("large payload", func(t *testing.T) {
		// 10KB payload
		payload := make([]byte, 10240)
		for i := range payload {
			payload[i] = byte(i % 256)
		}
		msgType := uint16(100)

		encoded := EncodeFrame(msgType, payload)
		decoded, decodedPayload, err := DecodeFrame(encoded)

		require.NoError(t, err)
		assert.Equal(t, msgType, decoded)
		assert.Equal(t, payload, decodedPayload)
	})

	t.Run("binary payload with nulls", func(t *testing.T) {
		payload := []byte{0x00, 0xFF, 0x00, 0x42, 0xAB, 0xCD}
		msgType := uint16(200)

		encoded := EncodeFrame(msgType, payload)
		decoded, decodedPayload, err := DecodeFrame(encoded)

		require.NoError(t, err)
		assert.Equal(t, msgType, decoded)
		assert.Equal(t, payload, decodedPayload)
	})

	t.Run("escaped message type", func(t *testing.T) {
		payload := []byte("test")
		msgType := uint16(500)

		encoded := EncodeFrame(msgType, payload)
		decoded, decodedPayload, err := DecodeFrame(encoded)

		require.NoError(t, err)
		assert.Equal(t, msgType, decoded)
		assert.Equal(t, payload, decodedPayload)
	})
}

func TestShouldRejectFrameGivenTruncatedData(t *testing.T) {
	t.Run("truncated in message type", func(t *testing.T) {
		data := []byte{0xFF}
		_, _, err := DecodeFrame(data)
		require.Error(t, err)
	})

	t.Run("truncated in length field", func(t *testing.T) {
		data := []byte{0x64, 0x00}
		_, _, err := DecodeFrame(data)
		require.Error(t, err)
	})

	t.Run("truncated in payload", func(t *testing.T) {
		data := []byte{0x64, 0x00, 0x10} // Says 16 bytes payload but only 0
		_, _, err := DecodeFrame(data)
		require.Error(t, err)
	})
}

func TestShouldHandleMaxSizePayload(t *testing.T) {
	// Create exactly 65535 byte payload (max allowed)
	payload := make([]byte, MaxPayloadSize)
	msgType := uint16(100)

	encoded := EncodeFrame(msgType, payload)
	decoded, decodedPayload, err := DecodeFrame(encoded)

	require.NoError(t, err)
	assert.Equal(t, msgType, decoded)
	assert.Equal(t, payload, decodedPayload)
}

func TestShouldRejectOversizePayload(t *testing.T) {
	// Payload larger than max should panic
	payload := make([]byte, MaxPayloadSize+1)

	assert.Panics(t, func() {
		EncodeFrame(100, payload)
	})
}

func TestShouldPreserveBinaryData(t *testing.T) {
	// Test that all byte values are preserved
	payload := make([]byte, 256)
	for i := 0; i < 256; i++ {
		payload[i] = byte(i)
	}

	encoded := EncodeFrame(100, payload)
	_, decodedPayload, err := DecodeFrame(encoded)

	require.NoError(t, err)
	assert.Equal(t, payload, decodedPayload)
}

func TestShouldEncodeFrameWithSingleByteType(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	msgType := uint16(100)

	frame := EncodeFrame(msgType, payload)

	// Header should be: [0x64][0x00 0x03]
	assert.Equal(t, byte(0x64), frame[0])
	assert.Equal(t, byte(0x00), frame[1])
	assert.Equal(t, byte(0x03), frame[2])
	assert.Equal(t, payload, frame[3:])
}

func TestShouldEncodeFrameWithEscapedType(t *testing.T) {
	payload := []byte{0x01, 0x02}
	msgType := uint16(500)

	frame := EncodeFrame(msgType, payload)

	// Header should be: [0xFF][0x01 0xF4][0x00 0x02]
	assert.Equal(t, byte(0xFF), frame[0])
	assert.Equal(t, byte(0x01), frame[1])
	assert.Equal(t, byte(0xF4), frame[2])
	assert.Equal(t, byte(0x00), frame[3])
	assert.Equal(t, byte(0x02), frame[4])
	assert.Equal(t, payload, frame[5:])
}

// Benchmarks
func BenchmarkEncodeMessageType(b *testing.B) {
	b.Run("single byte", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeMessageType(100)
		}
	})

	b.Run("escaped type", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeMessageType(500)
		}
	})
}

func BenchmarkDecodeMessageType(b *testing.B) {
	b.Run("single byte", func(b *testing.B) {
		data := []byte{0x64}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = DecodeMessageType(data)
		}
	})

	b.Run("escaped type", func(b *testing.B) {
		data := []byte{0xFF, 0x01, 0xF4}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = DecodeMessageType(data)
		}
	})
}

func BenchmarkEncodeFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		payload := make([]byte, 100)
		msgType := uint16(100)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeFrame(msgType, payload)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		payload := make([]byte, 10240)
		msgType := uint16(100)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = EncodeFrame(msgType, payload)
		}
	})
}

func BenchmarkEncodeFrameWithPayloadWriter(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		payload := make([]byte, 100)
		msgType := uint16(100)
		writer := func(buf *bytes.Buffer) {
			buf.Write(payload)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			frame, err := EncodeFrameWithPayloadWriter(msgType, writer)
			if err == nil {
				frame.Release()
			}
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		payload := make([]byte, 10240)
		msgType := uint16(100)
		writer := func(buf *bytes.Buffer) {
			buf.Write(payload)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			frame, err := EncodeFrameWithPayloadWriter(msgType, writer)
			if err == nil {
				frame.Release()
			}
		}
	})
}

func BenchmarkDecodeFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		payload := make([]byte, 100)
		frame := EncodeFrame(100, payload)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = DecodeFrame(frame)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		payload := make([]byte, 10240)
		frame := EncodeFrame(100, payload)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = DecodeFrame(frame)
		}
	})
}

func TestShouldRouteDomain(t *testing.T) {
	tests := []struct {
		msgType uint16
		domain  string
	}{
		{100, "kv"},
		{108, "kv"},
		{200, "queue"},
		{204, "queue"},
		{300, "rpc"},
		{304, "rpc"},
		{400, "lease"},
		{403, "lease"},
		{500, "notice"},
		{504, "notice"},
		{600, "stream"},
		{603, "stream"},
		{700, "schedule"},
		{702, "schedule"},
		{999, "unknown"},
	}

	for _, tt := range tests {
		domain := RouteDomain(tt.msgType)
		assert.Equal(t, tt.domain, domain)
	}
}
