package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cntryl/fitz-go/internal/testkit"
)

func newTestWSTransport(conn *testkit.MockWSConn) *WebSocketTransport {
	addr := conn.RemoteAddrHost
	if addr == "" {
		addr = "ws://localhost:4090/ws"
	}
	return &WebSocketTransport{
		conn:   conn,
		reader: bufio.NewReader(conn),
		addr:   addr,
	}
}

// TestShouldWriteBinaryFrame tests WebSocket binary frame writing.
func TestShouldWriteBinaryFrame(t *testing.T) {
	t.Run("valid payload", func(t *testing.T) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)

		payload := []byte{0x01, 0x02, 0x03}
		err := transport.Write(context.Background(), payload)

		require.NoError(t, err)
		assert.Len(t, conn.Messages, 1)
		assert.Equal(t, payload, conn.Messages[0])
	})

	t.Run("empty payload", func(t *testing.T) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)

		payload := []byte{}
		err := transport.Write(context.Background(), payload)

		require.NoError(t, err)
		assert.Len(t, conn.Messages, 1)
	})

	t.Run("large payload", func(t *testing.T) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)

		payload := make([]byte, 65535)
		err := transport.Write(context.Background(), payload)

		require.NoError(t, err)
		assert.Len(t, conn.Messages, 1)
	})
}

// TestShouldReadBinaryFrame tests WebSocket binary frame reading.
func TestShouldReadBinaryFrame(t *testing.T) {
	t.Run("binary message", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03, 0x04}
		conn := &testkit.MockWSConn{
			NextMessage: data,
			IsText:      false,
		}
		transport := newTestWSTransport(conn)

		frame, err := transport.Read(context.Background())

		require.NoError(t, err)
		assert.Equal(t, data, frame)
	})

	t.Run("empty message", func(t *testing.T) {
		conn := &testkit.MockWSConn{
			NextMessage: []byte{},
			IsText:      false,
		}
		transport := newTestWSTransport(conn)

		frame, err := transport.Read(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []byte{}, frame)
	})
}

// TestShouldRejectTextFrames tests that text frames are rejected.
func TestShouldRejectTextFrames(t *testing.T) {
	conn := &testkit.MockWSConn{
		NextMessage: []byte("text message"),
		IsText:      true,
	}
	transport := &WebSocketTransport{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}

	_, err := transport.Read(context.Background())

	assert.Error(t, err)
	assert.True(t, errors.Is(err, io.EOF))
}

// TestShouldParseWSURL tests WebSocket URL parsing.
func TestShouldParseWSURL(t *testing.T) {
	t.Run("ws scheme", func(t *testing.T) {
		// Just verify the URL parsing logic doesn't crash
		u, err := url.Parse("ws://localhost:4090/ws")
		require.NoError(t, err)
		assert.Equal(t, "ws", u.Scheme)
	})

	t.Run("wss scheme", func(t *testing.T) {
		u, err := url.Parse("wss://localhost:4090/ws")
		require.NoError(t, err)
		assert.Equal(t, "wss", u.Scheme)
	})

	t.Run("invalid url", func(t *testing.T) {
		// Invalid URL should fail at dial time, not parse time
		_, err := url.Parse("ht!tp://invalid")
		require.Error(t, err)
	})
}

// TestShouldTimeoutReadGivenDeadline tests WebSocket read timeout.
func TestShouldTimeoutWSReadGivenDeadline(t *testing.T) {
	conn := &testkit.MockWSConn{
		Blocked: true, // Will block forever
	}
	transport := newTestWSTransport(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := transport.Read(ctx)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

// TestShouldCloseGracefully tests WebSocket close.
func TestShouldCloseWSGracefully(t *testing.T) {
	conn := &testkit.MockWSConn{}
	transport := newTestWSTransport(conn)

	err := transport.Close()

	require.NoError(t, err)
	assert.True(t, conn.Closed)
}

// TestShouldReturnRemoteAddr tests WebSocket remote address.
func TestShouldReturnWSRemoteAddr(t *testing.T) {
	conn := &testkit.MockWSConn{
		RemoteAddrHost: "ws://example.com:4090/ws",
	}
	transport := newTestWSTransport(conn)

	addr := transport.RemoteAddr()

	assert.NotEmpty(t, addr)
}

// TestShouldHandleContextCancellation tests context cancellation.
func TestShouldHandleContextCancellation(t *testing.T) {
	conn := &testkit.MockWSConn{}
	transport := newTestWSTransport(conn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := transport.Write(ctx, []byte{0x01})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

// Mock WebSocket connection is now provided by testkit.MockWSConn from internal/testkit package

// Benchmarks
func BenchmarkWriteWSFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)
		payload := make([]byte, 100)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = transport.Write(context.Background(), payload)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		conn := &testkit.MockWSConn{
			Messages: make([][]byte, 0),
		}
		transport := newTestWSTransport(conn)
		payload := make([]byte, 10240)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = transport.Write(context.Background(), payload)
		}
	})
}

func BenchmarkReadWSFrame(b *testing.B) {
	frame := make([]byte, 100)
	conn := &testkit.MockWSConn{
		NextMessage: frame,
		IsText:      false,
	}
	transport := newTestWSTransport(conn)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = transport.Read(context.Background())
	}
}
