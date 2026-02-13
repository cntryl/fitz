package transport

import (
	"bufio"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cntryl/fitz-go/internal/testkit"
)

func newTestTCPTransport(conn *testkit.MockTCPConn) *TCPTransport {
	addr := ""
	if conn.RemoteAddr() != nil {
		addr = conn.RemoteAddr().String()
	}
	return &TCPTransport{
		conn:   conn,
		addr:   addr,
		reader: bufio.NewReader(conn),
	}
}

// TestShouldConnectViaTCP tests basic TCP connection establishment.
func TestShouldConnectViaTCP(t *testing.T) {
	// This test requires a mock listener or would connect to a real server
	// For unit test purposes, we verify the constructor doesn't panic
	addr := "localhost:9999"
	_, err := DialTCP(context.Background(), addr)
	// Expected to fail since no server is listening, but should not panic
	assert.Error(t, err)
}

// TestShouldWriteFrameWithLengthPrefix tests TCP frame writing.
func TestShouldWriteFrameWithLengthPrefix(t *testing.T) {
	t.Run("valid payload", func(t *testing.T) {
		// Create a mock connection pair
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)

		payload := []byte{0x01, 0x02, 0x03}
		err := transport.Write(context.Background(), payload)

		require.NoError(t, err)
		require.Greater(t, len(conn.Written), 4) // At least 4 bytes for length prefix
	})

	t.Run("empty payload", func(t *testing.T) {
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)

		payload := []byte{}
		err := transport.Write(context.Background(), payload)

		require.NoError(t, err)
	})
}

// TestShouldTimeoutReadGivenContext tests write timeout.
func TestShouldTimeoutWriteGivenContext(t *testing.T) {
	conn := &testkit.MockTCPConn{
		WriteDelay: 100 * time.Millisecond,
		Written:    make([]byte, 0),
	}
	transport := newTestTCPTransport(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := transport.Write(ctx, []byte{0x01})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

// TestShouldReadFrameWithLengthPrefix tests TCP frame reading.
func TestShouldReadFrameWithLengthPrefix(t *testing.T) {
	t.Run("valid prefixed data", func(t *testing.T) {
		// Prepare frame: [0x00 0x00 0x00 0x05][0x01 0x02 0x03 0x04 0x05]
		frame := []byte{0x00, 0x00, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05}
		conn := &testkit.MockTCPConn{
			ToRead: frame,
		}
		transport := newTestTCPTransport(conn)

		data, err := transport.Read(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05}, data)
	})

	t.Run("empty payload", func(t *testing.T) {
		// Prepare frame: [0x00 0x00 0x00 0x00]
		frame := []byte{0x00, 0x00, 0x00, 0x00}
		conn := &testkit.MockTCPConn{
			ToRead: frame,
		}
		transport := newTestTCPTransport(conn)

		data, err := transport.Read(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []byte{}, data)
	})
}

// TestShouldTimeoutReadGivenDeadline tests read timeout.
func TestShouldTimeoutReadGivenDeadline(t *testing.T) {
	conn := &testkit.MockTCPConn{
		Blocked: true, // Block forever
	}
	transport := newTestTCPTransport(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := transport.Read(ctx)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

// TestShouldCloseGracefully tests transport close.
func TestShouldCloseGracefully(t *testing.T) {
	conn := &testkit.MockTCPConn{}
	transport := newTestTCPTransport(conn)

	err := transport.Close()

	require.NoError(t, err)
	assert.True(t, conn.Closed)
}

// TestShouldReturnRemoteAddr tests remote address retrieval.
func TestShouldReturnRemoteAddr(t *testing.T) {
	conn := &testkit.MockTCPConn{
		RemoteAddrString: "127.0.0.1:4091",
	}
	transport := newTestTCPTransport(conn)

	addr := transport.RemoteAddr()

	assert.Equal(t, "127.0.0.1:4091", addr)
}

// TestShouldRejectContextCancelled tests context cancellation handling.
func TestShouldRejectContextCancelled(t *testing.T) {
	conn := &testkit.MockTCPConn{}
	transport := newTestTCPTransport(conn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before write

	err := transport.Write(ctx, []byte{0x01})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}

// Mock TCP connection is now provided by testkit.MockTCPConn from internal/testkit package

// Benchmarks
func BenchmarkWriteFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)
		payload := make([]byte, 100)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = transport.Write(context.Background(), payload)
		}
	})

	b.Run("10KB payload", func(b *testing.B) {
		conn := &testkit.MockTCPConn{
			Written: make([]byte, 0),
		}
		transport := newTestTCPTransport(conn)
		payload := make([]byte, 10240)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = transport.Write(context.Background(), payload)
		}
	})
}

func BenchmarkReadFrame(b *testing.B) {
	b.Run("100 byte payload", func(b *testing.B) {
		// Prepare frame with 100 byte payload
		frame := make([]byte, 4+100)
		frame[3] = 100 // Length = 100
		conn := &testkit.MockTCPConn{
			ToRead: frame,
		}
		transport := newTestTCPTransport(conn)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conn.ReadPos = 0 // Reset for next iteration
			_, _ = transport.Read(context.Background())
		}
	})
}
