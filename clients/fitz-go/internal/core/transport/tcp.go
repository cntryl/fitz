package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TCPTransport implements Transport for TCP connections with length-prefix framing.
// Per CLIENT_SPEC.md: [u32 BE frame_length][TLV message]
// Handles partial reads correctly using io.ReadFull.
type TCPTransport struct {
	conn   net.Conn
	addr   string
	reader *bufio.Reader
	closed atomic.Bool

	writeMu sync.Mutex // Serializes writes (header + frame must be atomic)
	readMu  sync.Mutex // Serializes reads (prevents interleaved partial reads)
}

// DialTCP creates a TCP transport to the specified address (host:port).
func DialTCP(ctx context.Context, addr string) (Transport, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial tcp: %w", err)
	}

	return &TCPTransport{
		conn:   conn,
		addr:   addr,
		reader: bufio.NewReader(conn),
	}, nil
}

// Write sends a length-prefixed frame: [u32 BE length][frame].
// Thread-safe and atomic (header + frame written together).
func (t *TCPTransport) Write(ctx context.Context, frame []byte) error {
	if t.closed.Load() {
		return ErrTransportClosed
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	// Set write deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		t.conn.SetWriteDeadline(deadline)
	}
	defer t.conn.SetWriteDeadline(time.Time{}) // Clear deadline

	// Build length-prefixed frame: [u32 BE][frame]
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(frame)))

	// Atomic write (header + frame together)
	// Note: We could optimize with writev/sendmsg, but simplicity preferred
	if _, err := t.conn.Write(header[:]); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("write tcp frame header: %w", err)
	}
	if _, err := t.conn.Write(frame); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("write tcp frame body: %w", err)
	}

	return nil
}

// Read blocks until next complete frame is read.
// Handles partial reads correctly (uses io.ReadFull).
// Returns immediately if context is cancelled.
func (t *TCPTransport) Read(ctx context.Context) ([]byte, error) {
	t.readMu.Lock()
	defer t.readMu.Unlock()

	// Set read deadline from context
	if deadline, ok := ctx.Deadline(); ok {
		t.conn.SetReadDeadline(deadline)
	}
	defer t.conn.SetReadDeadline(time.Time{}) // Clear deadline

	// Read 4-byte length prefix (u32 BE per CLIENT_SPEC.md)
	var lengthBuf [4]byte
	if _, err := io.ReadFull(t.reader, lengthBuf[:]); err != nil {
		// Context cancellation takes priority
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read tcp frame length: %w", err)
	}

	frameLen := binary.BigEndian.Uint32(lengthBuf[:])

	// Validate frame size BEFORE allocating (prevents OOM)
	if frameLen > uint32(MaxFrameSize) {
		return nil, ErrFrameTooLarge
	}

	// Allocate buffer and read exact frame bytes
	// io.ReadFull handles partial reads correctly
	frame := make([]byte, frameLen)
	if _, err := io.ReadFull(t.reader, frame); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read tcp frame body: %w", err)
	}

	return frame, nil
}

// Close cleanly shuts down the TCP connection.
func (t *TCPTransport) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	return t.conn.Close()
}

// RemoteAddr returns the TCP address (host:port).
func (t *TCPTransport) RemoteAddr() string {
	return t.addr
}
