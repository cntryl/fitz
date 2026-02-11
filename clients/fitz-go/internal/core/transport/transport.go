package transport

import (
	"context"
	"errors"
)

// Transport abstracts WebSocket and TCP framing per CLIENT_SPEC.md.
// Implementations handle protocol-specific framing details while exposing
// a uniform interface for reading and writing complete TLV frames.
type Transport interface {
	// Write sends a complete TLV frame to the server.
	// The frame must be fully encoded before calling Write.
	// Returns error if connection is broken or context is cancelled.
	Write(ctx context.Context, frame []byte) error

	// Read blocks until the next complete frame arrives.
	// MUST return immediately when ctx is cancelled (not wait for next frame).
	// Returns io.EOF when connection is closed gracefully.
	// Returns ErrFrameTooLarge if frame exceeds MaxFrameSize.
	Read(ctx context.Context) ([]byte, error)

	// Close cleanly shuts down the transport.
	// Safe to call multiple times (idempotent).
	Close() error

	// RemoteAddr returns the server address for logging and diagnostics.
	RemoteAddr() string
}

// Common errors
var (
	// ErrFrameTooLarge indicates frame exceeds MaxFrameSize (safety limit)
	ErrFrameTooLarge = errors.New("frame exceeds maximum size")

	// ErrTransportClosed indicates transport was already closed
	ErrTransportClosed = errors.New("transport closed")
)

// MaxFrameSize is the maximum allowed frame size (16 MB).
// Prevents OOM attacks from malicious or buggy servers.
// Per CLIENT_SPEC.md safety requirements.
const MaxFrameSize = 16 * 1024 * 1024 // 16 MB
