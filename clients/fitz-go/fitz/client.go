package fitz

import (
	"context"

	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/domains/kv"
	"github.com/cntryl/fitz-go/internal/domains/lease"
	"github.com/cntryl/fitz-go/internal/domains/notice"
	"github.com/cntryl/fitz-go/internal/domains/queue"
	"github.com/cntryl/fitz-go/internal/domains/rpc"
	"github.com/cntryl/fitz-go/internal/domains/schedule"
	"github.com/cntryl/fitz-go/internal/domains/stream"
)

// TokenProvider is the canonical type for JWT token providers.
// Re-exported from internal/core/types for public API use.
type TokenProvider = types.TokenProvider

// Client is the primary top-level Fitz client exposing each domain client.
// Domain clients returned by Notice(), Stream(), Queue(), etc. are safe for concurrent use by multiple goroutines.
// Implementations SHOULD provide a constructor (e.g., NewClient) that accepts:
//   - addr: broker address determining transport ("host:port", "tcp://...", "ws://...", "wss://...")
//   - tokenProvider: function for obtaining JWT tokens (supports renewal on reconnection)
type Client interface {
	// Connect establishes a connection to the broker using the address and
	// TokenProvider configured during client construction.
	//
	// Connect will call the TokenProvider to obtain a JWT token for the
	// CONN_OPEN handshake. This allows tokens to be refreshed on each
	// connection attempt.
	//
	// Per CLIENT_SPEC.md, both TCP and WebSocket transports MUST be supported
	// with identical wire protocol semantics (TLV framing).
	Connect(ctx context.Context) error

	// Close cleanly shuts down the client and associated resources.
	// This should send a CONN_CLOSE frame if the connection is active,
	// then close the underlying transport.
	Close() error

	// Domain clients. Each is safe for concurrent use.
	Notice() notice.Client
	Stream() stream.Client
	Queue() queue.Client
	RPC() rpc.Client
	KV() kv.Client
	Lease() lease.Client
	Schedule() schedule.Client
}
