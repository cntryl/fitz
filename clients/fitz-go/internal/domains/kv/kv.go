// Package kv implements the Fitz KV domain client.
// Per CLIENT_SPEC.md: Transaction-based key-value store with read/write and read-only transactions.
package kv

import (
	"context"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Client provides transaction-based key-value operations only. All data
// interactions MUST occur through transactions returned by Begin/BeginRead.
// Convenience helpers were intentionally removed to avoid accidental
// non-transactional use.
type Client interface {
	// Begin opens a read/write transaction scoped to the provided route.
	// Optional functional options can configure durability mode.
	Begin(ctx context.Context, route string, opts ...BeginOption) (Tx, error)

	// BeginRead opens a read-only transaction scoped to the provided route.
	BeginRead(ctx context.Context, route string) (ReadTx, error)
}

// BeginOption configures transaction BEGIN parameters.
type BeginOption func(*beginConfig)

// beginConfig holds configuration for BEGIN operations.
type beginConfig struct {
	durability uint8
}

// WithDurability sets the transaction durability mode.
// Default is DurabilityBuffered (faster, best-effort persistence).
// Use DurabilitySync for guaranteed durability (slower, fsync on commit).
func WithDurability(mode uint8) BeginOption {
	return func(cfg *beginConfig) {
		cfg.durability = mode
	}
}

// client is a concrete implementation of Client using the connection layer.
type client struct {
	conn *connection.Connection
}

// NewClient creates a new KV domain client backed by the provided connection.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn: conn,
	}
}

// Begin opens a read/write transaction scoped to the provided route.
// Per CLIENT_SPEC.md: Server assigns tx_id and returns it in response.
func (c *client) Begin(ctx context.Context, route string, opts ...BeginOption) (Tx, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.kv.Begin", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("kv.Begin", "route", route)
	}

	// Validate route format
	if err := types.ValidateRoute(route, "kv"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	// Apply options
	cfg := beginConfig{
		durability: DurabilityBuffered, // Default to buffered for performance
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Encode BEGIN request per CLIENT_SPEC.md
	// Send request and wait for response
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvBegin, beginPayloadWriter(route, TxModeReadWrite, cfg.durability))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("kv.Begin failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("BEGIN request failed: %w", err)
	}

	// Parse response: [u8 status][u64 BE tx_id] for success
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("kv.Begin failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("BEGIN failed: %w", mapKVError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("kv.Begin failed", "route", route, "status", "unexpected")
		}
		return nil, fmt.Errorf("BEGIN failed: unexpected status")
	}

	// Extract tx_id from remaining payload (server-assigned per CLIENT_SPEC.md)
	if len(remaining) < 8 { // tx_id is u64 (8 bytes)
		return nil, fmt.Errorf("invalid BEGIN response: expected at least 8 bytes for tx_id, got %d", len(remaining))
	}

	txID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse tx_id: %w", err)
	}

	// Create transaction with server-assigned tx_id
	tx := &transaction{
		route:    route,
		conn:     c.conn,
		readOnly: false,
		txID:     txID,
	}

	return tx, nil
}

// BeginRead opens a read-only transaction scoped to the provided route.
// Per CLIENT_SPEC.md: ReadOnly transactions must also call BEGIN on server.
func (c *client) BeginRead(ctx context.Context, route string) (ReadTx, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.kv.BeginRead", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("kv.BeginRead", "route", route)
	}

	// Validate route format
	if err := types.ValidateRoute(route, "kv"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	// Encode BEGIN request with ReadOnly mode
	// Send request and wait for response
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeKvBegin, beginPayloadWriter(route, TxModeReadOnly, DurabilityBuffered))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("kv.BeginRead failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("BEGIN request failed: %w", err)
	}

	// Parse response
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("kv.BeginRead failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("BEGIN failed: %w", mapKVError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("kv.BeginRead failed", "route", route, "status", "unexpected")
		}
		return nil, fmt.Errorf("BEGIN failed: unexpected status")
	}

	// Extract tx_id from remaining payload
	if len(remaining) < 8 {
		return nil, fmt.Errorf("invalid BEGIN response: expected at least 8 bytes for tx_id, got %d", len(remaining))
	}

	txID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse tx_id: %w", err)
	}

	// Create read-only transaction wrapped in readOnlyTransaction
	// to prevent casting to Tx (per CLIENT_SPEC.md).
	tx := &transaction{
		route:    route,
		conn:     c.conn,
		readOnly: true,
		txID:     txID,
	}

	return &readOnlyTransaction{inner: tx}, nil
}
