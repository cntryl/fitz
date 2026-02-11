package kv

import (
	"context"
	"fmt"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// Client provides transaction-based key-value operations only. All data
// interactions MUST occur through transactions returned by Begin/BeginRead.
// Convenience helpers were intentionally removed to avoid accidental
// non-transactional use.
type Client interface {
	// Begin opens a read/write transaction scoped to the provided route.
	Begin(ctx context.Context, route Route) (Tx, error)

	// BeginRead opens a read-only transaction scoped to the provided route.
	BeginRead(ctx context.Context, route Route) (ReadTx, error)
}

// client is a concrete implementation of Client using the provided mux provider.
type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new KV domain client backed by the provided mux provider.
func NewClient(mux transport.MuxProvider) Client {
	return &client{
		mux: mux,
	}
}

// Begin opens a read/write transaction scoped to the provided route.
func (c *client) Begin(ctx context.Context, route Route) (Tx, error) {
	if err := route.Validate(); err != nil {
		return nil, err
	}
	txID := nextTxID.Add(1)

	// Notify broker about transaction begin (best-effort). Broker may choose to
	// accept client-generated tx IDs for correlation.
	enc := transport.NewTLVEncoder()
	enc.AddOp(KVBegin) // wire code 100 per CLIENT_SPEC.md
	enc.AddString(transport.TagRoute, route.String())
	enc.AddUint64(transport.TagID, txID)
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send KV begin: %w", err)
	}

	return &transaction{
		route:    route,
		mux:      c.mux,
		readOnly: false,
		txID:     txID,
	}, nil
}

// BeginRead opens a read-only transaction scoped to the provided route.
func (c *client) BeginRead(ctx context.Context, route Route) (ReadTx, error) {
	if err := route.Validate(); err != nil {
		return nil, err
	}
	txID := nextTxID.Add(1)
	return &transaction{
		route:    route,
		mux:      c.mux,
		readOnly: true,
		txID:     txID,
	}, nil
}
