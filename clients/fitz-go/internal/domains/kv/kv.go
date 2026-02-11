package kv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cntryl/fitz-go/internal/core/transport"
)

// Client provides transaction-based key-value operations only. All data
// interactions MUST occur through transactions returned by Begin/BeginRead.
// Convenience helpers were intentionally removed to avoid accidental
// non-transactional use.
type Client interface {
	// Begin opens a read/write transaction scoped to the provided route.
	Begin(ctx context.Context, route string) (Tx, error)

	// BeginRead opens a read-only transaction scoped to the provided route.
	BeginRead(ctx context.Context, route string) (ReadTx, error)
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
func (c *client) Begin(ctx context.Context, route string) (Tx, error) {
	txID := nextTxID.Add(1)

	// Send BEGIN request to broker for acknowledgement.
	// Use new payload format per CLIENT_SPEC.md: [route_len][route][mode][durability]
	payload := EncodeBegin(route, TxModeReadWrite, DurabilitySync)

	frame := transport.Frame{
		Type:    uint8(KVBegin), // Cast to uint8 for old Frame format (TODO: Phase 3)
		Flags:   0,
		Channel: transport.ChannelKV,
		Body:    payload,
	}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send KV begin: %w", err)
	}

	// Create a temporary transaction to receive BEGIN acknowledgement.
	tx := &transaction{
		route:    route,
		mux:      c.mux,
		readOnly: false,
		txID:     txID,
	}

	// Wait for server acknowledgement with timeout.
	beginCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		select {
		case <-beginCtx.Done():
			return nil, fmt.Errorf("Begin operation timed out waiting for broker response")
		case respFrame, ok := <-c.mux.In():
			if !ok {
				return nil, errors.New("mux closed")
			}

			// Check if this is our response (same channel).
			if respFrame.Channel != transport.ChannelKV {
				continue
			}

			dec, err := transport.NewTLVDecoder(respFrame.Body)
			if err != nil {
				continue
			}

			// Check for error response.
			if dec.Has(transport.TagErr) {
				errMsg := dec.GetString(transport.TagErr)
				return nil, mapKVError(errMsg)
			}

			// BEGIN succeeded.
			return tx, nil
		}
	}
}

// BeginRead opens a read-only transaction scoped to the provided route.
func (c *client) BeginRead(ctx context.Context, route string) (ReadTx, error) {
	txID := nextTxID.Add(1)
	return &transaction{
		route:    route,
		mux:      c.mux,
		readOnly: true,
		txID:     txID,
	}, nil
}
