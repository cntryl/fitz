package rpc

import (
	"context"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/iter"
)

// Client is the API for the RPC domain.
type Client interface {
	// Call sends a request and returns a streaming iterator of Response frames.
	// The iterator yields responses until the worker signals stream_end or an
	// error occurs. Callers MUST call Close() on the returned iterator.
	Call(ctx context.Context, route string, body []byte, timeout time.Duration) (iter.Iterator[Response], error)
	// Subscribe registers a streaming worker handler for the given route.
	Subscribe(ctx context.Context, route string, handler RPCHandler) (Subscription, error)
	// Close stops the background receive loop and releases resources.
	Close() error
}

// Response is a single frame in a (possibly streaming) RPC response.
type Response struct {
	Sequence uint64
	Body     []byte
}

// ResponseWriter allows a handler to stream multiple response frames back to
// the caller. Send may be called zero or more times. When the handler returns,
// the framework automatically sends a final frame with stream_end=1.
type ResponseWriter interface {
	// Send emits a response frame with stream_end=0. The sequence number is
	// managed automatically (incrementing from 0).
	Send(body []byte) error
}

// RPCHandler processes inbound requests. The handler receives a ResponseWriter
// to stream results back. When the handler returns a nil error the framework
// sends a final empty frame with stream_end=1. If the handler returns an error
// the framework sends an error frame instead.
type RPCHandler func(ctx context.Context, req InboundRequest, w ResponseWriter) error

// Subscription allows unsubscribing a worker.
type Subscription interface {
	Unsubscribe()
}

// InboundRequest represents an inbound RPC request to a worker.
type InboundRequest struct {
	ID         uint64
	Route      string
	Body       []byte
	ReplyRoute string
}
