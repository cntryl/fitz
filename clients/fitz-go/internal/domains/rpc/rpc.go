// Package rpc implements the Fitz RPC domain client.
// Per CLIENT_SPEC.md: Bidirectional RPC with streaming responses.
package rpc

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// InboundRequest represents a request received by a worker.
type InboundRequest struct {
	CorrelationID [16]byte
	Route         string
	ReplyRoute    string
	Body          []byte
}

// ResponseWriter allows a worker to send responses.
type ResponseWriter interface {
	Response(body []byte) error
}

// RPCHandler handles incoming RPC requests.
type RPCHandler func(ctx context.Context, req InboundRequest, w ResponseWriter) error

// ResponseFrame represents a single response frame from a streaming RPC call.
type ResponseFrame struct {
	Body     []byte
	Sequence uint64
}

// Subscription represents an active worker registration.
// Call Unsubscribe to stop receiving requests and release the registration.
type Subscription struct {
	route  string
	client *client
}

// Unsubscribe removes this worker registration.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribeWorker(s.route)
	}
}

// Client is the RPC domain client interface.
type Client interface {
	// Subscribe registers a worker handler for the given route.
	Subscribe(ctx context.Context, route string, handler RPCHandler) (*Subscription, error)

	// Request sends an RPC request and returns an iterator over response frames.
	// Callers must call Close on the returned iterator when done to release resources.
	Request(ctx context.Context, route string, body []byte, timeout time.Duration) (iter.Iterator[ResponseFrame], error)
}

type client struct {
	conn *connection.Connection

	mu          sync.Mutex
	workers     map[string]RPCHandler // route -> handler
	pendingRPCs map[[16]byte]chan ResponseFrame
	initialized bool
}

// NewClient creates a new RPC domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:        conn,
		workers:     make(map[string]RPCHandler),
		pendingRPCs: make(map[[16]byte]chan ResponseFrame),
	}
	return c
}

// initRPCHandler registers the RPC response handler on first use.
func (c *client) initRPCHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterRPCResponseHandler(c.handleRPCResponse)
	c.conn.RegisterRPCRequestHandler(c.handleRPCRequest)
}

// handleRPCRequest handles incoming RPC REQUEST frames (302) forwarded to this worker.
// Payload: [u32 BE corrLen=16][16 bytes correlation_id][route][reply_route][body] (TLV).
func (c *client) handleRPCRequest(payload []byte) {
	if len(payload) < 20 {
		return
	}
	corrLen := binary.BigEndian.Uint32(payload[0:4])
	if corrLen != 16 || len(payload) < 20 {
		return
	}
	var correlationID [16]byte
	copy(correlationID[:], payload[4:20])
	c.handleWorkerRequest(correlationID, payload[20:])
}

// handleRPCResponse handles incoming RPC RESPONSE frames (303).
// Per server rpc_codec.rs: [bytes correlation_id][u64 seq][bytes body][u8 stream_end]
// where "bytes" = [u32 BE len][data] (TLV bytes format)
func (c *client) handleRPCResponse(correlationID [16]byte, payload []byte) {
	if len(payload) < 1 {
		return
	}

	// Check if this is a worker request (status byte for worker dispatch)
	// or a call response. We use the pending RPC map to differentiate.
	c.mu.Lock()
	ch, isCall := c.pendingRPCs[correlationID]
	c.mu.Unlock()

	if isCall {
		// This is a response to our Call
		// Parse: [u64 sequence][bytes body][u8 stream_end]
		offset := 0
		if offset+8 > len(payload) {
			return
		}
		seq := binary.BigEndian.Uint64(payload[offset : offset+8])
		offset += 8

		// body is TLV bytes: [u32 len][data]
		if offset+4 > len(payload) {
			return
		}
		bodyLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4

		if offset+int(bodyLen) > len(payload) {
			return
		}
		body := make([]byte, bodyLen)
		copy(body, payload[offset:offset+int(bodyLen)])
		offset += int(bodyLen)

		streamEnd := false
		if offset < len(payload) {
			streamEnd = payload[offset] == 1
		}

		if streamEnd {
			// End-of-stream: close channel and clean up; do not push a frame (caller sees N data frames then done).
			c.mu.Lock()
			delete(c.pendingRPCs, correlationID)
			c.mu.Unlock()
			close(ch)
		} else {
			select {
			case ch <- ResponseFrame{Body: body, Sequence: seq}:
			default:
			}
		}
		return
	}

	// This is a request dispatched to us as a worker
	c.handleWorkerRequest(correlationID, payload)
}

// handleWorkerRequest processes an incoming request for a registered worker.
// Server forwards REQUEST payload: [bytes correlation_id][string route][string reply_route][bytes body]
// where bytes/string = [u32 BE len][data] (TLV format)
// Note: correlationID was already parsed by the mux, but the remaining payload
// contains [string route][string reply_route][bytes body] after the correlation_id.
func (c *client) handleWorkerRequest(correlationID [16]byte, payload []byte) {
	offset := 0

	// Parse route (TLV string: [u32 len][string])
	if offset+4 > len(payload) {
		return
	}
	routeLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if offset+int(routeLen) > len(payload) {
		return
	}
	route := string(payload[offset : offset+int(routeLen)])
	offset += int(routeLen)

	// Parse reply_route (TLV string: [u32 len][string])
	if offset+4 > len(payload) {
		return
	}
	replyRouteLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if offset+int(replyRouteLen) > len(payload) {
		return
	}
	replyRoute := string(payload[offset : offset+int(replyRouteLen)])
	offset += int(replyRouteLen)

	// Parse body (TLV bytes: [u32 len][data])
	if offset+4 > len(payload) {
		return
	}
	bodyLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if offset+int(bodyLen) > len(payload) {
		return
	}
	body := make([]byte, bodyLen)
	copy(body, payload[offset:offset+int(bodyLen)])

	c.mu.Lock()
	handler, ok := c.workers[route]
	c.mu.Unlock()

	if !ok {
		return // No worker for this route
	}

	req := InboundRequest{
		CorrelationID: correlationID,
		Route:         route,
		ReplyRoute:    replyRoute,
		Body:          body,
	}

	w := &responseWriter{
		conn:          c.conn,
		correlationID: correlationID,
		seq:           0,
	}

	go func() {
		_ = handler(context.Background(), req, w)
		// Send stream_end
		w.sendEnd()
	}()
}

// Subscribe per CLIENT_SPEC.md:
// Request: [worker_route_len][worker_route]
// Response: [status]
func (c *client) Subscribe(ctx context.Context, route string, handler RPCHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.rpc.Subscribe", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("rpc.Subscribe", "route", route)
	}
	c.initRPCHandler()

	// Validate route format
	if err := types.ValidateRoute(route, "rpc"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcSubscribeWorker, rpcSubscribeWorkerPayloadWriter(route))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("rpc.Subscribe failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE_WORKER request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("rpc.Subscribe failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE_WORKER failed: %w", mapRPCError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("rpc.Subscribe failed", "route", route, "status", "unexpected")
		}
		return nil, fmt.Errorf("SUBSCRIBE_WORKER failed: unexpected status")
	}

	c.mu.Lock()
	c.workers[route] = handler
	c.mu.Unlock()

	return &Subscription{route: route, client: c}, nil
}

// unsubscribeWorker removes a worker registration.
func (c *client) unsubscribeWorker(route string) {
	c.mu.Lock()
	delete(c.workers, route)
	c.mu.Unlock()

	ctx := context.Background()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcUnsubscribeWorker, rpcUnsubscribeWorkerPayloadWriter(route))
	_ = resp // ignore response
	_ = err  // ignore errors
}

// Request per CLIENT_SPEC.md:
// Request: [correlation_id(16)][route_len][route][reply_route_len][reply_route][body_len][body]
// Response: [status] (ack that request was dispatched)
// Actual responses come via RPC RESPONSE (303) messages.
func (c *client) Request(ctx context.Context, route string, body []byte, timeout time.Duration) (iter.Iterator[ResponseFrame], error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.rpc.Request", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("rpc.Request", "route", route)
	}
	c.initRPCHandler()

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Validate route format
	if err := types.ValidateRoute(route, "rpc"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	// Generate correlation ID
	var correlationID [16]byte
	rand.Read(correlationID[:])

	// Create response channel
	ch := make(chan ResponseFrame, 32)

	c.mu.Lock()
	c.pendingRPCs[correlationID] = ch
	c.mu.Unlock()

	// Build request with writer path
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeRpcRequest, rpcRequestPayloadWriter(correlationID, route, "", body))
	if err != nil {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		close(ch)
		if log := c.conn.Logger(); log != nil {
			log.Error("rpc.Request failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("REQUEST failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		close(ch)
		if log := c.conn.Logger(); log != nil {
			log.Error("rpc.Request failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("REQUEST failed: %w", mapRPCError(err.Error()))
	}
	if !success {
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		close(ch)
		if log := c.conn.Logger(); log != nil {
			log.Error("rpc.Request failed", "route", route, "status", "unexpected")
		}
		return nil, fmt.Errorf("REQUEST failed: unexpected status")
	}

	iterator := &rpcIterator{
		ch:            ch,
		timeout:       timeout,
		ctx:           ctx,
		correlationID: correlationID,
		client:        c,
	}

	// Wait for first response or context cancellation
	// This ensures Request() returns error if context is canceled before any responses arrive
	select {
	case frame, ok := <-ch:
		if ok {
			// Got first response, put it back in the channel for the iterator
			// by creating a new channel and prepending this response
			newCh := make(chan ResponseFrame, 32)
			newCh <- frame
			go func() {
				for frame := range ch {
					newCh <- frame
				}
				close(newCh)
			}()
			iterator.ch = newCh
		}
		return iterator, nil
	case <-ctx.Done():
		// Context canceled before any response
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		iterator.mu.Lock()
		iterator.done = true
		iterator.err = ctx.Err()
		iterator.mu.Unlock()
		close(ch)
		return nil, ctx.Err()
	case <-time.After(10 * time.Second):
		// Timeout waiting for first response
		c.mu.Lock()
		delete(c.pendingRPCs, correlationID)
		c.mu.Unlock()
		iterator.mu.Lock()
		iterator.done = true
		iterator.err = ErrRPCTimeout
		iterator.mu.Unlock()
		return nil, ErrRPCTimeout
	}
}

// responseWriter implements ResponseWriter for workers.
type responseWriter struct {
	conn          *connection.Connection
	correlationID [16]byte
	seq           uint64
	mu            sync.Mutex
}

// Response emits one response frame. RPC RESPONSE (303) is one-way: server forwards to caller and does not ack the worker.
func (w *responseWriter) Response(body []byte) error {
	w.mu.Lock()
	seq := w.seq
	w.seq++
	w.mu.Unlock()

	return w.conn.SendFireAndForgetWithWriter(context.Background(), protocol.MessageTypeRpcResponse, rpcResponsePayloadWriter(w.correlationID, seq, body, false))
}

func (w *responseWriter) sendEnd() {
	w.mu.Lock()
	seq := w.seq
	w.mu.Unlock()

	payload, err := EncodeRPCResponse(w.correlationID, seq, nil, true)
	if err != nil {
		return
	}

	_ = w.conn.SendFireAndForget(context.Background(), protocol.MessageTypeRpcResponse, payload)
}

// rpcIterator iterates over response frames from a Call.
type rpcIterator struct {
	ch            chan ResponseFrame
	timeout       time.Duration
	ctx           context.Context
	correlationID [16]byte
	client        *client
	current       ResponseFrame
	err           error
	done          bool
	mu            sync.Mutex // Protects done and err
}

func (it *rpcIterator) Next() bool {
	it.mu.Lock()
	if it.done {
		it.mu.Unlock()
		return false
	}
	it.mu.Unlock()

	timer := time.NewTimer(it.timeout)
	defer timer.Stop()

	select {
	case frame, ok := <-it.ch:
		if !ok {
			it.mu.Lock()
			it.done = true
			it.mu.Unlock()
			return false
		}
		it.current = frame
		return true
	case <-timer.C:
		it.mu.Lock()
		it.err = ErrRPCTimeout
		it.done = true
		it.mu.Unlock()
		// Clean up pending RPC
		it.client.mu.Lock()
		delete(it.client.pendingRPCs, it.correlationID)
		it.client.mu.Unlock()
		return false
	case <-it.ctx.Done():
		it.mu.Lock()
		it.err = it.ctx.Err()
		it.done = true
		it.mu.Unlock()
		// Clean up pending RPC immediately when context is canceled
		it.client.mu.Lock()
		delete(it.client.pendingRPCs, it.correlationID)
		it.client.mu.Unlock()
		return false
	}
}

func (it *rpcIterator) Value() ResponseFrame {
	return it.current
}

func (it *rpcIterator) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

func (it *rpcIterator) Close() error {
	it.mu.Lock()
	it.done = true
	it.mu.Unlock()
	// Clean up pending RPC
	it.client.mu.Lock()
	delete(it.client.pendingRPCs, it.correlationID)
	it.client.mu.Unlock()
	return nil
}
