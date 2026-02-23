package connection

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// pendingRequest represents one in-flight request awaiting response.
type pendingRequest struct {
	responseChan chan []byte
	cancelFunc   context.CancelFunc
	sentAt       time.Time
}

// Multiplexer routes responses to pending requests using FIFO ordering.
// Per CLIENT_SPEC.md: Responses are matched to requests in order received.
// This matches the server's sequential processing model per actor/route.
type Multiplexer struct {
	// FIFO queue of pending requests per MessageType
	// Key = MessageType (100-199 for KV, 200-299 for Queue, etc.)
	// Value = queue of *pendingRequest (oldest at front)
	pending map[uint16]*list.List
	mu      sync.Mutex

	// Async delivery handlers (Notice NOTIFY, Schedule NOTIFY, RPC REQUEST to worker, RPC RESPONSE per CLIENT_SPEC.md)
	// notifyHandlers keyed by message type (209 Queue, 409 Lease, 504 Notice, 609 Stream) so multiple domains can subscribe.
	notifyHandlers        map[uint16]func(subID uint64, route string, payload []byte)
	scheduleNotifyHandler func(subID uint64, payload []byte)
	rpcReqHandler         func(payload []byte) // incoming RPC REQUEST (302) dispatched to worker
	rpcRespHandler        func(correlationID [16]byte, payload []byte)

	// Metrics for observability
	requestsInFlight atomic.Int64
	requestsTotal    atomic.Uint64
	responsesTotal   atomic.Uint64
	responsesDropped atomic.Uint64

	closed atomic.Bool
}

// NewMultiplexer creates a new multiplexer.
func NewMultiplexer() *Multiplexer {
	return &Multiplexer{
		pending:        make(map[uint16]*list.List),
		notifyHandlers: make(map[uint16]func(subID uint64, route string, payload []byte)),
	}
}

// RegisterRequest registers a pending request before sending.
// The responseChan will receive the response payload when it arrives.
// The cancelFunc is called if the request needs to be cleaned up.
func (m *Multiplexer) RegisterRequest(msgType uint16, responseChan chan []byte, cancelFunc context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed.Load() {
		close(responseChan)
		return
	}

	// Get or create FIFO queue for this MessageType
	queue, exists := m.pending[msgType]
	if !exists {
		queue = list.New()
		m.pending[msgType] = queue
	}

	// Add to back of queue (FIFO)
	req := &pendingRequest{
		responseChan: responseChan,
		cancelFunc:   cancelFunc,
		sentAt:       time.Now(),
	}
	queue.PushBack(req)

	m.requestsInFlight.Add(1)
	m.requestsTotal.Add(1)
}

// UnregisterRequest removes a pending request from the queue.
// Called when context is cancelled before response arrives.
func (m *Multiplexer) UnregisterRequest(msgType uint16, responseChan chan []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	queue, exists := m.pending[msgType]
	if !exists {
		return
	}

	// Find and remove matching request
	for e := queue.Front(); e != nil; e = e.Next() {
		req := e.Value.(*pendingRequest)
		if req.responseChan == responseChan {
			queue.Remove(e)
			m.requestsInFlight.Add(-1)
			return
		}
	}
}

// Dispatch routes a response to the appropriate handler.
// Called by the connection's dispatch loop when a frame arrives.
func (m *Multiplexer) Dispatch(msgType uint16, payload []byte) {
	// Handle async deliveries (per CLIENT_SPEC.md MessageType ranges)
	// Queue NOTIFY (209), Lease NOTIFY (409), Notice NOTIFY (504), Stream NOTIFY (609) use same payload format
	if msgType == 209 || msgType == 409 || msgType == 504 || msgType == 609 {
		if handler := m.notifyHandlers[msgType]; handler != nil {
			m.handleNotify(payload, handler)
		}
		return
	}
	if msgType == 705 { // Schedule NOTIFY
		m.handleScheduleNotify(payload)
		return
	}
	if msgType == 302 {
		// 302 can be either: (1) sync response (ack) to our outgoing REQUEST, or (2) async REQUEST forwarded to us as worker.
		// If we have a pending request for 302, this is the ack; otherwise deliver to worker handler.
		m.mu.Lock()
		queue, exists := m.pending[302]
		hasPending := exists && queue.Len() > 0
		m.mu.Unlock()
		if hasPending {
			// Sync response to caller's SendRequest(302) — fall through to sync path below
		} else if m.rpcReqHandler != nil {
			m.handleRpcRequest(payload)
			return
		}
		// If no pending and no handler, fall through to sync path (will drop if no pending)
	}
	if msgType == 303 {
		// 303 can be either: (1) sync response (ack) to our outgoing RESPONSE as worker, or (2) async RESPONSE forwarded to us as caller.
		// If we have a pending request for 303, this is the ack; otherwise deliver to caller's response handler.
		m.mu.Lock()
		queue303, exists303 := m.pending[303]
		hasPending303 := exists303 && queue303.Len() > 0
		m.mu.Unlock()
		if hasPending303 {
			// Sync response to worker's SendRequest(303) — fall through to sync path below
		} else {
			m.handleRpcResponse(payload)
			return
		}
	}

	// Synchronous request/response - route to oldest pending request
	m.mu.Lock()
	queue, exists := m.pending[msgType]
	if !exists || queue.Len() == 0 {
		m.mu.Unlock()
		// Unexpected response (no pending request)
		// This can happen if context was cancelled but response arrived
		m.responsesDropped.Add(1)
		return
	}

	// Pop oldest pending request (FIFO order)
	elem := queue.Front()
	req := queue.Remove(elem).(*pendingRequest)
	m.mu.Unlock()

	m.requestsInFlight.Add(-1)
	m.responsesTotal.Add(1)

	// Non-blocking send (prevents dispatch loop from stalling)
	select {
	case req.responseChan <- payload:
		// Success - response delivered
	case <-time.After(100 * time.Millisecond):
		// Slow consumer - drop response and close channel
		m.responsesDropped.Add(1)
		close(req.responseChan)
	}
}

// handleNotify processes NOTIFY messages (209 Queue, 409 Lease, 504 Notice, 609 Stream).
// Per CLIENT_SPEC.md: [u64 BE subscription_id][u32 route_len][route][u32 payload_len][payload]
func (m *Multiplexer) handleNotify(payload []byte, handler func(subID uint64, route string, payload []byte)) {
	if len(payload) < 8 {
		return // Malformed
	}

	offset := 0

	// Read subscription_id (u64 BE)
	subID := binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8

	// Read route length and route
	if len(payload) < offset+4 {
		return
	}
	routeLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4

	if len(payload) < offset+int(routeLen) {
		return
	}
	route := string(payload[offset : offset+int(routeLen)])
	offset += int(routeLen)

	// Read payload length and payload
	if len(payload) < offset+4 {
		return
	}
	payloadLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4

	if len(payload) < offset+int(payloadLen) {
		return
	}
	msgPayload := payload[offset : offset+int(payloadLen)]

	handler(subID, route, msgPayload)
}

// handleScheduleNotify processes Schedule NOTIFY messages (705).
// Per CLIENT_SPEC.md: [u64 BE subscription_id][u32 payload_len][payload]
func (m *Multiplexer) handleScheduleNotify(payload []byte) {
	if len(payload) < 8 {
		return
	}
	offset := 0
	subID := binary.BigEndian.Uint64(payload[offset : offset+8])
	offset += 8
	if len(payload) < offset+4 {
		return
	}
	payloadLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if len(payload) < offset+int(payloadLen) {
		return
	}
	msgPayload := payload[offset : offset+int(payloadLen)]
	if m.scheduleNotifyHandler != nil {
		m.scheduleNotifyHandler(subID, msgPayload)
	}
}

// handleRpcRequest processes RPC REQUEST messages (async delivery to worker).
// Payload is full request: [u32 len=16][16 bytes correlation_id][route][reply_route][body]
func (m *Multiplexer) handleRpcRequest(payload []byte) {
	if m.rpcReqHandler != nil {
		m.rpcReqHandler(payload)
	}
}

// handleRpcResponse processes RPC RESPONSE messages (async delivery).
// Per server rpc_codec.rs: [bytes correlation_id][u64 seq][bytes body][u8 stream_end]
// where "bytes" = [u32 BE len][data] (TLV bytes format)
func (m *Multiplexer) handleRpcResponse(payload []byte) {
	// Need at least [u32 len=16][16 bytes uuid] = 20 bytes for correlation_id
	if len(payload) < 20 {
		return
	}

	// Parse correlation_id as TLV bytes: [u32 BE len][16 bytes UUID]
	corrLen := binary.BigEndian.Uint32(payload[0:4])
	if corrLen != 16 || len(payload) < 4+int(corrLen) {
		return
	}

	var correlationID [16]byte
	copy(correlationID[:], payload[4:20])

	// Call registered handler with remaining payload (seq + body + stream_end)
	if m.rpcRespHandler != nil {
		m.rpcRespHandler(correlationID, payload[20:])
	}
}

// SetNotifyHandler registers the handler for NOTIFY messages for the given message type.
// msgType should be 209 (Queue), 409 (Lease), 504 (Notice), or 609 (Stream).
func (m *Multiplexer) SetNotifyHandler(msgType uint16, handler func(subID uint64, route string, payload []byte)) {
	if m.notifyHandlers == nil {
		m.notifyHandlers = make(map[uint16]func(subID uint64, route string, payload []byte))
	}
	m.notifyHandlers[msgType] = handler
}

// SetScheduleNotifyHandler registers the handler for Schedule NOTIFY messages (705).
func (m *Multiplexer) SetScheduleNotifyHandler(handler func(subID uint64, payload []byte)) {
	m.scheduleNotifyHandler = handler
}

// SetRPCRequestHandler registers the handler for RPC REQUEST messages (302).
// Called by the RPC domain client so workers receive forwarded requests.
func (m *Multiplexer) SetRPCRequestHandler(handler func(payload []byte)) {
	m.rpcReqHandler = handler
}

// SetRPCResponseHandler registers the handler for RPC RESPONSE messages.
// Called by the RPC domain client.
func (m *Multiplexer) SetRPCResponseHandler(handler func(correlationID [16]byte, payload []byte)) {
	m.rpcRespHandler = handler
}

// Close shuts down the multiplexer and fails all pending requests.
func (m *Multiplexer) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Close all pending request channels (signals error to waiters)
	for _, queue := range m.pending {
		for e := queue.Front(); e != nil; e = e.Next() {
			req := e.Value.(*pendingRequest)
			close(req.responseChan)
		}
	}

	// Clear pending requests
	m.pending = make(map[uint16]*list.List)

	return nil
}

// Metrics returns current multiplexer statistics.
func (m *Multiplexer) Metrics() MultiplexerMetrics {
	return MultiplexerMetrics{
		RequestsInFlight: m.requestsInFlight.Load(),
		RequestsTotal:    m.requestsTotal.Load(),
		ResponsesTotal:   m.responsesTotal.Load(),
		ResponsesDropped: m.responsesDropped.Load(),
	}
}

// MultiplexerMetrics contains multiplexer statistics.
type MultiplexerMetrics struct {
	RequestsInFlight int64
	RequestsTotal    uint64
	ResponsesTotal   uint64
	ResponsesDropped uint64
}

// String provides a human-readable representation of metrics.
func (m MultiplexerMetrics) String() string {
	return fmt.Sprintf(
		"Mux[in_flight=%d, total_req=%d, total_resp=%d, dropped=%d]",
		m.RequestsInFlight, m.RequestsTotal, m.ResponsesTotal, m.ResponsesDropped,
	)
}
