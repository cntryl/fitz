package connection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/fitz-go/internal/core/debug"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// State represents the connection lifecycle state.
type State int32

const (
	StateDisconnected   State = iota
	StateConnecting           // Transport dialing
	StateConnected            // Transport open
	StateAuthenticating       // CONNECT sent, awaiting confirmation
	StateAuthenticated        // First response received OR immediate auth (anonymous)
	StateClosed               // Connection terminated
)

// String returns the state name for logging.
func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "DISCONNECTED"
	case StateConnecting:
		return "CONNECTING"
	case StateConnected:
		return "CONNECTED"
	case StateAuthenticating:
		return "AUTHENTICATING"
	case StateAuthenticated:
		return "AUTHENTICATED"
	case StateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// Connection manages a single connection to the Fitz server.
// Handles authentication, dispatch loop, and request/response correlation.
type Connection struct {
	transport transport.Transport
	state     atomic.Int32 // State enum
	stateMu   sync.RWMutex // Protects state transitions

	// CONNECT configuration (per CLIENT_SPEC.md)
	jwt string

	// Authentication confirmation
	authConfirmed chan struct{} // Closed when auth succeeds
	authError     error         // Set if auth fails

	// Multiplexer for request/response correlation
	mux *Multiplexer

	// Dispatch loop control
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // Closed when dispatch loop exits

	// Connection error (set when connection closes)
	connError atomic.Value // stores error

	// Configuration
	cfg Config
}

// Config contains connection configuration.
type Config struct {
	JWT              string
	AuthTimeout      time.Duration // Default 5s (CLIENT_SPEC.md recommendation)
	ReadTimeout      time.Duration // Default 30s (per-read timeout)
	WriteTimeout     time.Duration // Default 10s
	ReconnectEnabled bool
	ReconnectBackoff time.Duration
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{
		AuthTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// Common errors
var (
	ErrNotAuthenticated      = errors.New("not authenticated")
	ErrAuthenticationFailed  = errors.New("authentication failed")
	ErrAuthenticationTimeout = errors.New("authentication timeout")
	ErrConnectionClosed      = errors.New("connection closed")
)

// New creates a new connection with the given transport.
func New(trans transport.Transport, cfg Config) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	// Apply defaults
	if cfg.AuthTimeout == 0 {
		cfg.AuthTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 10 * time.Second
	}

	return &Connection{
		transport:     trans,
		jwt:           cfg.JWT,
		authConfirmed: make(chan struct{}),
		mux:           NewMultiplexer(),
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		cfg:           cfg,
	}
}

// Start begins the connection lifecycle.
// Starts dispatch loop and performs CONNECT handshake.
// Blocks until authentication is confirmed or fails.
func (c *Connection) Start(ctx context.Context) error {
	c.setState(StateAuthenticating)

	// Start dispatch loop
	go c.dispatchLoop()

	// Send CONNECT
	if err := c.sendConnect(ctx); err != nil {
		c.Close()
		return fmt.Errorf("send CONNECT: %w", err)
	}

	// Wait for authentication confirmation
	authTimeout := c.cfg.AuthTimeout

	select {
	case <-c.authConfirmed:
		// Auth succeeded (first response received or immediate for anonymous)
		return nil

	case <-c.done:
		// Connection closed during auth (likely invalid JWT)
		if c.authError != nil {
			return c.authError
		}
		return ErrAuthenticationFailed

	case <-ctx.Done():
		// Caller cancelled
		c.Close()
		return ctx.Err()

	case <-time.After(authTimeout):
		// No response within timeout
		c.Close()
		return ErrAuthenticationTimeout
	}
}

// sendConnect sends the CONNECT message (MessageType=1).
// Per CLIENT_SPEC.md: [MessageType=1][Length][JWT bytes UTF-8]
// Empty JWT for anonymous mode.
func (c *Connection) sendConnect(ctx context.Context) error {
	payload := []byte(c.jwt)
	frame := protocol.EncodeFrame(protocol.MessageTypeConnect, payload)

	debug.FrameSend(protocol.MessageTypeConnect, payload)

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame); err != nil {
		debug.Log("CONNECT write failed: %v", err)
		return err
	}

	// For anonymous mode (empty JWT), confirm immediately
	// Per CLIENT_SPEC.md: Server stays silent on valid JWT
	if c.jwt == "" {
		debug.Log("Anonymous mode — confirming auth immediately")
		c.confirmAuthentication()
	}

	return nil
}

// confirmAuthentication marks authentication as successful.
// Called when first valid response arrives (or immediately for anonymous).
func (c *Connection) confirmAuthentication() {
	select {
	case <-c.authConfirmed:
		// Already confirmed
	default:
		c.setState(StateAuthenticated)
		close(c.authConfirmed)
	}
}

// dispatchLoop reads frames from transport and routes to multiplexer.
// Runs in its own goroutine, started by Start().
func (c *Connection) dispatchLoop() {
	defer close(c.done)
	defer c.mux.Close()

	firstResponse := true

	for {
		// Check if connection is closed
		if c.ctx.Err() != nil {
			return
		}

		// Read next frame from transport (context-aware)
		readCtx := c.ctx
		if c.cfg.ReadTimeout > 0 {
			var cancel context.CancelFunc
			readCtx, cancel = context.WithTimeout(c.ctx, c.cfg.ReadTimeout)
			defer cancel()
		}

		frame, err := c.transport.Read(readCtx)
		if err != nil {
			if debug.Enabled {
				debug.Log("Transport read error: %v", err)
			}
			c.handleReadError(err)
			return
		}

		debug.FrameRecvRaw(frame)

		// Decode frame (MessageType + payload)
		msgType, payload, err := protocol.DecodeFrame(frame)
		if err != nil {
			debug.DecodeError(frame, err)
			c.setConnError(fmt.Errorf("decode frame: %w", err))
			return
		}

		debug.FrameRecv(msgType, payload)

		// First valid response confirms authentication
		if firstResponse {
			debug.Log("First response received — auth confirmed")
			c.confirmAuthentication()
			firstResponse = false
		}

		// Route to multiplexer (non-blocking dispatch)
		c.mux.Dispatch(msgType, payload)
	}
}

// handleReadError processes transport read errors.
func (c *Connection) handleReadError(err error) {
	if errors.Is(err, context.Canceled) {
		// Clean shutdown
		c.setConnError(nil)
		return
	}

	// Connection closed by server (might be auth failure)
	if !c.isAuthenticated() {
		c.authError = ErrAuthenticationFailed
	}

	c.setConnError(err)
}

// SendRequest sends a synchronous request and waits for response.
// Used by domain client implementations.
func (c *Connection) SendRequest(ctx context.Context, msgType uint16, payload []byte) ([]byte, error) {
	// Check connection state
	if !c.isAuthenticated() {
		return nil, ErrNotAuthenticated
	}

	// Create response channel
	responseChan := make(chan []byte, 1)

	// Register with multiplexer (FIFO queue)
	c.mux.RegisterRequest(msgType, responseChan, nil)

	// Cleanup on context cancel or completion
	defer c.mux.UnregisterRequest(msgType, responseChan)

	// Encode frame
	frame := protocol.EncodeFrame(msgType, payload)

	debug.FrameSend(msgType, payload)

	// Send request
	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame); err != nil {
		debug.Log("Write error for msg_type=%d: %v", msgType, err)
		return nil, fmt.Errorf("write request: %w", err)
	}

	// Wait for response
	select {
	case resp, ok := <-responseChan:
		if !ok {
			// Channel closed (connection error or slow consumer timeout)
			if err := c.getConnError(); err != nil {
				return nil, err
			}
			return nil, ErrConnectionClosed
		}
		return resp, nil

	case <-ctx.Done():
		// Caller cancelled (UnregisterRequest called via defer)
		return nil, ctx.Err()

	case <-c.done:
		// Connection closed
		if err := c.getConnError(); err != nil {
			return nil, err
		}
		return nil, ErrConnectionClosed
	}
}

// SendOneWay sends a fire-and-forget frame (no response expected).
// Used for operations like Notice PUBLISH where the server does not reply.
func (c *Connection) SendOneWay(ctx context.Context, msgType uint16, payload []byte) error {
	if !c.isAuthenticated() {
		return ErrNotAuthenticated
	}

	frame := protocol.EncodeFrame(msgType, payload)

	debug.FrameSend(msgType, payload)

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame); err != nil {
		debug.Log("Write error for msg_type=%d: %v", msgType, err)
		return fmt.Errorf("write fire-and-forget: %w", err)
	}

	return nil
}

// Close cleanly shuts down the connection.
func (c *Connection) Close() error {
	// Cancel context (signals dispatch loop to stop)
	c.cancel()

	// Close transport FIRST to unblock any pending Read() calls.
	// Without this, the dispatch loop would block forever on transport.Read()
	// because the TCP read is a blocking I/O call that ignores context cancellation.
	err := c.transport.Close()

	// Wait for dispatch loop to exit (now guaranteed to return since transport is closed)
	<-c.done

	c.setState(StateClosed)

	return err
}

// RegisterNotifyHandler registers handler for Notice NOTIFY messages.
func (c *Connection) RegisterNotifyHandler(handler func(subID uint64, route string, payload []byte)) {
	c.mux.SetNotifyHandler(handler)
}

// RegisterRPCRequestHandler registers handler for RPC REQUEST messages (302).
func (c *Connection) RegisterRPCRequestHandler(handler func(payload []byte)) {
	c.mux.SetRPCRequestHandler(handler)
}

// RegisterRPCResponseHandler registers handler for RPC RESPONSE messages.
func (c *Connection) RegisterRPCResponseHandler(handler func(correlationID [16]byte, payload []byte)) {
	c.mux.SetRPCResponseHandler(handler)
}

// State management helpers

func (c *Connection) setState(state State) {
	c.state.Store(int32(state))
}

func (c *Connection) getState() State {
	return State(c.state.Load())
}

func (c *Connection) isAuthenticated() bool {
	select {
	case <-c.authConfirmed:
		return true
	default:
		return false
	}
}

func (c *Connection) setConnError(err error) {
	if err != nil {
		c.connError.Store(err)
	}
}

func (c *Connection) getConnError() error {
	if val := c.connError.Load(); val != nil {
		return val.(error)
	}
	return nil
}

// Metrics returns multiplexer metrics.
func (c *Connection) Metrics() MultiplexerMetrics {
	return c.mux.Metrics()
}
