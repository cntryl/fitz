package connection

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
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

	// Observability (optional)
	logger *slog.Logger
	tracer trace.Tracer
}

// Config contains connection configuration.
type Config struct {
	JWT              string
	AuthTimeout      time.Duration // Default 5s (CLIENT_SPEC.md recommendation)
	ReadTimeout      time.Duration // Default 30s (per-read timeout)
	WriteTimeout     time.Duration // Default 10s
	ReconnectEnabled bool
	ReconnectBackoff time.Duration

	// Observability (optional)
	Logger *slog.Logger // When nil, no logging.
	Tracer trace.Tracer // When nil, otel.Tracer(module) is used.
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

	tracer := cfg.Tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
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
		logger:        cfg.Logger,
		tracer:        tracer,
	}
}

// Logger returns the structured logger, or nil if not set.
func (c *Connection) Logger() *slog.Logger {
	return c.logger
}

// Tracer returns the OpenTelemetry tracer (never nil).
func (c *Connection) Tracer() trace.Tracer {
	return c.tracer
}

// Start begins the connection lifecycle.
// Starts dispatch loop and performs CONNECT handshake.
// Blocks until authentication is confirmed or fails.
func (c *Connection) Start(ctx context.Context) error {
	c.setState(StateAuthenticating)
	if c.logger != nil {
		c.logger.Info("connection authenticating")
	}

	// Start dispatch loop
	go c.dispatchLoop()

	// Send CONNECT
	if err := c.sendConnect(ctx); err != nil {
		if c.logger != nil {
			c.logger.Error("send CONNECT failed", "error", err)
		}
		c.Close()
		return fmt.Errorf("send CONNECT: %w", err)
	}

	// Wait for authentication confirmation
	authTimeout := c.cfg.AuthTimeout

	select {
	case <-c.authConfirmed:
		// Auth succeeded (first response received or immediate for anonymous)
		if c.logger != nil {
			c.logger.Info("connection authenticated")
		}
		return nil

	case <-c.done:
		// Connection closed during auth (likely invalid JWT)
		if c.logger != nil {
			c.logger.Error("authentication failed", "error", c.authError)
		}
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
		if c.logger != nil {
			c.logger.Error("authentication timeout")
		}
		c.Close()
		return ErrAuthenticationTimeout
	}
}

// sendConnect sends the CONNECT message (MessageType=1).
// Per CLIENT_SPEC.md: [MessageType=1][Length][JWT bytes UTF-8]
// Empty JWT for anonymous mode.
func (c *Connection) sendConnect(ctx context.Context) error {
	payload := []byte(c.jwt)
	frame := protocol.EncodeFrameOwned(protocol.MessageTypeConnect, payload)
	if frame == nil {
		return fmt.Errorf("encode CONNECT frame")
	}
	defer frame.Release()

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame.Bytes()); err != nil {
		return err
	}

	// For anonymous mode (empty JWT), confirm immediately
	// Per CLIENT_SPEC.md: Server stays silent on valid JWT
	if c.jwt == "" {
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
			frame, err := c.transport.Read(readCtx)
			cancel()
			if err != nil {
				c.handleReadError(err)
				return
			}

			// Decode frame (MessageType + payload)
			msgType, payload, err := protocol.DecodeFrame(frame)
			if err != nil {
				if c.logger != nil {
					c.logger.Error("decode frame failed", "error", err)
				}
				c.setConnError(fmt.Errorf("decode frame: %w", err))
				return
			}
			if c.logger != nil {
				c.logger.Debug("frame received", "msg_type", msgType)
			}

			// First valid response confirms authentication
			if firstResponse {
				c.confirmAuthentication()
				firstResponse = false
			}

			// Route to multiplexer (non-blocking dispatch)
			c.mux.Dispatch(msgType, payload)
			continue
		}

		frame, err := c.transport.Read(readCtx)
		if err != nil {
			c.handleReadError(err)
			return
		}

		// Decode frame (MessageType + payload)
		msgType, payload, err := protocol.DecodeFrame(frame)
		if err != nil {
			if c.logger != nil {
				c.logger.Error("decode frame failed", "error", err)
			}
			c.setConnError(fmt.Errorf("decode frame: %w", err))
			return
		}
		if c.logger != nil {
			c.logger.Debug("frame received", "msg_type", msgType)
		}

		// First valid response confirms authentication
		if firstResponse {
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
	if c.logger != nil {
		c.logger.Warn("read error", "error", err)
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
	frame := protocol.EncodeFrameOwned(msgType, payload)
	if frame == nil {
		return nil, fmt.Errorf("encode frame")
	}
	defer frame.Release()

	if c.logger != nil {
		c.logger.Debug("request sent", "msg_type", msgType)
	}

	// Send request
	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame.Bytes()); err != nil {
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

// SendRequestWithWriter sends a request using a payload writer to avoid allocations.
func (c *Connection) SendRequestWithWriter(ctx context.Context, msgType uint16, writePayload func(*bytes.Buffer)) ([]byte, error) {
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
	frame, err := protocol.EncodeFrameWithPayloadWriter(msgType, writePayload)
	if err != nil {
		return nil, fmt.Errorf("encode frame: %w", err)
	}
	defer frame.Release()

	if c.logger != nil {
		c.logger.Debug("request sent", "msg_type", msgType)
	}

	// Send request
	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame.Bytes()); err != nil {
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

// SendFireAndForget sends a frame without expecting a response.
// Used for operations like Notice PUBLISH or RPC response where the server does not reply.
func (c *Connection) SendFireAndForget(ctx context.Context, msgType uint16, payload []byte) error {
	if !c.isAuthenticated() {
		return ErrNotAuthenticated
	}

	frame := protocol.EncodeFrameOwned(msgType, payload)
	if frame == nil {
		return fmt.Errorf("encode frame")
	}
	defer frame.Release()

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame.Bytes()); err != nil {
		return fmt.Errorf("write fire-and-forget: %w", err)
	}

	return nil
}

// SendFireAndForgetWithWriter sends a fire-and-forget frame using a payload writer.
func (c *Connection) SendFireAndForgetWithWriter(ctx context.Context, msgType uint16, writePayload func(*bytes.Buffer)) error {
	if !c.isAuthenticated() {
		return ErrNotAuthenticated
	}

	frame, err := protocol.EncodeFrameWithPayloadWriter(msgType, writePayload)
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	defer frame.Release()

	writeCtx := ctx
	if c.cfg.WriteTimeout > 0 {
		var cancel context.CancelFunc
		writeCtx, cancel = context.WithTimeout(ctx, c.cfg.WriteTimeout)
		defer cancel()
	}

	if err := c.transport.Write(writeCtx, frame.Bytes()); err != nil {
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
	if c.logger != nil {
		c.logger.Info("connection closed")
	}

	return err
}

// RegisterNotifyHandler registers handler for NOTIFY messages for the given message type.
// msgType should be protocol.MessageTypeQueueNotify (209), MessageTypeLeaseNotify (409),
// MessageTypeNoticeNotify (504), or MessageTypeStreamNotify (609).
func (c *Connection) RegisterNotifyHandler(msgType uint16, handler func(subID uint64, route string, payload []byte)) {
	c.mux.SetNotifyHandler(msgType, handler)
}

// RegisterScheduleNotifyHandler registers handler for Schedule NOTIFY messages (705).
func (c *Connection) RegisterScheduleNotifyHandler(handler func(subID uint64, payload []byte)) {
	c.mux.SetScheduleNotifyHandler(handler)
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
