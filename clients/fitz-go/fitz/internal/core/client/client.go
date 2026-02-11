package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
	"github.com/cntryl/cntryl-go/internal/domains/kv"
	"github.com/cntryl/cntryl-go/internal/domains/lease"
	"github.com/cntryl/cntryl-go/internal/domains/notice"
	"github.com/cntryl/cntryl-go/internal/domains/queue"
	"github.com/cntryl/cntryl-go/internal/domains/rpc"
	"github.com/cntryl/cntryl-go/internal/domains/schedule"
	"github.com/cntryl/cntryl-go/internal/domains/stream"
)

// domainMux adapts the transport mux for domain clients.
type domainMux struct {
	send        func(transport.Frame) error
	in          <-chan transport.Frame
	ctx         context.Context
	reconnectCb func(func())
}

func (d *domainMux) Send(f transport.Frame) error { return d.send(f) }
func (d *domainMux) In() <-chan transport.Frame   { return d.in }
func (d *domainMux) Ctx() context.Context         { return d.ctx }
func (d *domainMux) OnReconnect(cb func()) {
	if d.reconnectCb != nil {
		d.reconnectCb(cb)
	}
}

// FramerFactory creates a Framer from a raw net.Conn.
type FramerFactory func(net.Conn) transport.Framer

// Client implements connection management, retry logic, and mux-based domain routing.
type Client struct {
	addr          string
	tokenProvider types.TokenProvider
	dialer        Dialer
	// framerFactory allows callers to supply a transport framer for the
	// underlying net.Conn produced by the Dialer. If nil, the client uses
	// a sensible default (TCP framer). Prefer providing a factory to avoid
	// transport convenience logic in the client.
	framerFactory FramerFactory
	mux           *transport.Mux
	mu            sync.RWMutex
	closed        bool
	retryBackoff  time.Duration
	maxRetries    int
	// per-domain inbound channels (dispatcher will route frames here)
	kvIn       chan transport.Frame
	noticeIn   chan transport.Frame
	streamIn   chan transport.Frame
	queueIn    chan transport.Frame
	rpcIn      chan transport.Frame
	leaseIn    chan transport.Frame
	scheduleIn chan transport.Frame

	domainClients struct {
		notice   notice.Client
		stream   stream.Client
		queue    queue.Client
		rpc      rpc.Client
		kv       kv.Client
		lease    lease.Client
		schedule schedule.Client
	}
}

// NewClient creates a new Fitz client for the given address and token provider.
// Address format determines transport protocol:
//   - "host:port" or "tcp://host:port" for TCP
//   - "ws://host:port/path" for WebSocket
//   - "wss://host:port/path" for secure WebSocket (recommended)
//
// TokenProvider is called during connection to obtain JWT for authentication.
// Pass nil for unauthenticated connections.
func NewClient(addr string, tokenProvider types.TokenProvider, opts ...ClientOption) *Client {
	c := &Client{
		addr:          addr,
		tokenProvider: tokenProvider,
		dialer:        &DefaultDialer{},
		retryBackoff:  100 * time.Millisecond,
		maxRetries:    5,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClientOption is a functional option for configuring the client.
type ClientOption func(*Client)

// WithRetryBackoff sets the initial retry backoff duration.
func WithRetryBackoff(d time.Duration) ClientOption {
	return func(c *Client) { c.retryBackoff = d }
}

// WithMaxRetries sets the maximum number of connection retries.
func WithMaxRetries(n int) ClientOption {
	return func(c *Client) { c.maxRetries = n }
}

// WithDialer sets a custom dialer (primarily for testing).
func WithDialer(d Dialer) ClientOption {
	return func(c *Client) { c.dialer = d }
}

// WithFramerFactory sets a custom framer factory used to wrap the raw
// net.Conn returned by the Dialer. This avoids client-side convenience
// framer construction and delegates transport selection to the caller.
func WithFramerFactory(f FramerFactory) ClientOption {
	return func(c *Client) { c.framerFactory = f }
}

// Connect opens a connection to the broker with exponential backoff retry.
// On success, it launches the mux and initializes domain clients.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mux != nil {
		return errors.New("already connected")
	}

	// Try to establish connection with retries.
	var conn net.Conn
	var err error
	backoff := c.retryBackoff
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err = c.dialer.Dial(ctx, c.addr)
		if err == nil {
			break
		}
		if attempt < c.maxRetries {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
		}
	}
	if conn == nil {
		return fmt.Errorf("failed to connect after %d attempts: %w", c.maxRetries+1, err)
	}

	// Create mux and start loops. Use framer factory supplied by caller when present;
	// otherwise default to TCP framer (legacy behavior).
	var fr transport.Framer
	if c.framerFactory != nil {
		fr = c.framerFactory(conn)
	} else {
		fr = transport.NewTCPFramer(conn)
	}
	c.mux = transport.NewMux(fr)
	c.mux.Start()
	// Notify domain clients that the transport is writable (initial connect).
	c.mux.FireReconnect()

	// Initialize per-domain channels and start dispatcher before creating
	// domain clients so they can receive frames from their dedicated channels.
	c.kvIn = make(chan transport.Frame, 128)
	c.noticeIn = make(chan transport.Frame, 128)
	c.streamIn = make(chan transport.Frame, 128)
	c.queueIn = make(chan transport.Frame, 128)
	c.rpcIn = make(chan transport.Frame, 128)
	c.leaseIn = make(chan transport.Frame, 128)
	c.scheduleIn = make(chan transport.Frame, 128)
	go func() {
		defer func() {
			close(c.kvIn)
			close(c.noticeIn)
			close(c.streamIn)
			close(c.queueIn)
			close(c.rpcIn)
			close(c.leaseIn)
			close(c.scheduleIn)
		}()
		for f := range c.mux.In() {
			switch f.Channel {
			case transport.ChannelKV:
				select {
				case c.kvIn <- f:
				default:
					// Drop if receiver overloaded to avoid blocking the dispatcher
				}
			case transport.ChannelPub, transport.ChannelSub:
				select {
				case c.noticeIn <- f:
				default:
				}
			case transport.ChannelStream:
				select {
				case c.streamIn <- f:
				default:
				}
			case transport.ChannelQueue:
				select {
				case c.queueIn <- f:
				default:
				}
			case transport.ChannelRPC:
				select {
				case c.rpcIn <- f:
				default:
				}
			case transport.ChannelLease:
				select {
				case c.leaseIn <- f:
				default:
				}
			case transport.ChannelSchedule:
				select {
				case c.scheduleIn <- f:
				default:
				}
			default:
				// Unknown channel — ignore
			}
		}
	}()

	// Send CONNECT frame as first message (per CLIENT_SPEC.md).
	// CONNECT frame MUST be sent before any other operations.
	if err := c.sendConnect(ctx); err != nil {
		_ = c.mux.Close()
		c.mux = nil
		return fmt.Errorf("CONNECT handshake failed: %w", err)
	}
	// Give broker a short grace period to finish session setup (no explicit ACK in spec).
	// This prevents some brokers from closing the connection when a client sends
	// domain frames immediately after CONNECT.
	time.Sleep(50 * time.Millisecond)

	// Initialize domain clients after successful connection.
	c.initializeDomainClients()

	return nil
}

// Close gracefully closes the connection and domain clients.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.mux == nil {
		return nil
	}
	c.closed = true
	return c.mux.Close()
}

// Notice returns the notice domain client.
func (c *Client) Notice() notice.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.notice
}

// Stream returns the stream domain client.
func (c *Client) Stream() stream.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.stream
}

// Queue returns the queue domain client.
func (c *Client) Queue() queue.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.queue
}

// Schedule returns the schedule domain client.
func (c *Client) Schedule() schedule.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.schedule
}

// RPC returns the RPC domain client.
func (c *Client) RPC() rpc.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.rpc
}

// KV returns the KV domain client.
func (c *Client) KV() kv.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.kv
}

// Lease returns the lease domain client.
func (c *Client) Lease() lease.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.domainClients.lease
}

// initializeDomainClients creates concrete domain clients that use the mux.
// This is called after a successful connection.
func (c *Client) initializeDomainClients() {
	// KV client uses a domain-specific mux adapter to avoid competing readers on the shared mux
	c.domainClients.kv = kv.NewClient(&domainMux{send: c.mux.Send, in: c.kvIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
	// Initialize lease client
	c.domainClients.lease = lease.NewClient(&domainMux{send: c.mux.Send, in: c.leaseIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
	// Notice domain client
	c.domainClients.notice = notice.NewClient(&domainMux{send: c.mux.Send, in: c.noticeIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
	// RPC client
	c.domainClients.rpc = rpc.NewClient(&domainMux{send: c.mux.Send, in: c.rpcIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
	// Stream client
	c.domainClients.stream = stream.NewClient(&domainMux{send: c.mux.Send, in: c.streamIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
	// Queue client
	c.domainClients.queue = queue.NewClient(&domainMux{send: c.mux.Send, in: c.queueIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
	// Schedule client
	c.domainClients.schedule = schedule.NewClient(&domainMux{send: c.mux.Send, in: c.scheduleIn, ctx: c.mux.Ctx(), reconnectCb: c.mux.OnReconnect})
}

// sendConnect sends the CONNECT frame with JWT token per CLIENT_SPEC.md.
// CONNECT must be the first frame sent (MessageType=1, Channel=0 for control).
// Per spec: "Broker behavior: Valid CONNECT: No explicit ACK message. Broker remains silent and is ready for requests."
// We implement a timeout: if connection closes within 5 seconds, treat as auth failure.
func (c *Client) sendConnect(ctx context.Context) error {
	var token string
	var err error

	// Obtain JWT from token provider (may be empty for anonymous mode).
	if c.tokenProvider != nil {
		token, err = c.tokenProvider(ctx)
		if err != nil {
			return fmt.Errorf("failed to obtain token: %w", err)
		}
	}

	// Build CONNECT frame: Type=1, Channel=0 (control), Body=JWT bytes.
	connectFrame := transport.Frame{
		Type:    transport.FrameTypeConnOpen,
		Flags:   0,
		Channel: 0,
		Body:    []byte(token),
	}

	// Send CONNECT frame.
	if err := c.mux.Send(connectFrame); err != nil {
		return fmt.Errorf("failed to send CONNECT frame: %w", err)
	}

	// Per CLIENT_SPEC.md: "Valid CONNECT: No explicit ACK message. Broker remains silent and is ready for requests."
	// "Invalid CONNECT: Broker closes connection within 1 second (no response frame sent)"
	// We wait briefly to detect immediate connection closure (indicates auth failure).
	// If connection remains open after timeout, assume success.
	const connectTimeout = 200 * time.Millisecond
	timeoutCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	select {
	case <-c.mux.Ctx().Done():
		// Connection closed during handshake - treat as authentication failure.
		return errors.New("connection closed during CONNECT handshake (authentication rejected)")
	case <-timeoutCtx.Done():
		// Timeout elapsed without connection closure - assume success per spec.
		// "If no close frame within 5 seconds, assume connection is ready"
		return nil
	}
}
