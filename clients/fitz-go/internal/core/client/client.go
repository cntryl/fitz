package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/transport"
)

// TransportType specifies the transport protocol.
type TransportType int

const (
	TransportAuto TransportType = iota // Auto-detect from URL
	TransportWebSocket
	TransportTCP
)

// Client implements the Fitz client with connection management.
// Per CLIENT_SPEC.md: Handles authentication, request/response correlation, and domain routing.
type Client struct {
	conn   *connection.Connection
	config *Config

	// Domain clients (to be added as domain implementations are completed)
	// kv       *kv.Client
	// notice   *notice.Client
	// queue    *queue.Client
	// rpc      *rpc.Client
	// stream   *stream.Client
	// lease    *lease.Client
	// schedule *schedule.Client

	closeOnce sync.Once
}

// Config contains client configuration.
type Config struct {
	// Connection
	URL          string
	JWT          string
	AuthTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// Reconnection (not yet implemented)
	ReconnectEnabled bool
	ReconnectBackoff time.Duration
	MaxReconnects    int

	// Transport
	TransportType TransportType // Auto, WebSocket, or TCP
}

// defaultConfig returns default client configuration.
func defaultConfig() *Config {
	return &Config{
		TransportType: TransportAuto,
		AuthTimeout:   5 * time.Second,
		ReadTimeout:   30 * time.Second,
		WriteTimeout:  10 * time.Second,
	}
}

// Option is a functional option for configuring the client.
type Option func(*Config)

// WithURL sets the server URL.
func WithURL(url string) Option {
	return func(c *Config) { c.URL = url }
}

// WithJWT sets the JWT token for authentication.
// Use empty string for anonymous mode.
func WithJWT(jwt string) Option {
	return func(c *Config) { c.JWT = jwt }
}

// WithAuthTimeout sets the authentication timeout.
func WithAuthTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.AuthTimeout = timeout }
}

// WithReadTimeout sets the per-read timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = timeout }
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *Config) { c.WriteTimeout = timeout }
}

// WithReconnect enables/disables automatic reconnection.
func WithReconnect(enabled bool, backoff time.Duration, maxAttempts int) Option {
	return func(c *Config) {
		c.ReconnectEnabled = enabled
		c.ReconnectBackoff = backoff
		c.MaxReconnects = maxAttempts
	}
}

// WithTransport sets the transport type.
func WithTransport(transportType TransportType) Option {
	return func(c *Config) { c.TransportType = transportType }
}

// Dial connects to a Fitz server and returns a ready-to-use client.
// Per CLIENT_SPEC.md: Performs CONNECT handshake and waits for authentication confirmation.
func Dial(ctx context.Context, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Determine transport type from URL if auto
	transportType := cfg.TransportType
	if transportType == TransportAuto {
		transportType = detectTransport(cfg.URL)
	}

	// Dial transport
	var trans transport.Transport
	var err error

	switch transportType {
	case TransportWebSocket:
		trans, err = transport.DialWebSocket(ctx, cfg.URL)
	case TransportTCP:
		trans, err = transport.DialTCP(ctx, cfg.URL)
	default:
		return nil, fmt.Errorf("unsupported transport type: %d", transportType)
	}

	if err != nil {
		return nil, fmt.Errorf("dial transport: %w", err)
	}

	// Create connection
	connCfg := connection.Config{
		JWT:          cfg.JWT,
		AuthTimeout:  cfg.AuthTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	conn := connection.New(trans, connCfg)

	// Start connection (dispatch loop + CONNECT handshake per CLIENT_SPEC.md)
	if err := conn.Start(ctx); err != nil {
		trans.Close()
		return nil, fmt.Errorf("start connection: %w", err)
	}

	client := &Client{
		conn:   conn,
		config: cfg,
	}

	// Initialize domain clients (when domain implementations are ready)
	// client.kv = kv.NewClient(conn)
	// client.notice = notice.NewClient(conn)
	// ... etc

	return client, nil
}

// detectTransport determines transport type from URL scheme.
func detectTransport(url string) TransportType {
	if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
		return TransportWebSocket
	}
	return TransportTCP
}

// validate checks if the configuration is valid.
func (c *Config) validate() error {
	if c.URL == "" {
		return errors.New("URL is required")
	}
	return nil
}

// Close gracefully shuts down the connection.
// Safe to call multiple times (idempotent).
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.conn.Close()
	})
	return err
}

// SendRequest is a low-level API for domain implementations.
// Sends a synchronous request and waits for response.
// Per CLIENT_SPEC.md: Responses matched via FIFO correlation.
func (c *Client) SendRequest(ctx context.Context, msgType uint16, payload []byte) ([]byte, error) {
	return c.conn.SendRequest(ctx, msgType, payload)
}

// RegisterNotifyHandler registers handler for Notice NOTIFY messages.
// Called by Notice domain client.
func (c *Client) RegisterNotifyHandler(handler func(subID uint64, route string, payload []byte)) {
	c.conn.RegisterNotifyHandler(handler)
}

// RegisterRPCResponseHandler registers handler for RPC RESPONSE messages.
// Called by RPC domain client.
func (c *Client) RegisterRPCResponseHandler(handler func(correlationID [16]byte, payload []byte)) {
	c.conn.RegisterRPCResponseHandler(handler)
}

// Metrics returns connection metrics.
func (c *Client) Metrics() connection.MultiplexerMetrics {
	return c.conn.Metrics()
}

// Domain client accessors (to be uncommented as domain implementations are added)

// KV returns the KV domain client.
// func (c *Client) KV() *kv.Client {
// 	return c.kv
// }

// Notice returns the Notice domain client.
// func (c *Client) Notice() *notice.Client {
// 	return c.notice
// }

// Queue returns the Queue domain client.
// func (c *Client) Queue() *queue.Client {
// 	return c.queue
// }

// RPC returns the RPC domain client.
// func (c *Client) RPC() *rpc.Client {
// 	return c.rpc
// }

// Stream returns the Stream domain client.
// func (c *Client) Stream() *stream.Client {
// 	return c.stream
// }

// Lease returns the Lease domain client.
// func (c *Client) Lease() *lease.Client {
// 	return c.lease
// }

// Schedule returns the Schedule domain client.
// func (c *Client) Schedule() *schedule.Client {
// 	return c.schedule
// }
