package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/domains/kv"
	"github.com/cntryl/fitz-go/internal/domains/lease"
	"github.com/cntryl/fitz-go/internal/domains/notice"
	"github.com/cntryl/fitz-go/internal/domains/queue"
	"github.com/cntryl/fitz-go/internal/domains/rpc"
	"github.com/cntryl/fitz-go/internal/domains/schedule"
	"github.com/cntryl/fitz-go/internal/domains/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	addr          string
	tokenProvider types.TokenProvider
	conn          *connection.Connection
	config        *Config

	// Domain clients
	kvClient       kv.Client
	noticeClient   notice.Client
	queueClient    queue.Client
	rpcClient      rpc.Client
	streamClient   stream.Client
	leaseClient    lease.Client
	scheduleClient schedule.Client

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

	// Observability (optional)
	Logger *slog.Logger // When nil, no logging.
	Tracer trace.Tracer // When nil, otel.Tracer(module) is used for spans.
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

// WithLogger sets the structured logger for connection and domain logging.
// When nil or not set, no logs are emitted.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) { c.Logger = logger }
}

// WithTracer sets the OpenTelemetry tracer for spans.
// When nil or not set, otel.Tracer(module) is used (no-op if no TracerProvider is set).
func WithTracer(tracer trace.Tracer) Option {
	return func(c *Config) { c.Tracer = tracer }
}

// NewClient creates a new Fitz client targeting the given address.
// Call Connect() to establish the connection.
func NewClient(addr string, tokenProvider types.TokenProvider) *Client {
	return &Client{
		addr:          addr,
		tokenProvider: tokenProvider,
		config:        defaultConfig(),
	}
}

// Connect establishes a connection to the broker using the address and
// TokenProvider configured during client construction.
func (c *Client) Connect(ctx context.Context) error {
	tracer := c.config.Tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
	}
	transportType := c.config.TransportType
	if transportType == TransportAuto {
		transportType = detectTransport(c.addr)
	}
	var span trace.Span
	ctx, span = tracer.Start(ctx, "fitz.Connect", trace.WithAttributes(
		attribute.String("fitz.addr", c.addr),
		attribute.String("fitz.transport", transportTypeString(transportType)),
	))
	defer span.End()

	if c.config.Logger != nil {
		c.config.Logger.Info("connect started", "addr", c.addr)
	}
	// Get JWT from token provider
	jwt := ""
	if c.tokenProvider != nil {
		var err error
		jwt, err = c.tokenProvider(ctx)
		if err != nil {
			if c.config.Logger != nil {
				c.config.Logger.Error("get token failed", "error", err)
			}
			return fmt.Errorf("get token: %w", err)
		}
	}

	c.config.URL = c.addr
	c.config.JWT = jwt

	if err := c.config.validate(); err != nil {
		if c.config.Logger != nil {
			c.config.Logger.Error("invalid config", "error", err)
		}
		return fmt.Errorf("invalid config: %w", err)
	}

	// Dial transport
	var trans transport.Transport
	var err error

	switch transportType {
	case TransportWebSocket:
		trans, err = transport.DialWebSocket(ctx, c.config.URL)
	case TransportTCP:
		trans, err = transport.DialTCP(ctx, c.config.URL)
	default:
		return fmt.Errorf("unsupported transport type: %d", transportType)
	}

	if err != nil {
		return fmt.Errorf("dial transport: %w", err)
	}

	// Create connection
	connCfg := connection.Config{
		JWT:          c.config.JWT,
		AuthTimeout:  c.config.AuthTimeout,
		ReadTimeout:  c.config.ReadTimeout,
		WriteTimeout: c.config.WriteTimeout,
		Logger:       c.config.Logger,
		Tracer:       c.config.Tracer,
	}

	conn := connection.New(trans, connCfg)

	// Start connection (dispatch loop + CONNECT handshake per CLIENT_SPEC.md)
	if err := conn.Start(ctx); err != nil {
		if c.config.Logger != nil {
			c.config.Logger.Error("start connection failed", "error", err)
		}
		trans.Close()
		return fmt.Errorf("start connection: %w", err)
	}

	c.conn = conn

	// Initialize domain clients
	c.kvClient = kv.NewClient(conn)
	c.noticeClient = notice.NewClient(conn)
	c.queueClient = queue.NewClient(conn)
	c.rpcClient = rpc.NewClient(conn)
	c.streamClient = stream.NewClient(conn)
	c.leaseClient = lease.NewClient(conn)
	c.scheduleClient = schedule.NewClient(conn)

	if c.config.Logger != nil {
		c.config.Logger.Info("connect success", "addr", c.addr)
	}
	return nil
}

// Dial connects to a Fitz server and returns a ready-to-use client.
// Per CLIENT_SPEC.md: Performs CONNECT handshake and waits for authentication confirmation.
func Dial(ctx context.Context, opts ...Option) (*Client, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.validate(); err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Error("invalid config", "error", err)
		}
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	transportType := cfg.TransportType
	if transportType == TransportAuto {
		transportType = detectTransport(cfg.URL)
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = otel.Tracer("github.com/cntryl/fitz-go")
	}
	var span trace.Span
	ctx, span = tracer.Start(ctx, "fitz.Connect", trace.WithAttributes(
		attribute.String("fitz.addr", cfg.URL),
		attribute.String("fitz.transport", transportTypeString(transportType)),
	))
	defer span.End()

	c := &Client{
		addr:   cfg.URL,
		config: cfg,
	}

	c.config = cfg

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
		if cfg.Logger != nil {
			cfg.Logger.Error("dial transport failed", "error", err)
		}
		return nil, fmt.Errorf("dial transport: %w", err)
	}

	// Create connection
	connCfg := connection.Config{
		JWT:          cfg.JWT,
		AuthTimeout:  cfg.AuthTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		Logger:       cfg.Logger,
		Tracer:       cfg.Tracer,
	}

	conn := connection.New(trans, connCfg)

	// Start connection (dispatch loop + CONNECT handshake per CLIENT_SPEC.md)
	if err := conn.Start(ctx); err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Error("start connection failed", "error", err)
		}
		trans.Close()
		return nil, fmt.Errorf("start connection: %w", err)
	}

	c.conn = conn

	// Initialize domain clients
	c.kvClient = kv.NewClient(conn)
	c.noticeClient = notice.NewClient(conn)
	c.queueClient = queue.NewClient(conn)
	c.rpcClient = rpc.NewClient(conn)
	c.streamClient = stream.NewClient(conn)
	c.leaseClient = lease.NewClient(conn)
	c.scheduleClient = schedule.NewClient(conn)

	if cfg.Logger != nil {
		cfg.Logger.Info("dial success", "url", cfg.URL)
	}
	return c, nil
}

// detectTransport determines transport type from URL scheme.
func detectTransport(url string) TransportType {
	if strings.HasPrefix(url, "ws://") || strings.HasPrefix(url, "wss://") {
		return TransportWebSocket
	}
	return TransportTCP
}

func transportTypeString(t TransportType) string {
	switch t {
	case TransportWebSocket:
		return "websocket"
	case TransportTCP:
		return "tcp"
	default:
		return "auto"
	}
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
		if c.config.Logger != nil {
			c.config.Logger.Info("client close")
		}
		if c.conn != nil {
			err = c.conn.Close()
		}
	})
	return err
}

// SendRequest is a low-level API for domain implementations.
// Sends a synchronous request and waits for response.
// Per CLIENT_SPEC.md: Responses matched via FIFO correlation.
func (c *Client) SendRequest(ctx context.Context, msgType uint16, payload []byte) ([]byte, error) {
	return c.conn.SendRequest(ctx, msgType, payload)
}

// RegisterNotifyHandler registers handler for NOTIFY messages for the given message type.
// msgType should be protocol.MessageTypeQueueNotify (209), MessageTypeLeaseNotify (409),
// MessageTypeNoticeNotify (504), or MessageTypeStreamNotify (609).
func (c *Client) RegisterNotifyHandler(msgType uint16, handler func(subID uint64, route string, payload []byte)) {
	c.conn.RegisterNotifyHandler(msgType, handler)
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

// Domain client accessors (implements fitz.Client interface)

// KV returns the KV domain client.
func (c *Client) KV() kv.Client {
	return c.kvClient
}

// Notice returns the Notice domain client.
func (c *Client) Notice() notice.Client {
	return c.noticeClient
}

// Queue returns the Queue domain client.
func (c *Client) Queue() queue.Client {
	return c.queueClient
}

// RPC returns the RPC domain client.
func (c *Client) RPC() rpc.Client {
	return c.rpcClient
}

// Stream returns the Stream domain client.
func (c *Client) Stream() stream.Client {
	return c.streamClient
}

// Lease returns the Lease domain client.
func (c *Client) Lease() lease.Client {
	return c.leaseClient
}

// Schedule returns the Schedule domain client.
func (c *Client) Schedule() schedule.Client {
	return c.scheduleClient
}
