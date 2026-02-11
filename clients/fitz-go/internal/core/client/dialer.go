package client

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	"crypto/tls"
)

// Dialer creates network connections for different transport protocols.
// This interface allows for easy testing and transport abstraction.
type Dialer interface {
	// Dial establishes a connection based on the address scheme.
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// DefaultDialer implements the Dialer interface with support for
// TCP and WebSocket transports.
type DefaultDialer struct{}

// Dial establishes a network connection based on the address scheme.
// Supports:
//   - "host:port" or "tcp://host:port" for TCP connections
//   - "ws://host:port/path" for WebSocket connections
//   - "wss://host:port/path" for secure WebSocket connections
func (d *DefaultDialer) Dial(ctx context.Context, addr string) (net.Conn, error) {
	// If no scheme, assume TCP
	if !strings.Contains(addr, "://") {
		return d.dialTCP(ctx, addr)
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid address: %w", err)
	}

	switch u.Scheme {
	case "tcp":
		return d.dialTCP(ctx, u.Host)
	case "ws", "wss":
		return d.dialWebSocket(ctx, addr)
	default:
		return nil, fmt.Errorf("unsupported transport scheme: %s", u.Scheme)
	}
}

// dialTCP establishes a TCP connection.
func (d *DefaultDialer) dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, "tcp", addr)
}

// dialWebSocket establishes a WebSocket connection and returns a net.Conn adapter.
// Minimal in-repo implementation (handshake + RFC6455 binary frames). Returns a
// wrapper implementing net.Conn by translating WebSocket binary messages to Read/Write.
func (d *DefaultDialer) dialWebSocket(ctx context.Context, addr string) (net.Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid websocket address: %w", err)
	}

	var c net.Conn
	if u.Scheme == "wss" {
		dialer := &tls.Dialer{}
		t, err := dialer.DialContext(ctx, "tcp", u.Host)
		if err != nil {
			return nil, err
		}
		c = t
	} else {
		var dialer net.Dialer
		t, err := dialer.DialContext(ctx, "tcp", u.Host)
		if err != nil {
			return nil, err
		}
		c = t
	}

	reader, err := doClientHandshake(c, u.Host, u.RequestURI())
	if err != nil {
		c.Close()
		return nil, err
	}

	return &wsConnAdapter{conn: c, reader: reader}, nil
}
