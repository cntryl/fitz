// Package lease implements the Fitz Lease domain client.
// Per CLIENT_SPEC.md: Distributed lease acquisition with fencing tokens.
package lease

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// Lease is a handle representing an acquired lease. Renew and Release are called on it.
type Lease struct {
	Token     []byte
	ExpiresAt int64

	route string
	conn  *connection.Connection
}

// Renew extends the lease TTL. Returns the new expiry timestamp.
func (l *Lease) Renew(ctx context.Context, ttlSecs uint64) (int64, error) {
	return l.renewWithToken(ctx, l.Token, ttlSecs)
}

// RenewWithToken renews using an explicit token (e.g. for testing invalid token).
func (l *Lease) RenewWithToken(ctx context.Context, token []byte, ttlSecs uint64) (int64, error) {
	return l.renewWithToken(ctx, token, ttlSecs)
}

func (l *Lease) renewWithToken(ctx context.Context, token []byte, ttlSecs uint64) (int64, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteString(buf, l.route)
	connection.WriteString(buf, "")
	connection.WriteU64BE(buf, tokenToU64(token))
	connection.WriteU64BE(buf, ttlSecs)

	resp, err := l.conn.SendRequest(ctx, protocol.MessageTypeLeaseRenew, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("RENEW request failed: %w", err)
	}
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("RENEW failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		return 0, fmt.Errorf("RENEW failed: unexpected status")
	}
	newExpiry := time.Now().Unix() + int64(ttlSecs)
	l.ExpiresAt = newExpiry
	return newExpiry, nil
}

// Release frees the lease.
func (l *Lease) Release(ctx context.Context) error {
	return l.releaseWithToken(ctx, l.Token)
}

// ReleaseWithToken releases using an explicit token (e.g. for testing invalid token).
func (l *Lease) ReleaseWithToken(ctx context.Context, token []byte) error {
	return l.releaseWithToken(ctx, token)
}

func (l *Lease) releaseWithToken(ctx context.Context, token []byte) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteString(buf, l.route)
	connection.WriteString(buf, "")
	connection.WriteU64BE(buf, tokenToU64(token))

	resp, err := l.conn.SendRequest(ctx, protocol.MessageTypeLeaseRelease, buf.Bytes())
	if err != nil {
		return fmt.Errorf("RELEASE request failed: %w", err)
	}
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("RELEASE failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		return fmt.Errorf("RELEASE failed: unexpected status")
	}
	return nil
}

// Client is the Lease domain client interface.
type Client interface {
	// Acquire attempts to acquire a lease on the given route.
	// Returns a Lease handle on success; use Renew and Release on it.
	// Returns ErrLeaseHeld when the lease is already held by another owner.
	Acquire(ctx context.Context, route string, ttlSecs uint64) (*Lease, error)

	// Query returns current lease status for the route.
	Query(ctx context.Context, route string) (*LeaseInfo, error)
}

// LeaseInfo holds lease query results.
type LeaseInfo struct {
	Held  bool
	Token []byte
	TTL   uint32
}

type client struct {
	conn *connection.Connection
}

// NewClient creates a new Lease domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn}
}

// Acquire per CLIENT_SPEC.md:
// Request: [route_len][route][owner_id_len][owner_id][ttl_secs]
// Response: [status][u8 has_token][u64 token if has=1] (optional u64)
func (c *client) Acquire(ctx context.Context, route string, ttlSecs uint64) (*Lease, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteString(buf, "")
	connection.WriteU64BE(buf, ttlSecs)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeLeaseAcquire, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("ACQUIRE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if isLeaseHeldError(err) {
			return nil, ErrLeaseHeld
		}
		return nil, fmt.Errorf("ACQUIRE failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		return nil, ErrLeaseHeld
	}

	if len(remaining) < 1 {
		return nil, fmt.Errorf("ACQUIRE response too short: got %d bytes", len(remaining))
	}
	hasToken := remaining[0]
	if hasToken != 1 || len(remaining) < 9 {
		return nil, fmt.Errorf("ACQUIRE response missing token")
	}
	fencingToken := binary.BigEndian.Uint64(remaining[1:9])

	tokenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tokenBytes, fencingToken)
	expiresAt := time.Now().Unix() + int64(ttlSecs)

	return &Lease{
		Token:     tokenBytes,
		ExpiresAt: expiresAt,
		route:     route,
		conn:      c.conn,
	}, nil
}

// Query per CLIENT_SPEC.md:
// Request: [route_len][route]
// Response: [status][u8 has_token][u64 token if has=1]
func (c *client) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeLeaseQuery, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("QUERY request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("QUERY failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("QUERY failed: unexpected status")
	}

	info := &LeaseInfo{}
	if len(remaining) < 1 {
		return info, nil
	}

	// Server sends optional u64: [u8 has_token][u64 token if has=1]
	hasToken := remaining[0]
	if hasToken == 0 {
		return info, nil // Lease not held
	}

	info.Held = true
	if len(remaining) >= 9 {
		tokenVal := binary.BigEndian.Uint64(remaining[1:9])
		info.Token = make([]byte, 8)
		binary.BigEndian.PutUint64(info.Token, tokenVal)
	}

	return info, nil
}

func tokenToU64(token []byte) uint64 {
	if len(token) >= 8 {
		return binary.BigEndian.Uint64(token[:8])
	}
	var padded [8]byte
	copy(padded[8-len(token):], token)
	return binary.BigEndian.Uint64(padded[:])
}

func isLeaseHeldError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return bytes.Contains([]byte(msg), []byte("held")) || bytes.Contains([]byte(msg), []byte("already"))
}
