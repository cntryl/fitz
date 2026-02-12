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

// Client is the Lease domain client interface.
type Client interface {
	// Acquire attempts to acquire a lease on the given route.
	// Returns (token, expiresAt, held, err). held=true if acquisition succeeded.
	Acquire(ctx context.Context, route string, ttlSecs uint64) (token []byte, expiresAt int64, held bool, err error)

	// Renew extends an existing lease with valid fencing token.
	// Returns the new expiry timestamp.
	Renew(ctx context.Context, route string, token []byte, ttlSecs uint64) (newExpiry int64, err error)

	// Release frees the lease with valid fencing token.
	Release(ctx context.Context, route string, token []byte) error

	// Query returns current lease status.
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
// Response: [status][fencing_token (u64 BE)] on success
func (c *client) Acquire(ctx context.Context, route string, ttlSecs uint64) ([]byte, int64, bool, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	// owner_id: use empty for auto-assigned
	connection.WriteString(buf, "")
	connection.WriteU64BE(buf, ttlSecs)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeLeaseAcquire, buf.Bytes())
	if err != nil {
		return nil, 0, false, fmt.Errorf("ACQUIRE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if isLeaseHeldError(err) {
			return nil, 0, false, ErrLeaseHeld
		}
		return nil, 0, false, fmt.Errorf("ACQUIRE failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		return nil, 0, false, nil
	}

	// Parse fencing_token from remaining bytes
	if len(remaining) < 8 {
		return nil, 0, false, fmt.Errorf("ACQUIRE response too short: got %d bytes", len(remaining))
	}
	fencingToken := binary.BigEndian.Uint64(remaining[:8])

	// Convert token to bytes
	tokenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tokenBytes, fencingToken)

	expiresAt := time.Now().Unix() + int64(ttlSecs)
	return tokenBytes, expiresAt, true, nil
}

// Renew per CLIENT_SPEC.md:
// Request: [route_len][route][owner_id_len][owner_id][fencing_token (u64)][ttl_secs]
func (c *client) Renew(ctx context.Context, route string, token []byte, ttlSecs uint64) (int64, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteString(buf, "")
	connection.WriteU64BE(buf, tokenToU64(token))
	connection.WriteU64BE(buf, ttlSecs)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeLeaseRenew, buf.Bytes())
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

	return time.Now().Unix() + int64(ttlSecs), nil
}

// Release per CLIENT_SPEC.md:
// Request: [route_len][route][owner_id_len][owner_id][fencing_token (u64)]
func (c *client) Release(ctx context.Context, route string, token []byte) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteString(buf, "")
	connection.WriteU64BE(buf, tokenToU64(token))

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeLeaseRelease, buf.Bytes())
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

// Query per CLIENT_SPEC.md:
// Request: [route_len][route]
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

	hasHolder := remaining[0]
	if hasHolder == 0 {
		return info, nil
	}

	info.Held = true
	offset := 1

	// Read token (fencing_token u64)
	if offset+8 <= len(remaining) {
		tokenVal := binary.BigEndian.Uint64(remaining[offset : offset+8])
		info.Token = make([]byte, 8)
		binary.BigEndian.PutUint64(info.Token, tokenVal)
		offset += 8
	}

	// Read TTL remaining (try u64 first, then u32)
	if offset+8 <= len(remaining) {
		ttlVal := binary.BigEndian.Uint64(remaining[offset : offset+8])
		info.TTL = uint32(ttlVal)
	} else if offset+4 <= len(remaining) {
		info.TTL = binary.BigEndian.Uint32(remaining[offset : offset+4])
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
