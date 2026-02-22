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
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := l.conn.Tracer().Start(ctx, "fitz.lease.Renew", trace.WithAttributes(
		attribute.String("fitz.route", l.route),
		attribute.Int("fitz.ttl_secs", int(ttlSecs)),
	))
	defer span.End()
	resp, err := l.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseRenew, leaseRenewPayloadWriter(l.route, tokenToU64(token), ttlSecs))
	if err != nil {
		if log := l.conn.Logger(); log != nil {
			log.Error("lease.Renew failed", "route", l.route, "error", err)
		}
		return 0, fmt.Errorf("RENEW request failed: %w", err)
	}
	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := l.conn.Logger(); log != nil {
			log.Error("lease.Renew failed", "route", l.route, "error", err)
		}
		return 0, fmt.Errorf("RENEW failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		if log := l.conn.Logger(); log != nil {
			log.Error("lease.Renew failed", "route", l.route, "status", "unexpected")
		}
		return 0, fmt.Errorf("RENEW failed: unexpected status")
	}
	// Per CLIENT_SPEC: success = [u8 status=0][u64 BE new_fencing_token]
	if len(remaining) >= 8 {
		newToken := binary.BigEndian.Uint64(remaining[0:8])
		l.Token = make([]byte, 8)
		binary.BigEndian.PutUint64(l.Token, newToken)
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
	ctx, span := l.conn.Tracer().Start(ctx, "fitz.lease.Release", trace.WithAttributes(attribute.String("fitz.route", l.route)))
	defer span.End()
	resp, err := l.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseRelease, leaseReleasePayloadWriter(l.route, tokenToU64(token)))
	if err != nil {
		if log := l.conn.Logger(); log != nil {
			log.Error("lease.Release failed", "route", l.route, "error", err)
		}
		return fmt.Errorf("RELEASE request failed: %w", err)
	}
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := l.conn.Logger(); log != nil {
			log.Error("lease.Release failed", "route", l.route, "error", err)
		}
		return fmt.Errorf("RELEASE failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		if log := l.conn.Logger(); log != nil {
			log.Error("lease.Release failed", "route", l.route, "status", "unexpected")
		}
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

// LeaseInfo holds lease query results per CLIENT_SPEC.md QUERY response.
type LeaseInfo struct {
	Held             bool
	Token            []byte // Not set from QUERY (server returns owner_id, not token)
	TTL              uint32 // Deprecated: use TTLRemainingSecs
	OwnerID          string // Set when Held (owner_id from server)
	TTLRemainingSecs uint64 // Seconds until expiry when Held
	PendingWaiters   uint32 // Count of clients waiting in queue
}

type client struct {
	conn *connection.Connection
}

// NewClient creates a new Lease domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn}
}

// Acquire per CLIENT_SPEC.md:
// Request: [route_len][route][owner_id_len][owner_id][ttl_secs][optional wait_seconds]
// Response (status=0): [u8 response_type (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued)][u64 BE fencing_token]
func (c *client) Acquire(ctx context.Context, route string, ttlSecs uint64) (*Lease, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.lease.Acquire", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int("fitz.ttl_secs", int(ttlSecs)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("lease.Acquire", "route", route, "ttl_secs", ttlSecs)
	}

	// Validate route format
	if err := types.ValidateRoute(route, "lease"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseAcquire, leaseAcquirePayloadWriter(route, ttlSecs))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("lease.Acquire failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("ACQUIRE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if isLeaseHeldError(err) {
			return nil, ErrLeaseHeld
		}
		if log := c.conn.Logger(); log != nil {
			log.Error("lease.Acquire failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("ACQUIRE failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("lease.Acquire failed", "route", route, "status", "held")
		}
		return nil, ErrLeaseHeld
	}

	if len(remaining) < 9 {
		return nil, fmt.Errorf("ACQUIRE response too short: got %d bytes", len(remaining))
	}
	responseType := remaining[0]
	fencingToken := binary.BigEndian.Uint64(remaining[1:9])

	switch responseType {
	case 0: // Acquired
	case 1: // AlreadyHeld (we already hold it, idempotent)
	default:
		// 2=Queued, 3=AlreadyQueued
		return nil, ErrLeaseQueued
	}

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
// Response (free): [status][u8 has_holder=0][u32 pending_waiters]
// Response (held): [status][u8 has_holder=1][owner_id_len][owner_id][u64 ttl_remaining_secs][u32 pending_waiters]
func (c *client) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.lease.Query", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("lease.Query", "route", route)
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeLeaseQuery, leaseQueryPayloadWriter(route))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("lease.Query failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("QUERY request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("lease.Query failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("QUERY failed: %w", mapLeaseError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("lease.Query failed", "route", route, "status", "unexpected")
		}
		return nil, fmt.Errorf("QUERY failed: unexpected status")
	}

	info := &LeaseInfo{}
	if len(remaining) < 1 {
		return info, nil
	}

	hasHolder := remaining[0]
	if hasHolder == 0 {
		// Lease free: [u32 pending_waiters]
		if len(remaining) >= 5 {
			info.PendingWaiters = binary.BigEndian.Uint32(remaining[1:5])
		}
		return info, nil
	}

	info.Held = true
	offset := 1
	// owner_id (string = u32 len + bytes)
	if offset+4 > len(remaining) {
		return info, nil
	}
	ownerIDLen := binary.BigEndian.Uint32(remaining[offset : offset+4])
	offset += 4
	if offset+int(ownerIDLen) > len(remaining) {
		return info, nil
	}
	info.OwnerID = string(remaining[offset : offset+int(ownerIDLen)])
	offset += int(ownerIDLen)
	// ttl_remaining_secs (u64)
	if offset+8 > len(remaining) {
		return info, nil
	}
	info.TTLRemainingSecs = binary.BigEndian.Uint64(remaining[offset : offset+8])
	info.TTL = uint32(info.TTLRemainingSecs)
	offset += 8
	// pending_waiters (u32)
	if offset+4 <= len(remaining) {
		info.PendingWaiters = binary.BigEndian.Uint32(remaining[offset : offset+4])
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
