package lease

import (
	"context"
	"fmt"
	"time"

	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/cntryl/fitz-go/internal/core/types"
)

// Client provides ephemeral exclusive lease primitives.
type Client interface {
	Acquire(ctx context.Context, route string, ttlSecs uint32) (token []byte, expiresAt int64, held bool, err error)
	Renew(ctx context.Context, route string, token []byte, ttlSecs uint32) (expiresAt int64, err error)
	Release(ctx context.Context, route string, token []byte) error
	Query(ctx context.Context, route string) (*LeaseInfo, error)
}

// LeaseInfo holds the current status of a lease as returned by Query.
type LeaseInfo struct {
	Held      bool
	Token     []byte
	TTL       uint32
	ExpiresAt int64
}

type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new lease domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	return &client{mux: mux}
}

// Acquire attempts to acquire a lease for the given route and TTL (seconds).
func (c *client) Acquire(ctx context.Context, route string, ttlSecs uint32) (token []byte, expiresAt int64, held bool, err error) {
	if err := types.ValidateRoute(route, "lease"); err != nil {
		return nil, 0, false, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(LeaseAcquire)
	enc.AddString(transport.TagRoute, route)
	enc.AddUint32(transport.TagTTL, ttlSecs)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelLease, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapLeaseError)
	if err != nil {
		return nil, 0, false, err
	}

	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, 0, false, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	leaseToken := dec.GetBytes(transport.TagLease)
	ttl, _ := dec.GetUint32(transport.TagTTL)
	if len(leaseToken) == 0 {
		return nil, 0, false, nil
	}
	expires := time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	return leaseToken, expires, true, nil
}

// Renew extends the lease TTL for an existing lease token.
func (c *client) Renew(ctx context.Context, route string, token []byte, ttlSecs uint32) (expiresAt int64, err error) {
	if err := types.ValidateRoute(route, "lease"); err != nil {
		return 0, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(LeaseRenew)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagLease, token)
	enc.AddUint32(transport.TagTTL, ttlSecs)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelLease, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapLeaseError)
	if err != nil {
		return 0, err
	}

	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return 0, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	ttl, _ := dec.GetUint32(transport.TagTTL)
	expires := time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	return expires, nil
}

// Release frees a lease indicated by token.
func (c *client) Release(ctx context.Context, route string, token []byte) error {
	if err := types.ValidateRoute(route, "lease"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(LeaseRelease)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagLease, token)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelLease, Body: enc.Encode()}

	_, err := transport.SendRecv(ctx, c.mux, frame, mapLeaseError)
	return err
}

// Query retrieves the current status of a lease for the given route.
func (c *client) Query(ctx context.Context, route string) (*LeaseInfo, error) {
	if err := types.ValidateRoute(route, "lease"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(LeaseQuery)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelLease, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapLeaseError)
	if err != nil {
		return nil, err
	}

	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	leaseToken := dec.GetBytes(transport.TagLease)
	ttl, _ := dec.GetUint32(transport.TagTTL)
	info := &LeaseInfo{
		Held:  len(leaseToken) > 0,
		Token: leaseToken,
		TTL:   ttl,
	}
	if info.Held {
		info.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second).Unix()
	}
	return info, nil
}
