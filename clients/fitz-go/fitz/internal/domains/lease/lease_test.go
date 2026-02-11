package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/core/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

// mockMux is a minimal mux provider for unit testing the lease client.
type mockMux struct {
	in      chan transport.Frame
	sent    []transport.Frame
	sendErr error // if non-nil, Send returns this error
}

func newMockMux() *mockMux {
	return &mockMux{in: make(chan transport.Frame, 16)}
}

func (m *mockMux) Send(f transport.Frame) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, f)
	return nil
}

func (m *mockMux) In() <-chan transport.Frame { return m.in }
func (m *mockMux) Ctx() context.Context       { return context.Background() }
func (m *mockMux) OnReconnect(cb func())      {}

// respFrame builds a FrameTypeResp on ChannelLease with given TLV body.
func respFrame(body []byte) transport.Frame {
	return transport.Frame{
		Type:    transport.FrameTypeResp,
		Channel: transport.ChannelLease,
		Body:    body,
	}
}

// errFrame builds a FrameTypeErr on ChannelLease with given error message.
func errFrame(msg string) transport.Frame {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagErr, msg)
	return transport.Frame{
		Type:    transport.FrameTypeErr,
		Channel: transport.ChannelLease,
		Body:    enc.Encode(),
	}
}

// leaseRespBody builds a TLV body with optional token and TTL for lease responses.
func leaseRespBody(token []byte, ttl uint32) []byte {
	enc := transport.NewTLVEncoder()
	if len(token) > 0 {
		enc.AddBytes(transport.TagLease, token)
	}
	enc.AddUint32(transport.TagTTL, ttl)
	return enc.Encode()
}

// ---------------------------------------------------------------------------
// Acquire Tests
// ---------------------------------------------------------------------------

func TestShouldReturnTokenGivenFreeLeaseWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(leaseRespBody([]byte("tok-1"), 30))

	// Act
	token, expiresAt, held, err := c.Acquire(context.Background(), "lease://r/a/res", 30)

	// Assert
	require.NoError(t, err)
	assert.True(t, held)
	assert.Equal(t, []byte("tok-1"), token)
	assert.Greater(t, expiresAt, time.Now().Unix()-1)
}

func TestShouldReturnNotHeldGivenEmptyTokenWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(leaseRespBody(nil, 0))

	// Act
	token, expiresAt, held, err := c.Acquire(context.Background(), "lease://r/a/res", 30)

	// Assert
	require.NoError(t, err)
	assert.False(t, held)
	assert.Nil(t, token)
	assert.Equal(t, int64(0), expiresAt)
}

func TestShouldReturnLeaseHeldErrorGivenHeldLeaseWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("lease held by another owner")

	// Act
	_, _, _, err := c.Acquire(context.Background(), "lease://r/a/res", 30)

	// Assert
	require.ErrorIs(t, err, ErrLeaseHeld)
}

func TestShouldReturnSendErrorGivenMuxFailureWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	m.sendErr = errors.New("connection lost")
	c := NewClient(m)

	// Act
	_, _, _, err := c.Acquire(context.Background(), "lease://r/a/res", 30)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send:")
}

func TestShouldReturnContextErrorGivenCancelledContextWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	_, _, _, err := c.Acquire(ctx, "lease://r/a/res", 30)

	// Assert
	require.ErrorIs(t, err, context.Canceled)
}

func TestShouldEncodeRouteAndTTLGivenValidInputWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(leaseRespBody([]byte("tok"), 10))

	// Act
	_, _, _, err := c.Acquire(context.Background(), "lease://prod/app/lock1", 60)

	// Assert
	require.NoError(t, err)
	require.Len(t, m.sent, 1)
	sent := m.sent[0]
	assert.Equal(t, transport.FrameTypeReq, sent.Type)
	assert.Equal(t, transport.ChannelLease, sent.Channel)
	dec, derr := transport.NewTLVDecoder(sent.Body)
	require.NoError(t, derr)
	op, opErr := dec.GetOp()
	require.NoError(t, opErr)
	assert.Equal(t, LeaseAcquire, op)
	assert.Equal(t, "lease://prod/app/lock1", dec.GetString(transport.TagRoute))
	ttl, _ := dec.GetUint32(transport.TagTTL)
	assert.Equal(t, uint32(60), ttl)
}

func TestShouldSkipNonLeaseFramesGivenMixedChannelsWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	// Inject a frame from a different channel first, then the real response.
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: 99, Body: []byte{0}}
	m.in <- respFrame(leaseRespBody([]byte("tok"), 10))

	// Act
	token, _, held, err := c.Acquire(context.Background(), "lease://r/a/res", 10)

	// Assert
	require.NoError(t, err)
	assert.True(t, held)
	assert.Equal(t, []byte("tok"), token)
}

func TestShouldReturnMuxClosedErrorGivenClosedMuxWhenAcquireCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	close(m.in)

	// Act
	_, _, _, err := c.Acquire(context.Background(), "lease://r/a/res", 30)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mux closed")
}

func TestShouldReturnTLVErrorGivenCorruptResponseWhenAcquireCalled(t *testing.T) {
	// Arrange — truncated TLV (tag byte only, no length)
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame([]byte{0xFF})

	// Act
	_, _, _, err := c.Acquire(context.Background(), "lease://r/a/res", 30)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid TLV")
}

// ---------------------------------------------------------------------------
// Renew Tests
// ---------------------------------------------------------------------------

func TestShouldReturnExpiryGivenValidTokenWhenRenewCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	enc := transport.NewTLVEncoder()
	enc.AddUint32(transport.TagTTL, 60)
	m.in <- respFrame(enc.Encode())

	// Act
	expiresAt, err := c.Renew(context.Background(), "lease://r/a/res", []byte("tok-1"), 60)

	// Assert
	require.NoError(t, err)
	assert.Greater(t, expiresAt, time.Now().Unix()-1)
}

func TestShouldReturnInvalidFenceErrorGivenBadTokenWhenRenewCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("invalid fencing token")

	// Act
	_, err := c.Renew(context.Background(), "lease://r/a/res", []byte("bad-tok"), 60)

	// Assert
	require.ErrorIs(t, err, ErrInvalidFence)
}

func TestShouldReturnExpiredErrorGivenExpiredLeaseWhenRenewCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("lease expired")

	// Act
	_, err := c.Renew(context.Background(), "lease://r/a/res", []byte("tok-1"), 60)

	// Assert
	require.ErrorIs(t, err, ErrLeaseExpired)
}

func TestShouldReturnSendErrorGivenMuxFailureWhenRenewCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	m.sendErr = errors.New("connection lost")
	c := NewClient(m)

	// Act
	_, err := c.Renew(context.Background(), "lease://r/a/res", []byte("tok"), 60)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send:")
}

func TestShouldEncodeTokenAndTTLGivenValidInputWhenRenewCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	enc := transport.NewTLVEncoder()
	enc.AddUint32(transport.TagTTL, 30)
	m.in <- respFrame(enc.Encode())

	// Act
	_, err := c.Renew(context.Background(), "lease://r/a/res", []byte("my-token"), 45)

	// Assert
	require.NoError(t, err)
	require.Len(t, m.sent, 1)
	sent := m.sent[0]
	assert.Equal(t, transport.FrameTypeReq, sent.Type)
	assert.Equal(t, transport.ChannelLease, sent.Channel)
	dec, derr := transport.NewTLVDecoder(sent.Body)
	require.NoError(t, derr)
	op, opErr := dec.GetOp()
	require.NoError(t, opErr)
	assert.Equal(t, LeaseRenew, op)
	assert.Equal(t, "lease://r/a/res", dec.GetString(transport.TagRoute))
	assert.Equal(t, []byte("my-token"), dec.GetBytes(transport.TagLease))
	ttl, _ := dec.GetUint32(transport.TagTTL)
	assert.Equal(t, uint32(45), ttl)
}

func TestShouldReturnContextErrorGivenCancelledContextWhenRenewCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	_, err := c.Renew(ctx, "lease://r/a/res", []byte("tok"), 30)

	// Assert
	require.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// Release Tests
// ---------------------------------------------------------------------------

func TestShouldSucceedGivenValidTokenWhenReleaseCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(nil)

	// Act
	err := c.Release(context.Background(), "lease://r/a/res", []byte("tok-1"))

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnInvalidFenceErrorGivenBadTokenWhenReleaseCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("invalid fencing token")

	// Act
	err := c.Release(context.Background(), "lease://r/a/res", []byte("wrong-tok"))

	// Assert
	require.ErrorIs(t, err, ErrInvalidFence)
}

func TestShouldReturnNotFoundErrorGivenNoLeaseWhenReleaseCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("lease not found")

	// Act
	err := c.Release(context.Background(), "lease://r/a/res", []byte("tok"))

	// Assert
	require.ErrorIs(t, err, ErrLeaseNotFound)
}

func TestShouldReturnSendErrorGivenMuxFailureWhenReleaseCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	m.sendErr = errors.New("broken pipe")
	c := NewClient(m)

	// Act
	err := c.Release(context.Background(), "lease://r/a/res", []byte("tok"))

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send:")
}

func TestShouldEncodeRouteAndTokenGivenValidInputWhenReleaseCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(nil)

	// Act
	err := c.Release(context.Background(), "lease://r/a/res", []byte("release-tok"))

	// Assert
	require.NoError(t, err)
	require.Len(t, m.sent, 1)
	sent := m.sent[0]
	assert.Equal(t, transport.FrameTypeReq, sent.Type)
	assert.Equal(t, transport.ChannelLease, sent.Channel)
	dec, derr := transport.NewTLVDecoder(sent.Body)
	require.NoError(t, derr)
	op, opErr := dec.GetOp()
	require.NoError(t, opErr)
	assert.Equal(t, LeaseRelease, op)
	assert.Equal(t, "lease://r/a/res", dec.GetString(transport.TagRoute))
	assert.Equal(t, []byte("release-tok"), dec.GetBytes(transport.TagLease))
}

func TestShouldReturnContextErrorGivenCancelledContextWhenReleaseCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	err := c.Release(ctx, "lease://r/a/res", []byte("tok"))

	// Assert
	require.ErrorIs(t, err, context.Canceled)
}

// ---------------------------------------------------------------------------
// Query Tests
// ---------------------------------------------------------------------------

func TestShouldReturnHeldInfoGivenActiveLeaseWhenQueryCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(leaseRespBody([]byte("active-tok"), 25))

	// Act
	info, err := c.Query(context.Background(), "lease://r/a/res")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.True(t, info.Held)
	assert.Equal(t, []byte("active-tok"), info.Token)
	assert.Equal(t, uint32(25), info.TTL)
	assert.Greater(t, info.ExpiresAt, time.Now().Unix()-1)
}

func TestShouldReturnNotHeldGivenFreeLeaseWhenQueryCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(leaseRespBody(nil, 0))

	// Act
	info, err := c.Query(context.Background(), "lease://r/a/res")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.False(t, info.Held)
	assert.Nil(t, info.Token)
	assert.Equal(t, int64(0), info.ExpiresAt)
}

func TestShouldReturnSendErrorGivenMuxFailureWhenQueryCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	m.sendErr = errors.New("transport down")
	c := NewClient(m)

	// Act
	_, err := c.Query(context.Background(), "lease://r/a/res")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send:")
}

func TestShouldEncodeOnlyRouteGivenValidInputWhenQueryCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- respFrame(leaseRespBody(nil, 0))

	// Act
	_, err := c.Query(context.Background(), "lease://prod/area/res")

	// Assert
	require.NoError(t, err)
	require.Len(t, m.sent, 1)
	sent := m.sent[0]
	assert.Equal(t, transport.FrameTypeReq, sent.Type)
	assert.Equal(t, transport.ChannelLease, sent.Channel)
	dec, derr := transport.NewTLVDecoder(sent.Body)
	require.NoError(t, derr)
	op, opErr := dec.GetOp()
	require.NoError(t, opErr)
	assert.Equal(t, LeaseQuery, op)
	assert.Equal(t, "lease://prod/area/res", dec.GetString(transport.TagRoute))
	// Query sends only route — no TagLease or TagTTL expected.
	assert.Nil(t, dec.GetBytes(transport.TagLease))
}

func TestShouldReturnContextErrorGivenCancelledContextWhenQueryCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	_, err := c.Query(ctx, "lease://r/a/res")

	// Assert
	require.ErrorIs(t, err, context.Canceled)
}

func TestShouldReturnQueryErrorGivenErrorFrameWhenQueryCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("lease not found for route")

	// Act
	_, err := c.Query(context.Background(), "lease://r/a/res")

	// Assert
	require.ErrorIs(t, err, ErrLeaseNotFound)
}

// ---------------------------------------------------------------------------
// Error Mapping Tests
// ---------------------------------------------------------------------------

func TestShouldMapLeaseHeldErrorGivenHeldMessageWhenMapCalled(t *testing.T) {
	// Arrange & Act
	err := mapLeaseError("lease held by another owner")

	// Assert
	assert.ErrorIs(t, err, ErrLeaseHeld)
}

func TestShouldMapInvalidFenceErrorGivenFenceMessageWhenMapCalled(t *testing.T) {
	// Arrange & Act
	err := mapLeaseError("invalid fencing token provided")

	// Assert
	assert.ErrorIs(t, err, ErrInvalidFence)
}

func TestShouldMapInvalidTokenErrorGivenTokenMessageWhenMapCalled(t *testing.T) {
	// Arrange & Act
	err := mapLeaseError("invalid token supplied")

	// Assert
	assert.ErrorIs(t, err, ErrInvalidFence)
}

func TestShouldMapExpiredErrorGivenExpiredMessageWhenMapCalled(t *testing.T) {
	// Arrange & Act
	err := mapLeaseError("lease expired")

	// Assert
	assert.ErrorIs(t, err, ErrLeaseExpired)
}

func TestShouldMapNotFoundErrorGivenNotFoundMessageWhenMapCalled(t *testing.T) {
	// Arrange & Act
	err := mapLeaseError("lease not found for route")

	// Assert
	assert.ErrorIs(t, err, ErrLeaseNotFound)
}

func TestShouldReturnGenericErrorGivenUnknownMessageWhenMapCalled(t *testing.T) {
	// Arrange & Act
	err := mapLeaseError("something unexpected happened")

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "something unexpected happened", err.Error())
	assert.NotErrorIs(t, err, ErrLeaseHeld)
	assert.NotErrorIs(t, err, ErrInvalidFence)
	assert.NotErrorIs(t, err, ErrLeaseExpired)
	assert.NotErrorIs(t, err, ErrLeaseNotFound)
}

// ---------------------------------------------------------------------------
// DecodeTLVError Tests (testing via transport.DecodeTLVError)
// ---------------------------------------------------------------------------

func TestShouldUseDefaultMessageGivenEmptyBodyWhenDecodeTLVErrorCalled(t *testing.T) {
	// Arrange
	f := transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelLease, Body: nil}

	// Act
	err := transport.DecodeTLVError(f, "default error message", mapLeaseError)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "default error message", err.Error())
}

func TestShouldUseDefaultGivenCorruptBodyWhenDecodeTLVErrorCalled(t *testing.T) {
	// Arrange — truncated TLV: tag byte present but no length bytes
	f := transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelLease, Body: []byte{0xFF}}

	// Act
	err := transport.DecodeTLVError(f, "fallback message", mapLeaseError)

	// Assert — message now includes diagnostic info from the decode failure
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fallback message")
}

func TestShouldMapErrorFromTLVGivenKnownErrorWhenDecodeTLVErrorCalled(t *testing.T) {
	// Arrange
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagErr, "lease expired")
	f := transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelLease, Body: enc.Encode()}

	// Act
	err := transport.DecodeTLVError(f, "should not use default", mapLeaseError)

	// Assert
	assert.ErrorIs(t, err, ErrLeaseExpired)
}
