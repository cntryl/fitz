package stream

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockMux is a minimal mux provider for unit testing stream client.
type mockMux struct {
	in      chan transport.Frame
	sent    []transport.Frame
	sendErr error
}

func newMockMux() *mockMux { return &mockMux{in: make(chan transport.Frame, 16)} }
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

func resp(body []byte) transport.Frame {
	return transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelStream, Body: body}
}

func errFrame(msg string) transport.Frame {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagErr, msg)
	return transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelStream, Body: enc.Encode()}
}

func TestShouldReturnSeqGivenValidRespWhenBeginCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagSeq, 123)
	m.in <- resp(enc.Encode())

	// Act
	seq, err := c.Begin(context.Background(), "stream://r/a/res")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, uint64(123), seq)
}

func TestShouldReturnSeqGivenAppendRespWhenAppendCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagSeq, 42)
	m.in <- resp(enc.Encode())

	// Act
	seq, err := c.Append(context.Background(), "stream://r/a/res", []byte("data"), nil)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, uint64(42), seq)
}

func TestShouldReturnLastRecordGivenExistingStreamWhenLastCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagSeq, 9)
	enc.AddBytes(transport.TagBody, []byte("last"))
	m.in <- resp(enc.Encode())

	// Act
	rec, err := c.Last(context.Background(), "stream://r/a/res")

	// Assert
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(9), rec.Offset)
	assert.Equal(t, []byte("last"), rec.Body)
}

func TestShouldCollectRecordsUntilStreamEndWhenReadCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	// send two records then a stream-end
	enc1 := transport.NewTLVEncoder()
	enc1.AddUint64(transport.TagSeq, 1)
	enc1.AddBytes(transport.TagBody, []byte("one"))
	m.in <- resp(enc1.Encode())
	enc2 := transport.NewTLVEncoder()
	enc2.AddUint64(transport.TagSeq, 2)
	enc2.AddBytes(transport.TagBody, []byte("two"))
	m.in <- resp(enc2.Encode())
	end := transport.NewTLVEncoder()
	end.AddUint8(transport.TagStreamEnd, 1)
	m.in <- resp(end.Encode())

	// Act
	it, err := c.ReadResource(context.Background(), "stream://r/a/res", 0, 10)
	require.NoError(t, err)
	defer it.Close()
	var recs []StreamRecord
	for it.Next() {
		recs = append(recs, it.Value())
	}

	// Assert
	require.NoError(t, it.Err())
	assert.Len(t, recs, 2)
	assert.Equal(t, []byte("one"), recs[0].Body)
	assert.Equal(t, []byte("two"), recs[1].Body)
}

func TestShouldSucceedGivenRespWhenCommitCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- resp([]byte{0})

	// Act
	err := c.Commit(context.Background(), "stream://r/a/res")

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnStreamConflictGivenErrorWhenCommitCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("stream conflict")

	// Act
	err := c.Commit(context.Background(), "stream://r/a/res")

	// Assert
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrStreamConflict)
}

func TestShouldSucceedGivenRespWhenRollbackCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- resp([]byte{0})

	// Act
	err := c.Rollback(context.Background(), "stream://r/a/res")

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnContextCanceledGivenCancelledContextWhenBeginCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	_, err := c.Begin(ctx, "stream://r/a/res")

	// Assert
	require.Error(t, err)
}

func TestShouldReturnTimeoutGivenNoResponseWhenAppendCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Act
	_, err := c.Append(ctx, "stream://r/a/res", []byte("x"), nil)

	// Assert
	require.Error(t, err)
}

func TestShouldReturnErrorOnPerFrameTimeout(t *testing.T) {
	m := newMockMux()
	c := NewClient(m)

	it, err := c.ReadResource(context.Background(), "stream://r/a/res", 0, 0, WithPerFrameTimeout(10*time.Millisecond))
	require.NoError(t, err)
	defer it.Close()

	// no frames are sent; Next() should eventually stop and Err() should be ErrStreamReadError
	for it.Next() {
	}

	require.Error(t, it.Err())
	require.ErrorIs(t, it.Err(), ErrStreamReadError)
}
