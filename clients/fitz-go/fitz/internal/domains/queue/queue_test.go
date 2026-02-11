package queue

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockMux is a minimal mux provider for unit testing the queue client.
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
	return transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelQueue, Body: body}
}
func errFrame(msg string) transport.Frame {
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagErr, msg)
	return transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelQueue, Body: enc.Encode()}
}

func makeIDTLV(id uint64) []byte {
	e := transport.NewTLVEncoder()
	e.AddUint64(transport.TagID, id)
	return e.Encode()
}

func TestShouldReturnIDGivenEnqueueRespWhenEnqueueCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, 999)
	m.in <- resp(enc.Encode())

	// Act
	id, err := c.Enqueue(context.Background(), "queue://r/a/q", []byte("hi"))

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "999", id)
}

func TestShouldReturnItemsGivenReserveRespWhenReserveCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	// Build binary counted-repeat response per spec:
	// [u8 status=0][u32 BE lease_count=2]
	// [u64 msg_id][u64 lease_token][u32 body_len][body bytes] × 2
	body := make([]byte, 0, 64)
	body = append(body, 0)                          // status = 0 (success)
	body = binary.BigEndian.AppendUint32(body, 2)   // lease_count = 2
	body = binary.BigEndian.AppendUint64(body, 1)   // message_id #1
	body = binary.BigEndian.AppendUint64(body, 111) // lease_token #1
	body = binary.BigEndian.AppendUint32(body, 3)   // body_len #1
	body = append(body, []byte("one")...)           // body #1
	body = binary.BigEndian.AppendUint64(body, 2)   // message_id #2
	body = binary.BigEndian.AppendUint64(body, 222) // lease_token #2
	body = binary.BigEndian.AppendUint32(body, 3)   // body_len #2
	body = append(body, []byte("two")...)           // body #2
	m.in <- resp(body)

	// Act
	items, err := c.Reserve(context.Background(), "queue://r/a/q", 30, 2)

	// Assert
	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, "1", items[0].ID)
	assert.Equal(t, []byte("one"), items[0].Body)
	assert.Equal(t, uint64(111), items[0].Token)
	assert.Equal(t, "2", items[1].ID)
}

func TestShouldSucceedGivenRespWhenExtendCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- resp([]byte{0})

	// Act
	err := c.Extend(context.Background(), "queue://r/a/q", "1", 111, 60)

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnInvalidTokenGivenErrorWhenExtendCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("invalid token")

	// Act
	err := c.Extend(context.Background(), "queue://r/a/q", "1", 111, 60)

	// Assert
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestShouldSucceedGivenRespWhenCompleteCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- resp([]byte{0})

	// Act
	err := c.Complete(context.Background(), "queue://r/a/q", "1", 111)

	// Assert
	require.NoError(t, err)
}

func TestShouldReturnMessageNotFoundGivenErrorWhenCompleteCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- errFrame("message not found")

	// Act
	err := c.Complete(context.Background(), "queue://r/a/q", "1", 111)

	// Assert
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestShouldReturnSendErrorGivenMuxFailureWhenEnqueueCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	m.sendErr = errors.New("no connection")
	c := NewClient(m)

	// Act
	_, err := c.Enqueue(context.Background(), "queue://r/a/q", []byte("x"))

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send:")
}

func TestShouldReturnContextErrorGivenCancelledContextWhenReserveCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	_, err := c.Reserve(ctx, "queue://r/a/q", 30, 1)

	// Assert
	require.Error(t, err)
}
