package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testNewClient creates an RPC client for testing.
func testNewClient(mux transport.MuxProvider) Client {
	return NewClient(mux)
}

// mockMux implements the muxProvider interface for tests.
type mockMux struct {
	sendCh chan transport.Frame
	inCh   chan transport.Frame
	ctx    context.Context
	cb     func()
}

func newMockMux() *mockMux {
	return &mockMux{sendCh: make(chan transport.Frame, 16), inCh: make(chan transport.Frame, 16), ctx: context.Background()}
}

func (m *mockMux) Send(f transport.Frame) error {
	m.sendCh <- f
	return nil
}

func (m *mockMux) In() <-chan transport.Frame { return m.inCh }
func (m *mockMux) Ctx() context.Context       { return m.ctx }
func (m *mockMux) OnReconnect(fn func())      { m.cb = fn }

// ---------------------------------------------------------------------------
// Helper: build a streaming RESPONSE frame
// ---------------------------------------------------------------------------

func buildResponseFrame(id, seq uint64, body []byte, streamEnd bool) transport.Frame {
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, id)
	enc.AddUint64(transport.TagSeq, seq)
	if len(body) > 0 {
		enc.AddBytes(transport.TagBody, body)
	}
	if streamEnd {
		enc.AddUint8(transport.TagStreamEnd, 1)
	} else {
		enc.AddUint8(transport.TagStreamEnd, 0)
	}
	return transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelRPC, Body: enc.Encode()}
}

func buildErrorFrame(id uint64, errMsg string) transport.Frame {
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, id)
	enc.AddString(transport.TagErr, errMsg)
	return transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelRPC, Body: enc.Encode()}
}

// ---------------------------------------------------------------------------
// Call tests
// ---------------------------------------------------------------------------

func TestShouldReturnSingleResponseGivenCallWhenServerReplies(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	route := "rpc://realm/area/resource"
	reqBody := []byte("hello")
	respBody := []byte("world")

	// Act: simulate broker replying with a single-frame streaming response
	errCh := make(chan error, 1)
	go func() {
		f := <-m.sendCh
		if f.Type != transport.FrameTypeReq {
			errCh <- fmt.Errorf("expected FrameTypeReq, got %d", f.Type)
			return
		}
		dec, err := transport.NewTLVDecoder(f.Body)
		if err != nil {
			errCh <- err
			return
		}
		id, err := dec.GetUint64(transport.TagID)
		if err != nil {
			errCh <- err
			return
		}
		// Send single response with stream_end=1
		m.inCh <- buildResponseFrame(id, 0, respBody, true)
		errCh <- nil
	}()

	it, err := c.Call(context.Background(), route, reqBody, time.Second)

	// Assert
	require.NoError(t, err)
	require.True(t, it.Next())
	assert.Equal(t, respBody, it.Value().Body)
	assert.Equal(t, uint64(0), it.Value().Sequence)
	assert.False(t, it.Next())
	assert.NoError(t, it.Err())
	assert.NoError(t, it.Close())
	require.NoError(t, <-errCh)
}

func TestShouldStreamMultipleResponsesGivenCallWhenServerStreams(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	route := "rpc://realm/area/resource"

	errCh := make(chan error, 1)
	go func() {
		f := <-m.sendCh
		dec, err := transport.NewTLVDecoder(f.Body)
		if err != nil {
			errCh <- err
			return
		}
		id, err := dec.GetUint64(transport.TagID)
		if err != nil {
			errCh <- err
			return
		}
		// Stream 3 frames then end
		m.inCh <- buildResponseFrame(id, 0, []byte("a"), false)
		m.inCh <- buildResponseFrame(id, 1, []byte("b"), false)
		m.inCh <- buildResponseFrame(id, 2, []byte("c"), true)
		errCh <- nil
	}()

	// Act
	it, err := c.Call(context.Background(), route, []byte("go"), time.Second)

	// Assert
	require.NoError(t, err)
	var results []string
	for it.Next() {
		results = append(results, string(it.Value().Body))
	}
	assert.NoError(t, it.Err())
	assert.Equal(t, []string{"a", "b", "c"}, results)
	assert.NoError(t, it.Close())
	require.NoError(t, <-errCh)
}

func TestShouldReturnErrorGivenCallWhenServerSendsErr(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	route := "rpc://realm/area/resource"
	serverErr := "boom"

	errCh := make(chan error, 1)
	go func() {
		f := <-m.sendCh
		dec, err := transport.NewTLVDecoder(f.Body)
		if err != nil {
			errCh <- err
			return
		}
		id, err := dec.GetUint64(transport.TagID)
		if err != nil {
			errCh <- err
			return
		}
		m.inCh <- buildErrorFrame(id, serverErr)
		errCh <- nil
	}()

	// Act
	it, err := c.Call(context.Background(), route, []byte("hello"), time.Second)

	// Assert
	require.NoError(t, err)
	assert.False(t, it.Next())
	require.Error(t, it.Err())
	assert.Contains(t, it.Err().Error(), serverErr)
	assert.NoError(t, it.Close())
	require.NoError(t, <-errCh)
}

func TestShouldReturnTimeoutGivenCallWhenNoResponse(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	// Act — don't reply
	it, err := c.Call(context.Background(), "rpc://realm/area/resource", []byte("x"), 50*time.Millisecond)

	// Assert
	require.NoError(t, err)
	assert.False(t, it.Next())
	require.Error(t, it.Err())
	assert.ErrorIs(t, it.Err(), context.DeadlineExceeded)
	assert.NoError(t, it.Close())
}

// ---------------------------------------------------------------------------
// Subscribe (worker) tests — handler now uses ResponseWriter
// ---------------------------------------------------------------------------

func TestShouldDispatchToWorkerGivenSubscribe(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	route := "rpc://realm/area/resource"
	reqBody := []byte("ping")
	respBody := []byte("pong")

	// Register a handler that sends one response via the writer
	sub, err := c.Subscribe(context.Background(), route, func(ctx context.Context, r InboundRequest, w ResponseWriter) error {
		return w.Send(respBody)
	})
	require.NoError(t, err)

	// drain the subscribe frame sent by Subscribe
	<-m.sendCh

	// send a request frame
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, 42)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, reqBody)
	m.inCh <- transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()}

	// Expect two frames: the streamed body (seq=0, stream_end=0) + end frame (stream_end=1)
	var frames []transport.Frame
	deadline := time.After(time.Second)
	for i := 0; i < 2; i++ {
		select {
		case f := <-m.sendCh:
			frames = append(frames, f)
		case <-deadline:
			t.Fatalf("timed out waiting for response frame %d", i)
		}
	}

	// First frame: body
	dec, err := transport.NewTLVDecoder(frames[0].Body)
	require.NoError(t, err)
	id, _ := dec.GetUint64(transport.TagID)
	assert.Equal(t, uint64(42), id)
	assert.Equal(t, respBody, dec.GetBytes(transport.TagBody))
	seq, _ := dec.GetUint64(transport.TagSeq)
	assert.Equal(t, uint64(0), seq)

	// Second frame: stream_end=1
	dec2, err := transport.NewTLVDecoder(frames[1].Body)
	require.NoError(t, err)
	endBytes := dec2.GetBytes(transport.TagStreamEnd)
	require.Len(t, endBytes, 1)
	assert.Equal(t, uint8(1), endBytes[0])

	// Unsubscribe
	sub.Unsubscribe()
}

func TestShouldStreamMultipleFramesGivenSubscribedWorkerWhenHandlerCallsSendMultipleTimes(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	route := "rpc://realm/area/resource"

	// Handler streams 3 chunks
	_, err := c.Subscribe(context.Background(), route, func(ctx context.Context, r InboundRequest, w ResponseWriter) error {
		_ = w.Send([]byte("chunk1"))
		_ = w.Send([]byte("chunk2"))
		_ = w.Send([]byte("chunk3"))
		return nil
	})
	require.NoError(t, err)
	<-m.sendCh // drain subscribe

	// Act
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, 99)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, []byte("go"))
	m.inCh <- transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()}

	// Assert: 3 data frames + 1 end frame = 4 total
	var frames []transport.Frame
	deadline := time.After(time.Second)
	for i := 0; i < 4; i++ {
		select {
		case f := <-m.sendCh:
			frames = append(frames, f)
		case <-deadline:
			t.Fatalf("timed out waiting for frame %d", i)
		}
	}

	// Verify sequence numbers are 0, 1, 2
	for i := 0; i < 3; i++ {
		dec, _ := transport.NewTLVDecoder(frames[i].Body)
		seq, _ := dec.GetUint64(transport.TagSeq)
		assert.Equal(t, uint64(i), seq)
	}

	// Last frame is stream_end=1
	dec, _ := transport.NewTLVDecoder(frames[3].Body)
	endBytes := dec.GetBytes(transport.TagStreamEnd)
	require.Len(t, endBytes, 1)
	assert.Equal(t, uint8(1), endBytes[0])
}

func TestShouldSendErrorFrameGivenSubscribedWorkerWhenHandlerReturnsError(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)

	route := "rpc://realm/area/resource"
	_, err := c.Subscribe(context.Background(), route, func(ctx context.Context, r InboundRequest, w ResponseWriter) error {
		return errors.New("handler failed")
	})
	require.NoError(t, err)
	<-m.sendCh // drain subscribe

	// Act
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, 77)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, []byte("x"))
	m.inCh <- transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()}

	// Assert: error frame
	select {
	case f := <-m.sendCh:
		require.Equal(t, transport.FrameTypeErr, f.Type)
		dec, _ := transport.NewTLVDecoder(f.Body)
		assert.Contains(t, dec.GetString(transport.TagErr), "handler failed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error frame")
	}
}
