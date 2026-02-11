package notice

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/assert"
)

// mockMux is a minimal mux provider that allows pushing inbound frames.
type mockMux struct {
	in  chan transport.Frame
	ctx context.Context
	// capture outbound frames for inspection if needed
	sent []transport.Frame
	cbs  []func()
	// autoAck controls whether Send enqueues a success response
	autoAck bool
}

func newMockMux() *mockMux {
	return &mockMux{in: make(chan transport.Frame, 16), ctx: context.Background(), autoAck: true}
}

func (m *mockMux) Send(f transport.Frame) error {
	m.sent = append(m.sent, f)
	// auto-ack notice requests so Subscribe/Unsubscribe/Publish can proceed
	if m.autoAck && f.Type == transport.FrameTypeReq {
		// Extract opcode from TLV body to check if it's a notice operation
		dec, err := transport.NewTLVDecoder(f.Body)
		if err != nil {
			return err
		}
		opCode, err := dec.GetOp()
		if err != nil {
			return err
		}
		if opCode == NoticeSubscribe || opCode == NoticeUnsubscribe || opCode == NoticeUnsubscribeAll || opCode == NoticePublish {
			// Create success response: TLV with opcode + binary status byte
			// Response format: TLV[TagOp] + status(0=success)
			encResp := transport.NewTLVEncoder()
			encResp.AddOp(opCode)
			// Add status as raw bytes: 0x00 = success
			encResp.AddBytes(0x40, []byte{0})
			m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: f.Channel, Body: encResp.Encode()}
		}
	}
	return nil
}

func (m *mockMux) In() <-chan transport.Frame { return m.in }

func (m *mockMux) Ctx() context.Context { return m.ctx }

func (m *mockMux) OnReconnect(cb func()) { m.cbs = append(m.cbs, cb) }

func (m *mockMux) triggerReconnect() {
	for _, cb := range m.cbs {
		cb()
	}
}

// testNewClient creates a Notice client for testing.
func testNewClient(mux transport.MuxProvider) Client {
	return NewClient(mux)
}

func TestShouldDeliverMessageToHandlerGivenSubscribedWhenNoticeArrives(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)
	recv := make(chan string, 1)
	_, err := c.Subscribe(context.Background(), "notice://realm/area/resource", func(ctx context.Context, msg NoticeMsg) error {
		recv <- string(msg.Body)
		return nil
	})
	assert.NoError(t, err)

	// Act
	body := encodeNotifyBody("notice://realm/area/resource", []byte("hello"))
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticeNotify)
	enc.AddBytes(transport.TagBody, body)
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSub, Body: enc.Encode()}

	// Assert
	select {
	case v := <-recv:
		assert.Equal(t, "hello", v)
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timeout waiting for handler")
	}
}

func TestShouldStopDeliveryGivenUnsubscribedWhenNoticeArrives(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)
	recv := make(chan string, 4)
	s, err := c.Subscribe(context.Background(), "notice://realm/area/resource", func(ctx context.Context, msg NoticeMsg) error {
		recv <- string(msg.Body)
		return nil
	})
	assert.NoError(t, err)

	body := encodeNotifyBody("notice://realm/area/resource", []byte("one"))
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticeNotify)
	enc.AddBytes(transport.TagBody, body)
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSub, Body: enc.Encode()}

	select {
	case v := <-recv:
		assert.Equal(t, "one", v)
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timeout waiting for first handler")
	}

	// Act — unsubscribe and send another message
	s.Unsubscribe()
	body2 := encodeNotifyBody("notice://realm/area/resource", []byte("two"))
	enc2 := transport.NewTLVEncoder()
	enc2.AddOp(NoticeNotify)
	enc2.AddBytes(transport.TagBody, body2)
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSub, Body: enc2.Encode()}

	// Assert — handler must not be invoked
	select {
	case <-recv:
		t.Fatalf("handler called after unsubscribe")
	case <-time.After(150 * time.Millisecond):
		// success: no call
	}
}

func TestShouldWaitForHandlerGivenInFlightWhenUnsubscribeCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)
	started := make(chan struct{})
	dur := 150 * time.Millisecond
	s, err := c.Subscribe(context.Background(), "notice://realm/area/resource", func(ctx context.Context, msg NoticeMsg) error {
		close(started)
		// simulate work
		<-time.After(dur)
		return nil
	})
	assert.NoError(t, err)

	body := encodeNotifyBody("notice://realm/area/resource", []byte("busy"))
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticeNotify)
	enc.AddBytes(transport.TagBody, body)
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSub, Body: enc.Encode()}

	// wait for handler to start
	select {
	case <-started:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timeout waiting for handler to start")
	}

	// Act — unsubscribe and ensure it waits for handler to finish
	start := time.Now()
	s.Unsubscribe()
	elapsed := time.Since(start)

	// Assert
	assert.GreaterOrEqual(t, int(elapsed.Milliseconds()), int(dur.Milliseconds()))
}

func TestShouldResubscribeGivenActiveSubscriptionsWhenReconnectOccurs(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := testNewClient(m)
	_, err := c.Subscribe(context.Background(), "notice://realm/x/resource", func(ctx context.Context, msg NoticeMsg) error { return nil })
	assert.NoError(t, err)
	assert.Greater(t, len(m.sent), 0)

	// Act — clear history and simulate reconnect
	m.sent = nil
	m.triggerReconnect()

	// Assert — reconnect should result in resubscribe frames sent
	time.Sleep(100 * time.Millisecond)
	if len(m.sent) == 0 {
		t.Fatalf("no resubscribe frames observed")
	}
}

func TestShouldNotDeadlockGivenBlockedDelivererWhenUnsubscribeCalled(t *testing.T) {
	m := newMockMux()
	c := testNewClient(m)
	started := make(chan struct{})
	block := make(chan struct{})

	// handler consumes the first message and then blocks
	s, err := c.Subscribe(context.Background(), "notice://realm/area/resource", func(ctx context.Context, msg NoticeMsg) error {
		select {
		case <-started:
			// second receive; should not happen after unsubscribe
		default:
			close(started)
		}
		// block to hold handler for a while
		<-block
		return nil
	})
	assert.NoError(t, err)

	// send first message (fills buffer and starts handler)
	body := encodeNotifyBody("notice://realm/area/resource", []byte("one"))
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticeNotify)
	enc.AddBytes(transport.TagBody, body)
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSub, Body: enc.Encode()}

	// wait for handler to start
	select {
	case <-started:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timeout waiting for handler to start")
	}

	// send second message which will block in deliver (buffer is 1)
	body2 := encodeNotifyBody("notice://realm/area/resource", []byte("two"))
	enc2 := transport.NewTLVEncoder()
	enc2.AddOp(NoticeNotify)
	enc2.AddBytes(transport.TagBody, body2)
	m.in <- transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSub, Body: enc2.Encode()}

	// unsubscribe in goroutine and ensure it doesn't deadlock on blocked deliverer.
	done := make(chan struct{})
	go func() {
		s.Unsubscribe()
		close(done)
	}()

	// At this point Unsubscribe should be waiting for the in-flight handler
	// (which is blocked). It must NOT be blocked by the blocked deliverer itself.
	select {
	case <-done:
		t.Fatalf("unsubscribe returned early before handler finished")
	case <-time.After(150 * time.Millisecond):
		// expected: still waiting for handler
	}

	// allow handler to finish so Unsubscribe can complete
	close(block)

	select {
	case <-done:
		// success
	case <-time.After(800 * time.Millisecond):
		t.Fatalf("unsubscribe didn't complete after handler finished")
	}
}

func TestShouldMatchRoutesGivenWildcardPatternsWhenCompared(t *testing.T) {
	tab := []struct {
		pattern string
		route   string
		match   bool
	}{
		{"notice://realm/*/res", "notice://realm/a/res", true},
		{"notice://realm/*/res", "notice://realm/a/b/res", false},
		{"notice://realm/**", "notice://realm/a/b/res", true},
		{"notice://realm/**/res", "notice://realm/a/b/res", true},
		{"notice://realm/**/res", "notice://realm/res", true},
		{"notice://realm/**/res", "notice://realm/a/res/x", false},
	}
	for _, c := range tab {
		if got := NoticeMatchRoute(c.pattern, c.route); got != c.match {
			t.Fatalf("pattern=%q route=%q expected=%v got=%v", c.pattern, c.route, c.match, got)
		}
	}
}

func encodeNotifyBody(route string, payload []byte) []byte {
	routeBytes := []byte(route)
	buf := make([]byte, 0, 8+4+len(routeBytes)+4+len(payload))
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	buf = append(buf, byte(len(routeBytes)>>24), byte(len(routeBytes)>>16), byte(len(routeBytes)>>8), byte(len(routeBytes)))
	buf = append(buf, routeBytes...)
	buf = append(buf, byte(len(payload)>>24), byte(len(payload)>>16), byte(len(payload)>>8), byte(len(payload)))
	buf = append(buf, payload...)
	return buf
}
