package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

// Frame represents a single top-level Fitz TLV frame transported over the
// connection. Format: type(u8) | flags(u8) | channel(u32 BE) | body
// Body is TLV payload and is left opaque for domain layers to decode.
// See `CLIENT_SPEC.md` for authoritative frame layout and TLV tag meanings.
type Frame struct {
	Type    uint8
	Flags   uint8
	Channel uint32
	Body    []byte
}

// FrameType constants (small set used by transport tests and domain layers).
const (
	FrameTypeConnOpen  uint8 = 1
	FrameTypeAck       uint8 = 2
	FrameTypeErr       uint8 = 3
	FrameTypeHeartbeat uint8 = 0xFE
	FrameTypeReq       uint8 = 10
	FrameTypeResp      uint8 = 11
)

// Well-known channel IDs used for multiplexing domain traffic.
const (
	ChannelControl  uint32 = 0
	ChannelPub      uint32 = 1
	ChannelSub      uint32 = 2
	ChannelRPC      uint32 = 3
	ChannelLease    uint32 = 4
	ChannelInternal uint32 = 5
	ChannelKV       uint32 = 6
	ChannelStream   uint32 = 7
	ChannelQueue    uint32 = 8
	ChannelSchedule uint32 = 9
)

// Framer abstracts transport framing so Mux no longer needs to guess transport
// type via interface assertions.
type Framer interface {
	ReadFrame() ([]byte, error)
	WriteFrame([]byte) error
	Close() error
}

const (
	// minimum header size: type(1) + flags(1) + channel(4)
	frameHeaderSize = 6
	// maximum allowed frame payload (type|flags|channel|body)
	MaxFrameSize = 1 << 20 // 1MiB
)

// TCPFramer implements Framer for length-prefixed TCP transport.
// Frame format: [u32 BE length][payload bytes]
// length = byte count of payload (payload excludes the 4-byte prefix)
type TCPFramer struct {
	conn io.ReadWriteCloser
	r    *bufio.Reader
}

func NewTCPFramer(conn io.ReadWriteCloser) Framer {
	return &TCPFramer{conn: conn, r: bufio.NewReader(conn)}
}

func (t *TCPFramer) ReadFrame() ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(t.r, lenBuf[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])
	if l < frameHeaderSize {
		return nil, errors.New("invalid frame length")
	}
	if l > MaxFrameSize {
		return nil, errors.New("frame size exceeds maximum")
	}
	payload := make([]byte, l)
	if _, err := io.ReadFull(t.r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (t *TCPFramer) WriteFrame(payload []byte) error {
	if uint32(len(payload)) > MaxFrameSize {
		return errors.New("frame size exceeds maximum")
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	// Write prefix + payload in single write when possible
	buf := make([]byte, 0, 4+len(payload))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, payload...)
	_, err := t.conn.Write(buf)
	return err
}

func (t *TCPFramer) Close() error { return t.conn.Close() }

// WebSocketFramer implements Framer for WebSocket-like connections.
// It requires a reader that exposes ReadMessage() ([]byte, error) and a writer
// that supports Write([]byte) (int, error). The provided fakeWS in tests
// satisfies this adapter.
type wsConn interface {
	ReadMessage() ([]byte, error)
	Write([]byte) (int, error)
	Close() error
}

type WebSocketFramer struct{ ws wsConn }

func NewWebSocketFramer(ws wsConn) Framer { return &WebSocketFramer{ws: ws} }

func (w *WebSocketFramer) ReadFrame() ([]byte, error) {
	data, err := w.ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	if len(data) < frameHeaderSize {
		return nil, errors.New("invalid frame length")
	}
	if len(data) > MaxFrameSize {
		return nil, errors.New("frame size exceeds maximum")
	}
	return data, nil
}

func (w *WebSocketFramer) WriteFrame(payload []byte) error {
	if len(payload) > MaxFrameSize {
		return errors.New("frame size exceeds maximum")
	}
	// For WebSocket transport, CONNECT frames should wrap token bytes in TagToken TLV
	// to preserve historical behavior. Payload layout: type(1) | flags(1) | channel(4) | body
	if len(payload) >= frameHeaderSize && payload[0] == FrameTypeConnOpen {
		header := append([]byte(nil), payload[:frameHeaderSize]...)
		body := payload[frameHeaderSize:]
		e := NewTLVEncoder()
		e.AddBytes(TagToken, body)
		payload = append(header, e.Encode()...)
	}
	_, err := w.ws.Write(payload)
	return err
}

func (w *WebSocketFramer) Close() error { return w.ws.Close() }

// Mux multiplexes/demultiplexes Frame values over a single Framer.
// It exposes an incoming frames channel and a Send method for outbound frames.
// Mux runs two goroutines (read/write) and handles clean shutdowns via context cancellation.
type Mux struct {
	fr        Framer
	inCh      chan Frame
	outCh     chan Frame
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	startOnce sync.Once
	writeMu   sync.Mutex // serialises writes
	// Reconnect callbacks are registered by domain clients. The mux no longer
	// attempts automatic reconnect orchestration; higher layers should call
	// FireReconnect when a new live connection is available.
	reconnectMu  sync.Mutex
	reconnectCbs []func()
}

// OnReconnect registers a callback to be invoked when a higher-level
// connection manager signals that the transport is writable again.
func (m *Mux) OnReconnect(cb func()) {
	m.reconnectMu.Lock()
	m.reconnectCbs = append(m.reconnectCbs, cb)
	m.reconnectMu.Unlock()
}

// FireReconnect invokes registered reconnect callbacks asynchronously.
// Call this after a new live connection becomes writable (e.g., after Connect).
func (m *Mux) FireReconnect() {
	m.reconnectMu.Lock()
	cbs := append([]func(){}, m.reconnectCbs...)
	m.reconnectMu.Unlock()
	for _, cb := range cbs {
		go cb()
	}
}

// NewMux constructs a Mux that uses the provided Framer. Call Start() to begin processing.
func NewMux(fr Framer) *Mux {
	ctx, cancel := context.WithCancel(context.Background())
	return &Mux{
		fr:     fr,
		inCh:   make(chan Frame, 128),
		outCh:  make(chan Frame, 128),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start launches the read and write loops. Safe to call multiple times (idempotent).
func (m *Mux) Start() {
	m.startOnce.Do(func() {
		go m.readLoop()
		go m.writeLoop()
	})
}

// In returns a read-only channel of inbound frames.
func (m *Mux) In() <-chan Frame { return m.inCh }

// Send enqueues a frame for sending. It returns an error if the mux is closed.
// Safe for concurrent callers.
func (m *Mux) Send(f Frame) error {
	select {
	case <-m.ctx.Done():
		return errors.New("transport mux closed")
	default:
	}
	select {
	case m.outCh <- f:
		return nil
	case <-m.ctx.Done():
		return errors.New("transport mux closed")
	}
}

// Close stops the loops and closes the underlying framer. Safe to call multiple times.
// Does NOT close inCh/outCh; readLoop is responsible for closing inCh on exit.
func (m *Mux) Close() error {
	var err error
	m.closeOnce.Do(func() {
		m.cancel()
		err = m.fr.Close()
	})
	return err
}

// Ctx returns the mux's context.
func (m *Mux) Ctx() context.Context { return m.ctx }

// StartHeartbeat sends periodic heartbeat frames until the mux context is done.
func (m *Mux) StartHeartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if err := m.Send(Frame{Type: FrameTypeHeartbeat}); err != nil {
				return
			}
		}
	}
}

// readLoop owns closing the inbound channel on exit.
func (m *Mux) readLoop() {
	defer close(m.inCh)
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}
		payload, err := m.fr.ReadFrame()
		if err != nil {
			// stop the mux and exit
			m.cancel()
			return
		}
		if len(payload) < frameHeaderSize {
			m.cancel()
			return
		}
		f := Frame{
			Type:    payload[0],
			Flags:   payload[1],
			Channel: binary.BigEndian.Uint32(payload[2:6]),
			Body:    append([]byte(nil), payload[6:]...),
		}
		select {
		case m.inCh <- f:
		case <-m.ctx.Done():
			return
		}
	}
}

// writeLoop writes outgoing frames until context is cancelled or write fails.
func (m *Mux) writeLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case f := <-m.outCh:
			m.writeMu.Lock()
			// Build payload: header + body. Let the framer handle transport-specific
			// behavior (e.g., CONNECT wrapping for WebSocket).
			var payload []byte
			var chBuf [4]byte
			binary.BigEndian.PutUint32(chBuf[:], f.Channel)
			header := []byte{f.Type, f.Flags, chBuf[0], chBuf[1], chBuf[2], chBuf[3]}
			payload = append(header, f.Body...)
			err := m.fr.WriteFrame(payload)
			m.writeMu.Unlock()
			if err != nil {
				m.cancel()
				return
			}
		}
	}
}
