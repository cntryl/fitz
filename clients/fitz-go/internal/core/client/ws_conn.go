package client

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"sync"
	"time"
)

// doClientHandshake performs a minimal RFC6455 handshake on the provided
// net.Conn. It writes an upgrade request and validates the server response.
// Returns the buffered reader which may contain bytes read-ahead during
// the handshake; callers MUST use this reader for subsequent reads.
func doClientHandshake(conn net.Conn, host, path string) (*bufio.Reader, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("failed to generate websocket key: %w", err)
	}
	secKey := base64.StdEncoding.EncodeToString(key)

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, host, secKey)

	if _, err := conn.Write([]byte(req)); err != nil {
		return nil, fmt.Errorf("websocket handshake write failed: %w", err)
	}

	// Read response headers
	r := bufio.NewReader(conn)
	tp := textproto.NewReader(r)
	status, err := tp.ReadLine()
	if err != nil {
		return nil, fmt.Errorf("handshake read failed: %w", err)
	}
	if !strings.Contains(status, "101") {
		return nil, fmt.Errorf("unexpected handshake status: %s", status)
	}

	hdrs, err := tp.ReadMIMEHeader()
	if err != nil {
		return nil, fmt.Errorf("handshake header read failed: %w", err)
	}

	accept := hdrs.Get("Sec-WebSocket-Accept")
	if accept == "" {
		return nil, fmt.Errorf("missing Sec-WebSocket-Accept header")
	}

	// validate accept
	h := sha1.New()
	h.Write([]byte(secKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	exp := base64.StdEncoding.EncodeToString(h.Sum(nil))
	if accept != exp {
		return nil, fmt.Errorf("invalid Sec-WebSocket-Accept: got %s, want %s", accept, exp)
	}

	return r, nil
}

// maxWSFrameSize is the maximum allowed WebSocket frame payload (4 MB).
const maxWSFrameSize = 4 << 20

// wsConnAdapter adapts a raw net.Conn to behave like a WebSocket binary
// transport with simple frame encoding/decoding.
type wsConnAdapter struct {
	conn    net.Conn
	reader  io.Reader // buffered reader from handshake; preserves read-ahead bytes
	readBuf []byte
	readMu  sync.Mutex
	writeMu sync.Mutex
}

// Read implements net.Conn.Read by returning buffered frame payload data.
func (w *wsConnAdapter) Read(p []byte) (int, error) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	if len(w.readBuf) > 0 {
		n := copy(p, w.readBuf)
		w.readBuf = w.readBuf[n:]
		return n, nil
	}

	for {
		opcode, payload, err := readFrame(w.reader)
		if err != nil {
			return 0, err
		}
		// Handle control frames
		switch opcode {
		case 9: // Ping -> respond with Pong (must be masked per RFC 6455 §5.1)
			w.writeMu.Lock()
			_ = writeFrame(w.conn, 10, payload, true)
			w.writeMu.Unlock()
			continue
		case 10: // Pong -> ignore
			continue
		case 8: // Close
			return 0, io.EOF
		}

		if opcode != 2 && opcode != 1 { // prefer binary (2), accept text (1)
			// unexpected opcode
			return 0, fmt.Errorf("unexpected opcode: %d", opcode)
		}

		n := copy(p, payload)
		if n < len(payload) {
			w.readBuf = payload[n:]
		}
		return n, nil
	}
}

// ReadMessage reads a single WebSocket binary message and returns its payload.
func (w *wsConnAdapter) ReadMessage() ([]byte, error) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	for {
		opcode, payload, err := readFrame(w.reader)
		if err != nil {
			return nil, err
		}
		switch opcode {
		case 2, 1:
			return payload, nil
		case 9: // Ping (must be masked per RFC 6455 §5.1)
			w.writeMu.Lock()
			_ = writeFrame(w.conn, 10, payload, true)
			w.writeMu.Unlock()
			continue
		case 10: // Pong
			continue
		case 8: // Close
			return nil, io.EOF
		default:
			continue
		}
	}
}

// Write sends a single binary WebSocket frame containing p.
func (w *wsConnAdapter) Write(p []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if err := writeFrame(w.conn, 2, p, true); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close attempts a graceful close (sends close frame with 1000 status) then closes underlying conn.
func (w *wsConnAdapter) Close() error {
	w.writeMu.Lock()
	// RFC 6455 §5.5.1: close frame should include a 2-byte status code.
	closePayload := []byte{0x03, 0xE8}            // 1000 = normal closure
	_ = writeFrame(w.conn, 8, closePayload, true) // best-effort, masked
	w.writeMu.Unlock()
	return w.conn.Close()
}

func (w *wsConnAdapter) LocalAddr() net.Addr  { return w.conn.LocalAddr() }
func (w *wsConnAdapter) RemoteAddr() net.Addr { return w.conn.RemoteAddr() }
func (w *wsConnAdapter) SetDeadline(t time.Time) error {
	if err := w.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return w.conn.SetWriteDeadline(t)
}
func (w *wsConnAdapter) SetReadDeadline(t time.Time) error  { return w.conn.SetReadDeadline(t) }
func (w *wsConnAdapter) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }

// Helper functions for framing
func writeFrame(w io.Writer, opcode byte, payload []byte, mask bool) error {
	// first byte: FIN=1, opcode
	hdr := []byte{0x80 | (opcode & 0x0f)}

	payloadLen := len(payload)
	if payloadLen <= 125 {
		b := byte(payloadLen)
		if mask {
			b |= 0x80
		}
		hdr = append(hdr, b)
	} else if payloadLen <= 0xffff {
		b := byte(126)
		if mask {
			b |= 0x80
		}
		hdr = append(hdr, b)
		ext := []byte{byte(payloadLen >> 8), byte(payloadLen)}
		hdr = append(hdr, ext...)
	} else {
		b := byte(127)
		if mask {
			b |= 0x80
		}
		hdr = append(hdr, b)
		ex := make([]byte, 8)
		sz := uint64(payloadLen)
		for i := 7; i >= 0; i-- {
			ex[i] = byte(sz & 0xff)
			sz >>= 8
		}
		hdr = append(hdr, ex...)
	}

	if _, err := w.Write(hdr); err != nil {
		return err
	}

	if mask {
		maskKey := make([]byte, 4)
		if _, err := rand.Read(maskKey); err != nil {
			return err
		}
		if _, err := w.Write(maskKey); err != nil {
			return err
		}
		// apply mask
		masked := make([]byte, len(payload))
		for i := range payload {
			masked[i] = payload[i] ^ maskKey[i%4]
		}
		_, err := w.Write(masked)
		return err
	}

	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) (byte, []byte, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:2]); err != nil {
		return 0, nil, err
	}
	fin := b[0] & 0x80
	opcode := b[0] & 0x0f
	mask := b[1]&0x80 != 0
	len7 := int(b[1] & 0x7f)
	var length uint64
	if len7 <= 125 {
		length = uint64(len7)
	} else if len7 == 126 {
		var ex [2]byte
		if _, err := io.ReadFull(r, ex[:2]); err != nil {
			return 0, nil, err
		}
		length = uint64(ex[0])<<8 | uint64(ex[1])
	} else if len7 == 127 {
		var ex [8]byte
		if _, err := io.ReadFull(r, ex[:8]); err != nil {
			return 0, nil, err
		}
		for i := 0; i < 8; i++ {
			length = (length << 8) | uint64(ex[i])
		}
	}

	var maskKey [4]byte
	if mask {
		if _, err := io.ReadFull(r, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}

	if length > maxWSFrameSize {
		return 0, nil, fmt.Errorf("websocket frame too large: %d bytes (max %d)", length, maxWSFrameSize)
	}

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}

	if mask {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}

	_ = fin // currently unused
	return opcode, payload, nil
}
