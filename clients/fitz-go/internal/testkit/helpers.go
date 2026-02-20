package testkit

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Payload factories and precomputed test data generators

// GenerateRoute creates a route string for testing.
func GenerateRoute(domain string) string {
	return fmt.Sprintf("ftz://1/%s/test/test/resource%d", domain, rand.Int31())
}

// GenerateKey creates a key of specified size for testing.
func GenerateKey(size int) []byte {
	key := make([]byte, size)
	for i := 0; i < size; i++ {
		key[i] = byte((i + 42) % 256)
	}
	return key
}

// GenerateValue creates a value of specified size for testing.
func GenerateValue(size int) []byte {
	value := make([]byte, size)
	for i := 0; i < size; i++ {
		value[i] = byte((i + 84) % 256)
	}
	return value
}

// GenerateFrame creates a complete TLV frame with specified message type and payload size.
func GenerateFrame(msgType uint16, payloadSize int) []byte {
	payload := GenerateValue(payloadSize)
	if msgType <= 254 {
		frame := make([]byte, 1+2+payloadSize)
		frame[0] = byte(msgType)
		binary.BigEndian.PutUint16(frame[1:3], uint16(payloadSize))
		copy(frame[3:], payload)
		return frame
	}
	// Escaped type
	frame := make([]byte, 3+2+payloadSize)
	frame[0] = 0xFF
	binary.BigEndian.PutUint16(frame[1:3], msgType)
	binary.BigEndian.PutUint16(frame[3:5], uint16(payloadSize))
	copy(frame[5:], payload)
	return frame
}

// PrecomputeFrames creates precomputed test frames for benchmarking.
func PrecomputeFrames(count int, avgSize int) [][]byte {
	frames := make([][]byte, count)
	for i := 0; i < count; i++ {
		frames[i] = GenerateFrame(uint16(100+i%10), avgSize)
	}
	return frames
}

// BrokerConnectable checks if a broker is reachable.
func BrokerConnectable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// TCPFrameWrapper wraps a frame with TCP length prefix.
func TCPFrameWrapper(frame []byte) []byte {
	tcpFrame := make([]byte, 4+len(frame))
	binary.BigEndian.PutUint32(tcpFrame[0:4], uint32(len(frame)))
	copy(tcpFrame[4:], frame)
	return tcpFrame
}

// Validation helpers

// AssertFrameValid performs basic validation on a frame.
func AssertFrameValid(t *testing.T, frame []byte) {
	if len(frame) < 3 {
		t.Fatalf("frame too short: %d bytes", len(frame))
	}
	// Decode message type to verify structure
	if frame[0] == 0xFF {
		if len(frame) < 5 {
			t.Fatalf("escaped frame too short: %d bytes", len(frame))
		}
		// Verify length field at offset 3-4
		_ = binary.BigEndian.Uint16(frame[3:5])
	} else {
		// Verify length field at offset 1-2
		_ = binary.BigEndian.Uint16(frame[1:3])
	}
}

// AssertLengthPrefix verifies and extracts TCP length prefix.
func AssertLengthPrefix(t *testing.T, data []byte) uint32 {
	if len(data) < 4 {
		t.Fatalf("insufficient data for TCP length prefix: %d bytes", len(data))
	}
	return binary.BigEndian.Uint32(data[0:4])
}

// AssertPayloadStructure verifies payload format without decoding details.
// Used to check that payload has minimum expected structure.
func AssertPayloadStructure(t *testing.T, payload []byte, minSize int) {
	if len(payload) < minSize {
		t.Errorf("payload too short: %d bytes (expected at least %d)", len(payload), minSize)
	}
}

// AssertRouteValid checks if a route string is valid.
func AssertRouteValid(t *testing.T, route string) {
	if !strings.HasPrefix(route, "ftz://") {
		t.Errorf("route missing ftz:// prefix: %s", route)
	}
	parts := strings.Split(route, "/")
	if len(parts) < 5 {
		t.Errorf("route insufficient components: %s", route)
	}
}

// Extraction helpers

// ExtractRouteFromPayload extracts a route string from a payload.
// Assumes format: [route_len (4)][route][...]
func ExtractRouteFromPayload(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", fmt.Errorf("payload too short for route length")
	}
	routeLen := binary.BigEndian.Uint32(payload[0:4])
	if int(routeLen)+4 > len(payload) {
		return "", fmt.Errorf("payload too short for route data")
	}
	return string(payload[4 : 4+routeLen]), nil
}

// ExtractKeyValueFromPayload extracts key and value from a KV payload.
// Assumes format: [...][key_len (4)][key][value_len (4)][value][...]
// startOffset is the byte position where key_len begins.
func ExtractKeyValueFromPayload(payload []byte, startOffset int) (key []byte, value []byte, err error) {
	if len(payload) < startOffset+4 {
		return nil, nil, fmt.Errorf("payload too short for key length")
	}
	keyLen := binary.BigEndian.Uint32(payload[startOffset : startOffset+4])
	offset := startOffset + 4

	if int(keyLen)+offset > len(payload) {
		return nil, nil, fmt.Errorf("payload too short for key data")
	}
	key = payload[offset : offset+int(keyLen)]
	offset += int(keyLen)

	if len(payload) < offset+4 {
		return nil, nil, fmt.Errorf("payload too short for value length")
	}
	valueLen := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4

	if int(valueLen)+offset > len(payload) {
		return nil, nil, fmt.Errorf("payload too short for value data")
	}
	value = payload[offset : offset+int(valueLen)]

	return key, value, nil
}

// Context helpers

// TimeoutContext creates a context with a specific timeout for testing.
func TimeoutContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// FastContext creates a very short timeout context for testing timeouts.
func FastContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Millisecond)
}

// ContextWithCancel creates a cancellable context for testing.
func ContextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// String and bytes helpers

// GenerateString creates a string of the specified length.
func GenerateString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[i%len(charset)]
	}
	return string(b)
}

// StringToBytes is a helper to convert string to bytes.
func StringToBytes(s string) []byte {
	return []byte(s)
}

// BytesToString is a helper to convert bytes to string.
func BytesToString(b []byte) string {
	return string(b)
}

// Unique identifier generators

// UniqueOperationID generates a unique operation ID for test isolation.
func UniqueOperationID() string {
	return fmt.Sprintf("op_%d_%d", time.Now().UnixNano(), rand.Int63())
}

// UniqueRoute generates a unique route for test isolation.
func UniqueRoute(domain string) string {
	return fmt.Sprintf("ftz://1/%s/realm_%d/area_%s/resource_%d",
		domain,
		time.Now().UnixNano(),
		generateRandomID(6),
		rand.Int31(),
	)
}

// UniqueRealm generates a unique realm name.
func UniqueRealm() string {
	return fmt.Sprintf("realm_%s_%d", generateRandomID(4), time.Now().UnixNano())
}

// UniqueArea generates a unique area name.
func UniqueArea() string {
	return fmt.Sprintf("area_%s_%d", generateRandomID(4), time.Now().UnixNano())
}

// UniqueResource generates a unique resource name.
func UniqueResource() string {
	return fmt.Sprintf("resource_%s_%d", generateRandomID(4), time.Now().UnixNano())
}

func generateRandomID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// AssertDomainErrorCode asserts that err is a domain error with the given code.
// If err is nil or not a *coreerrors.DomainError, the test fails.
// Use this when the client surfaces server error codes so tests are stable against message text changes.
func AssertDomainErrorCode(t testing.TB, err error, code coreerrors.ErrorCode) {
	t.Helper()
	require.Error(t, err, "expected a non-nil error")
	var domainErr *coreerrors.DomainError
	require.True(t, errors.As(err, &domainErr), "expected error to be *errors.DomainError, got: %T", err)
	assert.Equal(t, code, domainErr.Code, "domain error code mismatch: message=%q", domainErr.Message)
}
