package queue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/cntryl/fitz-go/internal/core/encoding"
)

// Wire opcodes for Queue domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	QueueEnqueue  uint16 = 200
	QueueReserve  uint16 = 202
	QueueExtend   uint16 = 203
	QueueComplete uint16 = 204
)

// Domain-specific errors. Returned when the server rejects a queue operation.
//   - ErrInvalidToken: complete or extend used an invalid or wrong lease token.
//   - ErrLeaseExpiredQ: lease on a reserved message has expired (Q suffix to avoid clash with lease package).
//   - ErrMessageNotFound: the message id or token is unknown.
//   - ErrQueueNotFound: the queue route does not exist.
//   - ErrQueueFull: backpressure.
var (
	ErrInvalidToken    = errors.New("invalid token")
	ErrLeaseExpiredQ   = errors.New("lease expired")
	ErrMessageNotFound = errors.New("message not found")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrQueueFull       = errors.New("queue full")
)

// Queue error codes sent by the server as [u8 status=1][u8 error_code].
const (
	queueErrCodeInvalidToken  = 1
	queueErrCodeLeaseExpired  = 2
	queueErrCodeNotFound      = 3
	queueErrCodeQueueNotFound = 4
)

// parseQueueResponse handles the Queue domain's non-standard error format.
// Queue errors can be either:
//   - [u8 1][u8 error_code]        (2 bytes, for typed errors)
//   - [u8 1][u32 len][error_msg]   (5+ bytes, for BadRequest/Error)
//
// This replaces ParseStandardResponse for queue operations.
func parseQueueResponse(payload []byte) (bool, []byte, error) {
	if len(payload) < 1 {
		return false, nil, errors.New("response too short")
	}

	status := payload[0]
	if status == 0 {
		return true, payload[1:], nil
	}

	// Error response — check if it's a 1-byte error code or a string error
	if len(payload) == 2 {
		// [u8 1][u8 error_code]
		code := payload[1]
		switch code {
		case queueErrCodeInvalidToken:
			return false, nil, ErrInvalidToken
		case queueErrCodeLeaseExpired:
			return false, nil, ErrLeaseExpiredQ
		case queueErrCodeNotFound:
			return false, nil, ErrMessageNotFound
		case queueErrCodeQueueNotFound:
			return false, nil, ErrQueueNotFound
		default:
			return false, nil, fmt.Errorf("unknown queue error code: %d", code)
		}
	}

	// [u8 1][u32 len][error_msg] — standard string error; map to sentinels for consistent handling
	if len(payload) >= 5 {
		msgLen := binary.BigEndian.Uint32(payload[1:5])
		if int(5+msgLen) <= len(payload) {
			return false, nil, mapQueueError(string(payload[5 : 5+msgLen]))
		}
	}

	return false, nil, fmt.Errorf("malformed queue error response (%d bytes)", len(payload))
}

// mapQueueError maps a broker error message to a domain-specific Go error.
func mapQueueError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "token"):
		return ErrInvalidToken
	case strings.Contains(l, "expired"):
		return ErrLeaseExpiredQ
	case strings.Contains(l, "not found"):
		if strings.Contains(l, "message") {
			return ErrMessageNotFound
		}
		return ErrQueueNotFound
	case strings.Contains(l, "full"):
		return ErrQueueFull
	default:
		return errors.New(msg)
	}
}

// EncodeEnqueue encodes a Queue ENQUEUE request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][body_len][body][has_delay][delay_seconds if has_delay]
func EncodeEnqueue(route string, body []byte, delaySeconds uint64) []byte {
	routeBytes := []byte(route)
	hasDelay := uint8(0)
	if delaySeconds > 0 {
		hasDelay = 1
	}

	payloadSize := 4 + len(routeBytes) + 4 + len(body) + 1
	if hasDelay == 1 {
		payloadSize += 8
	}
	payload := make([]byte, 0, payloadSize)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, uint32(len(routeBytes)))
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u32 BE] body_len
	bodyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLenBytes, uint32(len(body)))
	payload = append(payload, bodyLenBytes...)

	// [bytes] body
	payload = append(payload, body...)

	// [u8] has_delay
	payload = append(payload, hasDelay)

	// [u64 BE] delay_seconds (if has_delay)
	if hasDelay == 1 {
		delayBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(delayBytes, delaySeconds)
		payload = append(payload, delayBytes...)
	}

	return payload
}

// EncodeReserve encodes a Queue RESERVE request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][lease_seconds][has_batch_size][batch_size if present][has_wait_seconds][wait_seconds if present]
func EncodeReserve(route string, leaseSeconds uint64, batchSize uint32, waitSeconds uint64) []byte {
	routeBytes := []byte(route)
	hasBatchSize := uint8(0)
	if batchSize > 0 {
		hasBatchSize = 1
	}
	hasWaitSeconds := uint8(0)
	if waitSeconds > 0 {
		hasWaitSeconds = 1
	}

	payloadSize := 4 + len(routeBytes) + 8 + 1 + 1
	if hasBatchSize == 1 {
		payloadSize += 4
	}
	if hasWaitSeconds == 1 {
		payloadSize += 8
	}
	payload := make([]byte, 0, payloadSize)

	// [u32 BE] route_len
	routeLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(routeLenBytes, uint32(len(routeBytes)))
	payload = append(payload, routeLenBytes...)

	// [bytes] route
	payload = append(payload, routeBytes...)

	// [u64 BE] lease_seconds
	leaseBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(leaseBytes, leaseSeconds)
	payload = append(payload, leaseBytes...)

	// [u8] has_batch_size
	payload = append(payload, hasBatchSize)

	// [u32 BE] batch_size (if has_batch_size)
	if hasBatchSize == 1 {
		batchBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(batchBytes, batchSize)
		payload = append(payload, batchBytes...)
	}

	// [u8] has_wait_seconds
	payload = append(payload, hasWaitSeconds)

	// [u64 BE] wait_seconds (if has_wait_seconds)
	if hasWaitSeconds == 1 {
		waitBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(waitBytes, waitSeconds)
		payload = append(payload, waitBytes...)
	}

	return payload
}

// EncodeExtend encodes a Queue EXTEND request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][message_id][lease_token][lease_seconds]
func EncodeExtend(route string, messageID uint64, leaseToken uint64, leaseSeconds uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
		encoding.WriteU64(buf, leaseSeconds)
	}), nil
}

// EncodeComplete encodes a Queue COMPLETE request payload per CLIENT_SPEC.md.
// Spec: [route_len][route][message_id][lease_token]
func EncodeComplete(route string, messageID uint64, leaseToken uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func enqueuePayloadWriter(route string, body []byte, delaySeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		routeBytes := []byte(route)
		hasDelay := uint8(0)
		if delaySeconds > 0 {
			hasDelay = 1
		}
		// [u32 BE] route_len
		var routeLenBytes [4]byte
		encoding.WriteU32(buf, uint32(len(routeBytes)))
		_ = routeLenBytes // silence unused
		// [bytes] route
		buf.Write(routeBytes)
		// [u32 BE] body_len
		encoding.WriteU32(buf, uint32(len(body)))
		// [bytes] body
		buf.Write(body)
		// [u8] has_delay
		buf.WriteByte(hasDelay)
		// [u64 BE] delay_seconds (if has_delay)
		if hasDelay == 1 {
			encoding.WriteU64(buf, delaySeconds)
		}
	}
}

func reservePayloadWriter(route string, leaseSeconds uint64, batchSize uint32, waitSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		routeBytes := []byte(route)
		hasBatchSize := uint8(0)
		if batchSize > 0 {
			hasBatchSize = 1
		}
		hasWaitSeconds := uint8(0)
		if waitSeconds > 0 {
			hasWaitSeconds = 1
		}
		// [u32 BE] route_len
		encoding.WriteU32(buf, uint32(len(routeBytes)))
		// [bytes] route
		buf.Write(routeBytes)
		// [u64 BE] lease_seconds
		encoding.WriteU64(buf, leaseSeconds)
		// [u8] has_batch_size
		buf.WriteByte(hasBatchSize)
		// [u32 BE] batch_size (if has_batch_size)
		if hasBatchSize == 1 {
			encoding.WriteU32(buf, batchSize)
		}
		// [u8] has_wait_seconds
		buf.WriteByte(hasWaitSeconds)
		// [u64 BE] wait_seconds (if has_wait_seconds)
		if hasWaitSeconds == 1 {
			encoding.WriteU64(buf, waitSeconds)
		}
	}
}

func extendPayloadWriter(route string, messageID uint64, leaseToken uint64, leaseSeconds uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
		encoding.WriteU64(buf, leaseSeconds)
	}
}

func completePayloadWriter(route string, messageID uint64, leaseToken uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, messageID)
		encoding.WriteU64(buf, leaseToken)
	}
}
func subscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
	}
}

func unsubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, pattern)
	}
}
