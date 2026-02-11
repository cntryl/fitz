package queue

import (
	"encoding/binary"
	"errors"
	"strings"
)

// Wire opcodes for Queue domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	QueueEnqueue  uint16 = 200
	QueueReserve  uint16 = 202
	QueueExtend   uint16 = 203
	QueueComplete uint16 = 204
)

// Domain-specific errors.
var (
	ErrInvalidToken    = errors.New("invalid token")
	ErrLeaseExpiredQ   = errors.New("lease expired")
	ErrMessageNotFound = errors.New("message not found")
	ErrQueueNotFound   = errors.New("queue not found")
	ErrQueueFull       = errors.New("queue full")
)

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
