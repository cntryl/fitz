package queue

import (
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
