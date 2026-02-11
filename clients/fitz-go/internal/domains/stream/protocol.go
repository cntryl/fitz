package stream

import (
	"errors"
	"strings"
)

// Wire opcodes for Stream domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	StreamBegin       uint16 = 600
	StreamAppend      uint16 = 601
	StreamCommit      uint16 = 602
	StreamRollback    uint16 = 603
	StreamRead        uint16 = 604
	StreamLast        uint16 = 605
	StreamGetMetadata uint16 = 606
)

// Domain-specific errors.
var (
	ErrStreamNotFound  = errors.New("stream not found")
	ErrStreamConflict  = errors.New("stream conflict")
	ErrStreamReadError = errors.New("stream read error")
)

// mapStreamError maps a broker error message to a domain-specific Go error.
func mapStreamError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "not found"):
		return ErrStreamNotFound
	case strings.Contains(l, "conflict"):
		return ErrStreamConflict
	default:
		return errors.New(msg)
	}
}
