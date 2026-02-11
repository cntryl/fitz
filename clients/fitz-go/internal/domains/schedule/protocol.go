package schedule

import (
	"errors"
	"strings"
)

// Wire opcodes for Schedule domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	ScheduleCreate uint16 = 700
	ScheduleCancel uint16 = 701
	ScheduleList   uint16 = 702
)

// Domain-specific errors.
var (
	ErrScheduleNotFound = errors.New("schedule not found")
)

// mapScheduleError maps a broker error message to a domain-specific Go error.
func mapScheduleError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "not found"):
		return ErrScheduleNotFound
	default:
		return errors.New(msg)
	}
}
