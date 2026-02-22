package schedule

import (
	"bytes"
	"errors"
	"strings"

	"github.com/cntryl/fitz-go/internal/core/encoding"
)

// Wire opcodes for Schedule domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	ScheduleCreate      uint16 = 700
	ScheduleCancel      uint16 = 701
	ScheduleList        uint16 = 702
	ScheduleSubscribe   uint16 = 703
	ScheduleUnsubscribe uint16 = 704
	ScheduleNotify      uint16 = 705 // Server -> Client only
)

// Domain-specific errors. Returned when the server rejects a schedule operation.
//   - ErrScheduleNotFound: cancel or list referenced a schedule that does not exist.
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

// Payload writer helpers per CLIENT_SPEC.md (flat concatenation, no nested TLV).

// scheduleCreatePayloadWriter encodes CREATE request: [route_len][route][cron_len][cron][payload_len][payload]
func scheduleCreatePayloadWriter(route string, cronExpr string, payload []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteString(buf, cronExpr)
		encoding.WriteBytes(buf, payload)
	}
}

// scheduleCancelPayloadWriter encodes CANCEL request: [route_len][route] (route-based identity per spec)
func scheduleCancelPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func scheduleListPayloadWriter(offset, limit uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		// Optional pagination parameters (default to 0, 100 on server if not provided)
		encoding.WriteOptionalU64(buf, offset)
		encoding.WriteOptionalU64(buf, limit)
	}
}

func scheduleSubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteString(buf, pattern)
	}
}

func scheduleUnsubscribePayloadWriter(pattern string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteString(buf, pattern)
	}
}
