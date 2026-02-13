package schedule

import (
	"bytes"
	"encoding/binary"
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

// EncodeScheduleCreate encodes a SCHEDULE CREATE request per CLIENT_SPEC.md.
// Wire format: [bytes payload] where payload is a nested TLV blob:
// [u8 type=1][u16 BE cron_len][cron_bytes]
// [u8 type=2][u16 BE target_resource_len][target_resource_bytes]
// [u8 type=3][u16 BE target_operation_len][target_operation_bytes]
func EncodeScheduleCreate(route string, cronExpr string, payload []byte) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		innerLen := schedulePayloadLen(cronExpr, route, payload)
		encoding.WriteU32(buf, uint32(innerLen))
		writeSchedulePayload(buf, cronExpr, route, payload)
	}), nil
}

// EncodeScheduleCancel encodes a SCHEDULE CANCEL request per CLIENT_SPEC.md.
// Wire format: [string schedule_id]
func EncodeScheduleCancel(scheduleID string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteString(buf, scheduleID)
	}), nil
}

// EncodeScheduleList encodes a SCHEDULE LIST request per CLIENT_SPEC.md.
// Wire format: empty payload
func EncodeScheduleList() ([]byte, error) {
	return nil, nil
}

// EncodeScheduleSubscribe encodes a SCHEDULE SUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route_pattern]
func EncodeScheduleSubscribe(pattern string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteString(buf, pattern)
	}), nil
}

// EncodeScheduleUnsubscribe encodes a SCHEDULE UNSUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route_pattern]
func EncodeScheduleUnsubscribe(pattern string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteString(buf, pattern)
	}), nil
}

// writeSchedulePayload builds the nested TLV blob that the server's
// SchedulePayload::decode expects. Format per record:
//
//	[u8 type][u16 BE value_len][value_bytes]
//
// Types: 1=cron, 2=target_resource, 3=target_operation
func writeSchedulePayload(buf *bytes.Buffer, cron, targetResource string, targetOperation []byte) {
	buf.WriteByte(1)
	writeU16BE(buf, uint16(len(cron)))
	buf.WriteString(cron)

	buf.WriteByte(2)
	writeU16BE(buf, uint16(len(targetResource)))
	buf.WriteString(targetResource)

	buf.WriteByte(3)
	writeU16BE(buf, uint16(len(targetOperation)))
	buf.Write(targetOperation)
}

func schedulePayloadLen(cron, targetResource string, targetOperation []byte) int {
	return (1 + 2 + len(cron)) + (1 + 2 + len(targetResource)) + (1 + 2 + len(targetOperation))
}

func writeU16BE(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}
