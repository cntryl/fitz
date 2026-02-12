package schedule

import (
	"encoding/binary"
	"errors"
	"strings"
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

// encodeSchedulePayload builds the nested TLV blob that the server's
// SchedulePayload::decode expects. Format per record:
//
//	[u8 type][u16 BE value_len][value_bytes]
//
// Types: 1=cron, 2=target_resource, 3=target_operation
func encodeSchedulePayload(cron, targetResource, targetOperation string) []byte {
	cronBytes := []byte(cron)
	resBytes := []byte(targetResource)
	opBytes := []byte(targetOperation)

	// Calculate total size: 3 records, each with 1-byte type + 2-byte length + value
	size := (1 + 2 + len(cronBytes)) + (1 + 2 + len(resBytes)) + (1 + 2 + len(opBytes))
	buf := make([]byte, 0, size)

	// Type 1: cron
	buf = append(buf, 1)
	buf = appendU16BE(buf, uint16(len(cronBytes)))
	buf = append(buf, cronBytes...)

	// Type 2: target_resource
	buf = append(buf, 2)
	buf = appendU16BE(buf, uint16(len(resBytes)))
	buf = append(buf, resBytes...)

	// Type 3: target_operation
	buf = append(buf, 3)
	buf = appendU16BE(buf, uint16(len(opBytes)))
	buf = append(buf, opBytes...)

	return buf
}

func appendU16BE(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}
