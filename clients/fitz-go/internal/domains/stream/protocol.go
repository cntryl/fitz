package stream

import (
	"bytes"
	"errors"
	"strings"

	"github.com/cntryl/fitz-go/internal/core/encoding"
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
	StreamSubscribe   uint16 = 607
	StreamUnsubscribe uint16 = 608
	StreamNotify      uint16 = 609 // Server -> Client only
)

// Domain-specific errors. Returned when the server rejects a stream operation.
//   - ErrStreamNotFound: the stream route does not exist.
//   - ErrStreamConflict: Begin failed due to expected-offset mismatch (OCC).
//   - ErrStreamReadError: read failed (e.g. offset beyond watermark).
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

// EncodeStreamBegin encodes a STREAM BEGIN request per CLIENT_SPEC.md.
// Wire format: [string route][u64 expected_offset][u8 has_ingest_metadata][bytes? ingest_metadata]
// ingestMetadata is optional; pass nil to omit.
func EncodeStreamBegin(route string, expectedOffset uint64, ingestMetadata []byte) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, expectedOffset)
		if ingestMetadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, ingestMetadata)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

// EncodeStreamAppend encodes a STREAM APPEND request per CLIENT_SPEC.md.
// Wire format: [u64 session_id][bytes body][u8 has_metadata][bytes? metadata]
// metadata is optional; pass nil to omit.
func EncodeStreamAppend(sessionID uint64, body []byte, metadata []byte) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		encoding.WriteBytes(buf, body)
		if metadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, metadata)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

// EncodeStreamCommit encodes a STREAM COMMIT request per CLIENT_SPEC.md.
// Wire format: [u64 session_id][u8 mode]
// mode: 0=Buffered, 1=Sync
func EncodeStreamCommit(sessionID uint64, mode uint8) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		buf.WriteByte(mode)
	}), nil
}

// EncodeStreamRollback encodes a STREAM ROLLBACK request per CLIENT_SPEC.md.
// Wire format: [u64 session_id]
func EncodeStreamRollback(sessionID uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
	}), nil
}

// EncodeStreamRead encodes a STREAM READ request per CLIENT_SPEC.md.
// Wire format: [string route][u64 from_offset][u64 limit][u8 has_max_bytes][u64? max_bytes]
// maxBytes is optional; pass nil to omit.
func EncodeStreamRead(route string, fromOffset uint64, limit uint64, maxBytes *uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
		encoding.WriteU64(buf, limit)
		if maxBytes != nil {
			buf.WriteByte(1)
			encoding.WriteU64(buf, *maxBytes)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

// EncodeStreamLast encodes a STREAM LAST request per CLIENT_SPEC.md.
// Wire format: [string route]
func EncodeStreamLast(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// EncodeStreamGetMetadata encodes a STREAM GET_METADATA request per CLIENT_SPEC.md.
// Wire format: [string route]
func EncodeStreamGetMetadata(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// EncodeStreamSubscribe encodes a STREAM SUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route][u64 from_offset]
func EncodeStreamSubscribe(route string, fromOffset uint64) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
	}), nil
}

// EncodeStreamUnsubscribe encodes a STREAM UNSUBSCRIBE request per CLIENT_SPEC.md.
// Wire format: [string route]
func EncodeStreamUnsubscribe(route string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func streamBeginPayloadWriter(route string, expectedOffset uint64, ingestMetadata []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, expectedOffset)
		if ingestMetadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, ingestMetadata)
		} else {
			buf.WriteByte(0)
		}
	}
}

func streamAppendPayloadWriter(sessionID uint64, body []byte, metadata []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		encoding.WriteBytes(buf, body)
		if metadata != nil {
			buf.WriteByte(1)
			encoding.WriteBytes(buf, metadata)
		} else {
			buf.WriteByte(0)
		}
	}
}

func streamCommitPayloadWriter(sessionID uint64, mode uint8) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
		buf.WriteByte(mode)
	}
}

func streamRollbackPayloadWriter(sessionID uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteU64(buf, sessionID)
	}
}

func streamReadPayloadWriter(route string, fromOffset uint64, limit uint64, maxBytes *uint64) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
		encoding.WriteU64(buf, fromOffset)
		encoding.WriteU64(buf, limit)
		if maxBytes != nil {
			buf.WriteByte(1)
			encoding.WriteU64(buf, *maxBytes)
		} else {
			buf.WriteByte(0)
		}
	}
}

func streamLastPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

func streamGetMetadataPayloadWriter(route string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, route)
	}
}

// Subscription payload writers for SUBSCRIBE/UNSUBSCRIBE

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
