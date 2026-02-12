// Package stream implements the Fitz Stream domain client.
// Per CLIENT_SPEC.md: Append-only log with transactional semantics.
package stream

import (
	"context"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// Record represents a single stream record.
type Record struct {
	Offset uint64
	Body   []byte
}

// Metadata holds stream metadata.
type Metadata struct {
	FirstOffset uint64
	LastOffset  uint64
	RecordCount uint64
}

// StreamSession is a write session for appending to a stream.
// Obtained from Begin; use Append, then Commit or Rollback.
// Expected offset (OCC) is established at Begin and tracked by the session/server;
// Append does not take or send expected_offset.
// Per CLIENT_SPEC.md, operations on a session MUST be sequential.
type StreamSession interface {
	// Append adds a record to the stream. Returns the assigned offset when available.
	Append(ctx context.Context, body []byte) (offset uint64, err error)
	// Commit finalizes the write session and makes appends durable.
	Commit(ctx context.Context) error
	// Rollback discards uncommitted appends.
	Rollback(ctx context.Context) error
}

// Client is the Stream domain client interface.
type Client interface {
	// Begin starts a write session on the given route.
	// expectedOffset is the client's view of the stream's next offset; server rejects on mismatch (OCC).
	// Returns a session on which to call Append, then Commit or Rollback.
	Begin(ctx context.Context, route string, expectedOffset uint64) (StreamSession, error)

	// ReadResource reads records from the given route starting at fromOffset.
	ReadResource(ctx context.Context, route string, fromOffset uint64, limit uint64) (iter.Iterator[Record], error)

	// Last returns the most recent record in the stream.
	Last(ctx context.Context, route string) (*Record, error)

	// GetMetadata returns stream metadata.
	GetMetadata(ctx context.Context, route string) (*Metadata, error)
}

type client struct {
	conn *connection.Connection
}

// session is the concrete implementation of StreamSession.
type session struct {
	route     string
	sessionID uint64
	conn      *connection.Connection
}

// NewClient creates a new Stream domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn}
}

// Begin per server stream_codec.rs:
// Request: [string route][u64 expected_offset][optional bytes ingest_metadata]
// Response: [status][u8 has_session_id][u64 session_id if has=1][bytes data]
// Expected offset (OCC) is sent only here; the session tracks it internally on the server.
func (c *client) Begin(ctx context.Context, route string, expectedOffset uint64) (StreamSession, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteU64BE(buf, expectedOffset)
	connection.WriteU8(buf, 0) // no ingest metadata (optional bytes flag=0)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamBegin, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("BEGIN request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("BEGIN failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("BEGIN failed: unexpected status")
	}

	if len(remaining) < 1 {
		return nil, fmt.Errorf("BEGIN response too short")
	}
	hasSessionID := remaining[0]
	if hasSessionID != 1 || len(remaining) < 9 {
		return nil, fmt.Errorf("BEGIN response missing session_id")
	}

	sessionID, _, err := connection.ReadU64BE(remaining, 1)
	if err != nil {
		return nil, fmt.Errorf("parse session_id: %w", err)
	}

	return &session{route: route, sessionID: sessionID, conn: c.conn}, nil
}

// Append per server stream_codec.rs. Expected offset is tracked by the session (established at Begin).
// Request: [u64 session_id][bytes body][optional bytes metadata]
func (s *session) Append(ctx context.Context, body []byte) (uint64, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, s.sessionID)
	connection.WriteBytes(buf, body)
	connection.WriteU8(buf, 0) // no metadata (optional bytes flag=0)

	resp, err := s.conn.SendRequest(ctx, protocol.MessageTypeStreamAppend, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("APPEND request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("APPEND failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return 0, fmt.Errorf("APPEND failed: unexpected status")
	}

	offset := 0
	if offset < len(remaining) {
		hasSessionID := remaining[offset]
		offset++
		if hasSessionID == 1 && offset+8 <= len(remaining) {
			offset += 8
		}
	}
	if offset+4 <= len(remaining) {
		dataLen, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err == nil && dataLen >= 8 && newOffset+int(dataLen) <= len(remaining) {
			assignedOffset, _, _ := connection.ReadU64BE(remaining, newOffset)
			return assignedOffset, nil
		}
	}
	return 0, nil
}

// Commit per server stream_codec.rs:
// Request: [u64 session_id][u8 mode] where mode: 0=Buffered, 1=Sync
func (s *session) Commit(ctx context.Context) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, s.sessionID)
	connection.WriteU8(buf, 0) // mode = Buffered

	resp, err := s.conn.SendRequest(ctx, protocol.MessageTypeStreamCommit, buf.Bytes())
	if err != nil {
		return fmt.Errorf("COMMIT request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("COMMIT failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return fmt.Errorf("COMMIT failed: unexpected status")
	}
	return nil
}

// Rollback per server stream_codec.rs:
// Request: [u64 session_id]
func (s *session) Rollback(ctx context.Context) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, s.sessionID)

	resp, err := s.conn.SendRequest(ctx, protocol.MessageTypeStreamRollback, buf.Bytes())
	if err != nil {
		return fmt.Errorf("ROLLBACK request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("ROLLBACK failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return fmt.Errorf("ROLLBACK failed: unexpected status")
	}
	return nil
}

// ReadResource per server stream_codec.rs:
// Request: [string route][u64 from_offset][u64 limit][optional u64 max_bytes]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) ReadResource(ctx context.Context, route string, fromOffset uint64, limit uint64) (iter.Iterator[Record], error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteU64BE(buf, fromOffset)
	connection.WriteU64BE(buf, limit)
	connection.WriteU8(buf, 0) // no max_bytes (optional u64 flag=0)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamRead, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("READ request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("READ failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("READ failed: unexpected status")
	}

	// Skip optional session_id and extract data blob
	data := skipOptionalSessionIDAndGetData(remaining)

	// Parse records from data
	records, err := parseReadResponse(data)
	if err != nil {
		return nil, fmt.Errorf("parse READ response: %w", err)
	}

	return iter.NewSliceIterator(records), nil
}

// Last per server stream_codec.rs:
// Request: [string route]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) Last(ctx context.Context, route string) (*Record, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamLast, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("LAST request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("LAST failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("LAST failed: unexpected status")
	}

	// Skip optional session_id and extract data blob
	data := skipOptionalSessionIDAndGetData(remaining)

	// Empty data means no record (stream empty or server stub)
	if len(data) == 0 {
		return nil, nil
	}

	record, err := parseRecord(data, 0)
	if err != nil {
		return nil, fmt.Errorf("parse LAST response: %w", err)
	}

	return record, nil
}

// GetMetadata per server stream_codec.rs:
// Request: [string route]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) GetMetadata(ctx context.Context, route string) (*Metadata, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamGetMetadata, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("GET_METADATA request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("GET_METADATA failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("GET_METADATA failed: unexpected status")
	}

	// Skip optional session_id and extract data blob
	data := skipOptionalSessionIDAndGetData(remaining)

	meta := &Metadata{}
	offset := 0

	// Parse metadata fields from data
	if offset+8 <= len(data) {
		meta.FirstOffset, offset, _ = connection.ReadU64BE(data, offset)
	}
	if offset+8 <= len(data) {
		meta.LastOffset, offset, _ = connection.ReadU64BE(data, offset)
	}
	if offset+8 <= len(data) {
		meta.RecordCount, _, _ = connection.ReadU64BE(data, offset)
	}

	return meta, nil
}

// skipOptionalSessionIDAndGetData parses the common stream response format:
// [u8 has_session_id][u64 session_id if has=1][u32 data_len][data]
// Returns the data portion (after the data_len prefix).
func skipOptionalSessionIDAndGetData(remaining []byte) []byte {
	offset := 0
	if offset >= len(remaining) {
		return nil
	}

	// Skip optional session_id
	hasSessionID := remaining[offset]
	offset++
	if hasSessionID == 1 && offset+8 <= len(remaining) {
		offset += 8
	}

	// Read data blob: [u32 data_len][data]
	if offset+4 > len(remaining) {
		return nil
	}
	dataLen, newOffset, err := connection.ReadU32BE(remaining, offset)
	if err != nil {
		return nil
	}
	if newOffset+int(dataLen) > len(remaining) {
		return remaining[newOffset:]
	}
	return remaining[newOffset : newOffset+int(dataLen)]
}

// parseReadResponse parses records from a READ response data blob.
func parseReadResponse(data []byte) ([]Record, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var records []Record
	offset := 0

	// Parse record count if available
	if len(data) >= 4 {
		count, newOffset, err := connection.ReadU32BE(data, offset)
		if err == nil {
			offset = newOffset
			for i := uint32(0); i < count && offset < len(data); i++ {
				rec, err := parseRecord(data, offset)
				if err != nil {
					break
				}
				records = append(records, *rec)
				// Advance offset past this record
				offset += 8 // offset field
				if offset+4 <= len(data) {
					bodyLen, _, _ := connection.ReadU32BE(data, offset)
					offset += 4 + int(bodyLen)
				}
			}
			return records, nil
		}
	}

	// Fallback: try parsing as a flat sequence of records
	for offset < len(data) {
		rec, err := parseRecord(data, offset)
		if err != nil {
			break
		}
		records = append(records, *rec)
		offset += 8 // offset
		if offset+4 <= len(data) {
			bodyLen, _, _ := connection.ReadU32BE(data, offset)
			offset += 4 + int(bodyLen)
		} else {
			break
		}
	}

	return records, nil
}

// parseRecord parses a single record from the payload at the given offset.
func parseRecord(data []byte, offset int) (*Record, error) {
	rec := &Record{}

	// Read offset (u64)
	var err error
	rec.Offset, offset, err = connection.ReadU64BE(data, offset)
	if err != nil {
		return nil, fmt.Errorf("parse record offset: %w", err)
	}

	// Read body
	bodyData, _, err := connection.ReadBytes(data, offset)
	if err != nil {
		return nil, fmt.Errorf("parse record body: %w", err)
	}
	rec.Body = make([]byte, len(bodyData))
	copy(rec.Body, bodyData)

	return rec, nil
}
