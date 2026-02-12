// Package stream implements the Fitz Stream domain client.
// Per CLIENT_SPEC.md: Append-only log with transactional semantics.
package stream

import (
	"context"
	"fmt"
	"sync"

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

// Client is the Stream domain client interface.
type Client interface {
	// Begin starts a write session on the given route.
	Begin(ctx context.Context, route string) (sessionID uint64, err error)

	// Append adds a record to the stream.
	// expectedOffset is optional; pass nil to skip optimistic concurrency check.
	Append(ctx context.Context, route string, body []byte, expectedOffset *uint64) (offset uint64, err error)

	// Commit finalizes the write session.
	Commit(ctx context.Context, route string) error

	// Rollback discards uncommitted appends.
	Rollback(ctx context.Context, route string) error

	// ReadResource reads records from the given route starting at fromOffset.
	ReadResource(ctx context.Context, route string, fromOffset uint64, limit uint64) (iter.Iterator[Record], error)

	// Last returns the most recent record in the stream.
	Last(ctx context.Context, route string) (*Record, error)

	// GetMetadata returns stream metadata.
	GetMetadata(ctx context.Context, route string) (*Metadata, error)
}

type client struct {
	conn *connection.Connection
	mu   sync.Mutex
	// Track active session per route
	sessions map[string]uint64
}

// NewClient creates a new Stream domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn:     conn,
		sessions: make(map[string]uint64),
	}
}

// Begin per CLIENT_SPEC.md:
// Request: [route_len][route][expected_offset(u64)][has_ingest_metadata(u8)]
// Response: [status][session_id(u64)][data_len][data]
func (c *client) Begin(ctx context.Context, route string) (uint64, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteU64BE(buf, 0) // expected_offset = 0 (any)
	connection.WriteU8(buf, 0)    // no ingest metadata

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamBegin, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("BEGIN request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("BEGIN failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		return 0, fmt.Errorf("BEGIN failed: unexpected status")
	}

	// Parse session_id
	sessionID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return 0, fmt.Errorf("parse session_id: %w", err)
	}

	// Store session for this route
	c.mu.Lock()
	c.sessions[route] = sessionID
	c.mu.Unlock()

	return sessionID, nil
}

// Append per CLIENT_SPEC.md:
// Request: [session_id(u64)][route_len][route][body_len][body][has_metadata(u8)]
// Response: [status][data_len][data]
func (c *client) Append(ctx context.Context, route string, body []byte, expectedOffset *uint64) (uint64, error) {
	c.mu.Lock()
	sessionID, ok := c.sessions[route]
	c.mu.Unlock()

	if !ok {
		return 0, fmt.Errorf("no active session for route %s; call Begin first", route)
	}

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, sessionID)
	connection.WriteString(buf, route)
	connection.WriteBytes(buf, body)
	connection.WriteU8(buf, 0) // no metadata

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamAppend, buf.Bytes())
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

	// Parse offset from data
	// Response data may contain the assigned offset
	if len(remaining) >= 4 {
		dataLen, offset, err := connection.ReadU32BE(remaining, 0)
		if err == nil && dataLen >= 8 && offset+int(dataLen) <= len(remaining) {
			assignedOffset, _, _ := connection.ReadU64BE(remaining, offset)
			return assignedOffset, nil
		}
	}

	// If we can't parse a specific offset, return 0 (server managed)
	return 0, nil
}

// Commit per CLIENT_SPEC.md:
// Request: [session_id(u64)][route_len][route][mode(u8)]
// Response: [status][data_len][data]
func (c *client) Commit(ctx context.Context, route string) error {
	c.mu.Lock()
	sessionID, ok := c.sessions[route]
	c.mu.Unlock()

	if !ok {
		return fmt.Errorf("no active session for route %s", route)
	}

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, sessionID)
	connection.WriteString(buf, route)
	connection.WriteU8(buf, 0) // mode = Buffered

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamCommit, buf.Bytes())
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

	// Clear session
	c.mu.Lock()
	delete(c.sessions, route)
	c.mu.Unlock()

	return nil
}

// Rollback per CLIENT_SPEC.md:
// Request: [session_id(u64)][route_len][route]
func (c *client) Rollback(ctx context.Context, route string) error {
	c.mu.Lock()
	sessionID, ok := c.sessions[route]
	c.mu.Unlock()

	if !ok {
		return fmt.Errorf("no active session for route %s", route)
	}

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, sessionID)
	connection.WriteString(buf, route)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeStreamRollback, buf.Bytes())
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

	c.mu.Lock()
	delete(c.sessions, route)
	c.mu.Unlock()

	return nil
}

// ReadResource per CLIENT_SPEC.md:
// Request: [route_len][route][from_offset(u64)][limit(u64)][has_max_bytes(u8)]
// Response: [status][records...]
func (c *client) ReadResource(ctx context.Context, route string, fromOffset uint64, limit uint64) (iter.Iterator[Record], error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteU64BE(buf, fromOffset)
	connection.WriteU64BE(buf, limit)
	connection.WriteU8(buf, 0) // no max_bytes

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

	// Parse records from response
	records, err := parseReadResponse(remaining)
	if err != nil {
		return nil, fmt.Errorf("parse READ response: %w", err)
	}

	return iter.NewSliceIterator(records), nil
}

// Last per CLIENT_SPEC.md:
// Request: [route_len][route]
// Response: [status][record data]
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

	record, err := parseRecord(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse LAST response: %w", err)
	}

	return record, nil
}

// GetMetadata per CLIENT_SPEC.md:
// Request: [route_len][route]
// Response: [status][metadata]
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

	meta := &Metadata{}
	offset := 0

	// Try to parse data_len + data
	if len(remaining) >= 4 {
		dataLen, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err == nil {
			offset = newOffset
			_ = dataLen
			// Parse metadata fields from data
			if offset+8 <= len(remaining) {
				meta.FirstOffset, offset, _ = connection.ReadU64BE(remaining, offset)
			}
			if offset+8 <= len(remaining) {
				meta.LastOffset, offset, _ = connection.ReadU64BE(remaining, offset)
			}
			if offset+8 <= len(remaining) {
				meta.RecordCount, _, _ = connection.ReadU64BE(remaining, offset)
			}
		}
	}

	return meta, nil
}

// parseReadResponse parses records from a READ response.
func parseReadResponse(remaining []byte) ([]Record, error) {
	if len(remaining) == 0 {
		return nil, nil
	}

	var records []Record
	offset := 0

	// Try to read data_len first (some responses wrap in data_len)
	if len(remaining) >= 4 {
		dataLen, newOffset, err := connection.ReadU32BE(remaining, 0)
		if err == nil && int(dataLen)+newOffset <= len(remaining) {
			// Has data_len wrapper
			offset = newOffset
			remaining = remaining[offset : offset+int(dataLen)]
			offset = 0
		}
	}

	// Parse record count if available
	if len(remaining) >= 4 {
		count, newOffset, err := connection.ReadU32BE(remaining, offset)
		if err == nil {
			offset = newOffset
			for i := uint32(0); i < count && offset < len(remaining); i++ {
				rec, err := parseRecord(remaining, offset)
				if err != nil {
					break
				}
				records = append(records, *rec)
				// Advance offset past this record
				offset += 8 // offset field
				if offset+4 <= len(remaining) {
					bodyLen, _, _ := connection.ReadU32BE(remaining, offset)
					offset += 4 + int(bodyLen)
				}
			}
			return records, nil
		}
	}

	// Fallback: try parsing as a flat sequence of records
	for offset < len(remaining) {
		rec, err := parseRecord(remaining, offset)
		if err != nil {
			break
		}
		records = append(records, *rec)
		offset += 8 // offset
		if offset+4 <= len(remaining) {
			bodyLen, _, _ := connection.ReadU32BE(remaining, offset)
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
