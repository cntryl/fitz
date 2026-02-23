// Package stream implements the Fitz Stream domain client.
// Per CLIENT_SPEC.md: Append-only log with transactional semantics.
package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/iter"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

// CommitNotification is a notification of stream data availability.
type CommitNotification struct {
	Route string
}

// CommitHandler handles stream commit notifications.
type CommitHandler func(context.Context, CommitNotification) error

// Subscription represents a stream subscription.
type Subscription struct {
	subID   uint64
	pattern string
	client  *client
	handler CommitHandler
}

// Unsubscribe removes the subscription.
func (sub *Subscription) Unsubscribe() {
	if sub.client != nil {
		sub.client.unsubscribe(sub)
	}
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

	// Read reads records from the given route starting at fromOffset.
	Read(ctx context.Context, route string, fromOffset uint64, limit uint64) (iter.Iterator[Record], error)

	// Peek returns the most recent record in the stream.
	Peek(ctx context.Context, route string) (*Record, error)

	// GetMetadata returns stream metadata.
	GetMetadata(ctx context.Context, route string) (*Metadata, error)

	// Subscribe registers a handler for stream commit notifications.
	// Pattern should be a wildcard pattern (e.g., "stream://realm/area/resource/available").
	Subscribe(ctx context.Context, pattern string, handler CommitHandler) (*Subscription, error)
}

type client struct {
	conn          *connection.Connection
	mu            sync.RWMutex
	subscriptions map[uint64]*Subscription
	initialized   bool
}

// session is the concrete implementation of StreamSession.
type session struct {
	route     string
	sessionID uint64
	conn      *connection.Connection
}

// NewClient creates a new Stream domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn:          conn,
		subscriptions: make(map[uint64]*Subscription),
	}
}

// Begin per server stream_codec.rs:
// Request: [string route][u64 expected_offset][optional bytes ingest_metadata]
// Response: [status][u8 has_session_id][u64 session_id if has=1][bytes data]
// Expected offset (OCC) is sent only here; the session tracks it internally on the server.
func (c *client) Begin(ctx context.Context, route string, expectedOffset uint64) (StreamSession, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Begin", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int64("fitz.expected_offset", int64(expectedOffset)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("stream.Begin", "route", route, "expected_offset", expectedOffset)
	}

	// Validate route format
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamBegin, streamBeginPayloadWriter(route, expectedOffset, nil))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Begin failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("BEGIN request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Begin failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("BEGIN failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Begin failed", "route", route, "status", "unexpected")
		}
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
	ctx, span := s.conn.Tracer().Start(ctx, "fitz.stream.Append", trace.WithAttributes(
		attribute.String("fitz.route", s.route),
		attribute.Int64("fitz.session_id", int64(s.sessionID)),
	))
	defer span.End()
	resp, err := s.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamAppend, streamAppendPayloadWriter(s.sessionID, body, nil))
	if err != nil {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Send failed", "route", s.route, "session_id", s.sessionID, "error", err)
		}
		return 0, fmt.Errorf("SEND request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Send failed", "route", s.route, "session_id", s.sessionID, "error", err)
		}
		return 0, fmt.Errorf("SEND failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Send failed", "route", s.route, "session_id", s.sessionID, "status", "unexpected")
		}
		return 0, fmt.Errorf("SEND failed: unexpected status")
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
	ctx, span := s.conn.Tracer().Start(ctx, "fitz.stream.Commit", trace.WithAttributes(
		attribute.String("fitz.route", s.route),
		attribute.Int64("fitz.session_id", int64(s.sessionID)),
	))
	defer span.End()
	resp, err := s.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamCommit, streamCommitPayloadWriter(s.sessionID, 0))
	if err != nil {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Commit failed", "route", s.route, "session_id", s.sessionID, "error", err)
		}
		return fmt.Errorf("COMMIT request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Commit failed", "route", s.route, "session_id", s.sessionID, "error", err)
		}
		return fmt.Errorf("COMMIT failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Commit failed", "route", s.route, "session_id", s.sessionID, "status", "unexpected")
		}
		return fmt.Errorf("COMMIT failed: unexpected status")
	}
	return nil
}

// Rollback per server stream_codec.rs:
// Request: [u64 session_id]
func (s *session) Rollback(ctx context.Context) error {
	ctx, span := s.conn.Tracer().Start(ctx, "fitz.stream.Rollback", trace.WithAttributes(
		attribute.String("fitz.route", s.route),
		attribute.Int64("fitz.session_id", int64(s.sessionID)),
	))
	defer span.End()
	resp, err := s.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamRollback, streamRollbackPayloadWriter(s.sessionID))
	if err != nil {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Rollback failed", "route", s.route, "session_id", s.sessionID, "error", err)
		}
		return fmt.Errorf("ROLLBACK request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Rollback failed", "route", s.route, "session_id", s.sessionID, "error", err)
		}
		return fmt.Errorf("ROLLBACK failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := s.conn.Logger(); log != nil {
			log.Error("stream.Rollback failed", "route", s.route, "session_id", s.sessionID, "status", "unexpected")
		}
		return fmt.Errorf("ROLLBACK failed: unexpected status")
	}
	return nil
}

// Read per server stream_codec.rs:
// Request: [string route][u64 from_offset][u64 limit][optional u64 max_bytes]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) Read(ctx context.Context, route string, fromOffset uint64, limit uint64) (iter.Iterator[Record], error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Read", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int64("fitz.from_offset", int64(fromOffset)),
		attribute.Int64("fitz.limit", int64(limit)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("stream.Read", "route", route, "from_offset", fromOffset, "limit", limit)
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamRead, streamReadPayloadWriter(route, fromOffset, limit, nil))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Read failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("READ request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Peek failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("READ failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Peek failed", "route", route, "status", "unexpected")
		}
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

// Peek per server stream_codec.rs:
// Request: [string route]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) Peek(ctx context.Context, route string) (*Record, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Peek", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("stream.Peek", "route", route)
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamLast, streamLastPayloadWriter(route))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Peek failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("PEEK request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Peek failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("PEEK failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Peek failed", "route", route, "status", "unexpected")
		}
		return nil, fmt.Errorf("PEEK failed: unexpected status")
	}

	// Skip optional session_id and extract data blob
	data := skipOptionalSessionIDAndGetData(remaining)

	// Empty data means no record (stream empty or server stub)
	if len(data) == 0 {
		return nil, nil
	}

	record, err := parseRecord(data, 0)
	if err != nil {
		return nil, fmt.Errorf("parse PEEK response: %w", err)
	}

	return record, nil
}

// GetMetadata per server stream_codec.rs:
// Request: [string route]
// Response: [status][u8 has_session_id][u64?][bytes data]
func (c *client) GetMetadata(ctx context.Context, route string) (*Metadata, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.GetMetadata", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("stream.GetMetadata", "route", route)
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamGetMetadata, streamGetMetadataPayloadWriter(route))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.GetMetadata failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("GET_METADATA request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.GetMetadata failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("GET_METADATA failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.GetMetadata failed", "route", route, "status", "unexpected")
		}
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

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterNotifyHandler(protocol.MessageTypeStreamNotify, c.handleNotify)
}

// handleNotify is called by the mux when a NOTIFY (609) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	c.mu.RUnlock()

	if !ok {
		return // Unknown subscription
	}

	notif := CommitNotification{
		Route: route,
	}

	// Call handler asynchronously to avoid blocking the dispatch loop
	go func() {
		_ = sub.handler(context.Background(), notif)
	}()
}

// Subscribe registers a handler for stream commit notifications.
// Pattern should be a wildcard pattern (e.g., "stream://realm/area/resource/available").
func (c *client) Subscribe(ctx context.Context, pattern string, handler CommitHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.stream.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("stream.Subscribe", "pattern", pattern)
	}
	c.initNotifyHandler()

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamSubscribe, subscribePayloadWriter(pattern))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Subscribe failed", "pattern", pattern, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Subscribe failed", "pattern", pattern, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", mapStreamError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("stream.Subscribe failed", "pattern", pattern, "status", "unexpected")
		}
		return nil, fmt.Errorf("SUBSCRIBE failed: unexpected status")
	}

	// Parse optional subscription_id: [u8 has_sub_id][u64 sub_id if has=1]
	if len(remaining) < 1 {
		return nil, fmt.Errorf("SUBSCRIBE response too short: got %d bytes", len(remaining))
	}
	hasSubID := remaining[0]
	if hasSubID != 1 {
		return nil, fmt.Errorf("SUBSCRIBE response missing subscription_id")
	}
	if len(remaining) < 9 {
		return nil, fmt.Errorf("SUBSCRIBE response too short for subscription_id: got %d bytes", len(remaining))
	}

	subID, _, err := connection.ReadU64BE(remaining, 1)
	if err != nil {
		return nil, fmt.Errorf("parse subscription_id: %w", err)
	}

	sub := &Subscription{
		subID:   subID,
		pattern: pattern,
		client:  c,
		handler: handler,
	}

	c.mu.Lock()
	c.subscriptions[subID] = sub
	c.mu.Unlock()

	return sub, nil
}

// unsubscribe removes a subscription.
func (c *client) unsubscribe(sub *Subscription) {
	c.mu.Lock()
	delete(c.subscriptions, sub.subID)
	c.mu.Unlock()

	// Send UNSUBSCRIBE to server (best-effort, ignore errors).
	ctx := context.Background()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeStreamUnsubscribe, unsubscribePayloadWriter(sub.pattern))
	if err != nil {
		return
	}
	connection.ParseStandardResponse(resp)
}
