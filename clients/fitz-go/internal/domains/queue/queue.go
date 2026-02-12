// Package queue implements the Fitz Queue domain client.
// Per CLIENT_SPEC.md: FIFO message queue with lease-based processing.
package queue

import (
	"context"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// ReservationItem represents a reserved queue message.
type ReservationItem struct {
	ID    uint64
	Token uint64
	Body  []byte
}

// Client is the Queue domain client interface.
type Client interface {
	// Enqueue adds a message to the queue. Returns the server-assigned message ID.
	Enqueue(ctx context.Context, route string, body []byte) (msgID uint64, err error)

	// Reserve reserves up to batchSize messages with the given lease duration.
	Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]ReservationItem, error)

	// Extend extends the lease on a reserved message.
	Extend(ctx context.Context, route string, msgID uint64, token uint64, leaseSecs uint64) error

	// Complete acknowledges processing of a reserved message.
	Complete(ctx context.Context, route string, msgID uint64, token uint64) error
}

type client struct {
	conn *connection.Connection
}

// NewClient creates a new Queue domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn}
}

// Enqueue per CLIENT_SPEC.md:
// Request: [route_len][route][body_len][body][has_delay(u8)][delay_secs?]
// Response: [status][message_id (u64 BE)]
func (c *client) Enqueue(ctx context.Context, route string, body []byte) (uint64, error) {
	payload := EncodeEnqueue(route, body, 0)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeQueueEnqueue, payload)
	if err != nil {
		return 0, fmt.Errorf("ENQUEUE request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("ENQUEUE failed: %w", err)
	}
	if !success {
		return 0, fmt.Errorf("ENQUEUE failed: unexpected status")
	}

	if len(remaining) < 8 {
		return 0, fmt.Errorf("ENQUEUE response too short: got %d bytes", len(remaining))
	}

	msgID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return 0, fmt.Errorf("parse message_id: %w", err)
	}

	return msgID, nil
}

// Reserve per CLIENT_SPEC.md:
// Request: [route_len][route][lease_seconds][has_batch_size][batch_size?][has_wait_seconds][wait_seconds?]
// Response: [status][lease_count(u32)][{message_id, lease_token, body_len, body}...]
func (c *client) Reserve(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]ReservationItem, error) {
	payload := EncodeReserve(route, leaseSecs, batchSize, 0)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeQueueReserve, payload)
	if err != nil {
		return nil, fmt.Errorf("RESERVE request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("RESERVE failed: %w", err)
	}
	if !success {
		return nil, fmt.Errorf("RESERVE failed: unexpected status")
	}

	// Parse lease_count
	if len(remaining) < 4 {
		return nil, nil // No items
	}

	count, offset, err := connection.ReadU32BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse lease_count: %w", err)
	}

	items := make([]ReservationItem, 0, count)
	for i := uint32(0); i < count; i++ {
		var item ReservationItem

		// message_id (u64)
		item.ID, offset, err = connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse message_id at item %d: %w", i, err)
		}

		// lease_token (u64)
		item.Token, offset, err = connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse lease_token at item %d: %w", i, err)
		}

		// body_len + body
		var bodyData []byte
		bodyData, offset, err = connection.ReadBytes(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse body at item %d: %w", i, err)
		}
		item.Body = make([]byte, len(bodyData))
		copy(item.Body, bodyData)

		items = append(items, item)
	}

	return items, nil
}

// Extend per CLIENT_SPEC.md:
// Request: [route_len][route][message_id(u64)][lease_token(u64)][lease_seconds(u64)]
// Response: [status]
func (c *client) Extend(ctx context.Context, route string, msgID uint64, token uint64, leaseSecs uint64) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteU64BE(buf, msgID)
	connection.WriteU64BE(buf, token)
	connection.WriteU64BE(buf, leaseSecs)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeQueueExtend, buf.Bytes())
	if err != nil {
		return fmt.Errorf("EXTEND request failed: %w", err)
	}

	success, _, err := parseQueueResponse(resp)
	if err != nil {
		return fmt.Errorf("EXTEND failed: %w", err)
	}
	if !success {
		return fmt.Errorf("EXTEND failed: unexpected status")
	}

	return nil
}

// Complete per CLIENT_SPEC.md:
// Request: [route_len][route][message_id(u64)][lease_token(u64)]
// Response: [status]
func (c *client) Complete(ctx context.Context, route string, msgID uint64, token uint64) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteU64BE(buf, msgID)
	connection.WriteU64BE(buf, token)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeQueueComplete, buf.Bytes())
	if err != nil {
		return fmt.Errorf("COMPLETE request failed: %w", err)
	}

	success, _, err := parseQueueResponse(resp)
	if err != nil {
		return fmt.Errorf("COMPLETE failed: %w", err)
	}
	if !success {
		return fmt.Errorf("COMPLETE failed: unexpected status")
	}

	return nil
}
