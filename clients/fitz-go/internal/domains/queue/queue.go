// Package queue implements the Fitz Queue domain client.
// Per CLIENT_SPEC.md: FIFO message queue with lease-based processing.
package queue

import (
	"context"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/retry"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// QueueItem represents a received (reserved) queue message.
// Extend and Complete are called on the item; route and token are tracked internally.
type QueueItem struct {
	ID    uint64
	Token uint64
	Body  []byte

	route string
	conn  *connection.Connection
}

// Extend extends the lease on this queue item.
func (q *QueueItem) Extend(ctx context.Context, leaseSecs uint64) error {
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueExtend, extendPayloadWriter(q.route, q.ID, q.Token, leaseSecs))
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

// Complete acknowledges processing of this queue item and removes it from the queue.
func (q *QueueItem) Complete(ctx context.Context) error {
	return q.CompleteWithToken(ctx, q.Token)
}

// CompleteWithToken completes the item using an explicit token (e.g. for testing invalid token).
// Normally use Complete(ctx) which uses the item's token.
func (q *QueueItem) CompleteWithToken(ctx context.Context, token uint64) error {
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueComplete, completePayloadWriter(q.route, q.ID, token))
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

// Client is the Queue domain client interface.
type Client interface {
	// Send adds a message to the queue. Returns the server-assigned message ID.
	Send(ctx context.Context, route string, body []byte) (msgID uint64, err error)

	// SendWithRetry adds a message to the queue with exponential backoff retry on backpressure.
	// Retries up to maxRetries times if the queue is full (error code 4005).
	SendWithRetry(ctx context.Context, route string, body []byte, maxRetries int) (msgID uint64, err error)

	// Receive reserves up to batchSize messages with the given lease duration.
	// Each returned QueueItem has Extend and Complete methods.
	Receive(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error)

	// ReceiveWithRetry reserves messages with exponential backoff retry on backpressure.
	ReceiveWithRetry(ctx context.Context, route string, leaseSecs uint64, batchSize uint32, maxRetries int) ([]*QueueItem, error)
}

type client struct {
	conn *connection.Connection
}

// NewClient creates a new Queue domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn}
}

// Send per CLIENT_SPEC.md:
// Request: [route_len][route][body_len][body][has_delay(u8)][delay_secs?]
// Response: [status][message_id (u64 BE)]
func (c *client) Send(ctx context.Context, route string, body []byte) (uint64, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueEnqueue, enqueuePayloadWriter(route, body, 0))
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

// SendWithRetry adds a message to the queue with exponential backoff retry on backpressure.
// If maxRetries is 0, no retries are attempted; returns immediately on error.
func (c *client) SendWithRetry(ctx context.Context, route string, body []byte, maxRetries int) (uint64, error) {
	var msgID uint64

	err := retry.Do(ctx, retry.DefaultBackoff, maxRetries, func() error {
		var err error
		msgID, err = c.Send(ctx, route, body)
		return err
	}, func(err error) bool {
		// Retry on ErrQueueFull only
		return err == ErrQueueFull
	})

	return msgID, err
}

// ReceiveWithRetry reserves messages with exponential backoff retry on backpressure.
func (c *client) ReceiveWithRetry(ctx context.Context, route string, leaseSecs uint64, batchSize uint32, maxRetries int) ([]*QueueItem, error) {
	var items []*QueueItem

	err := retry.Do(ctx, retry.DefaultBackoff, maxRetries, func() error {
		var err error
		items, err = c.Receive(ctx, route, leaseSecs, batchSize)
		return err
	}, func(err error) bool {
		// Retry on ErrQueueFull only
		return err == ErrQueueFull
	})

	return items, err
}

// Receive per CLIENT_SPEC.md:
// Request: [route_len][route][lease_seconds][has_batch_size][batch_size?][has_wait_seconds][wait_seconds?]
// Response: [status][lease_count(u32)][{message_id, lease_token, body_len, body}...]
func (c *client) Receive(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueReserve, reservePayloadWriter(route, leaseSecs, batchSize, 0))
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

	if len(remaining) < 4 {
		return nil, nil
	}

	count, offset, err := connection.ReadU32BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse lease_count: %w", err)
	}

	items := make([]*QueueItem, 0, count)
	for i := uint32(0); i < count; i++ {
		item := &QueueItem{route: route, conn: c.conn}

		item.ID, offset, err = connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse message_id at item %d: %w", i, err)
		}

		item.Token, offset, err = connection.ReadU64BE(remaining, offset)
		if err != nil {
			return nil, fmt.Errorf("parse lease_token at item %d: %w", i, err)
		}

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
