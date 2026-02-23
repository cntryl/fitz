// Package queue implements the Fitz Queue domain client.
// Per CLIENT_SPEC.md: FIFO message queue with lease-based processing.
package queue

import (
	"context"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/core/types"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// QueueItem represents a received (reserved) queue message.
// Extend and Ack are called on the item; route and token are tracked internally.
type QueueItem struct {
	ID    uint64
	Token uint64
	Body  []byte

	route string
	conn  *connection.Connection
}

// AvailabilityNotification represents an availability notification from the queue.
type AvailabilityNotification struct {
	Route string
}

// AvailabilityHandler is called when availability notification arrives.
type AvailabilityHandler func(ctx context.Context, notif AvailabilityNotification) error

// Subscription represents an active queue availability subscription.
// Call Unsubscribe to stop receiving and release the subscription.
type Subscription struct {
	subID   uint64
	pattern string
	client  *client
	handler AvailabilityHandler
}

// Unsubscribe removes this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

// Extend extends the lease on this queue item.
func (q *QueueItem) Extend(ctx context.Context, leaseSecs uint64) error {
	ctx, span := q.conn.Tracer().Start(ctx, "fitz.queue.Extend", trace.WithAttributes(
		attribute.String("fitz.route", q.route),
		attribute.Int64("fitz.message_id", int64(q.ID)),
	))
	defer span.End()
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueExtend, extendPayloadWriter(q.route, q.ID, q.Token, leaseSecs))
	if err != nil {
		if log := q.conn.Logger(); log != nil {
			log.Error("queue.Extend failed", "route", q.route, "id", q.ID, "error", err)
		}
		return fmt.Errorf("EXTEND request failed: %w", err)
	}
	success, _, err := parseQueueResponse(resp)
	if err != nil {
		if log := q.conn.Logger(); log != nil {
			log.Error("queue.Extend failed", "route", q.route, "id", q.ID, "error", err)
		}
		return fmt.Errorf("EXTEND failed: %w", err)
	}
	if !success {
		if log := q.conn.Logger(); log != nil {
			log.Error("queue.Extend failed", "route", q.route, "id", q.ID, "status", "unexpected")
		}
		return fmt.Errorf("EXTEND failed: unexpected status")
	}
	return nil
}

// Ack acknowledges processing of this queue item and removes it from the queue.
func (q *QueueItem) Ack(ctx context.Context) error {
	return q.AckWithToken(ctx, q.Token)
}

// AckWithToken acknowledges the item using an explicit token (e.g. for testing invalid token).
// Normally use Ack(ctx) which uses the item's token.
func (q *QueueItem) AckWithToken(ctx context.Context, token uint64) error {
	ctx, span := q.conn.Tracer().Start(ctx, "fitz.queue.Ack", trace.WithAttributes(
		attribute.String("fitz.route", q.route),
		attribute.Int64("fitz.message_id", int64(q.ID)),
	))
	defer span.End()
	resp, err := q.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueComplete, completePayloadWriter(q.route, q.ID, token))
	if err != nil {
		if log := q.conn.Logger(); log != nil {
			log.Error("queue.Ack failed", "route", q.route, "id", q.ID, "error", err)
		}
		return fmt.Errorf("ACK request failed: %w", err)
	}
	success, _, err := parseQueueResponse(resp)
	if err != nil {
		if log := q.conn.Logger(); log != nil {
			log.Error("queue.Ack failed", "route", q.route, "id", q.ID, "error", err)
		}
		return fmt.Errorf("ACK failed: %w", err)
	}
	if !success {
		if log := q.conn.Logger(); log != nil {
			log.Error("queue.Ack failed", "route", q.route, "id", q.ID, "status", "unexpected")
		}
		return fmt.Errorf("ACK failed: unexpected status")
	}
	return nil
}

// Client is the Queue domain client interface.
type Client interface {
	// Send adds a message to the queue. Returns the server-assigned message ID.
	Send(ctx context.Context, route string, body []byte) (msgID uint64, err error)

	// Receive reserves up to batchSize messages with the given lease duration.
	// Each returned QueueItem has Extend and Ack methods.
	Receive(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error)

	// Subscribe registers a handler for availability notifications (empty -> non-empty transition).
	// Returns a Subscription that can be used to unsubscribe.
	// Per CLIENT_SPEC.md, the pattern parameter is optional; if provided, it will be used to filter notifications.
	Subscribe(ctx context.Context, pattern string, handler AvailabilityHandler) (*Subscription, error)
}

type client struct {
	conn *connection.Connection

	mu            sync.RWMutex
	subscriptions map[uint64]*Subscription // subID -> subscription
	initialized   bool
}

// NewClient creates a new Queue domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{
		conn:          conn,
		subscriptions: make(map[uint64]*Subscription),
	}
}

// Send per CLIENT_SPEC.md:
// Request: [route_len][route][body_len][body][has_delay(u8)][delay_secs?]
// Response: [status][message_id (u64 BE)]
func (c *client) Send(ctx context.Context, route string, body []byte) (uint64, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.queue.Send", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("queue.Send", "route", route)
	}

	// Validate route format
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return 0, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueEnqueue, enqueuePayloadWriter(route, body, 0))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Send failed", "route", route, "error", err)
		}
		return 0, fmt.Errorf("ENQUEUE request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Send failed", "route", route, "error", err)
		}
		return 0, fmt.Errorf("ENQUEUE failed: %w", err)
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Send failed", "route", route, "status", "unexpected")
		}
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

// Receive per CLIENT_SPEC.md:
// Request: [route_len][route][lease_seconds][has_batch_size][batch_size?][has_wait_seconds][wait_seconds?]
// Response: [status][lease_count(u32)][{message_id, lease_token, body_len, body}...]
func (c *client) Receive(ctx context.Context, route string, leaseSecs uint64, batchSize uint32) ([]*QueueItem, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.queue.Receive", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.Int64("fitz.lease_secs", int64(leaseSecs)),
		attribute.Int("fitz.batch_size", int(batchSize)),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("queue.Receive", "route", route, "lease_secs", leaseSecs, "batch_size", batchSize)
	}

	// Validate route format
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return nil, fmt.Errorf("invalid route: %w", err)
	}

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueReserve, reservePayloadWriter(route, leaseSecs, batchSize, 0))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Receive failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("RESERVE request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Receive failed", "route", route, "error", err)
		}
		return nil, fmt.Errorf("RESERVE failed: %w", err)
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Receive failed", "route", route, "status", "unexpected")
		}
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

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterNotifyHandler(protocol.MessageTypeQueueNotify, c.handleNotify)
}

// handleNotify is called by the mux when a NOTIFY (209) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	c.mu.RUnlock()

	if !ok {
		return // Unknown subscription
	}

	notif := AvailabilityNotification{
		Route: route,
	}

	// Call handler asynchronously to avoid blocking the dispatch loop
	go func() {
		_ = sub.handler(context.Background(), notif)
	}()
}

// Subscribe registers a handler for availability notifications.
// Pattern should be a wildcard pattern (e.g., "queue://realm/area/resource/*" or "queue://realm/area/resource").
func (c *client) Subscribe(ctx context.Context, pattern string, handler AvailabilityHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.queue.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("queue.Subscribe", "pattern", pattern)
	}
	c.initNotifyHandler()

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueSubscribe, subscribePayloadWriter(pattern))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Subscribe failed", "pattern", pattern, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := parseQueueResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Subscribe failed", "pattern", pattern, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", err)
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("queue.Subscribe failed", "pattern", pattern, "status", "unexpected")
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
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeQueueUnsubscribe, unsubscribePayloadWriter(sub.pattern))
	if err != nil {
		return
	}
	parseQueueResponse(resp)
}
