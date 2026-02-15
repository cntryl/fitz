// Package schedule implements the Fitz Schedule domain client.
// Per CLIENT_SPEC.md: Cron-based task scheduling.
package schedule

import (
	"context"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// ScheduleEntry represents a schedule returned by List.
type ScheduleEntry struct {
	ID string
}

// Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705).
type Notification struct {
	Payload []byte
}

// ScheduleHandler is called when a schedule fires for a subscribed pattern.
type ScheduleHandler func(ctx context.Context, n Notification)

// Subscription represents an active subscription to schedule fire notifications.
// Call Unsubscribe to stop receiving notifications.
type Subscription struct {
	subID   uint64
	pattern string
	client  *client
	handler ScheduleHandler
}

// Unsubscribe stops receiving schedule fire notifications for this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

// Client is the Schedule domain client interface.
type Client interface {
	// Create creates a cron-based schedule. Returns the schedule ID.
	Create(ctx context.Context, route string, cronExpr string, payload []byte) (id string, err error)

	// Cancel cancels a schedule by ID.
	Cancel(ctx context.Context, scheduleID string) error

	// List returns all schedules for the given route.
	List(ctx context.Context, route string) ([]ScheduleEntry, error)

	// Subscribe subscribes to schedule fire notifications for the given route pattern.
	// When a schedule fires, the handler is invoked with the schedule's payload.
	// Subscriptions are session-scoped and lost on disconnect.
	Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error)
}

type client struct {
	conn *connection.Connection

	mu            sync.RWMutex
	initialized   bool
	subscriptions map[uint64]*Subscription
}

func (c *client) initScheduleNotifyHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	if c.subscriptions == nil {
		c.subscriptions = make(map[uint64]*Subscription)
	}
	c.conn.RegisterScheduleNotifyHandler(c.handleScheduleNotify)
}

func (c *client) handleScheduleNotify(subID uint64, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	c.mu.RUnlock()
	if !ok {
		return
	}
	msg := Notification{
		Payload: make([]byte, len(payload)),
	}
	copy(msg.Payload, payload)
	go func() {
		sub.handler(context.Background(), msg)
	}()
}

// NewClient creates a new Schedule domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn, subscriptions: make(map[uint64]*Subscription)}
}

// Create per server schedule_codec.rs:
// Request: [bytes payload] where payload is a nested TLV blob containing:
//
//	[u8 type=1][u16 BE cron_len][cron_bytes]
//	[u8 type=2][u16 BE target_resource_len][target_resource_bytes]
//	[u8 type=3][u16 BE target_operation_len][target_operation_bytes]
//
// Response: [status][u8 has_schedule_id][string schedule_id if has=1]
func (c *client) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleCreate, scheduleCreatePayloadWriter(route, cronExpr, payload))
	if err != nil {
		return "", fmt.Errorf("CREATE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return "", fmt.Errorf("CREATE failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		return "", fmt.Errorf("CREATE failed: unexpected status")
	}

	// Parse optional schedule_id: [u8 has_schedule_id][string schedule_id if has=1]
	if len(remaining) < 1 {
		return "", fmt.Errorf("CREATE response too short")
	}
	hasScheduleID := remaining[0]
	if hasScheduleID != 1 {
		return "", nil // No schedule ID returned
	}

	scheduleID, _, err := connection.ReadString(remaining, 1)
	if err != nil {
		return "", fmt.Errorf("parse schedule_id: %w", err)
	}

	return scheduleID, nil
}

// Cancel per server schedule_codec.rs:
// Request: [string schedule_id]
// Response: [status][optional string schedule_id]
func (c *client) Cancel(ctx context.Context, scheduleID string) error {
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleCancel, scheduleCancelPayloadWriter(scheduleID))
	if err != nil {
		return fmt.Errorf("CANCEL request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("CANCEL failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		return fmt.Errorf("CANCEL failed: unexpected status")
	}

	return nil
}

// List per server schedule_codec.rs:
// Request: empty payload (no parameters)
// Response: [status][optional string schedule_id]
func (c *client) List(ctx context.Context, route string) ([]ScheduleEntry, error) {
	// Server expects empty payload for LIST
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleList, scheduleListPayloadWriter())
	if err != nil {
		return nil, fmt.Errorf("LIST request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("LIST failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("LIST failed: unexpected status")
	}

	// Response uses optional_string format: [u8 has_id][string id if has=1]
	var entries []ScheduleEntry
	offset := 0
	for offset < len(remaining) {
		if offset >= len(remaining) {
			break
		}
		hasEntry := remaining[offset]
		offset++

		if hasEntry == 0 {
			break // No more entries (or None sentinel)
		}

		// Read schedule_id
		id, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			break
		}
		offset = newOffset

		entries = append(entries, ScheduleEntry{ID: id})
	}

	return entries, nil
}

// Subscribe per CLIENT_SPEC.md (703):
// Request: [string route_pattern]
// Response (status=0): [u8 has_subscription_id (1)][u64 BE subscription_id]
func (c *client) Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error) {
	c.initScheduleNotifyHandler()

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleSubscribe, scheduleSubscribePayloadWriter(pattern))
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		return nil, fmt.Errorf("SUBSCRIBE failed: unexpected status")
	}

	if len(remaining) < 1 {
		return nil, fmt.Errorf("SUBSCRIBE response too short")
	}
	hasSubscriptionID := remaining[0]
	if hasSubscriptionID != 1 {
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

// Unsubscribe per CLIENT_SPEC.md (704):
// Request: [string route_pattern]
func (c *client) unsubscribe(sub *Subscription) {
	c.mu.Lock()
	delete(c.subscriptions, sub.subID)
	c.mu.Unlock()

	// Best-effort unsubscribe; ignore errors to match notice semantics.
	ctx := context.Background()
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleUnsubscribe, scheduleUnsubscribePayloadWriter(sub.pattern))
	if err != nil {
		return
	}
	connection.ParseStandardResponse(resp)
}
