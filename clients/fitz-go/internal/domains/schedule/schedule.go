// Package schedule implements the Fitz Schedule domain client.
// Per CLIENT_SPEC.md: Cron-based task scheduling.
package schedule

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// ScheduleEntry represents a schedule returned by List.
type ScheduleEntry struct {
	ID string
}

// FireNotification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705).
type FireNotification struct {
	Route   string
	Payload []byte
}

// ScheduleHandler is called when a schedule fires for a subscribed pattern.
type ScheduleHandler func(ctx context.Context, n FireNotification)

// Subscription represents an active subscription to schedule fire notifications.
// Call Unsubscribe to stop receiving notifications.
type Subscription struct {
	subID   uint64
	pattern string
	client  *client
	handler ScheduleHandler
}

// Unsubscribe stops receiving schedule fire notifications for this subscription.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	return s.client.Unsubscribe(ctx, s)
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
	// When a schedule fires, the handler is invoked with the schedule's route and payload.
	// Subscriptions are session-scoped and lost on disconnect.
	Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error)

	// Unsubscribe stops receiving notifications for the subscription.
	Unsubscribe(ctx context.Context, sub *Subscription) error
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

func (c *client) handleScheduleNotify(subID uint64, route string, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	c.mu.RUnlock()
	if !ok {
		return
	}
	msg := FireNotification{
		Route:   route,
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
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	// Build the inner nested TLV blob (SchedulePayload format)
	innerTLV := encodeSchedulePayload(cronExpr, route, string(payload))

	// Wrap with WriteBytes: [u32 BE len][inner_tlv_blob]
	connection.WriteBytes(buf, innerTLV)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeScheduleCreate, buf.Bytes())
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
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, scheduleID)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeScheduleCancel, buf.Bytes())
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
	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeScheduleList, nil)
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
// Request: [u32 BE route_pattern_len][bytes route_pattern]
// Response (status=0): [u8 has_schedule_id (1)][u32 BE schedule_id_len][bytes schedule_id] (subscription_id as string)
func (c *client) Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error) {
	c.initScheduleNotifyHandler()

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteString(buf, pattern)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeScheduleSubscribe, buf.Bytes())
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
	hasScheduleID := remaining[0]
	if hasScheduleID != 1 {
		return nil, fmt.Errorf("SUBSCRIBE response missing subscription_id")
	}
	subIDStr, _, err := connection.ReadString(remaining, 1)
	if err != nil {
		return nil, fmt.Errorf("parse subscription_id: %w", err)
	}
	subID, err := strconv.ParseUint(subIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("subscription_id not numeric: %w", err)
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
// Request: [u32 BE route_pattern_len][bytes route_pattern]
func (c *client) Unsubscribe(ctx context.Context, sub *Subscription) error {
	c.mu.Lock()
	delete(c.subscriptions, sub.subID)
	c.mu.Unlock()

	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	connection.WriteString(buf, sub.pattern)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeScheduleUnsubscribe, buf.Bytes())
	if err != nil {
		return fmt.Errorf("UNSUBSCRIBE request failed: %w", err)
	}
	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("UNSUBSCRIBE failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		return fmt.Errorf("UNSUBSCRIBE failed: unexpected status")
	}
	return nil
}
