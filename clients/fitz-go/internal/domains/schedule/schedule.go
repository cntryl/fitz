// Package schedule implements the Fitz Schedule domain client.
// Per CLIENT_SPEC.md: Cron-based task scheduling.
package schedule

import (
	"context"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ScheduleEntry represents a schedule returned by List (per CLIENT_SPEC: route, cron, payload).
type ScheduleEntry struct {
	ID      string // Route (spec uses route as identity)
	Route   string
	Cron    string
	Payload []byte
}

// Notification is the payload delivered when a schedule fires (SCHEDULE_NOTIFY 705).
type Notification struct {
	Payload []byte
}

// ScheduleHandler is called when a schedule fires for a subscribed pattern.
// It is fire-and-forget; the return value is not used and the server does not ack handler result.
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
	// Create creates a cron-based schedule at the given route (upsert per spec). Returns the schedule route (identity).
	Create(ctx context.Context, route string, cronExpr string, payload []byte) (id string, err error)

	// Cancel cancels a schedule by route (route-based identity per CLIENT_SPEC).
	Cancel(ctx context.Context, route string) error

	// List returns all schedules in the current realm (no parameters per spec).
	List(ctx context.Context) ([]ScheduleEntry, error)

	// Subscribe subscribes to schedule fire notifications for the given route pattern.
	// When a schedule fires, the handler is invoked with the schedule's payload.
	// Subscriptions are session-scoped and lost on disconnect.
	Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error)
}

type client struct {
	conn *connection.Connection

	mu              sync.RWMutex
	initialized     bool
	subscriptions   map[uint64]*Subscription
	nextClientSubID uint64
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

// Create per CLIENT_SPEC.md: Request [route_len][route][cron_len][cron][payload_len][payload].
// Response: status=0 (success); optional [u8 has_schedule_id][string schedule_id] when present.
// Returns route as identity (spec uses route-based identity).
func (c *client) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.schedule.Create", trace.WithAttributes(
		attribute.String("fitz.route", route),
		attribute.String("fitz.cron", cronExpr),
	))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("schedule.Create", "route", route, "cron", cronExpr)
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleCreate, scheduleCreatePayloadWriter(route, cronExpr, payload))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Create failed", "route", route, "error", err)
		}
		return "", fmt.Errorf("CREATE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Create failed", "route", route, "error", err)
		}
		return "", fmt.Errorf("CREATE failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Create failed", "route", route, "status", "unexpected")
		}
		return "", fmt.Errorf("CREATE failed: unexpected status")
	}

	// Optional schedule_id when server sends it; otherwise route is identity
	if len(remaining) >= 1 && remaining[0] == 1 {
		if len(remaining) >= 5 {
			id, _, err := connection.ReadString(remaining, 1)
			if err == nil {
				return id, nil
			}
		}
	}
	return route, nil
}

// Cancel per CLIENT_SPEC.md: Request [route_len][route] (route-based identity).
func (c *client) Cancel(ctx context.Context, route string) error {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.schedule.Cancel", trace.WithAttributes(attribute.String("fitz.route", route)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("schedule.Cancel", "route", route)
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleCancel, scheduleCancelPayloadWriter(route))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Cancel failed", "route", route, "error", err)
		}
		return fmt.Errorf("CANCEL request failed: %w", err)
	}

	success, _, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Cancel failed", "route", route, "error", err)
		}
		return fmt.Errorf("CANCEL failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Cancel failed", "route", route, "status", "unexpected")
		}
		return fmt.Errorf("CANCEL failed: unexpected status")
	}

	return nil
}

// List per CLIENT_SPEC.md: Request empty (no parameters). Response: [status][has_entry]; when has_entry=1: [route_len][route][cron_len][cron][payload_len][payload]. Read until has_entry=0.
// Note: Spec defines streaming LIST (multiple response frames). This implementation reads a single response frame; if the server sends multiple frames per LIST, connection layer would need a SendStreamingRequest or similar to read until has_entry=0 across frames.
func (c *client) List(ctx context.Context) ([]ScheduleEntry, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.schedule.List")
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("schedule.List")
	}
	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleList, scheduleListPayloadWriter())
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.List failed", "error", err)
		}
		return nil, fmt.Errorf("LIST request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.List failed", "error", err)
		}
		return nil, fmt.Errorf("LIST failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.List failed", "status", "unexpected")
		}
		return nil, fmt.Errorf("LIST failed: unexpected status")
	}

	var entries []ScheduleEntry
	offset := 0
	for offset < len(remaining) {
		if offset+1 > len(remaining) {
			break
		}
		hasEntry := remaining[offset]
		offset++

		if hasEntry == 0 {
			break
		}

		// Per spec: route_len, route, cron_len, cron, payload_len, payload
		routeStr, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			break
		}
		offset = newOffset
		cronStr, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			break
		}
		offset = newOffset
		payloadBytes, newOffset, err := connection.ReadBytes(remaining, offset)
		if err != nil {
			break
		}
		offset = newOffset
		payloadCopy := make([]byte, len(payloadBytes))
		copy(payloadCopy, payloadBytes)
		entries = append(entries, ScheduleEntry{
			ID:      routeStr,
			Route:   routeStr,
			Cron:    cronStr,
			Payload: payloadCopy,
		})
	}

	return entries, nil
}

// Subscribe per CLIENT_SPEC.md (703): Request [route_pattern]. Response (status=0) only; no subscription_id in response.
// When server sends optional subscription_id in response, we use it for NOTIFY (705) matching; otherwise use client-generated id.
func (c *client) Subscribe(ctx context.Context, pattern string, handler ScheduleHandler) (*Subscription, error) {
	ctx, span := c.conn.Tracer().Start(ctx, "fitz.schedule.Subscribe", trace.WithAttributes(attribute.String("fitz.pattern", pattern)))
	defer span.End()
	if log := c.conn.Logger(); log != nil {
		log.Debug("schedule.Subscribe", "pattern", pattern)
	}
	c.initScheduleNotifyHandler()

	resp, err := c.conn.SendRequestWithWriter(ctx, protocol.MessageTypeScheduleSubscribe, scheduleSubscribePayloadWriter(pattern))
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Subscribe failed", "pattern", pattern, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Subscribe failed", "pattern", pattern, "error", err)
		}
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", mapScheduleError(err.Error()))
	}
	if !success {
		if log := c.conn.Logger(); log != nil {
			log.Error("schedule.Subscribe failed", "pattern", pattern, "status", "unexpected")
		}
		return nil, fmt.Errorf("SUBSCRIBE failed: unexpected status")
	}

	var subID uint64
	if len(remaining) >= 9 && remaining[0] == 1 {
		subID, _, _ = connection.ReadU64BE(remaining, 1)
	}
	if subID == 0 {
		c.mu.Lock()
		c.nextClientSubID++
		subID = c.nextClientSubID
		c.mu.Unlock()
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
