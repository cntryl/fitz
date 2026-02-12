// Package notice implements the Fitz Notice domain client.
// Per CLIENT_SPEC.md: Pub/sub with wildcard pattern matching.
package notice

import (
	"context"
	"fmt"
	"sync"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// NoticeMsg represents a received notification.
type NoticeMsg struct {
	Route string
	Body  []byte
}

// NoticeHandler is called when a notification arrives.
type NoticeHandler func(ctx context.Context, msg NoticeMsg) error

// Subscription represents an active notice subscription.
type Subscription struct {
	subID   uint64
	route   string
	client  *client
	handler NoticeHandler
}

// Unsubscribe removes this subscription.
func (s *Subscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s)
	}
}

// Client is the Notice domain client interface.
type Client interface {
	// Publish sends a notification to a route (fire-and-forget).
	Publish(ctx context.Context, route string, body []byte) error

	// Subscribe registers a handler for notifications matching the pattern.
	// Returns a Subscription that can be used to unsubscribe.
	Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*Subscription, error)
}

type client struct {
	conn *connection.Connection

	mu            sync.RWMutex
	subscriptions map[uint64]*Subscription // subID -> subscription
	initialized   bool
}

// NewClient creates a new Notice domain client.
func NewClient(conn *connection.Connection) Client {
	c := &client{
		conn:          conn,
		subscriptions: make(map[uint64]*Subscription),
	}
	return c
}

// initNotifyHandler registers the NOTIFY handler on first use.
func (c *client) initNotifyHandler() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized {
		return
	}
	c.initialized = true
	c.conn.RegisterNotifyHandler(c.handleNotify)
}

// handleNotify is called by the mux when a NOTIFY (504) frame arrives.
func (c *client) handleNotify(subID uint64, route string, payload []byte) {
	c.mu.RLock()
	sub, ok := c.subscriptions[subID]
	c.mu.RUnlock()

	if !ok {
		return // Unknown subscription
	}

	msg := NoticeMsg{
		Route: route,
		Body:  make([]byte, len(payload)),
	}
	copy(msg.Body, payload)

	// Call handler asynchronously to avoid blocking the dispatch loop
	go func() {
		_ = sub.handler(context.Background(), msg)
	}()
}

// Publish per CLIENT_SPEC.md:
// Request: [route_len][route][payload_len][payload]
// Notice PUBLISH is fire-and-forget (no response expected per spec).
// However, the server may still send a response, so we handle it.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	payload := encodePublish(route, body)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeNoticePublish, payload)
	if err != nil {
		return fmt.Errorf("PUBLISH request failed: %w", err)
	}

	// Parse response (server sends ack even for fire-and-forget)
	_, _, err = connection.ParseStandardResponse(resp)
	if err != nil {
		return fmt.Errorf("PUBLISH failed: %w", err)
	}

	return nil
}

// Subscribe per CLIENT_SPEC.md:
// Request: [pattern_len][pattern]
// Response: [status][subscription_id(u64)]
func (c *client) Subscribe(ctx context.Context, pattern string, handler NoticeHandler) (*Subscription, error) {
	c.initNotifyHandler()

	payload := encodeSubscribe(pattern)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeNoticeSubscribe, payload)
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE request failed: %w", err)
	}

	success, remaining, err := connection.ParseStandardResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("SUBSCRIBE failed: %w", err)
	}
	if !success {
		return nil, fmt.Errorf("SUBSCRIBE failed: unexpected status")
	}

	// Parse subscription_id
	if len(remaining) < 8 {
		return nil, fmt.Errorf("SUBSCRIBE response too short: got %d bytes", len(remaining))
	}

	subID, _, err := connection.ReadU64BE(remaining, 0)
	if err != nil {
		return nil, fmt.Errorf("parse subscription_id: %w", err)
	}

	sub := &Subscription{
		subID:   subID,
		route:   pattern,
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

	// Send UNSUBSCRIBE to server (best-effort, ignore errors)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteU64BE(buf, sub.subID)

	ctx := context.Background()
	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeNoticeUnsubscribe, buf.Bytes())
	if err != nil {
		return
	}
	connection.ParseStandardResponse(resp)
}
