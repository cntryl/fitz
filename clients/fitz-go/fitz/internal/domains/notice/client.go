package notice

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/transport"
)

// client is the concrete implementation of the Notice domain client.
type client struct {
	mux        transport.MuxProvider
	mu         sync.Mutex
	subs       map[string][]*subscription
	ackMu      sync.Mutex
	ackWaiters map[string][]chan error
	once       sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewClient creates a new Notice domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{mux: mux, subs: make(map[string][]*subscription), ackWaiters: make(map[string][]chan error), ctx: ctx, cancel: cancel}
	mux.OnReconnect(func() {
		c.resubscribeAll(context.Background())
	})
	c.startRecv()
	return c
}

// CtxKeyNoticeBuf is an internal context key to allow tests to override per-subscription buffer size.
type CtxKeyNoticeBuf struct{}

// subscription represents a single local subscription and runs a goroutine that
// invokes the user's handler serially for messages delivered to this subscription.
type subscription struct {
	route   string
	handler NoticeHandler
	inbox   chan NoticeMsg
	client  *client
	wg      sync.WaitGroup
	once    sync.Once
	mu      sync.Mutex
	done    chan struct{}
}

// Unsubscribe removes the subscription and waits for any in-flight handler to finish.
func (s *subscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)

		s.client.mu.Lock()
		chs := s.client.subs[s.route]
		for i, ss := range chs {
			if ss == s {
				chs = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		if len(chs) == 0 {
			delete(s.client.subs, s.route)
			s.client.mu.Unlock()
			_ = s.client.sendUnsubscribe(context.Background(), s.route)
		} else {
			s.client.subs[s.route] = chs
			s.client.mu.Unlock()
		}

		s.wg.Wait()
	})
}

// deliver sends msg to the subscription inbox, or drops if unsubscribed.
func (s *subscription) deliver(msg NoticeMsg) {
	select {
	case <-s.done:
		return
	case s.inbox <- msg:
		return
	}
}

// ---------------------------------------------------------------------------
// Client methods
// ---------------------------------------------------------------------------

// Subscribe registers interest in notifications matching the route.
func (c *client) Subscribe(ctx context.Context, route string, handler NoticeHandler) (Subscription, error) {
	if err := ValidateNoticeRoute(route, true); err != nil {
		return nil, err
	}
	buf := 1
	if v := ctx.Value(CtxKeyNoticeBuf{}); v != nil {
		if bi, ok := v.(int); ok {
			buf = bi
		}
	}
	s := &subscription{route: route, handler: handler, inbox: make(chan NoticeMsg, buf), client: c, done: make(chan struct{})}

	c.mu.Lock()
	chs := c.subs[route]
	first := len(chs) == 0
	chs = append(chs, s)
	c.subs[route] = chs
	c.mu.Unlock()

	if first {
		if err := c.sendSubscribe(ctx, route); err != nil {
			c.mu.Lock()
			chs := c.subs[route]
			for i := len(chs) - 1; i >= 0; i-- {
				if chs[i] == s {
					chs = append(chs[:i], chs[i+1:]...)
					break
				}
			}
			if len(chs) == 0 {
				delete(c.subs, route)
			} else {
				c.subs[route] = chs
			}
			c.mu.Unlock()
			return nil, err
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.Unsubscribe()
				return
			case <-s.done:
				return
			case msg := <-s.inbox:
				s.wg.Add(1)
				func(m NoticeMsg) {
					defer s.wg.Done()
					msgCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					_ = handler(msgCtx, m)
				}(msg)
			}
		}
	}()
	return s, nil
}

// Publish sends a notification to the given route with body bytes.
func (c *client) Publish(ctx context.Context, route string, body []byte) error {
	if err := ValidateNoticeRoute(route, false); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticePublish)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Channel: transport.ChannelPub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send publish: %w", err)
	}
	return c.waitForAck(ctx, NoticePublish)
}

// Close stops the background receive loop and releases resources.
func (c *client) Close() error {
	c.cancel()
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (c *client) startRecv() {
	c.once.Do(func() {
		go func() {
			for {
				select {
				case <-c.ctx.Done():
					return
				case f, ok := <-c.mux.In():
					if !ok {
						return
					}
					// Decode the opcode from TLV TagOp
					dec, err := transport.NewTLVDecoder(f.Body)
					if err != nil {
						continue
					}
					opCode, err := dec.GetOp()
					if err != nil {
						continue
					}
					if opCode == NoticeNotify {
						// Extract the notification body from TLV TagBody
						notifyBody := dec.GetBytes(transport.TagBody)
						if notifyBody == nil {
							continue
						}
						route, body, ok := DecodeNotify(notifyBody)
						if !ok {
							continue
						}
						msg := NoticeMsg{Route: route, Metadata: nil, Body: body}
						c.mu.Lock()
						var targets []*subscription
						for pat, subs := range c.subs {
							if NoticeMatchRoute(pat, route) {
								targets = append(targets, subs...)
							}
						}
						c.mu.Unlock()
						for _, s := range targets {
							s.deliver(msg)
						}
						continue
					}
					if IsNoticeResponseType(opCode) {
						// Extract status from TLV body
						statusBytes := dec.GetBytes(0x40)
						if statusBytes == nil {
							// Fallback: try to decode from full TLV body
							statusBytes = f.Body
						}
						routeKey, err := DecodeNoticeResponseKey(opCode, statusBytes)
						c.notifyWaiters(routeKey, err)
						continue
					}
				}
			}
		}()
	})
}

func (c *client) sendSubscribe(ctx context.Context, route string) error {
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticeSubscribe)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Channel: transport.ChannelSub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}
	return c.waitForAck(ctx, NoticeSubscribe)
}

func (c *client) sendUnsubscribe(ctx context.Context, route string) error {
	enc := transport.NewTLVEncoder()
	enc.AddOp(NoticeUnsubscribe)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{
		Type:    transport.FrameTypeReq,
		Channel: transport.ChannelSub,
		Body:    enc.Encode(),
	}
	if err := c.mux.Send(frame); err != nil {
		return fmt.Errorf("send unsubscribe: %w", err)
	}
	return c.waitForAck(ctx, NoticeUnsubscribe)
}

func (c *client) resubscribeAll(ctx context.Context) {
	c.mu.Lock()
	routes := make([]string, 0, len(c.subs))
	for r := range c.subs {
		routes = append(routes, r)
	}
	c.mu.Unlock()
	for _, r := range routes {
		_ = c.sendSubscribe(ctx, r)
	}
}

func (c *client) waitForAck(ctx context.Context, op uint16) error {
	key := NoticeWaitKey(op)
	ch := make(chan error, 1)
	c.ackMu.Lock()
	c.ackWaiters[key] = append(c.ackWaiters[key], ch)
	c.ackMu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		c.removeWaiter(key, ch)
		return fmt.Errorf("timed out waiting for broker acknowledgement")
	}
}

func (c *client) notifyWaiters(key string, err error) {
	if key == "" {
		return
	}
	c.ackMu.Lock()
	waiters := c.ackWaiters[key]
	delete(c.ackWaiters, key)
	c.ackMu.Unlock()
	for _, w := range waiters {
		w <- err
		close(w)
	}
}

func (c *client) removeWaiter(key string, ch chan error) {
	c.ackMu.Lock()
	ws := c.ackWaiters[key]
	for i, w := range ws {
		if w == ch {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(c.ackWaiters, key)
	} else {
		c.ackWaiters[key] = ws
	}
	c.ackMu.Unlock()
}
