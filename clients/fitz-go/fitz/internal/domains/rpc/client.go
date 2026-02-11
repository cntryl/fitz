package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/iter"
	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
)

// client is the concrete implementation of the RPC domain client.
type client struct {
	mux        transport.MuxProvider
	handlersMu sync.Mutex
	handlers   map[string][]*subscription
	nextReqID  uint64
	pendingMu  sync.Mutex
	pending    map[uint64]chan transport.Frame
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewClient creates a new RPC domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{mux: mux, handlers: make(map[string][]*subscription), pending: make(map[uint64]chan transport.Frame), ctx: ctx, cancel: cancel}
	c.startRecv()
	mux.OnReconnect(func() {
		c.resubscribeAll(context.Background())
	})
	return c
}

type subscription struct {
	route   string
	handler RPCHandler
	c       *client
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

func (s *subscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.c.handlersMu.Lock()
		chs := s.c.handlers[s.route]
		for i, ss := range chs {
			if ss == s {
				chs = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		noneLeft := len(chs) == 0
		if noneLeft {
			delete(s.c.handlers, s.route)
		} else {
			s.c.handlers[s.route] = chs
		}
		s.c.handlersMu.Unlock()
		s.wg.Wait()

		// Notify broker only when the last handler for this route is removed.
		if noneLeft {
			enc := transport.NewTLVEncoder()
			enc.AddOp(RPCUnsubscribeWorker)
			enc.AddString(transport.TagRoute, s.route)
			_ = s.c.mux.Send(transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()})
		}
	})
}

// ---------------------------------------------------------------------------
// ResponseWriter implementation
// ---------------------------------------------------------------------------

type responseWriter struct {
	id  uint64
	seq uint64
	mux transport.MuxProvider
}

func (w *responseWriter) Send(body []byte) error {
	enc := transport.NewTLVEncoder()
	enc.AddOp(RPCResponse)
	enc.AddUint64(transport.TagID, w.id)
	enc.AddUint64(transport.TagSeq, w.seq)
	enc.AddBytes(transport.TagBody, body)
	enc.AddUint8(transport.TagStreamEnd, 0)
	w.seq++
	return w.mux.Send(transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()})
}

// sendEnd sends the final stream_end=1 frame (empty body).
func (w *responseWriter) sendEnd() error {
	enc := transport.NewTLVEncoder()
	enc.AddOp(RPCResponse)
	enc.AddUint64(transport.TagID, w.id)
	enc.AddUint64(transport.TagSeq, w.seq)
	enc.AddUint8(transport.TagStreamEnd, 1)
	return w.mux.Send(transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()})
}

// sendError sends an error frame.
func (w *responseWriter) sendError(herr error) error {
	enc := transport.NewTLVEncoder()
	enc.AddUint64(transport.TagID, w.id)
	enc.AddString(transport.TagErr, herr.Error())
	return w.mux.Send(transport.Frame{Type: transport.FrameTypeErr, Channel: transport.ChannelRPC, Body: enc.Encode()})
}

// ---------------------------------------------------------------------------
// Receive loop
// ---------------------------------------------------------------------------

// startRecv dispatches inbound RPC frames to workers or pending Call correlations.
func (c *client) startRecv() {
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case f, ok := <-c.mux.In():
				if !ok {
					goto cleanup
				}
				if f.Channel != transport.ChannelRPC {
					continue
				}
				switch f.Type {
				case transport.FrameTypeReq:
					c.dispatchRequest(f)
				case transport.FrameTypeResp, transport.FrameTypeErr:
					c.deliverPending(f)
				}
			}
		}
	cleanup:
		// mux channel closed — close all pending waiters.
		c.pendingMu.Lock()
		for id, ch := range c.pending {
			close(ch)
			delete(c.pending, id)
		}
		c.pendingMu.Unlock()
	}()
}

func (c *client) dispatchRequest(f transport.Frame) {
	dec, err := transport.NewTLVDecoder(f.Body)
	if err != nil {
		return
	}
	id, _ := dec.GetUint64(transport.TagID)
	route := dec.GetString(transport.TagRoute)
	body := dec.GetBytes(transport.TagBody)
	reply := dec.GetString(transport.TagReplyRoute)
	req := InboundRequest{ID: id, Route: route, Body: body, ReplyRoute: reply}

	c.handlersMu.Lock()
	handlers := append([]*subscription(nil), c.handlers[route]...)
	c.handlersMu.Unlock()

	for _, sub := range handlers {
		sub.wg.Add(1)
		go func(s *subscription) {
			defer s.wg.Done()
			select {
			case <-s.done:
				return
			default:
			}
			w := &responseWriter{id: id, mux: c.mux}
			herr := s.handler(c.ctx, req, w)
			if herr != nil {
				_ = w.sendError(herr)
			} else {
				_ = w.sendEnd()
			}
		}(sub)
	}
}

func (c *client) deliverPending(f transport.Frame) {
	dec, err := transport.NewTLVDecoder(f.Body)
	if err != nil {
		return
	}
	id, err := dec.GetUint64(transport.TagID)
	if err != nil {
		return
	}
	c.pendingMu.Lock()
	ch, found := c.pending[id]
	c.pendingMu.Unlock()
	if found {
		// Block with a short timeout instead of silently dropping.
		select {
		case ch <- f:
		case <-time.After(5 * time.Second):
			// Receiver overloaded — frame lost. This is a last resort.
		}
	}
}

// removePending removes and closes the pending channel for a correlation ID.
func (c *client) removePending(id uint64) {
	c.pendingMu.Lock()
	delete(c.pending, id)
	c.pendingMu.Unlock()
}

// ---------------------------------------------------------------------------
// Call (caller side — returns streaming iterator)
// ---------------------------------------------------------------------------

// Call sends a request and returns a streaming iterator that yields Response
// frames until the worker signals stream_end=1 or an error occurs.
// The caller MUST call Close() on the returned iterator.
func (c *client) Call(ctx context.Context, route string, body []byte, timeout time.Duration) (iter.Iterator[Response], error) {
	if err := types.ValidateRoute(route, "rpc"); err != nil {
		return nil, err
	}
	reqID := atomic.AddUint64(&c.nextReqID, 1)
	enc := transport.NewTLVEncoder()
	enc.AddOp(RPCRequest)
	enc.AddString(transport.TagRoute, route)
	enc.AddUint64(transport.TagID, reqID)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()}

	ch := make(chan transport.Frame, 8)
	c.pendingMu.Lock()
	c.pending[reqID] = ch
	c.pendingMu.Unlock()

	if err := c.mux.Send(frame); err != nil {
		c.removePending(reqID)
		return nil, fmt.Errorf("send request: %w", err)
	}

	items := make(chan Response, 8)
	errCh := make(chan error, 1)
	callCtx, cancel := context.WithTimeout(ctx, timeout)

	go func() {
		defer close(items)
		defer c.removePending(reqID)
		for {
			select {
			case <-callCtx.Done():
				errCh <- fmt.Errorf("rpc call timed out: %w", context.DeadlineExceeded)
				return
			case f, ok := <-ch:
				if !ok {
					errCh <- errors.New("connection closed")
					return
				}
				if f.Type == transport.FrameTypeErr {
					dec, _ := transport.NewTLVDecoder(f.Body)
					errCh <- errors.New(dec.GetString(transport.TagErr))
					return
				}
				dec, derr := transport.NewTLVDecoder(f.Body)
				if derr != nil {
					errCh <- derr
					return
				}
				// Check stream_end flag.
				streamEnd := false
				if dec.Has(transport.TagStreamEnd) {
					b := dec.GetBytes(transport.TagStreamEnd)
					streamEnd = len(b) > 0 && b[0] == 1
				}
				seq, _ := dec.GetUint64(transport.TagSeq)
				respBody := dec.GetBytes(transport.TagBody)

				// If this is a final frame with no body, signal completion.
				if streamEnd && len(respBody) == 0 {
					errCh <- nil
					return
				}
				// Deliver the response to the iterator.
				select {
				case items <- Response{Sequence: seq, Body: respBody}:
				case <-callCtx.Done():
					errCh <- fmt.Errorf("rpc call timed out: %w", context.DeadlineExceeded)
					return
				}
				if streamEnd {
					errCh <- nil
					return
				}
			}
		}
	}()

	// Wrap cancel so the iterator's Close cancels the timeout context.
	wrappedCancel := func() { cancel() }
	return iter.NewChannelIterator(items, errCh, wrappedCancel), nil
}

// ---------------------------------------------------------------------------
// Subscribe / Resubscribe
// ---------------------------------------------------------------------------

// Subscribe registers a streaming worker handler for the given route and
// notifies the broker.
func (c *client) Subscribe(ctx context.Context, route string, handler RPCHandler) (Subscription, error) {
	if err := types.ValidateRoute(route, "rpc"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(RPCSubscribeWorker)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send subscribe_worker: %w", err)
	}
	s := &subscription{route: route, handler: handler, c: c, done: make(chan struct{})}
	c.handlersMu.Lock()
	c.handlers[route] = append(c.handlers[route], s)
	c.handlersMu.Unlock()
	return s, nil
}

func (c *client) resubscribeAll(ctx context.Context) {
	c.handlersMu.Lock()
	routes := make([]string, 0, len(c.handlers))
	for r := range c.handlers {
		routes = append(routes, r)
	}
	c.handlersMu.Unlock()
	for _, r := range routes {
		enc := transport.NewTLVEncoder()
		enc.AddOp(RPCSubscribeWorker)
		enc.AddString(transport.TagRoute, r)
		_ = c.mux.Send(transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelRPC, Body: enc.Encode()})
	}
}

// Close stops the background receive loop and releases resources.
func (c *client) Close() error {
	c.cancel()
	return nil
}
