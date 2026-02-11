package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/cntryl/cntryl-go/internal/core/iter"
	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
)

// Client is the API for the Stream domain.
type Client interface {
	Begin(ctx context.Context, route string) (uint64, error)
	Append(ctx context.Context, route string, body []byte, expectedOffset *uint64) (uint64, error)
	Commit(ctx context.Context, route string) error
	Rollback(ctx context.Context, route string) error
	ReadResource(ctx context.Context, route string, from uint64, limit uint32, opts ...ReadOption) (iter.Iterator[StreamRecord], error)
	Last(ctx context.Context, route string) (*StreamRecord, error)
	GetMetadata(ctx context.Context, route string) (map[string]string, error)
}

// StreamRecord is a minimal record returned by stream reads.
type StreamRecord struct {
	Offset uint64
	Body   []byte
}

// ReadOption configures behaviour of ReadResource iterator.
type ReadOption func(*readOptions)

type readOptions struct {
	bufferSize      int
	perFrameTimeout time.Duration
}

// WithBufferSize sets the internal channel buffer for the read iterator.
func WithBufferSize(n int) ReadOption {
	return func(o *readOptions) {
		if n > 0 {
			o.bufferSize = n
		}
	}
}

// WithPerFrameTimeout sets a timeout between consecutive frames. If no frame
// arrives within this duration, the iterator stops with ErrStreamReadError.
func WithPerFrameTimeout(d time.Duration) ReadOption {
	return func(o *readOptions) { o.perFrameTimeout = d }
}

type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new Stream domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	return &client{mux: mux}
}

// Append appends a record and returns the new offset assigned by the broker.
func (c *client) Append(ctx context.Context, route string, body []byte, expectedOffset *uint64) (uint64, error) {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return 0, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamAppend)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	if expectedOffset != nil {
		enc.AddUint64(transport.TagExpectedOffset, *expectedOffset)
	}
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}

	// Append uses a custom response loop because the response may arrive as a
	// data frame (not FrameTypeResp) carrying TagSeq.
	if err := c.mux.Send(frame); err != nil {
		return 0, fmt.Errorf("send append: %w", err)
	}
	ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		f, err := transport.RecvFrame(ackCtx, c.mux.In(), transport.ChannelStream)
		if err != nil {
			return 0, err
		}
		dec, derr := transport.NewTLVDecoder(f.Body)
		if derr != nil {
			continue
		}
		if dec.Has(transport.TagErr) {
			return 0, mapStreamError(dec.GetString(transport.TagErr))
		}
		seq, _ := dec.GetUint64(transport.TagSeq)
		if seq != 0 {
			return seq, nil
		}
	}
}

// ReadResource returns an iterator that streams records from the broker.
func (c *client) ReadResource(ctx context.Context, route string, from uint64, limit uint32, opts ...ReadOption) (iter.Iterator[StreamRecord], error) {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return nil, err
	}
	ro := readOptions{bufferSize: 8}
	for _, o := range opts {
		o(&ro)
	}
	if ro.bufferSize <= 0 {
		ro.bufferSize = 8
	}

	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamRead)
	enc.AddString(transport.TagRoute, route)
	enc.AddUint64(transport.TagSeq, from)
	enc.AddUint32(transport.TagLimit, limit)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}
	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send read: %w", err)
	}

	records := make(chan StreamRecord, ro.bufferSize)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(records)
		count := uint32(0)
		var timer *time.Timer
		if ro.perFrameTimeout > 0 {
			timer = time.NewTimer(ro.perFrameTimeout)
			defer timer.Stop()
		}
		for {
			var to <-chan time.Time
			if timer != nil {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(ro.perFrameTimeout)
				to = timer.C
			}
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case <-to:
				errCh <- ErrStreamReadError
				return
			case f, ok := <-c.mux.In():
				if !ok {
					errCh <- fmt.Errorf("mux closed")
					return
				}
				if f.Channel != transport.ChannelStream {
					continue
				}
				dec, derr := transport.NewTLVDecoder(f.Body)
				if derr != nil {
					continue
				}
				if dec.Has(transport.TagErr) {
					errCh <- mapStreamError(dec.GetString(transport.TagErr))
					return
				}
				if dec.Has(transport.TagStreamEnd) {
					errCh <- nil
					return
				}
				seq, _ := dec.GetUint64(transport.TagSeq)
				body := dec.GetBytes(transport.TagBody)
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case records <- StreamRecord{Offset: seq, Body: body}:
					count++
					if limit != 0 && count >= limit {
						errCh <- nil
						return
					}
				}
			}
		}
	}()

	return iter.NewChannelIterator(records, errCh, cancel), nil
}

// Begin requests a new stream session and returns the starting offset.
func (c *client) Begin(ctx context.Context, route string) (uint64, error) {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return 0, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamBegin)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapStreamError)
	if err != nil {
		return 0, err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return 0, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	seq, _ := dec.GetUint64(transport.TagSeq)
	return seq, nil
}

// Commit finalizes the session for a stream resource.
func (c *client) Commit(ctx context.Context, route string) error {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamCommit)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}
	_, err := transport.SendRecv(ctx, c.mux, frame, mapStreamError)
	return err
}

// Rollback aborts an active session.
func (c *client) Rollback(ctx context.Context, route string) error {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamRollback)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}
	_, err := transport.SendRecv(ctx, c.mux, frame, mapStreamError)
	return err
}

// Last returns the last record in the stream (if any).
func (c *client) Last(ctx context.Context, route string) (*StreamRecord, error) {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamLast)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapStreamError)
	if err != nil {
		return nil, err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	seq, _ := dec.GetUint64(transport.TagSeq)
	body := dec.GetBytes(transport.TagBody)
	return &StreamRecord{Offset: seq, Body: body}, nil
}

// GetMetadata requests stream metadata and returns it as key/value pairs.
func (c *client) GetMetadata(ctx context.Context, route string) (map[string]string, error) {
	if err := types.ValidateRoute(route, "stream"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(StreamGetMetadata)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelStream, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapStreamError)
	if err != nil {
		return nil, err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return nil, fmt.Errorf("invalid TLV in response: %w", derr)
	}
	meta := make(map[string]string)
	if dec.Has(transport.TagBody) {
		meta["body"] = string(dec.GetBytes(transport.TagBody))
	}
	return meta, nil
}
