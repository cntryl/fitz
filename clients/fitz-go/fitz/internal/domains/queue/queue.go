package queue

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
)

// Client is the API for the Queue domain.
type Client interface {
	Enqueue(ctx context.Context, route string, body []byte) (string, error)
	Reserve(ctx context.Context, route string, leaseSecs uint32, batchSize uint32) ([]QueueItem, error)
	Extend(ctx context.Context, route string, id string, token uint64, leaseSecs uint32) error
	Complete(ctx context.Context, route string, id string, token uint64) error
}

// QueueItem is a reserved message returned from Reserve.
type QueueItem struct {
	ID    string
	Body  []byte
	Token uint64
}

type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new Queue domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	return &client{mux: mux}
}

// Enqueue sends a message to the queue for the given route and returns a message ID.
func (c *client) Enqueue(ctx context.Context, route string, body []byte) (string, error) {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return "", err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueEnqueue)
	enc.AddString(transport.TagRoute, route)
	enc.AddBytes(transport.TagBody, body)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	if err != nil {
		return "", err
	}

	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return "", fmt.Errorf("invalid TLV in response: %w", derr)
	}
	id, _ := dec.GetUint64(transport.TagID)
	if id == 0 {
		return "", nil
	}
	return fmt.Sprintf("%d", id), nil
}

// Reserve requests messages from the queue with lease semantics.
// The response uses the spec's binary counted-repeat format:
// [u8 status][u32 BE lease_count][repeat: u64 message_id, u64 lease_token, u32 body_len, bytes body]
func (c *client) Reserve(ctx context.Context, route string, leaseSecs uint32, batchSize uint32) ([]QueueItem, error) {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueReserve)
	enc.AddString(transport.TagRoute, route)
	enc.AddUint32(transport.TagTTL, leaseSecs)
	if batchSize > 0 {
		enc.AddUint32(transport.TagBatchSize, batchSize)
	}
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	if err != nil {
		return nil, err
	}

	return decodeReserveResponse(resp.Body)
}

// decodeReserveResponse parses the binary counted-repeat format for RESERVE per CLIENT_SPEC.md:
// [u8 status][u32 BE lease_count][repeat: u64 message_id, u64 lease_token, u32 body_len, bytes body]
func decodeReserveResponse(data []byte) ([]QueueItem, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty reserve response")
	}
	offset := 0

	// Status byte: 0 = success, 1 = error.
	status := data[offset]
	offset++

	if status != 0 {
		// Error response: [u32 error_len][bytes error_msg]
		if offset+4 > len(data) {
			return nil, fmt.Errorf("truncated reserve error response")
		}
		errLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if offset+int(errLen) > len(data) {
			return nil, fmt.Errorf("truncated reserve error message")
		}
		errMsg := string(data[offset : offset+int(errLen)])
		return nil, mapQueueError(errMsg)
	}

	// Success: [u32 BE lease_count][repeat ...]
	if offset+4 > len(data) {
		return nil, fmt.Errorf("truncated reserve response: missing lease_count")
	}
	count := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	items := make([]QueueItem, 0, count)
	for i := uint32(0); i < count; i++ {
		// u64 message_id
		if offset+8 > len(data) {
			return nil, fmt.Errorf("truncated reserve response: missing message_id at index %d", i)
		}
		msgID := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8

		// u64 lease_token
		if offset+8 > len(data) {
			return nil, fmt.Errorf("truncated reserve response: missing lease_token at index %d", i)
		}
		leaseToken := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8

		// u32 body_len
		if offset+4 > len(data) {
			return nil, fmt.Errorf("truncated reserve response: missing body_len at index %d", i)
		}
		bodyLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// bytes body
		if offset+int(bodyLen) > len(data) {
			return nil, fmt.Errorf("truncated reserve response: missing body at index %d", i)
		}
		body := make([]byte, bodyLen)
		copy(body, data[offset:offset+int(bodyLen)])
		offset += int(bodyLen)

		items = append(items, QueueItem{
			ID:    fmt.Sprintf("%d", msgID),
			Body:  body,
			Token: leaseToken,
		})
	}
	return items, nil
}

// Extend extends the lease for a reserved message.
func (c *client) Extend(ctx context.Context, route string, id string, token uint64, leaseSecs uint32) error {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueExtend)
	enc.AddString(transport.TagRoute, route)
	if id == "" {
		return fmt.Errorf("queue extend: id cannot be empty")
	}
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("queue extend: invalid id %q: %w", id, err)
	}
	enc.AddUint64(transport.TagID, v)
	enc.AddUint64(transport.TagLease, token)
	enc.AddUint32(transport.TagTTL, leaseSecs)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	_, err = transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	return err
}

// Complete acknowledges completion of a reserved message.
func (c *client) Complete(ctx context.Context, route string, id string, token uint64) error {
	if err := types.ValidateRoute(route, "queue"); err != nil {
		return err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(QueueComplete)
	enc.AddString(transport.TagRoute, route)
	if id == "" {
		return fmt.Errorf("queue complete: id cannot be empty")
	}
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("queue complete: invalid id %q: %w", id, err)
	}
	enc.AddUint64(transport.TagID, v)
	enc.AddUint64(transport.TagLease, token)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelQueue, Body: enc.Encode()}

	_, err = transport.SendRecv(ctx, c.mux, frame, mapQueueError)
	return err
}
