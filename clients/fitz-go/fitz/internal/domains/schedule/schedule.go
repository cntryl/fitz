package schedule

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/cntryl/cntryl-go/internal/core/types"
)

// Client provides schedule management APIs.
type Client interface {
	Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error)
	Cancel(ctx context.Context, id string) error
	List(ctx context.Context, route string) ([]ScheduleEntry, error)
}

// ScheduleEntry is a simple representation returned by List.
type ScheduleEntry struct {
	ID       string
	Route    string
	CronExpr string
}

type client struct {
	mux transport.MuxProvider
}

// NewClient creates a new Schedule domain client backed by the transport mux.
func NewClient(mux transport.MuxProvider) Client {
	return &client{mux: mux}
}

// Create schedules a job; returns the schedule ID assigned by the broker.
func (c *client) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	if err := types.ValidateRoute(route, "schedule"); err != nil {
		return "", err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(ScheduleCreate)
	enc.AddString(transport.TagRoute, route)
	enc.AddString(transport.TagCron, cronExpr)
	if len(payload) > 0 {
		enc.AddBytes(transport.TagBody, payload)
	}
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	resp, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	if err != nil {
		return "", err
	}
	dec, derr := transport.NewTLVDecoder(resp.Body)
	if derr != nil {
		return "", fmt.Errorf("invalid TLV in response: %w", derr)
	}
	id, _ := dec.GetUint64(transport.TagID)
	return fmt.Sprintf("%d", id), nil
}

// Cancel cancels a scheduled job by id.
func (c *client) Cancel(ctx context.Context, id string) error {
	enc := transport.NewTLVEncoder()
	enc.AddOp(ScheduleCancel)
	enc.AddString(transport.TagID, id)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	_, err := transport.SendRecv(ctx, c.mux, frame, mapScheduleError)
	return err
}

// List returns schedule entries for a route.
// Per CLIENT_SPEC.md, LIST uses multi-frame streaming: the broker sends one
// response frame per schedule entry, with a final frame having has_schedule_id=0.
// Each frame body format: [u8 status][u8 has_schedule_id][u32 BE id_len][bytes id][schedule data...]
// Schedule data is TLV-encoded with TagRoute and TagCron.
func (c *client) List(ctx context.Context, route string) ([]ScheduleEntry, error) {
	if err := types.ValidateRoute(route, "schedule"); err != nil {
		return nil, err
	}
	enc := transport.NewTLVEncoder()
	enc.AddOp(ScheduleList)
	enc.AddString(transport.TagRoute, route)
	frame := transport.Frame{Type: transport.FrameTypeReq, Channel: transport.ChannelSchedule, Body: enc.Encode()}

	if err := c.mux.Send(frame); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	var entries []ScheduleEntry
	for {
		resp, err := transport.RecvFrame(ctx, c.mux.In(), transport.ChannelSchedule)
		if err != nil {
			return nil, err
		}
		if resp.Type == transport.FrameTypeErr {
			return nil, transport.DecodeTLVError(resp, "schedule list failed", mapScheduleError)
		}

		entry, done, err := decodeScheduleListFrame(resp.Body)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// decodeScheduleListFrame decodes a single LIST streaming response frame.
// Format: [u8 status][u8 has_schedule_id][u32 BE schedule_id_len][bytes schedule_id][TLV schedule data...]
// Returns the entry, whether this is the terminal frame (has_schedule_id=0), and any error.
func decodeScheduleListFrame(data []byte) (ScheduleEntry, bool, error) {
	if len(data) < 2 {
		return ScheduleEntry{}, false, fmt.Errorf("truncated schedule list frame")
	}
	offset := 0

	status := data[offset]
	offset++

	if status != 0 {
		// Error in stream — parse error message if available.
		if offset+4 <= len(data) {
			errLen := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4
			if offset+int(errLen) <= len(data) {
				return ScheduleEntry{}, false, mapScheduleError(string(data[offset : offset+int(errLen)]))
			}
		}
		return ScheduleEntry{}, false, mapScheduleError("unknown schedule error")
	}

	hasID := data[offset]
	offset++

	if hasID == 0 {
		// Terminal frame: no more schedules.
		return ScheduleEntry{}, true, nil
	}

	// [u32 BE schedule_id_len][bytes schedule_id]
	if offset+4 > len(data) {
		return ScheduleEntry{}, false, fmt.Errorf("truncated schedule list frame: missing id_len")
	}
	idLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(idLen) > len(data) {
		return ScheduleEntry{}, false, fmt.Errorf("truncated schedule list frame: missing id bytes")
	}
	scheduleID := string(data[offset : offset+int(idLen)])
	offset += int(idLen)

	// Remaining bytes are TLV-encoded schedule data (route, cron).
	entry := ScheduleEntry{ID: scheduleID}
	if offset < len(data) {
		dec, err := transport.NewTLVDecoder(data[offset:])
		if err == nil {
			entry.Route = dec.GetString(transport.TagRoute)
			entry.CronExpr = dec.GetString(transport.TagCron)
		}
	}
	return entry, false, nil
}
