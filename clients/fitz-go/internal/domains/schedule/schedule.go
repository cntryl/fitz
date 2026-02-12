// Package schedule implements the Fitz Schedule domain client.
// Per CLIENT_SPEC.md: Cron-based task scheduling.
package schedule

import (
	"context"
	"fmt"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/cntryl/fitz-go/internal/protocol"
)

// ScheduleEntry represents a schedule returned by List.
type ScheduleEntry struct {
	ID string
}

// Client is the Schedule domain client interface.
type Client interface {
	// Create creates a cron-based schedule. Returns the schedule ID.
	Create(ctx context.Context, route string, cronExpr string, payload []byte) (id string, err error)

	// Cancel cancels a schedule by ID.
	Cancel(ctx context.Context, scheduleID string) error

	// List returns all schedules for the given route.
	List(ctx context.Context, route string) ([]ScheduleEntry, error)
}

type client struct {
	conn *connection.Connection
}

// NewClient creates a new Schedule domain client.
func NewClient(conn *connection.Connection) Client {
	return &client{conn: conn}
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
