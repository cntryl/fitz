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

// Create per CLIENT_SPEC.md:
// Request: [route_len][route][cron_len][cron][target_resource_len][target_resource][target_operation_len][target_operation]
// Response: [status][schedule_id_len][schedule_id]
func (c *client) Create(ctx context.Context, route string, cronExpr string, payload []byte) (string, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)
	connection.WriteString(buf, cronExpr)
	// target_resource = route (schedule fires back to itself)
	connection.WriteString(buf, route)
	// target_operation = payload as string
	connection.WriteBytes(buf, payload)

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

	// Parse schedule_id
	scheduleID, _, err := connection.ReadString(remaining, 0)
	if err != nil {
		return "", fmt.Errorf("parse schedule_id: %w", err)
	}

	return scheduleID, nil
}

// Cancel per CLIENT_SPEC.md:
// Request: [route_len][route][schedule_id_len][schedule_id]
// Response: [status]
func (c *client) Cancel(ctx context.Context, scheduleID string) error {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	// For cancel, we pass the route as empty and the schedule_id
	// The server identifies the schedule by ID
	connection.WriteString(buf, "")
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

// List per CLIENT_SPEC.md:
// Request: [route_len][route]
// Response: [status][entries...]
func (c *client) List(ctx context.Context, route string) ([]ScheduleEntry, error) {
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)

	connection.WriteString(buf, route)

	resp, err := c.conn.SendRequest(ctx, protocol.MessageTypeScheduleList, buf.Bytes())
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

	// Parse entries list
	var entries []ScheduleEntry
	offset := 0
	for offset < len(remaining) {
		// Check for has_entry flag
		if offset >= len(remaining) {
			break
		}

		hasEntry := remaining[offset]
		offset++

		if hasEntry == 0 {
			break // End of list
		}

		// Read schedule_id
		id, newOffset, err := connection.ReadString(remaining, offset)
		if err != nil {
			break // Can't parse more
		}
		offset = newOffset

		entries = append(entries, ScheduleEntry{ID: id})

		// Skip any additional schedule data (cron, payload, etc.)
		// Try to read past remaining fields until next has_entry flag
		// This is best-effort; we consume what we can
	}

	return entries, nil
}
