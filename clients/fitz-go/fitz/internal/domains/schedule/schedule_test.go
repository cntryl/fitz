package schedule

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cntryl/cntryl-go/internal/core/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockMux is a minimal mux provider for unit testing the schedule client.
type mockMux struct {
	in      chan transport.Frame
	sent    []transport.Frame
	sendErr error
}

func newMockMux() *mockMux { return &mockMux{in: make(chan transport.Frame, 16)} }
func (m *mockMux) Send(f transport.Frame) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, f)
	return nil
}
func (m *mockMux) In() <-chan transport.Frame { return m.in }
func (m *mockMux) Ctx() context.Context       { return context.Background() }
func (m *mockMux) OnReconnect(cb func())      {}

// buildScheduleListFrame creates a single LIST streaming response frame.
// Format: [u8 status=0][u8 has_schedule_id][u32 id_len][id bytes][TLV route + cron]
func buildScheduleListFrame(id, route, cronExpr string) transport.Frame {
	body := make([]byte, 0, 64)
	body = append(body, 0) // status = 0
	body = append(body, 1) // has_schedule_id = 1
	body = binary.BigEndian.AppendUint32(body, uint32(len(id)))
	body = append(body, []byte(id)...)
	// Append TLV-encoded schedule data.
	enc := transport.NewTLVEncoder()
	enc.AddString(transport.TagRoute, route)
	enc.AddString(transport.TagCron, cronExpr)
	body = append(body, enc.Encode()...)
	return transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSchedule, Body: body}
}

// buildScheduleListEndFrame creates the terminal LIST frame (has_schedule_id=0).
func buildScheduleListEndFrame() transport.Frame {
	body := []byte{0, 0} // status=0, has_schedule_id=0
	return transport.Frame{Type: transport.FrameTypeResp, Channel: transport.ChannelSchedule, Body: body}
}

func TestShouldReturnEntriesGivenMultiFrameResponseWhenListCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- buildScheduleListFrame("123", "schedule://r/a/job1", "* * * * *")
	m.in <- buildScheduleListFrame("456", "schedule://r/a/job2", "0 */5 * * *")
	m.in <- buildScheduleListEndFrame()

	// Act
	entries, err := c.List(context.Background(), "schedule://r/a/res")

	// Assert
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "123", entries[0].ID)
	assert.Equal(t, "schedule://r/a/job1", entries[0].Route)
	assert.Equal(t, "* * * * *", entries[0].CronExpr)
	assert.Equal(t, "456", entries[1].ID)
	assert.Equal(t, "schedule://r/a/job2", entries[1].Route)
	assert.Equal(t, "0 */5 * * *", entries[1].CronExpr)
}

func TestShouldReturnEmptyListGivenImmediateEndFrameWhenListCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	m.in <- buildScheduleListEndFrame()

	// Act
	entries, err := c.List(context.Background(), "schedule://r/a/res")

	// Assert
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestShouldReturnErrorGivenCancelledContextWhenListCalled(t *testing.T) {
	// Arrange
	m := newMockMux()
	c := NewClient(m)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Act
	_, err := c.List(ctx, "schedule://r/a/res")

	// Assert
	require.Error(t, err)
}
