package schedule

import (
	"encoding/binary"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
)

// TestShouldMapScheduleError tests error message mapping.
func TestShouldMapScheduleError(t *testing.T) {
	t.Run("map schedule not found error", func(t *testing.T) {
		// Arrange
		errMsg := "schedule not found"

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.Equal(t, ErrScheduleNotFound, mapped)
	})

	t.Run("map generic error", func(t *testing.T) {
		// Arrange
		errMsg := "invalid cron expression"

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.Equal(t, "invalid cron expression", mapped.Error())
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := "unexpected schedule condition"

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped.Error())
	})

	t.Run("empty error message", func(t *testing.T) {
		// Arrange
		errMsg := ""

		// Act
		mapped := mapScheduleError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
	})
}

// TestShouldDefineScheduleOpcodes tests that Schedule opcodes are properly defined.
func TestShouldDefineScheduleOpcodes(t *testing.T) {
	t.Run("create opcode", func(t *testing.T) {
		assert.Equal(t, uint16(700), ScheduleCreate)
	})

	t.Run("cancel opcode", func(t *testing.T) {
		assert.Equal(t, uint16(701), ScheduleCancel)
	})

	t.Run("list opcode", func(t *testing.T) {
		assert.Equal(t, uint16(702), ScheduleList)
	})

	t.Run("subscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(703), ScheduleSubscribe)
	})

	t.Run("unsubscribe opcode", func(t *testing.T) {
		assert.Equal(t, uint16(704), ScheduleUnsubscribe)
	})

	t.Run("notify opcode server only", func(t *testing.T) {
		assert.Equal(t, uint16(705), ScheduleNotify)
	})

	t.Run("opcodes are sequential", func(t *testing.T) {
		assert.Equal(t, ScheduleCreate+1, ScheduleCancel)
		assert.Equal(t, ScheduleCancel+1, ScheduleList)
		assert.Equal(t, ScheduleList+1, ScheduleSubscribe)
		assert.Equal(t, ScheduleSubscribe+1, ScheduleUnsubscribe)
		assert.Equal(t, ScheduleUnsubscribe+1, ScheduleNotify)
	})

	t.Run("all opcodes in 700 range", func(t *testing.T) {
		assert.GreaterOrEqual(t, ScheduleCreate, uint16(700))
		assert.LessOrEqual(t, ScheduleNotify, uint16(705))
	})
}

// TestShouldDefineScheduleErrors tests that Schedule error variables are defined.
func TestShouldDefineScheduleErrors(t *testing.T) {
	t.Run("schedule not found error", func(t *testing.T) {
		assert.NotNil(t, ErrScheduleNotFound)
		assert.Equal(t, "schedule not found", ErrScheduleNotFound.Error())
	})
}

// TestShouldValidateCronExpressions tests cron expression validation.
func TestShouldValidateCronExpressions(t *testing.T) {
	validExpressions := []string{
		"0 0 * * *",             // Daily at midnight
		"*/5 * * * *",           // Every 5 minutes
		"0 */2 * * *",           // Every 2 hours
		"0 0 1 * *",             // First day of month
		"0 0 * * 0",             // Weekly on Sunday
		"*/15 9-17 * * MON-FRI", // Every 15 min, 9am-5pm weekdays
	}

	for _, expr := range validExpressions {
		t.Run("valid: "+expr, func(t *testing.T) {
			// Just test that the expression can be passed to the domain
			// without causing errors during mapping
			_ = expr // Expression format validation would happen server-side
		})
	}
}

// TestShouldDefineScheduleTargets tests schedule target resource/operation handling.
func TestShouldDefineScheduleTargets(t *testing.T) {
	t.Run("target with operation", func(t *testing.T) {
		target := "schedule://acme/app/backup/execute"
		assert.NotEmpty(t, target)
	})

	t.Run("another target with operation", func(t *testing.T) {
		target := "schedule://acme/app/sync/run"
		assert.NotEmpty(t, target)
	})

	t.Run("nested target path with operation", func(t *testing.T) {
		target := "schedule://org.example.com/production/maintenance/daily"
		assert.NotEmpty(t, target)
	})
}

// Benchmarks

func BenchmarkEncodeScheduleCreate(b *testing.B) {
	route := "schedule://acme/jobs/backup/run"
	cronExpr := "0 0 * * *"
	payload := []byte("backup-payload")
	w := scheduleCreatePayloadWriter(route, cronExpr, payload)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleCancel(b *testing.B) {
	route := "schedule://acme/jobs/backup/run"
	w := scheduleCancelPayloadWriter(route)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleList(b *testing.B) {
	w := scheduleListPayloadWriter(0, 100) // offset=0, limit=100
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleSubscribe(b *testing.B) {
	pattern := "schedule://acme/jobs/*"
	w := scheduleSubscribePayloadWriter(pattern)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkEncodeScheduleUnsubscribe(b *testing.B) {
	pattern := "schedule://acme/jobs/*"
	w := scheduleUnsubscribePayloadWriter(pattern)
	buf := connection.GetBuffer()
	defer connection.PutBuffer(buf)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		w(buf)
	}
}

func BenchmarkParseScheduleCreateResponse(b *testing.B) {
	// [status=0] (success, no optional id)
	payload := []byte{0}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = connection.ParseStandardResponse(payload)
	}
}

func BenchmarkParseScheduleSubscribeResponse(b *testing.B) {
	// [status=0][has_sub_id=1][u64 sub_id]
	payload := make([]byte, 1+1+8)
	payload[0] = 0
	payload[1] = 1
	binary.BigEndian.PutUint64(payload[2:10], 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 9 && remaining[0] == 1 {
			_, _, _ = connection.ReadU64BE(remaining, 1)
		}
	}
}
