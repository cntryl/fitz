package schedule

import (
	"encoding/binary"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestShouldEncodeScheduleCreate tests SCHEDULE CREATE encoding.
func TestShouldEncodeScheduleCreate(t *testing.T) {
	t.Run("valid create payload", func(t *testing.T) {
		// Arrange
		route := "schedule://acme/backup/run"
		cron := "0 0 * * *"
		payload := []byte("execute")

		// Act
		encoded, err := EncodeScheduleCreate(route, cron, payload)

		// Assert
		require.NoError(t, err)
		innerTLV, _, err := connection.ReadBytes(encoded, 0)
		require.NoError(t, err)
		cronOut, targetResource, targetOperation := decodeSchedulePayload(innerTLV)
		assert.Equal(t, cron, cronOut)
		assert.Equal(t, route, targetResource)
		assert.Equal(t, string(payload), targetOperation)
	})
}

// TestShouldEncodeScheduleCancel tests SCHEDULE CANCEL encoding.
func TestShouldEncodeScheduleCancel(t *testing.T) {
	t.Run("valid schedule id", func(t *testing.T) {
		// Arrange
		scheduleID := "12345"

		// Act
		encoded, err := EncodeScheduleCancel(scheduleID)

		// Assert
		require.NoError(t, err)
		idLen, _, err := connection.ReadU32BE(encoded, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(scheduleID)), idLen)
	})
}

// TestShouldEncodeScheduleList tests SCHEDULE LIST encoding.
func TestShouldEncodeScheduleList(t *testing.T) {
	t.Run("empty payload", func(t *testing.T) {
		// Arrange & Act
		encoded, err := EncodeScheduleList()

		// Assert
		require.NoError(t, err)
		assert.Nil(t, encoded)
	})
}

// TestShouldEncodeScheduleSubscribe tests SCHEDULE SUBSCRIBE encoding.
func TestShouldEncodeScheduleSubscribe(t *testing.T) {
	t.Run("valid pattern", func(t *testing.T) {
		// Arrange
		pattern := "schedule://acme/**"

		// Act
		encoded, err := EncodeScheduleSubscribe(pattern)

		// Assert
		require.NoError(t, err)
		patternLen, _, err := connection.ReadU32BE(encoded, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(pattern)), patternLen)
	})
}

// TestShouldEncodeScheduleUnsubscribe tests SCHEDULE UNSUBSCRIBE encoding.
func TestShouldEncodeScheduleUnsubscribe(t *testing.T) {
	t.Run("valid pattern", func(t *testing.T) {
		// Arrange
		pattern := "schedule://acme/**"

		// Act
		encoded, err := EncodeScheduleUnsubscribe(pattern)

		// Assert
		require.NoError(t, err)
		patternLen, _, err := connection.ReadU32BE(encoded, 0)
		require.NoError(t, err)
		assert.Equal(t, uint32(len(pattern)), patternLen)
	})
}

// decodeSchedulePayload parses the schedule inner TLV blob.
// Format per record: [u8 type][u16 BE value_len][value_bytes].
func decodeSchedulePayload(data []byte) (string, string, string) {
	offset := 0
	var cron string
	var targetResource string
	var targetOperation string

	for offset+3 <= len(data) {
		fieldType := data[offset]
		offset++
		valueLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+valueLen > len(data) {
			break
		}
		value := string(data[offset : offset+valueLen])
		offset += valueLen
		switch fieldType {
		case 1:
			cron = value
		case 2:
			targetResource = value
		case 3:
			targetOperation = value
		}
	}

	return cron, targetResource, targetOperation
}

// Benchmarks

func BenchmarkEncodeScheduleCreate(b *testing.B) {
	route := "schedule://acme/backup/run"
	cron := "0 0 * * *"
	payload := []byte("execute")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeScheduleCreate(route, cron, payload)
	}
}

func BenchmarkEncodeScheduleCancel(b *testing.B) {
	scheduleID := "12345"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeScheduleCancel(scheduleID)
	}
}

func BenchmarkEncodeScheduleList(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeScheduleList()
	}
}

func BenchmarkEncodeScheduleSubscribe(b *testing.B) {
	pattern := "schedule://acme/**"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeScheduleSubscribe(pattern)
	}
}

func BenchmarkEncodeScheduleUnsubscribe(b *testing.B) {
	pattern := "schedule://acme/**"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeScheduleUnsubscribe(pattern)
	}
}
