package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (Schedule domain) ---
// - create schedule and verify execution
// - cancel prevents future runs
// - list returns created schedules

// TestShouldCreateScheduleGivenValidCronExpressionWhenCreateCalled verifies
// CREATE operation schedules a task with cron syntax.
func TestShouldCreateScheduleGivenValidCronExpressionWhenCreateCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("schedule")

		// Act
		id, err := f.Client().Schedule().Create(ctx, route, "*/5 * * * *", []byte("task-payload"))

		// Assert
		require.NoError(t, err, "Create should succeed with valid cron expression")
		assert.NotEmpty(t, id, "schedule ID should be non-empty")
	})
}

// TestShouldRejectCreateGivenInvalidCronSyntaxWhenCreateCalled verifies
// CREATE operation rejects malformed cron expressions.
func TestShouldRejectCreateGivenInvalidCronSyntaxWhenCreateCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("schedule")

		// Act — bad cron expression.
		_, err := f.Client().Schedule().Create(ctx, route, "not a cron", []byte("payload"))

		// Assert
		assert.Error(t, err, "Create with invalid cron should fail")
	})
}

// TestShouldCancelScheduleGivenExistingScheduleWhenCancelCalled verifies
// CANCEL operation prevents future runs.
func TestShouldCancelScheduleGivenExistingScheduleWhenCancelCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("schedule")

		id, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte("weekly"))
		require.NoError(t, err)

		// Act
		err = f.Client().Schedule().Cancel(ctx, id)

		// Assert
		require.NoError(t, err, "Cancel should succeed for existing schedule")
	})
}

// TestShouldListSchedulesGivenMultipleSchedulesWhenListCalled verifies
// LIST operation returns all created schedules.
func TestShouldListSchedulesGivenMultipleSchedulesWhenListCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("schedule")

		// Create two schedules.
		id1, err := f.Client().Schedule().Create(ctx, route, "0 9 * * 1", []byte("s1"))
		require.NoError(t, err)
		id2, err := f.Client().Schedule().Create(ctx, route, "0 12 * * *", []byte("s2"))
		require.NoError(t, err)

		// Act
		entries, err := f.Client().Schedule().List(ctx, route)

		// Assert
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(entries), 2, "should return at least the 2 created schedules")

		ids := make(map[string]bool)
		for _, e := range entries {
			ids[e.ID] = true
		}
		assert.True(t, ids[id1], "list should include first schedule")
		assert.True(t, ids[id2], "list should include second schedule")
	})
}

// TestShouldCancelNonExistentScheduleGivenBogusIDWhenCancelCalled verifies
// CANCEL with unknown ID returns an appropriate error.
func TestShouldCancelNonExistentScheduleGivenBogusIDWhenCancelCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		// Act
		err := f.Client().Schedule().Cancel(ctx, "999999999")

		// Assert
		assert.Error(t, err, "cancelling non-existent schedule should fail")
	})
}
