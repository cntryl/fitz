package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (Stream domain) ---
// - begin/append/commit cycle succeeds
// - read returns records in offset order
// - read beyond watermark fails appropriately
// - append with mismatched expected_offset fails
// - rollback discards uncommitted appends
// - multiple sessions can read concurrently

// TestShouldAppendRecordsGivenValidSessionWhenAppendCalled verifies the
// basic stream lifecycle: BEGIN → APPEND → COMMIT.
func TestShouldAppendRecordsGivenValidSessionWhenAppendCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("stream")

		_, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err, "Begin should succeed")

		// Act — append two records.
		offset1, err := f.Client().Stream().Append(ctx, route, []byte("record-1"), nil)
		require.NoError(t, err, "first Append should succeed")

		offset2, err := f.Client().Stream().Append(ctx, route, []byte("record-2"), nil)
		require.NoError(t, err, "second Append should succeed")

		err = f.Client().Stream().Commit(ctx, route)

		// Assert
		require.NoError(t, err, "Commit should succeed")
		assert.Less(t, offset1, offset2, "offsets should be strictly increasing")
	})
}

// TestShouldReadRecordsInOrderGivenOffsetRangeWhenReadCalled verifies READ
// operation returns records in strict offset order.
func TestShouldReadRecordsInOrderGivenOffsetRangeWhenReadCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("stream")

		_, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			_, err := f.Client().Stream().Append(ctx, route, []byte{byte(i)}, nil)
			require.NoError(t, err)
		}
		require.NoError(t, f.Client().Stream().Commit(ctx, route))

		// Act — read from offset 0, limit 10.
		iter, err := f.Client().Stream().ReadResource(ctx, route, 0, 10)
		require.NoError(t, err)
		defer iter.Close()

		// Assert — offsets should be ascending.
		var offsets []uint64
		for iter.Next() {
			offsets = append(offsets, iter.Value().Offset)
		}
		require.NoError(t, iter.Err())
		for i := 1; i < len(offsets); i++ {
			assert.Greater(t, offsets[i], offsets[i-1], "offsets must be strictly increasing")
		}
	})
}

// TestShouldRejectAppendGivenMismatchedOffsetWhenOptimisticConcurrency verifies
// optimistic concurrency control using expected_offset.
func TestShouldRejectAppendGivenMismatchedOffsetWhenOptimisticConcurrency(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("stream")

		_, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)

		_, err = f.Client().Stream().Append(ctx, route, []byte("first"), nil)
		require.NoError(t, err)
		require.NoError(t, f.Client().Stream().Commit(ctx, route))

		// Act — append with a wrong expected_offset.
		_, err = f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)

		wrongOffset := uint64(99999)
		_, err = f.Client().Stream().Append(ctx, route, []byte("conflict"), &wrongOffset)

		// Assert
		assert.Error(t, err, "append with mismatched expected_offset should fail")
	})
}

// TestShouldRollbackUncommittedAppendsGivenActiveSessionWhenRollbackCalled
// verifies ROLLBACK discards uncommitted appends; they are not readable.
func TestShouldRollbackUncommittedAppendsGivenActiveSessionWhenRollbackCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("stream")

		_, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)

		_, err = f.Client().Stream().Append(ctx, route, []byte("ephemeral"), nil)
		require.NoError(t, err)

		// Act
		err = f.Client().Stream().Rollback(ctx, route)

		// Assert
		require.NoError(t, err, "Rollback should succeed")

		// Read should return no records.
		iter, err := f.Client().Stream().ReadResource(ctx, route, 0, 10)
		require.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}
		assert.Equal(t, 0, count, "rolled-back records should not be visible")
	})
}

// TestShouldReturnLastRecordGivenExistingStreamWhenLastCalled verifies
// LAST operation returns the most recent record.
func TestShouldReturnLastRecordGivenExistingStreamWhenLastCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("stream")

		_, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = f.Client().Stream().Append(ctx, route, []byte("first"), nil)
		require.NoError(t, err)
		_, err = f.Client().Stream().Append(ctx, route, []byte("last-one"), nil)
		require.NoError(t, err)
		require.NoError(t, f.Client().Stream().Commit(ctx, route))

		// Act
		rec, err := f.Client().Stream().Last(ctx, route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, rec)
		assert.Equal(t, []byte("last-one"), rec.Body)
	})
}

// TestShouldGetMetadataGivenExistingStreamWhenGetMetadataCalled verifies
// GET_METADATA operation returns stream metadata.
func TestShouldGetMetadataGivenExistingStreamWhenGetMetadataCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("stream")

		// Ensure stream exists.
		_, err := f.Client().Stream().Begin(ctx, route)
		require.NoError(t, err)
		_, err = f.Client().Stream().Append(ctx, route, []byte("data"), nil)
		require.NoError(t, err)
		require.NoError(t, f.Client().Stream().Commit(ctx, route))

		// Act
		meta, err := f.Client().Stream().GetMetadata(ctx, route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, meta, "metadata should not be nil")
	})
}
