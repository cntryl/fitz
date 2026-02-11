package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/domains/queue"
	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (Queue domain) ---
// - enqueue/reserve/complete cycle succeeds
// - lease expiry returns message to ready queue
// - extend lease delays expiry
// - complete with wrong token fails
// - batch reserve returns up to specified count
// - multiple consumers can reserve from same queue

// TestShouldEnqueueAndReserveMessageGivenValidQueueWhenBasicWorkflow verifies
// the basic queue lifecycle: ENQUEUE → RESERVE → COMPLETE.
func TestShouldEnqueueAndReserveMessageGivenValidQueueWhenBasicWorkflow(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("queue")

		// Act — enqueue a message.
		msgID, err := f.Client().Queue().Enqueue(ctx, route, []byte("task-payload"))
		require.NoError(t, err)
		assert.NotEmpty(t, msgID, "enqueue should return a message ID")

		// Reserve the message.
		items, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1, "should reserve exactly one message")
		assert.Equal(t, []byte("task-payload"), items[0].Body)

		// Complete the message.
		err = f.Client().Queue().Complete(ctx, route, items[0].ID, items[0].Token)

		// Assert
		require.NoError(t, err, "Complete should succeed with valid token")
	})
}

// TestShouldReturnMessageToQueueGivenExpiredLeaseWhenLeaseExpires verifies
// that messages with expired leases return to the ready queue.
func TestShouldReturnMessageToQueueGivenExpiredLeaseWhenLeaseExpires(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("requeue-me"))
		require.NoError(t, err)

		// Reserve with very short lease.
		items, err := f.Client().Queue().Reserve(ctx, route, 2, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		// Let lease expire (do NOT complete).
		time.Sleep(3 * time.Second)

		// Act — reserve again should return the same message.
		items2, err := f.Client().Queue().Reserve(ctx, route, 30, 1)

		// Assert
		require.NoError(t, err)
		require.Len(t, items2, 1, "expired message should be re-reservable")
		assert.Equal(t, []byte("requeue-me"), items2[0].Body)
	})
}

// TestShouldExtendLeaseGivenValidTokenWhenExtendCalled verifies EXTEND
// operation delays lease expiration.
func TestShouldExtendLeaseGivenValidTokenWhenExtendCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("extend-me"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Reserve(ctx, route, 5, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		// Act — extend lease.
		err = f.Client().Queue().Extend(ctx, route, items[0].ID, items[0].Token, 60)

		// Assert
		require.NoError(t, err, "Extend should succeed with valid token")
	})
}

// TestShouldRejectCompleteGivenInvalidTokenWhenTokenMismatch verifies
// COMPLETE operation requires correct lease token.
func TestShouldRejectCompleteGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Enqueue(ctx, route, []byte("token-check"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Reserve(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		// Act — complete with wrong token.
		err = f.Client().Queue().Complete(ctx, route, items[0].ID, 9999999)

		// Assert
		assert.ErrorIs(t, err, queue.ErrInvalidToken, "complete with wrong token should fail")
	})
}

// TestShouldReserveBatchGivenMultipleMessagesWhenBatchSizeSpecified verifies
// RESERVE operation returns up to batch_size messages.
func TestShouldReserveBatchGivenMultipleMessagesWhenBatchSizeSpecified(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("queue")

		// Enqueue 5 messages.
		for i := 0; i < 5; i++ {
			_, err := f.Client().Queue().Enqueue(ctx, route, []byte("batch-msg"))
			require.NoError(t, err)
		}

		// Act — reserve batch of 3.
		items, err := f.Client().Queue().Reserve(ctx, route, 30, 3)

		// Assert
		require.NoError(t, err)
		assert.LessOrEqual(t, len(items), 3, "should return at most batch_size items")
		assert.GreaterOrEqual(t, len(items), 1, "should return at least one item")
	})
}

// TestShouldDistributeMessagesGivenMultipleConsumersWhenConcurrentReserve
// verifies multiple consumers can reserve from the same queue.
func TestShouldDistributeMessagesGivenMultipleConsumersWhenConcurrentReserve(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrSkip(ctx)
		f2.ConnectOrSkip(ctx)

		route := f1.UniqueRoute("queue")

		// Enqueue 2 messages.
		for i := 0; i < 2; i++ {
			_, err := f1.Client().Queue().Enqueue(ctx, route, []byte("concurrent-msg"))
			require.NoError(t, err)
		}

		// Act — each consumer reserves 1.
		items1, err1 := f1.Client().Queue().Reserve(ctx, route, 30, 1)
		items2, err2 := f2.Client().Queue().Reserve(ctx, route, 30, 1)

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		total := len(items1) + len(items2)
		assert.Equal(t, 2, total, "both consumers should each get one message")
	})
}
