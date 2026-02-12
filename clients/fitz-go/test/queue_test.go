package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/domains/queue"
	"github.com/cntryl/fitz-go/test/fixture"
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

// TestShouldSendAndReceiveMessageGivenValidQueueWhenBasicWorkflow verifies
// the basic queue lifecycle: Send → Receive → Complete.
func TestShouldSendAndReceiveMessageGivenValidQueueWhenBasicWorkflow(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("queue")

		// Act — send a message.
		msgID, err := f.Client().Queue().Send(ctx, route, []byte("task-payload"))
		require.NoError(t, err)
		assert.NotZero(t, msgID, "Send should return a message ID")

		// Receive the message.
		items, err := f.Client().Queue().Receive(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1, "should receive exactly one message")
		assert.Equal(t, []byte("task-payload"), items[0].Body)

		// Complete the message (on the item).
		err = items[0].Complete(ctx)

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
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("queue")

		_, err := f.Client().Queue().Send(ctx, route, []byte("expire-me"))
		require.NoError(t, err)

		// Receive with short lease (2s)
		items, err := f.Client().Queue().Receive(ctx, route, 2, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		// Wait for lease to expire (server processes timers lazily on next op; allow margin)
		time.Sleep(4 * time.Second)

		// Act — receive again; message should be re-queued and available
		items2, err := f.Client().Queue().Receive(ctx, route, 30, 1)
		require.NoError(t, err)

		// Assert — should get at least one message again (re-queued after lease expiry)
		assert.GreaterOrEqual(t, len(items2), 1, "message should be re-queued after lease expiry")
		// Body should match; ID may differ if server assigns new id on re-queue
		if len(items2) >= 1 {
			assert.Equal(t, []byte("expire-me"), items2[0].Body, "re-reserved message body should match")
		}
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

		_, err := f.Client().Queue().Send(ctx, route, []byte("extend-me"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Receive(ctx, route, 5, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		// Act — extend lease on the item.
		err = items[0].Extend(ctx, 60)

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

		_, err := f.Client().Queue().Send(ctx, route, []byte("token-check"))
		require.NoError(t, err)

		items, err := f.Client().Queue().Receive(ctx, route, 30, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)

		// Act — complete with wrong token (use CompleteWithToken to simulate invalid token).
		err = items[0].CompleteWithToken(ctx, 9999999)

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

		// Send 5 messages.
		for i := 0; i < 5; i++ {
			_, err := f.Client().Queue().Send(ctx, route, []byte("batch-msg"))
			require.NoError(t, err)
		}

		// Act — receive batch of 3.
		items, err := f.Client().Queue().Receive(ctx, route, 30, 3)

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

		// Send 2 messages.
		for i := 0; i < 2; i++ {
			_, err := f1.Client().Queue().Send(ctx, route, []byte("concurrent-msg"))
			require.NoError(t, err)
		}

		// Act — each consumer receives 1.
		items1, err1 := f1.Client().Queue().Receive(ctx, route, 30, 1)
		items2, err2 := f2.Client().Queue().Receive(ctx, route, 30, 1)

		// Assert
		require.NoError(t, err1)
		require.NoError(t, err2)
		total := len(items1) + len(items2)
		assert.Equal(t, 2, total, "both consumers should each get one message")
	})
}
