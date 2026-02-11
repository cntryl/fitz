package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cntryl/cntryl-go/internal/domains/notice"
	"github.com/cntryl/cntryl-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (Notice domain) ---
// - subscribe to pattern, receive matching publications
// - multiple subscriptions on same pattern both receive
// - publish with no subscribers returns ok
// - unsubscribe stops delivery
// - wildcard patterns match correctly

// TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches
// verifies the basic pub/sub lifecycle: SUBSCRIBE → PUBLISH → NOTIFY.
func TestShouldReceiveNotificationGivenActiveSubscriptionWhenPublishMatches(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("notice")
		received := make(chan notice.NoticeMsg, 1)

		sub, err := f.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg notice.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Act
		err = f.Client().Notice().Publish(ctx, route, []byte("hello"))

		// Assert
		require.NoError(t, err, "Publish should succeed")
		select {
		case msg := <-received:
			assert.Equal(t, route, msg.Route)
			assert.Equal(t, []byte("hello"), msg.Body)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for notification")
		}
	})
}

// TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish
// verifies a single PUBLISH reaches all matching subscriptions.
func TestShouldFanoutToAllSubscribersGivenMultipleSubscriptionsWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("notice")

		var mu sync.Mutex
		var count int

		handler := func(_ context.Context, _ notice.NoticeMsg) error {
			mu.Lock()
			count++
			mu.Unlock()
			return nil
		}

		sub1, err := f.Client().Notice().Subscribe(ctx, route, handler)
		require.NoError(t, err)
		defer sub1.Unsubscribe()

		sub2, err := f.Client().Notice().Subscribe(ctx, route, handler)
		require.NoError(t, err)
		defer sub2.Unsubscribe()

		// Act
		err = f.Client().Notice().Publish(ctx, route, []byte("fanout"))

		// Assert
		require.NoError(t, err)
		time.Sleep(500 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, 2, count, "both subscribers should receive the notification")
		mu.Unlock()
	})
}

// TestShouldSucceedGivenNoSubscribersWhenPublishCalled verifies a PUBLISH
// with no active subscribers still returns success (fire-and-forget semantics).
func TestShouldSucceedGivenNoSubscribersWhenPublishCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("notice")

		// Act — publish with no subscribers.
		err := f.Client().Notice().Publish(ctx, route, []byte("nobody-listening"))

		// Assert
		assert.NoError(t, err, "publish with no subscribers should succeed")
	})
}

// TestShouldStopReceivingGivenUnsubscribeWhenPublish verifies unsubscribe
// removes subscription; subsequent publishes are not delivered.
func TestShouldStopReceivingGivenUnsubscribeWhenPublish(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		route := f.UniqueRoute("notice")
		received := make(chan notice.NoticeMsg, 10)

		sub, err := f.Client().Notice().Subscribe(ctx, route, func(_ context.Context, msg notice.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)

		// Confirm subscription works.
		require.NoError(t, f.Client().Notice().Publish(ctx, route, []byte("before")))
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for first notification")
		}

		// Act — unsubscribe then publish.
		sub.Unsubscribe()
		require.NoError(t, f.Client().Notice().Publish(ctx, route, []byte("after")))

		// Assert — should not receive anything after unsubscribe.
		select {
		case msg := <-received:
			t.Fatalf("received unexpected notification after unsubscribe: %s", msg.Body)
		case <-time.After(500 * time.Millisecond):
			// Good — nothing received.
		}
	})
}

// TestShouldMatchWildcardGivenPatternSubscriptionWhenPublishToConcreteRoute
// verifies wildcard pattern matching per CLIENT_SPEC.md.
func TestShouldMatchWildcardGivenPatternSubscriptionWhenPublishToConcreteRoute(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)

		realm := f.UniqueRealm()
		area := f.UniqueArea()
		pattern := "notice://" + realm + "/" + area + "/*"
		concrete := "notice://" + realm + "/" + area + "/events"

		received := make(chan notice.NoticeMsg, 1)
		sub, err := f.Client().Notice().Subscribe(ctx, pattern, func(_ context.Context, msg notice.NoticeMsg) error {
			received <- msg
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		// Act
		err = f.Client().Notice().Publish(ctx, concrete, []byte("wildcard-test"))

		// Assert
		require.NoError(t, err)
		select {
		case msg := <-received:
			assert.Equal(t, concrete, msg.Route)
			assert.Equal(t, []byte("wildcard-test"), msg.Body)
		case <-time.After(5 * time.Second):
			t.Fatal("wildcard subscription did not match concrete publish")
		}
	})
}
