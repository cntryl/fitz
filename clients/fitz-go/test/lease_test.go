package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cntryl/fitz-go/internal/domains/lease"
	"github.com/cntryl/fitz-go/test/fixture"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Acceptance criteria from CLIENT_SPEC.md (Lease domain) ---
// - acquire succeeds when free, fails when held
// - renew with valid token extends TTL
// - renew with invalid token fails
// - release with valid token releases
// - release with invalid token fails
// - expired lease acquirable by new owner

// TestShouldAcquireLeaseGivenAvailableLeaseWhenAcquireCalled verifies
// ACQUIRE operation succeeds when lease is free.
func TestShouldAcquireLeaseGivenAvailableLeaseWhenAcquireCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		// Act
		l, err := f.Client().Lease().Acquire(ctx, route, 30)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, l, "lease should be granted when free")
		assert.NotEmpty(t, l.Token, "token should be non-empty on successful acquire")
		assert.Greater(t, l.ExpiresAt, time.Now().Unix()-1, "ExpiresAt should be in the future")
	})
}

// TestShouldRejectAcquireGivenHeldLeaseWhenAcquireCalled verifies
// ACQUIRE operation fails when lease is already held by a different owner.
func TestShouldRejectAcquireGivenHeldLeaseWhenAcquireCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange — two sessions, same lease route
		f1 := fixture.NewTestFixture(t, transport)
		f2 := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f1.ConnectOrSkip(ctx)
		f2.ConnectOrSkip(ctx)
		route := f1.UniqueRoute("lease")

		// First session acquires
		_, err := f1.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)

		// Act — second session tries to acquire same lease
		l2, err2 := f2.Client().Lease().Acquire(ctx, route, 30)

		// Assert — should fail or not be granted
		if err2 != nil {
			assert.ErrorIs(t, err2, lease.ErrLeaseHeld)
			return
		}
		assert.Nil(t, l2, "second acquire should not be granted when lease held by another session")
	})
}

// TestShouldExtendTTLGivenValidTokenWhenRenewCalled verifies RENEW
// operation extends lease TTL with valid fencing token.
func TestShouldExtendTTLGivenValidTokenWhenRenewCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		l, err := f.Client().Lease().Acquire(ctx, route, 10)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Act
		newExpiry, err := l.Extend(ctx, 60)

		// Assert
		require.NoError(t, err)
		assert.Greater(t, newExpiry, time.Now().Unix(), "extended expiry should be in the future")
	})
}

// TestShouldRejectRenewGivenInvalidTokenWhenTokenMismatch verifies RENEW
// operation rejects invalid fencing tokens.
func TestShouldRejectRenewGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Act — extend with a fabricated (wrong) token.
		_, err = l.ExtendWithToken(ctx, []byte("wrong-token"), 60)

		// Assert
		require.Error(t, err, "extend with invalid token should fail")
	})
}

// TestShouldReleaseLeaseGivenValidTokenWhenReleaseCalled verifies RELEASE
// operation frees the lease with valid fencing token.
func TestShouldReleaseLeaseGivenValidTokenWhenReleaseCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Act
		err = l.Release(ctx)

		// Assert
		require.NoError(t, err)

		// Re-acquire should succeed after release.
		l2, err2 := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err2)
		require.NotNil(t, l2, "lease should be acquirable after release")
		assert.NotEmpty(t, l2.Token)
	})
}

// TestShouldRejectReleaseGivenInvalidTokenWhenTokenMismatch verifies RELEASE
// operation rejects invalid fencing tokens.
func TestShouldRejectReleaseGivenInvalidTokenWhenTokenMismatch(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Act — release with wrong token.
		err = l.ReleaseWithToken(ctx, []byte("wrong-token"))

		// Assert
		require.Error(t, err, "release with invalid token should fail")
	})
}

// TestShouldExpireLeaseGivenTTLElapsedWhenNoRenew verifies expired leases
// automatically release and become acquirable by new owners.
func TestShouldExpireLeaseGivenTTLElapsedWhenNoRenew(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		l, err := f.Client().Lease().Acquire(ctx, route, 2)
		require.NoError(t, err)
		require.NotNil(t, l)
		require.NotEmpty(t, l.Token)

		// Wait for TTL to expire.
		time.Sleep(3 * time.Second)

		// Act — re-acquire after expiry should succeed.
		l2, err2 := f.Client().Lease().Acquire(ctx, route, 30)

		// Assert
		require.NoError(t, err2)
		require.NotNil(t, l2, "expired lease should be re-acquirable")
		assert.NotEmpty(t, l2.Token)
	})
}

// TestShouldQueryLeaseStatusGivenExistingLeaseWhenQueryCalled verifies
// QUERY operation returns current lease holder information.
func TestShouldQueryLeaseStatusGivenExistingLeaseWhenQueryCalled(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")

		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Act
		info, err := f.Client().Lease().Query(ctx, route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.True(t, info.Held, "query should report lease as held")
		// Per CLIENT_SPEC QUERY response: server returns owner_id, ttl_remaining_secs, pending_waiters (not token)
		assert.True(t, info.TTLRemainingSecs > 0 || info.OwnerID != "" || len(info.Token) > 0, "query should return holder info")
	})
}

// TestShouldNotifyGivenSubscriptionWhenLeaseReleased verifies Subscribe delivers
// change notifications on release for matching routes.
func TestShouldNotifyGivenSubscriptionWhenLeaseReleased(t *testing.T) {
	fixture.RunWithBothTransports(t, func(t *testing.T, transport fixture.TransportType) {
		// Arrange
		f := fixture.NewTestFixture(t, transport)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		f.ConnectOrSkip(ctx)
		route := f.UniqueRoute("lease")
		notifications := make(chan lease.ChangeNotification, 1)

		sub, err := f.Client().Lease().Subscribe(ctx, route, func(_ context.Context, notif lease.ChangeNotification) error {
			notifications <- notif
			return nil
		})
		require.NoError(t, err)
		defer sub.Unsubscribe()

		l, err := f.Client().Lease().Acquire(ctx, route, 30)
		require.NoError(t, err)

		// Act
		require.NoError(t, l.Release(ctx))

		// Assert
		select {
		case notif := <-notifications:
			assert.Equal(t, route, notif.Route)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for lease change notification")
		}
	})
}
