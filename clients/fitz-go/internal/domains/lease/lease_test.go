package lease

import (
	"encoding/binary"
	"testing"

	"github.com/cntryl/fitz-go/internal/core/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldEncodeLeaseAcquireRequest tests ACQUIRE operation encoding.
func TestShouldEncodeLeaseAcquireRequest(t *testing.T) {
	t.Run("valid route and ttl", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks"
		ttlSecs := uint64(300)

		// Act
		payload, err := EncodeLeaseAcquire(route, ttlSecs)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
		// Verify route length prefix
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})

	t.Run("zero ttl", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks"

		// Act
		payload, err := EncodeLeaseAcquire(route, 0)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Zero is valid for TTL (server may use default)
	})

	t.Run("max ttl", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeLeaseAcquire("path", 0xFFFFFFFFFFFFFFFF)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Max uint64 should be accepted
	})
}

// TestShouldEncodeLeaseRenewRequest tests RENEW operation encoding.
func TestShouldEncodeLeaseRenewRequest(t *testing.T) {
	t.Run("valid token and ttl", func(t *testing.T) {
		// Arrange
		token := uint64(0x0123456789ABCDEF)
		ttlSecs := uint64(600)

		// Act
		payload, err := EncodeLeaseRenew("resource", token, ttlSecs)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 24) // Sufficient for all fields
	})

	t.Run("zero token", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeLeaseRenew("path", 0, 300) // Zero token (invalid, but tests encoding)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
	})
}

// TestShouldEncodeLeaseReleaseRequest tests RELEASE operation encoding.
func TestShouldEncodeLeaseReleaseRequest(t *testing.T) {
	t.Run("valid token", func(t *testing.T) {
		// Arrange
		token := uint64(0xFEDCBA9876543210)

		// Act
		payload, err := EncodeLeaseRelease("resource", token)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 8)
	})

	t.Run("empty resource", func(t *testing.T) {
		// Arrange & Act
		payload, err := EncodeLeaseRelease("", 12345)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		// Empty resource is encoded as 0-length string
	})
}

// TestShouldEncodeLeaseQueryRequest tests QUERY operation encoding.
func TestShouldEncodeLeaseQueryRequest(t *testing.T) {
	t.Run("query with route", func(t *testing.T) {
		// Arrange
		route := "lease://acme/app/locks/resource1"

		// Act
		payload, err := EncodeLeaseQuery(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Greater(t, len(payload), 4)
		// Verify route is encoded correctly
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})

	t.Run("query with complex route", func(t *testing.T) {
		// Arrange
		route := "lease://org.example.com/system/distributed-locks"

		// Act
		payload, err := EncodeLeaseQuery(route)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, payload)
		routeLen := binary.BigEndian.Uint32(payload[0:4])
		assert.Equal(t, uint32(len(route)), routeLen)
	})
}

// TestShouldMapLeaseErrors tests error mapping.
func TestShouldMapLeaseErrors(t *testing.T) {
	t.Run("map held error", func(t *testing.T) {
		// Arrange
		errMsg := "the lease is held by another owner"

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseHeld, mapped)
	})

	t.Run("map invalid fence error", func(t *testing.T) {
		// Arrange
		errMsg := "invalid fencing token provided"

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrInvalidFence, mapped)
	})

	t.Run("map expired error", func(t *testing.T) {
		// Arrange
		errMsg := "lease has expired"

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseExpired, mapped)
	})

	t.Run("map not found error", func(t *testing.T) {
		// Arrange
		errMsg := "resource not found"

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.Equal(t, ErrLeaseNotFound, mapped)
	})

	t.Run("unknown error returns wrapped message", func(t *testing.T) {
		// Arrange
		errMsg := "some unknown error condition"

		// Act
		mapped := mapLeaseError(errMsg)

		// Assert
		assert.NotNil(t, mapped)
		assert.Equal(t, errMsg, mapped.Error())
	})
}

// Benchmarks

func BenchmarkEncodeLeaseAcquire(b *testing.B) {
	b.Run("short route", func(b *testing.B) {
		route := "lease://a/b/c"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeLeaseAcquire(route, 300)
		}
	})

	b.Run("long route", func(b *testing.B) {
		route := "lease://prod/locks/critical-section"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeLeaseAcquire(route, 300)
		}
	})
}

func BenchmarkEncodeLeaseRenew(b *testing.B) {
	b.Run("standard", func(b *testing.B) {
		token := uint64(0x123456789ABCDEF0)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EncodeLeaseRenew("resource", token, 600)
		}
	})
}

func BenchmarkEncodeLeaseRelease(b *testing.B) {
	token := uint64(0xFEDCBA9876543210)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeLeaseRelease("resource", token)
	}
}

func BenchmarkEncodeLeaseQuery(b *testing.B) {
	route := "lease://acme/app/locks/resource"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeLeaseQuery(route)
	}
}

func BenchmarkParseLeaseAcquireResponse(b *testing.B) {
	// [status=0][response_type=0][u64 BE fencing_token]
	payload := make([]byte, 1+1+8)
	payload[0] = 0
	payload[1] = 0 // Acquired
	binary.BigEndian.PutUint64(payload[2:10], 0x123456789ABCDEF0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 9 {
			_ = remaining[0]
			_ = binary.BigEndian.Uint64(remaining[1:9])
		}
	}
}

func BenchmarkParseLeaseQueryResponse(b *testing.B) {
	// [status=0][has_holder=0][u32 pending_waiters=0]
	payload := make([]byte, 1+1+4)
	payload[0] = 0
	payload[1] = 0
	binary.BigEndian.PutUint32(payload[2:6], 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		success, remaining, _ := connection.ParseStandardResponse(payload)
		if success && len(remaining) >= 5 {
			_ = remaining[0]
			_ = binary.BigEndian.Uint32(remaining[1:5])
		}
	}
}
