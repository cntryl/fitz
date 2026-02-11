package lease

import (
	"errors"
	"strings"
)

// Wire opcodes for Lease domain (per CLIENT_SPEC.md 400–403).
const (
	LeaseAcquire uint16 = 400
	LeaseRenew   uint16 = 401
	LeaseRelease uint16 = 402
	LeaseQuery   uint16 = 403
)

// Domain-specific errors (mapped from broker error responses).
var (
	ErrLeaseHeld     = errors.New("lease held")
	ErrInvalidFence  = errors.New("invalid fencing token")
	ErrLeaseExpired  = errors.New("lease expired")
	ErrLeaseNotFound = errors.New("lease not found")
)

// mapLeaseError maps a broker error message to a domain-specific Go error.
func mapLeaseError(msg string) error {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "held"):
		return ErrLeaseHeld
	case strings.Contains(lower, "invalid") && (strings.Contains(lower, "fence") || strings.Contains(lower, "token")):
		return ErrInvalidFence
	case strings.Contains(lower, "expired"):
		return ErrLeaseExpired
	case strings.Contains(lower, "not found"):
		return ErrLeaseNotFound
	default:
		return errors.New(msg)
	}
}
