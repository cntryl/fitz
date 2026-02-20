// Package errors provides domain-agnostic error handling for the Fitz client.
//
// Domain packages (kv, lease, notice, queue, rpc, schedule, stream) expose
// sentinel errors (e.g. ErrLeaseHeld, ErrQueueFull) and map server messages
// to them so callers can use errors.Is for handling. ErrorCode and DomainError
// in this package are for optional code-based handling when the server sends
// numeric error codes on the wire; testkit.AssertDomainErrorCode uses them.
package errors

import (
	"fmt"
)

// Error code ranges by domain (from Fitz server)
const (
	// KV Domain (1000-1099)
	KvTransactionNotFound = 1001
	KvInvalidMode         = 1002
	KvKeyNotFound         = 1003
	KvIsolationConflict   = 1004
	KvWriteInReadonly     = 1005
	KvKeyExists           = 1006
	KvInvalidRoute        = 1007
	KvRealmMismatch       = 1008
	KvBackendError        = 1009
	KvTransactionAborted  = 1010

	// Stream Domain (2000-2099)
	StreamConcurrencyConflict = 2001
	StreamOffsetTooFarAhead   = 2002
	StreamInvalidReadBound    = 2003
	StreamReadBeyondWatermark = 2004
	StreamResourceNotFound    = 2005
	StreamInvalidSubscription = 2010
	StreamSubscriptionLimit   = 2011

	// Notice Domain (3000-3099)
	NoticeInvalidRoute      = 3001
	NoticeInvalidPattern    = 3002
	NoticeSubscriptionLimit = 3003
	NoticeTransportClosed   = 3004

	// Queue Domain (4000-4099)
	QueueInvalidToken    = 4001
	QueueLeaseExpired    = 4002
	QueueMessageNotFound = 4003
	QueueNotFound        = 4004
	QueueFull            = 4005 // Backpressure signal

	// Lease Domain (5000-5099)
	LeaseHeld         = 5001
	LeaseInvalidFence = 5002
	LeaseExpired      = 5003
	LeaseNotFound     = 5004

	// RPC Domain (6000-6099). 6004 = no workers for route or timeout before any reply (per CLIENT_ACCEPTANCE_CRITERIA).
	RpcTimeout             = 6001
	RpcWorkerNotFound      = 6002
	RpcBackpressure        = 6003 // Backpressure signal
	RpcRouteNotRegistered  = 6004
	RpcCorrelationNotFound = 6005

	// Schedule Domain (7000-7099)
	ScheduleNotFound            = 7001
	ScheduleInvalidCron         = 7002
	ScheduleLimit               = 7003
	ScheduleParseError          = 7004
	ScheduleInvalidTarget       = 7005
	ScheduleInvalidSubscription = 7006
	ScheduleSubscriptionLimit   = 7007
)

// IsBackpressure returns true if the error code indicates backpressure
func IsBackpressure(code uint32) bool {
	return code == QueueFull || code == RpcBackpressure
}

// ErrorCode represents a Fitz protocol error
type ErrorCode uint32

func (e ErrorCode) String() string {
	switch e {
	// KV errors
	case KvTransactionNotFound:
		return "transaction_not_found"
	case KvInvalidMode:
		return "invalid_mode"
	case KvKeyNotFound:
		return "key_not_found"
	case KvIsolationConflict:
		return "isolation_conflict"
	case KvWriteInReadonly:
		return "write_in_readonly"
	case KvKeyExists:
		return "key_exists"
	case KvInvalidRoute:
		return "invalid_route"
	case KvRealmMismatch:
		return "realm_mismatch"
	case KvBackendError:
		return "backend_error"
	case KvTransactionAborted:
		return "transaction_aborted"

	// Stream errors
	case StreamConcurrencyConflict:
		return "concurrency_conflict"
	case StreamOffsetTooFarAhead:
		return "offset_too_far_ahead"
	case StreamInvalidReadBound:
		return "invalid_read_bound"
	case StreamReadBeyondWatermark:
		return "read_beyond_watermark"
	case StreamResourceNotFound:
		return "stream_resource_not_found"
	case StreamInvalidSubscription:
		return "stream_invalid_subscription"
	case StreamSubscriptionLimit:
		return "stream_subscription_limit"

	// Notice errors
	case NoticeInvalidRoute:
		return "notice_invalid_route"
	case NoticeInvalidPattern:
		return "notice_invalid_pattern"
	case NoticeSubscriptionLimit:
		return "notice_subscription_limit"
	case NoticeTransportClosed:
		return "notice_transport_closed"

	// Queue errors
	case QueueInvalidToken:
		return "queue_invalid_token"
	case QueueLeaseExpired:
		return "queue_lease_expired"
	case QueueMessageNotFound:
		return "queue_message_not_found"
	case QueueNotFound:
		return "queue_not_found"
	case QueueFull:
		return "queue_full"

	// Lease errors
	case LeaseHeld:
		return "lease_held"
	case LeaseInvalidFence:
		return "lease_invalid_fence"
	case LeaseExpired:
		return "lease_expired"
	case LeaseNotFound:
		return "lease_not_found"

	// RPC errors
	case RpcTimeout:
		return "rpc_timeout"
	case RpcWorkerNotFound:
		return "rpc_worker_not_found"
	case RpcBackpressure:
		return "rpc_backpressure"
	case RpcRouteNotRegistered:
		return "rpc_route_not_registered"
	case RpcCorrelationNotFound:
		return "rpc_correlation_not_found"

	// Schedule errors
	case ScheduleNotFound:
		return "schedule_not_found"
	case ScheduleInvalidCron:
		return "schedule_invalid_cron"
	case ScheduleLimit:
		return "schedule_limit"
	case ScheduleParseError:
		return "schedule_parse_error"
	case ScheduleInvalidTarget:
		return "schedule_invalid_target"
	case ScheduleInvalidSubscription:
		return "schedule_invalid_subscription"
	case ScheduleSubscriptionLimit:
		return "schedule_subscription_limit"

	default:
		return fmt.Sprintf("unknown_error_%d", e)
	}
}

// Error represents a domain error with numeric code
type DomainError struct {
	Code    ErrorCode
	Message string
}

func (e *DomainError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewDomainError creates a new domain error
func NewDomainError(code uint32, message string) *DomainError {
	return &DomainError{
		Code:    ErrorCode(code),
		Message: message,
	}
}
