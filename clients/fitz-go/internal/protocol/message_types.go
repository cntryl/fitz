package protocol

// MessageType constants per CLIENT_SPEC.md
// Each domain has a reserved range for its operations

const (
	// Control messages (0-99)
	MessageTypeConnect uint16 = 1

	// KV Domain (100-199)
	MessageTypeKvBegin       uint16 = 100
	MessageTypeKvCommit      uint16 = 101
	MessageTypeKvRollback    uint16 = 102
	MessageTypeKvGet         uint16 = 103
	MessageTypeKvPut         uint16 = 104
	MessageTypeKvInsert      uint16 = 105
	MessageTypeKvDelete      uint16 = 106
	MessageTypeKvDeleteRange uint16 = 107
	MessageTypeKvScan        uint16 = 108

	// Queue Domain (200-299)
	MessageTypeQueueEnqueue  uint16 = 200
	MessageTypeQueueReserve  uint16 = 202
	MessageTypeQueueExtend   uint16 = 203
	MessageTypeQueueComplete uint16 = 204

	// RPC Domain (300-399)
	MessageTypeRpcSubscribeWorker   uint16 = 300
	MessageTypeRpcUnsubscribeWorker uint16 = 301
	MessageTypeRpcRequest           uint16 = 302
	MessageTypeRpcResponse          uint16 = 303
	MessageTypeRpcAck               uint16 = 304

	// Lease Domain (400-499)
	MessageTypeLeaseAcquire uint16 = 400
	MessageTypeLeaseRenew   uint16 = 401
	MessageTypeLeaseRelease uint16 = 402
	MessageTypeLeaseQuery   uint16 = 403

	// Notice Domain (500-599)
	MessageTypeNoticePublish       uint16 = 500
	MessageTypeNoticeSubscribe     uint16 = 501
	MessageTypeNoticeUnsubscribe   uint16 = 502
	MessageTypeNoticeNotification  uint16 = 503
	MessageTypeNoticeAcknowledge   uint16 = 504

	// Stream Domain (600-699)
	// NOTE: Server-authoritative numbering is BEGIN=600, APPEND=601, COMMIT=602, etc.
	// The values below were historically misaligned; new constants use server-authoritative numbers.
	MessageTypeStreamAppend      uint16 = 600 // TODO: Should be 601 per server; kept for backward compat
	MessageTypeStreamRead        uint16 = 601 // TODO: Should be 604 per server; kept for backward compat
	MessageTypeStreamBegin       uint16 = 602 // TODO: Should be 600 per server; kept for backward compat
	MessageTypeStreamCommit      uint16 = 603 // TODO: Should be 602 per server; kept for backward compat
	MessageTypeStreamSubscribe   uint16 = 607
	MessageTypeStreamUnsubscribe uint16 = 608
	MessageTypeStreamNotify      uint16 = 609 // Server -> Client only

	// Schedule Domain (700-799)
	MessageTypeScheduleAt          uint16 = 700
	MessageTypeScheduleCron        uint16 = 701
	MessageTypeScheduleList        uint16 = 702
	MessageTypeScheduleSubscribe   uint16 = 703
	MessageTypeScheduleUnsubscribe uint16 = 704
	MessageTypeScheduleNotify      uint16 = 705 // Server -> Client only
)

// RouteDomain returns the domain name for a given MessageType
// Used for routing frames to domain handlers
func RouteDomain(msgType uint16) string {
	switch {
	case msgType >= 100 && msgType <= 199:
		return "kv"
	case msgType >= 200 && msgType <= 299:
		return "queue"
	case msgType >= 300 && msgType <= 399:
		return "rpc"
	case msgType >= 400 && msgType <= 499:
		return "lease"
	case msgType >= 500 && msgType <= 599:
		return "notice"
	case msgType >= 600 && msgType <= 699:
		return "stream"
	case msgType >= 700 && msgType <= 799:
		return "schedule"
	default:
		return "unknown"
	}
}
