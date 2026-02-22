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

	// Queue Domain (200-299). 201 = ENQUEUE_BATCH is reserved per CLIENT_SPEC; do not use.
	MessageTypeQueueEnqueue     uint16 = 200
	MessageTypeQueueReserve     uint16 = 202
	MessageTypeQueueExtend      uint16 = 203
	MessageTypeQueueComplete    uint16 = 204
	MessageTypeQueueSubscribe   uint16 = 207
	MessageTypeQueueUnsubscribe uint16 = 208
	MessageTypeQueueNotify      uint16 = 209 // Server -> Client only

	// RPC Domain (300-399)
	MessageTypeRpcSubscribeWorker   uint16 = 300
	MessageTypeRpcUnsubscribeWorker uint16 = 301
	MessageTypeRpcRequest           uint16 = 302
	MessageTypeRpcResponse          uint16 = 303
	MessageTypeRpcAck               uint16 = 304

	// Lease Domain (400-499)
	MessageTypeLeaseAcquire     uint16 = 400
	MessageTypeLeaseRenew       uint16 = 401
	MessageTypeLeaseRelease     uint16 = 402
	MessageTypeLeaseQuery       uint16 = 403
	MessageTypeLeaseSubscribe   uint16 = 407
	MessageTypeLeaseUnsubscribe uint16 = 408
	MessageTypeLeaseNotify      uint16 = 409 // Server -> Client only

	// Notice Domain (500-599)
	MessageTypeNoticePublish        uint16 = 500
	MessageTypeNoticeSubscribe      uint16 = 501
	MessageTypeNoticeUnsubscribe    uint16 = 502
	MessageTypeNoticeUnsubscribeAll uint16 = 503
	MessageTypeNoticeNotify         uint16 = 504 // Server -> Client only

	// Stream Domain (600-699) — server-authoritative numbering
	MessageTypeStreamBegin       uint16 = 600
	MessageTypeStreamAppend      uint16 = 601
	MessageTypeStreamCommit      uint16 = 602
	MessageTypeStreamRollback    uint16 = 603
	MessageTypeStreamRead        uint16 = 604
	MessageTypeStreamLast        uint16 = 605
	MessageTypeStreamGetMetadata uint16 = 606
	MessageTypeStreamSubscribe   uint16 = 607
	MessageTypeStreamUnsubscribe uint16 = 608
	MessageTypeStreamNotify      uint16 = 609 // Server -> Client only

	// Schedule Domain (700-799)
	MessageTypeScheduleCreate      uint16 = 700
	MessageTypeScheduleCancel      uint16 = 701
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
