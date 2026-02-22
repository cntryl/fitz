package rpc

import (
	"bytes"
	"errors"
	"strings"

	"github.com/cntryl/fitz-go/internal/core/encoding"
)

// Wire opcodes for RPC domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	RPCSubscribeWorker   uint16 = 300
	RPCUnsubscribeWorker uint16 = 301
	RPCRequest           uint16 = 302
	RPCResponse          uint16 = 303
	RPCAck               uint16 = 304
)

// Domain-specific errors. Returned when Call fails or the server rejects a request.
//   - ErrNoWorkers: no worker is registered for the route (or route not registered).
//   - ErrRPCTimeout: the request timed out before receiving a response.
//   - ErrRPCBackpressure: server backpressure.
var (
	ErrNoWorkers       = errors.New("no workers available")
	ErrRPCTimeout      = errors.New("rpc timeout")
	ErrRPCBackpressure = errors.New("rpc backpressure")
)

// mapRPCError maps a broker error message to a domain-specific Go error.
func mapRPCError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "no workers"):
		return ErrNoWorkers
	case strings.Contains(l, "timeout"):
		return ErrRPCTimeout
	case strings.Contains(l, "backpressure"):
		return ErrRPCBackpressure
	default:
		return errors.New(msg)
	}
}

// EncodeRPCSubscribeWorker encodes an RPC SUBSCRIBE_WORKER request per CLIENT_SPEC.md.
// Wire format: [string worker_route]
func EncodeRPCSubscribeWorker(workerRoute string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
	}), nil
}

// EncodeRPCUnsubscribeWorker encodes an RPC UNSUBSCRIBE_WORKER request per CLIENT_SPEC.md.
// Wire format: [string worker_route]
func EncodeRPCUnsubscribeWorker(workerRoute string) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
	}), nil
}

// EncodeRPCRequest encodes an RPC REQUEST per CLIENT_SPEC.md.
// Wire format: [bytes correlation_id][string route][string reply_route][bytes body]
// where bytes = [u32 BE length][data] and string = [u32 BE length][data]
func EncodeRPCRequest(correlationID [16]byte, route string, replyRoute string, body []byte) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteBytes(buf, correlationID[:])
		encoding.WriteRoute(buf, route)
		encoding.WriteRoute(buf, replyRoute)
		encoding.WriteBytes(buf, body)
	}), nil
}

// EncodeRPCResponse encodes an RPC RESPONSE per CLIENT_SPEC.md.
// Wire format: [bytes correlation_id][u64 sequence][bytes body][u8 stream_end]
// where bytes = [u32 BE length][data]
func EncodeRPCResponse(correlationID [16]byte, sequence uint64, body []byte, streamEnd bool) ([]byte, error) {
	return encoding.EncodeWithBuffer(func(buf *bytes.Buffer) {
		encoding.WriteBytes(buf, correlationID[:])
		encoding.WriteU64(buf, sequence)
		encoding.WriteBytes(buf, body)
		if streamEnd {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}), nil
}

// Payload writer helpers for zero-copy frame encoding

func rpcSubscribeWorkerPayloadWriter(workerRoute string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
	}
}

func rpcUnsubscribeWorkerPayloadWriter(workerRoute string) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteRoute(buf, workerRoute)
	}
}

func rpcRequestPayloadWriter(correlationID [16]byte, route string, replyRoute string, body []byte) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteBytes(buf, correlationID[:])
		encoding.WriteRoute(buf, route)
		encoding.WriteRoute(buf, replyRoute)
		encoding.WriteBytes(buf, body)
	}
}

func rpcResponsePayloadWriter(correlationID [16]byte, sequence uint64, body []byte, streamEnd bool) func(*bytes.Buffer) {
	return func(buf *bytes.Buffer) {
		encoding.WriteBytes(buf, correlationID[:])
		encoding.WriteU64(buf, sequence)
		encoding.WriteBytes(buf, body)
		if streamEnd {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	}
}
