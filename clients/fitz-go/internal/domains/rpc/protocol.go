package rpc

import (
	"errors"
	"strings"
)

// Wire opcodes for RPC domain (per CLIENT_SPEC.md). Values are message type identifiers.
const (
	RPCSubscribeWorker   uint16 = 300
	RPCUnsubscribeWorker uint16 = 301
	RPCRequest           uint16 = 302
	RPCResponse          uint16 = 303
	RPCAck               uint16 = 304
)

// Domain-specific errors.
var (
	ErrNoWorkers  = errors.New("no workers available")
	ErrRPCTimeout = errors.New("rpc timeout")
)

// mapRPCError maps a broker error message to a domain-specific Go error.
func mapRPCError(msg string) error {
	l := strings.ToLower(msg)
	switch {
	case strings.Contains(l, "no workers"):
		return ErrNoWorkers
	default:
		return errors.New(msg)
	}
}
