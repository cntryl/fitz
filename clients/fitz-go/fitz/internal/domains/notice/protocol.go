package notice

import (
	"errors"
	"fmt"
)

// Wire operation codes for Notice domain. Values are message type identifiers.
const (
	NoticePublish        uint16 = 500
	NoticeSubscribe      uint16 = 501
	NoticeUnsubscribe    uint16 = 502
	NoticeUnsubscribeAll uint16 = 503
	NoticeNotify         uint16 = 504
)

// Domain-specific errors.
var (
	ErrNoticeRouteInvalid = errors.New("invalid notice route")
	ErrNoticeTimeout      = errors.New("notice operation timed out")
	ErrNoticeSendFailed   = errors.New("notice send failed")
)

// ---------------------------------------------------------------------------
// Wire encoding / decoding helpers (custom binary format, not TLV)
// ---------------------------------------------------------------------------

func encodePublish(route string, body []byte) []byte {
	routeBytes := []byte(route)
	buf := make([]byte, 0, 8+4+len(routeBytes)+4+len(body))
	buf = appendU64(buf, 0)
	buf = appendU32(buf, uint32(len(routeBytes)))
	buf = append(buf, routeBytes...)
	buf = appendU32(buf, uint32(len(body)))
	buf = append(buf, body...)
	return buf
}

func encodeSubscribe(route string) []byte {
	pat := []byte(route)
	buf := make([]byte, 0, 8+4+len(pat)+8+4)
	buf = appendU64(buf, 0)
	buf = appendU32(buf, uint32(len(pat)))
	buf = append(buf, pat...)
	buf = appendU64(buf, 0)
	buf = appendU32(buf, 0)
	return buf
}

func encodeUnsubscribe(route string) []byte {
	return encodeSubscribe(route)
}

func DecodeNotify(body []byte) (string, []byte, bool) {
	_, route, ok := decodeFirstRoute(body)
	if !ok {
		return "", nil, false
	}
	payload, ok := decodePayload(body)
	if !ok {
		return "", nil, false
	}
	return route, payload, true
}

func decodeFirstRoute(body []byte) (int, string, bool) {
	if len(body) < 12 {
		return 0, "", false
	}
	idx := 0
	idx += 8 // family_id
	routeLen := readU32(body[idx:])
	idx += 4
	if int(idx+int(routeLen)) > len(body) {
		return 0, "", false
	}
	route := string(body[idx : idx+int(routeLen)])
	idx += int(routeLen)
	return idx, route, true
}

func decodePayload(body []byte) ([]byte, bool) {
	idx, _, ok := decodeFirstRoute(body)
	if !ok || idx+4 > len(body) {
		return nil, false
	}
	plen := readU32(body[idx:])
	idx += 4
	if int(idx+int(plen)) > len(body) {
		return nil, false
	}
	payload := append([]byte(nil), body[idx:idx+int(plen)]...)
	return payload, true
}

func decodeStatus(body []byte) (uint8, string, bool) {
	if len(body) < 1 {
		return 0, "", false
	}
	status := body[0]
	if status == 0 {
		return 0, "", true
	}
	if len(body) < 5 {
		return status, "", true
	}
	msgLen := uint32(body[1])<<24 | uint32(body[2])<<16 | uint32(body[3])<<8 | uint32(body[4])
	if int(5+msgLen) > len(body) {
		return status, "", true
	}
	return status, string(body[5 : 5+msgLen]), true
}

func IsNoticeResponseType(t uint16) bool {
	return t == NoticePublish || t == NoticeSubscribe || t == NoticeUnsubscribe || t == NoticeUnsubscribeAll
}

func DecodeNoticeResponseKey(op uint16, body []byte) (string, error) {
	status, errMsg, ok := decodeStatus(body)
	if !ok {
		return "", nil
	}
	if status != 0 {
		return "", fmt.Errorf("notice error: %s", errMsg)
	}
	return NoticeWaitKey(op), nil
}

func NoticeWaitKey(op uint16) string {
	return fmt.Sprintf("%d", op)
}

// ---------------------------------------------------------------------------
// Binary helpers
// ---------------------------------------------------------------------------

func appendU64(buf []byte, v uint64) []byte {
	return append(buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendU32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func readU32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// ---------------------------------------------------------------------------
// Route validation
// ---------------------------------------------------------------------------

func stripNoticeScheme(route string) string {
	const prefix = "notice://"
	if len(route) >= len(prefix) && route[:len(prefix)] == prefix {
		return route[len(prefix):]
	}
	return route
}

func ValidateNoticeRoute(route string, allowWildcards bool) error {
	if len(route) < 9 || route[:9] != "notice://" {
		return fmt.Errorf("notice route must start with notice://")
	}
	path := stripNoticeScheme(route)
	segs := splitSlash(path)
	if len(segs) != 3 {
		if allowWildcards && len(segs) == 2 && segs[1] == "**" {
			return nil
		}
		return fmt.Errorf("notice route must have realm/area/resource")
	}
	if segs[0] == "" || segs[1] == "" || segs[2] == "" {
		return fmt.Errorf("notice route segments must be non-empty")
	}
	if !allowWildcards {
		if containsWildcard(segs[1]) || containsWildcard(segs[2]) {
			return fmt.Errorf("notice publish route cannot contain wildcards")
		}
		return nil
	}
	if segs[0] == "*" || segs[0] == "**" {
		return fmt.Errorf("notice realm cannot be wildcard")
	}
	return nil
}

// NoticeMatchRoute matches a subscription pattern against a notification route.
func NoticeMatchRoute(pattern, route string) bool {
	pat := stripNoticeScheme(pattern)
	rt := stripNoticeScheme(route)
	if pat == rt {
		return true
	}
	pSegs := splitSlash(pat)
	rSegs := splitSlash(rt)
	pi, ri := 0, 0
	for pi < len(pSegs) && ri < len(rSegs) {
		if pSegs[pi] == "**" {
			if pi == len(pSegs)-1 {
				return true
			}
			next := pSegs[pi+1]
			for ri < len(rSegs) {
				if rSegs[ri] == next {
					break
				}
				ri++
			}
			pi++
			continue
		}
		if pSegs[pi] == "*" {
			pi++
			ri++
			continue
		}
		if pSegs[pi] != rSegs[ri] {
			return false
		}
		pi++
		ri++
	}
	for pi < len(pSegs) && pSegs[pi] == "**" {
		pi++
	}
	return pi == len(pSegs) && ri == len(rSegs)
}

// ---------------------------------------------------------------------------
// Tiny helpers (avoid importing strings for two uses)
// ---------------------------------------------------------------------------

func splitSlash(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func containsWildcard(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '*' {
			return true
		}
	}
	return false
}
