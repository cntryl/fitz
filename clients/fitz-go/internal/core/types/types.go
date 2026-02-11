// Package types defines shared types used across the fitz SDK.
// This package has no dependencies on other internal packages to ensure
// it can be imported anywhere without creating cycles.
package types

import (
	"context"
	"fmt"
	"strings"
)

// TokenProvider is a function that returns a JWT token for authentication.
// It is called during connection establishment and reconnection attempts,
// allowing for token renewal and refresh logic. Return an empty string for
// unauthenticated connections.
type TokenProvider func(ctx context.Context) (string, error)

// ValidateRoute checks that a route string matches the expected format:
//
//	<expectedScheme>://realm/area/resource
//
// All three path segments must be non-empty. The scheme prefix is validated
// against expectedScheme (e.g., "queue", "stream", "rpc", "lease", "schedule").
func ValidateRoute(route string, expectedScheme string) error {
	prefix := expectedScheme + "://"
	if !strings.HasPrefix(route, prefix) {
		return fmt.Errorf("%s route must start with %s", expectedScheme, prefix)
	}
	path := route[len(prefix):]
	segs := strings.SplitN(path, "/", 4)
	if len(segs) != 3 {
		return fmt.Errorf("%s route must have exactly 3 segments: realm/area/resource", expectedScheme)
	}
	if segs[0] == "" || segs[1] == "" || segs[2] == "" {
		return fmt.Errorf("%s route segments must be non-empty", expectedScheme)
	}
	return nil
}
