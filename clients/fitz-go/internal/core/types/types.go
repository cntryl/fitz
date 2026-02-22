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

// ValidateRoute checks that a route string matches the expected format.
//
// Default format (most domains):
//
//	<expectedScheme>://realm/area/resource
//
// Schedule format:
//
//	<expectedScheme>://realm/area/resource/operation
//
// All path segments must be non-empty. The scheme prefix is validated
// against expectedScheme (e.g., "queue", "stream", "rpc", "lease", "schedule").
func ValidateRoute(route string, expectedScheme string) error {
	prefix := expectedScheme + "://"
	if !strings.HasPrefix(route, prefix) {
		return fmt.Errorf("%s route must start with %s", expectedScheme, prefix)
	}
	path := route[len(prefix):]
	requiredSegments := 3
	segmentDesc := "realm/area/resource"
	if expectedScheme == "schedule" {
		requiredSegments = 4
		segmentDesc = "realm/area/resource/operation"
	}
	segs := strings.SplitN(path, "/", requiredSegments+1)
	if len(segs) != requiredSegments {
		return fmt.Errorf("%s route must have exactly %d segments: %s", expectedScheme, requiredSegments, segmentDesc)
	}
	for i := 0; i < requiredSegments; i++ {
		if segs[i] == "" {
			return fmt.Errorf("%s route segments must be non-empty", expectedScheme)
		}
	}
	return nil
}
