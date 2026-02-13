package integration

import (
	"bytes"
	"context"
	"time"

	"github.com/cntryl/fitz-go/internal/core/client"
	"github.com/cntryl/fitz-go/internal/core/types"
)

// GeneratePayload creates a payload of the specified size filled with repeated character.
func GeneratePayload(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}

// IsBrokerAvailable checks if the Fitz broker is available.
func IsBrokerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var tokenProvider types.TokenProvider = func(ctx context.Context) (string, error) {
		return "", nil
	}

	c := client.NewClient("localhost:4091", tokenProvider)
	err := c.Connect(ctx)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// GenerateRoute creates a route string for the given domain.
func GenerateRoute(domain string, resource string) string {
	return "ftz://1/" + domain + "/bench/" + resource
}
