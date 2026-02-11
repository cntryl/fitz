package notice

import (
	"context"
)

// Client is the API for the Notice domain (pub/sub notifications).
type Client interface {
	Subscribe(ctx context.Context, route string, handler NoticeHandler) (Subscription, error)
	Publish(ctx context.Context, route string, body []byte) error
	Close() error
}

// NoticeHandler processes an inbound notification.
type NoticeHandler func(context.Context, NoticeMsg) error

// Subscription allows unsubscribing from a notice route.
type Subscription interface {
	Unsubscribe()
}

// NoticeMsg is an inbound notification delivered to a handler.
type NoticeMsg struct {
	Route    string
	Metadata NoticeMetadata
	Body     []byte
}

// NoticeMetadata holds optional key/value metadata on a notification.
type NoticeMetadata map[string]string
