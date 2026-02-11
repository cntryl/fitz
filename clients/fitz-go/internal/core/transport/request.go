package transport

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
)

// RecvFrame waits for the next frame on the given channel ID, filtering
// out frames for other channels. Returns an error on context cancel or mux close.
func RecvFrame(ctx context.Context, in <-chan Frame, channel uint32) (Frame, error) {
	for {
		select {
		case <-ctx.Done():
			return Frame{}, ctx.Err()
		case f, ok := <-in:
			if !ok {
				return Frame{}, errors.New("mux closed")
			}
			if f.Channel != channel {
				continue
			}
			return f, nil
		}
	}
}

// SendRecv sends a frame and waits for a single response on the same channel.
// It returns the response frame on success or an error on context cancellation,
// mux closure, or if the response is an error frame (decoded via errMapper).
// If errMapper is nil, raw error text is returned as a Go error.
func SendRecv(ctx context.Context, mux MuxProvider, req Frame, errMapper func(string) error) (Frame, error) {
	if err := mux.Send(req); err != nil {
		return Frame{}, fmt.Errorf("send: %w", err)
	}
	for {
		f, err := RecvFrame(ctx, mux.In(), req.Channel)
		if err != nil {
			return Frame{}, err
		}
		switch f.Type {
		case FrameTypeResp:
			return f, nil
		case FrameTypeErr:
			return f, DecodeTLVError(f, "operation failed", errMapper)
		default:
			continue // skip unrecognised frame types
		}
	}
}

// DecodeTLVError extracts the error message from a TLV error frame and maps it
// via errMapper. If errMapper is nil the raw message is returned as an error.
// If TLV decoding fails, the decode error and raw body are included for diagnostics.
func DecodeTLVError(f Frame, defaultMsg string, errMapper func(string) error) error {
	dec, err := NewTLVDecoder(f.Body)
	if err != nil {
		wrapped := fmt.Errorf("%s (TLV decode failed: %w, body hex: %s)", defaultMsg, err, hex.EncodeToString(f.Body))
		if errMapper != nil {
			return errMapper(wrapped.Error())
		}
		return wrapped
	}
	msg := dec.GetString(TagErr)
	if msg == "" {
		msg = defaultMsg
	}
	if errMapper != nil {
		return errMapper(msg)
	}
	return errors.New(msg)
}
