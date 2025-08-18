// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"errors"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var ErrCloseAndExhaustTimedOut = errors.New("timed out waiting to exhaust stream after calling CloseSend, will continue exhausting stream in background")

type Stream[T any] interface {
	CloseSend() error
	Recv() (T, error)
}

// CloseAndExhaust closes and then tries to exhaust stream. This ensures:
//   - the gRPC library can release any resources associated with the stream (see https://pkg.go.dev/google.golang.org/grpc#ClientConn.NewStream)
//   - instrumentation middleware correctly observes the end of the stream, rather than reporting it as "context canceled"
//
// Note that this method may block for up to 200ms if the stream has not already been exhausted.
// If the stream has not been exhausted after this time, it will return ErrCloseAndExhaustTimedOut and continue exhausting the stream in the background.
func CloseAndExhaust[T any](stream Stream[T]) error {
	err := stream.CloseSend() //nolint:forbidigo // This is the one place we want to call this method.
	if err != nil {
		return err
	}

	done := make(chan struct{})

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				close(done)
				return
			}

			// If the message has a buffer reference, free it.
			// If the stream returned an error above, there's no message, so there's nothing to free.
			if buffer, ok := any(msg).(mimirpb.MessageWithBufferRef); ok {
				buffer.FreeBuffer()
			}
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(200 * time.Millisecond):
		return ErrCloseAndExhaustTimedOut
	}
}
