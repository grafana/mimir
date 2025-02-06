// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"

	util_log "github.com/grafana/mimir/pkg/util/log"
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
			if _, err := stream.Recv(); err != nil {
				close(done)
				return
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

type LoggerWithRate struct {
	log.Logger
	count *atomic.Int64
}

func NewLoggerWithRate(logger log.Logger) LoggerWithRate {
	if logger == nil {
		logger = util_log.Logger
	}
	return LoggerWithRate{
		Logger: logger,
		count:  atomic.NewInt64(0),
	}
}

func (l LoggerWithRate) LogIfNeeded(msgs []string) {
	next := l.count.Inc()
	if next%1000 == 0 || next%1000 == 1 {
		level.Info(l.Logger).Log(msgs)
	}
}
