// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
)

type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *eventRecorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *eventRecorder) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

type recordingCloser struct {
	rec *eventRecorder
	err error
}

func (c *recordingCloser) Close() error {
	c.rec.add("cleanup-close")
	return c.err
}

func newRecordingRulerService(rec *eventRecorder) services.Service {
	return services.NewBasicService(nil, func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, func(error) error {
		rec.add("ruler-stop")
		return nil
	})
}

func newFailingRecordingRulerService(rec *eventRecorder, fail <-chan struct{}, runtimeErr error) services.Service {
	return services.NewBasicService(nil, func(ctx context.Context) error {
		select {
		case <-fail:
			return runtimeErr
		case <-ctx.Done():
			return nil
		}
	}, func(error) error {
		rec.add("ruler-stop")
		return nil
	})
}

func TestWrapRulerServiceWithCleanup(t *testing.T) {
	t.Run("stops ruler before cleanup", func(t *testing.T) {
		rec := &eventRecorder{}
		wrapped := wrapRulerServiceWithCleanup(newRecordingRulerService(rec), &recordingCloser{rec: rec})

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), wrapped))
		require.NoError(t, services.StopAndAwaitTerminated(t.Context(), wrapped))
		require.Equal(t, []string{"ruler-stop", "cleanup-close"}, rec.get())
	})

	t.Run("startup cancellation stops ruler before cleanup", func(t *testing.T) {
		rec := &eventRecorder{}
		started := make(chan struct{})
		rulerService := services.NewBasicService(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}, nil, func(error) error {
			rec.add("ruler-stop")
			return nil
		})
		wrapped := wrapRulerServiceWithCleanup(rulerService, &recordingCloser{rec: rec})

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() {
			errCh <- services.StartAndAwaitRunning(ctx, wrapped)
		}()
		<-started
		cancel()

		require.ErrorIs(t, <-errCh, context.Canceled)
		require.ErrorIs(t, wrapped.AwaitTerminated(t.Context()), context.Canceled)
		require.Equal(t, []string{"ruler-stop", "cleanup-close"}, rec.get())
	})

	t.Run("runtime failure is propagated", func(t *testing.T) {
		rec := &eventRecorder{}
		runtimeErr := errors.New("ruler failed")
		fail := make(chan struct{})
		wrapped := wrapRulerServiceWithCleanup(newFailingRecordingRulerService(rec, fail, runtimeErr), &recordingCloser{rec: rec})

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), wrapped))
		close(fail)

		require.ErrorIs(t, wrapped.AwaitTerminated(t.Context()), runtimeErr)
		require.Equal(t, []string{"ruler-stop", "cleanup-close"}, rec.get())
	})

	t.Run("cleanup error is returned on shutdown", func(t *testing.T) {
		rec := &eventRecorder{}
		cleanupErr := errors.New("cleanup failed")
		wrapped := wrapRulerServiceWithCleanup(newRecordingRulerService(rec), &recordingCloser{rec: rec, err: cleanupErr})

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), wrapped))
		require.ErrorIs(t, services.StopAndAwaitTerminated(t.Context(), wrapped), cleanupErr)
		require.Equal(t, []string{"ruler-stop", "cleanup-close"}, rec.get())
	})

	t.Run("ruler failure and cleanup error are aggregated", func(t *testing.T) {
		rec := &eventRecorder{}
		runtimeErr := errors.New("ruler failed")
		cleanupErr := errors.New("cleanup failed")
		fail := make(chan struct{})
		wrapped := wrapRulerServiceWithCleanup(newFailingRecordingRulerService(rec, fail, runtimeErr), &recordingCloser{rec: rec, err: cleanupErr})

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), wrapped))
		close(fail)

		err := wrapped.AwaitTerminated(t.Context())
		require.ErrorIs(t, err, runtimeErr)
		require.ErrorIs(t, err, cleanupErr)
		require.Equal(t, 1, strings.Count(err.Error(), runtimeErr.Error()))
		require.Equal(t, []string{"ruler-stop", "cleanup-close"}, rec.get())
	})
}
