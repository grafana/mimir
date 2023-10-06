// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrpcInflightMethodLimiter(t *testing.T) {
	t.Run("nil push receiver", func(t *testing.T) {
		l := newGrpcInflightMethodLimiter(func() pushReceiver { return nil })

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})

		ctx, err = l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.ErrorIs(t, err, errNoIngester)
		require.Panics(t, func() {
			// In practice, this will not be called, since l.RPCCallStarting(ingesterPushMethod) returns error.
			l.RPCCallFinished(context.WithValue(ctx, ingesterPushStarted, true))
		})
	})

	t.Run("push receiver, no error", func(t *testing.T) {
		m := &mockPushReceiver{startErr: nil}

		l := newGrpcInflightMethodLimiter(func() pushReceiver { return m })

		ctx, err := l.RPCCallStarting(context.Background(), "test", nil)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		ctx, err = l.RPCCallStarting(context.Background(), ingesterPushMethod, nil)
		require.NoError(t, err)
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		require.NotPanics(t, func() {
			l.RPCCallFinished(ctx)
		})
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 1, m.finishCalls)
	})
}

type mockPushReceiver struct {
	startCalls  int
	finishCalls int

	startErr error
}

func (i *mockPushReceiver) StartPushRequest() error {
	i.startCalls++
	return i.startErr
}

func (i *mockPushReceiver) FinishPushRequest() {
	i.finishCalls++
}
