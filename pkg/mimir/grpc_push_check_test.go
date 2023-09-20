// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrpcInflightMethodLimiter(t *testing.T) {
	t.Run("nil push receiver", func(t *testing.T) {
		l := newGrpcInflightMethodLimiter(func() pushReceiver { return nil })

		require.NoError(t, l.RPCCallStarting("test"))
		require.NotPanics(t, func() {
			l.RPCCallFinished("test")
		})

		require.ErrorIs(t, l.RPCCallStarting(ingesterPushMethod), errNoIngester)
		require.Panics(t, func() {
			// In practice, this will not be called, since l.RPCCallStarting(ingesterPushMethod) returns error.
			l.RPCCallFinished(ingesterPushMethod)
		})
	})

	t.Run("push receiver, no error", func(t *testing.T) {
		m := &mockPushReceiver{startErr: nil}

		l := newGrpcInflightMethodLimiter(func() pushReceiver { return m })
		require.NoError(t, l.RPCCallStarting("test"))
		require.NotPanics(t, func() {
			l.RPCCallFinished("test")
		})
		require.Equal(t, 0, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		require.NoError(t, l.RPCCallStarting(ingesterPushMethod))
		require.Equal(t, 1, m.startCalls)
		require.Equal(t, 0, m.finishCalls)

		require.NotPanics(t, func() {
			l.RPCCallFinished(ingesterPushMethod)
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
