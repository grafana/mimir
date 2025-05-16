// SPDX-License-Identifier: AGPL-3.0-only

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestContextWaitGroup_WaitWithContext(t *testing.T) {
	t.Parallel()

	t.Run("should complete when wait group is done", func(t *testing.T) {
		var cwg ContextWaitGroup
		cwg.Add(1)

		// Start a goroutine that will complete the wait group after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cwg.Done()
		}()

		// Wait with a context that has a longer timeout than the goroutine delay
		const contextTimeout = time.Minute
		ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
		defer cancel()

		t0 := time.Now()
		err := cwg.WaitWithContext(ctx)
		require.NoError(t, err, "WaitWithContext should not return an error when wait group completes")
		require.Less(t, time.Since(t0), contextTimeout/2, "WaitWithContext should return before the context timeout")
	})

	t.Run("should return error when context is canceled", func(t *testing.T) {
		var cwg ContextWaitGroup
		cwg.Add(1)

		// Create a context that will be canceled before the wait group completes
		t0 := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// Start the wait with context
		err := cwg.WaitWithContext(ctx)

		// Verify that we got a context deadline exceeded error
		require.Error(t, err, "WaitWithContext should return an error when context is canceled")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		require.GreaterOrEqual(t, time.Since(t0), 50*time.Millisecond, "WaitWithContext should return after the context timeout")

		// Clean up the wait group to avoid goroutine leak
		cwg.Done()
	})

	t.Run("should complete immediately with empty wait group", func(t *testing.T) {
		var cwg ContextWaitGroup

		// Wait with context should return immediately
		ctx := context.Background()
		err := cwg.WaitWithContext(ctx)

		require.NoError(t, err, "WaitWithContext should not return an error with empty wait group")
	})
}
