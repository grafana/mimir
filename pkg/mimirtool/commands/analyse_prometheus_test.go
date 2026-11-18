// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithBackoff(t *testing.T) {
	t.Run("successful first attempt does not wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			attempts := 0
			err := withBackoff(t.Context(), func() error {
				attempts++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, attempts)
			require.Zero(t, time.Since(start))
		})
	})

	t.Run("failed attempts have bounded exponential delays", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			expectedErr := errors.New("backend overloaded")
			var attempts []time.Time
			err := withBackoff(t.Context(), func() error {
				attempts = append(attempts, time.Now())
				return expectedErr
			})
			require.ErrorIs(t, err, expectedErr)
			require.Len(t, attempts, 10)

			minDelay := 100 * time.Millisecond
			maxDelay := 200 * time.Millisecond
			for i := 1; i < len(attempts); i++ {
				delay := attempts[i].Sub(attempts[i-1])
				require.GreaterOrEqual(t, delay, minDelay)
				require.LessOrEqual(t, delay, maxDelay)
				if maxDelay < 10*time.Second {
					minDelay = min(minDelay*2, 10*time.Second)
					maxDelay = min(maxDelay*2, 10*time.Second)
				}
			}
			// No extra wait is needed once the attempt limit is reached.
			require.Equal(t, attempts[len(attempts)-1], time.Now())
		})
	})

	t.Run("successful retry stops immediately", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			attempts := 0
			err := withBackoff(t.Context(), func() error {
				attempts++
				if attempts == 1 {
					return errors.New("backend overloaded")
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 2, attempts)
			require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
			require.Less(t, time.Since(start), 200*time.Millisecond)
		})
	})

	t.Run("deadline interrupts the wait and preserves the query error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			start := time.Now()
			attempts := 0
			expectedErr := errors.New("backend overloaded")
			err := withBackoff(ctx, func() error {
				attempts++
				return expectedErr
			})
			require.ErrorIs(t, err, expectedErr)
			require.Equal(t, 1, attempts)
			require.Equal(t, 50*time.Millisecond, time.Since(start))
		})
	})

	t.Run("canceled context makes no attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := withBackoff(ctx, func() error {
			t.Fatal("query attempted after cancellation")
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}
