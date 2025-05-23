// SPDX-License-Identifier: AGPL-3.0-only

package sync

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWaitWithTimeout(t *testing.T) {
	t.Run("completes before timeout", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		// Start a goroutine that will complete quickly
		go func() {
			time.Sleep(10 * time.Millisecond)
			wg.Done()
		}()

		// Wait with a longer timeout
		err := WaitWithTimeout(&wg, 10*time.Second)
		assert.NoError(t, err, "WaitWithTimeout should not return an error when WaitGroup completes before timeout")
	})

	t.Run("times out", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		// Wait with a shorter timeout
		err := WaitWithTimeout(&wg, 10*time.Millisecond)
		require.Error(t, err, "WaitWithTimeout should return an error when timeout occurs")
		assert.Contains(t, err.Error(), "timed out", "Error message should indicate timeout")

		// Don't leak goroutines.
		wg.Done()
	})
}
