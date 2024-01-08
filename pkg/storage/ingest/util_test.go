// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngesterPartition(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := IngesterPartition("ingester-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartition("ingester-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = IngesterPartition("ingester-zone-c-0")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)

		actual, err = IngesterPartition("ingester-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)

		actual, err = IngesterPartition("ingester-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 5, actual)

		actual, err = IngesterPartition("ingester-zone-c-1")
		require.NoError(t, err)
		assert.EqualValues(t, 6, actual)

		actual, err = IngesterPartition("ingester-zone-a-2")
		require.NoError(t, err)
		assert.EqualValues(t, 8, actual)

		actual, err = IngesterPartition("ingester-zone-b-2")
		require.NoError(t, err)
		assert.EqualValues(t, 9, actual)

		actual, err = IngesterPartition("ingester-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 10, actual)

		actual, err = IngesterPartition("mimir-write-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)

		actual, err = IngesterPartition("mimir-write-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 5, actual)

		actual, err = IngesterPartition("mimir-write-zone-c-1")
		require.NoError(t, err)
		assert.EqualValues(t, 6, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := IngesterPartition("ingester-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartition("ingester-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)

		actual, err = IngesterPartition("mimir-write-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = IngesterPartition("mimir-write-1")
		require.NoError(t, err)
		assert.EqualValues(t, 4, actual)
	})

	t.Run("should return error if the ingester ID has a non supported format", func(t *testing.T) {
		_, err := IngesterPartition("unknown")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-X-0")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-a-")
		require.Error(t, err)

		_, err = IngesterPartition("ingester-zone-a")
		require.Error(t, err)
	})
}

func TestResultPromise(t *testing.T) {
	t.Run("wait() should block until a result has been notified", func(t *testing.T) {
		var (
			wg  = sync.WaitGroup{}
			rw  = newResultPromise[int]()
			ctx = context.Background()
		)

		// Spawn few goroutines waiting for the result.
		wg.Add(3)

		for i := 0; i < 3; i++ {
			runAsync(&wg, func() {
				actual, err := rw.wait(ctx)
				require.NoError(t, err)
				require.Equal(t, 12345, actual)
			})
		}

		// Notify the result.
		rw.notify(12345, nil)

		// Wait until all goroutines have done.
		wg.Wait()
	})

	t.Run("wait() should block until an error has been notified", func(t *testing.T) {
		var (
			wg        = sync.WaitGroup{}
			rw        = newResultPromise[int]()
			ctx       = context.Background()
			resultErr = errors.New("test error")
		)

		// Spawn few goroutines waiting for the result.
		wg.Add(3)

		for i := 0; i < 3; i++ {
			runAsync(&wg, func() {
				actual, err := rw.wait(ctx)
				require.Equal(t, resultErr, err)
				require.Equal(t, 0, actual)
			})
		}

		// Notify the result.
		rw.notify(0, resultErr)

		// Wait until all goroutines have done.
		wg.Wait()
	})

	t.Run("wait() should return when the input context timeout expires", func(t *testing.T) {
		var (
			rw  = newResultPromise[int]()
			ctx = context.Background()
		)

		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		actual, err := rw.wait(ctxWithTimeout)
		require.Equal(t, context.DeadlineExceeded, err)
		require.Equal(t, 0, actual)
	})
}
