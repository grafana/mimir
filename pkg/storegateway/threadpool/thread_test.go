// SPDX-License-Identifier: AGPL-3.0-only

package threadpool

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestOSThread_Call(t *testing.T) {
	t.Run("run by thread", func(t *testing.T) {
		test.VerifyNoLeak(t)

		stopping := make(chan struct{})
		thread := newOSThread(stopping)
		t.Cleanup(func() {
			close(stopping)
			thread.join()
		})

		thread.start()
		res, err := thread.execute(func() (interface{}, error) {
			return 42, nil
		})

		assert.Equal(t, 42, res.(int))
		assert.NoError(t, err)
	})

	t.Run("run by thread but stopped", func(t *testing.T) {
		test.VerifyNoLeak(t)

		stopping := make(chan struct{})
		thread := newOSThread(stopping)
		thread.start()
		close(stopping)
		thread.join()

		res, err := thread.execute(func() (interface{}, error) {
			return 42, nil
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, ErrPoolStopped)
	})
}
