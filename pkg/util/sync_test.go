// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/sync_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroup(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup    func(wg *sync.WaitGroup) (context.Context, context.CancelFunc)
		expected error
	}{
		"WaitGroup is done": {
			setup: func(wg *sync.WaitGroup) (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 100*time.Millisecond)
			},
			expected: nil,
		},
		"WaitGroup is not done and timeout expires": {
			setup: func(wg *sync.WaitGroup) (context.Context, context.CancelFunc) {
				wg.Add(1)

				return context.WithTimeout(context.Background(), 100*time.Millisecond)
			},
			expected: context.DeadlineExceeded,
		},
		"WaitGroup is not done and context is cancelled before timeout expires": {
			setup: func(wg *sync.WaitGroup) (context.Context, context.CancelFunc) {
				wg.Add(1)

				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)

				go func() {
					time.Sleep(100 * time.Millisecond)
					cancel()
				}()

				return ctx, cancel
			},
			expected: context.Canceled,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			wg := sync.WaitGroup{}
			ctx, cancel := testData.setup(&wg)
			defer cancel()

			success := WaitGroup(ctx, &wg)
			assert.Equal(t, testData.expected, success)
		})
	}
}
