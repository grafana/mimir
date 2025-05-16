// SPDX-License-Identifier: AGPL-3.0-only

package sync

import (
	"context"
	"sync"
)

type ContextWaitGroup struct {
	sync.WaitGroup
}

func (cwg *ContextWaitGroup) WaitWithContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		cwg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
