// SPDX-License-Identifier: AGPL-3.0-only

package sync

import (
	"fmt"
	"sync"
	"time"
)

func WaitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("WaitGroup.Wait() timed out after %s", timeout)
	case <-done:
		return nil
	}
}
