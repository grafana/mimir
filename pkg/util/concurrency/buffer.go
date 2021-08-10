// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/concurrency/buffer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package concurrency

import (
	"bytes"
	"sync"
)

type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (sb *SyncBuffer) Write(p []byte) (n int, err error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	return sb.buf.Write(p)
}

func (sb *SyncBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	return sb.buf.String()
}
