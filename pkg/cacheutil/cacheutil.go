// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cacheutil/cacheutil.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cacheutil

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/util/gate"
)

// doWithBatch do func with batch and gate. batchSize==0 means one batch. gate==nil means no gate.
func doWithBatch(ctx context.Context, totalSize int, batchSize int, ga gate.Gate, f func(startIndex, endIndex int) error) error {
	if totalSize == 0 {
		return nil
	}
	if batchSize <= 0 {
		return f(0, totalSize)
	}
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < totalSize; i += batchSize {
		j := i + batchSize
		if j > totalSize {
			j = totalSize
		}
		if ga != nil {
			if err := ga.Start(ctx); err != nil {
				return nil
			}
		}
		startIndex, endIndex := i, j
		g.Go(func() error {
			if ga != nil {
				defer ga.Done()
			}
			return f(startIndex, endIndex)
		})
	}
	return g.Wait()
}
