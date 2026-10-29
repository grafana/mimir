// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"

	"github.com/prometheus/prometheus/promql"
)

// queryCostEngine bounds concurrent estimates independently of the selected engine.
// Estimation bypasses query construction, so engine execution limits do not apply.
type queryCostEngine struct {
	promql.QueryEngine
	slots              chan struct{}
	timeoutImmediately bool
}

func (e *queryCostEngine) AcquireQuerySlot(ctx context.Context, _ string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return func() {}, err
	}
	if e.timeoutImmediately {
		return func() {}, context.DeadlineExceeded
	}

	select {
	case e.slots <- struct{}{}:
		return func() { <-e.slots }, nil
	case <-ctx.Done():
		return func() {}, ctx.Err()
	}
}
