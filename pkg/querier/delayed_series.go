// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/util/annotations"
)

type delayedSeriesReadKey struct{}

// withDelayedSeriesRead marks a read that can select series from the tenant's delayed_series limit.
// Those series are not in ingesters, so the store-gateways are queried up to the query end instead of
// up to now minus -querier.query-store-after.
func withDelayedSeriesRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, delayedSeriesReadKey{}, true)
}

func isDelayedSeriesRead(ctx context.Context) bool {
	v, _ := ctx.Value(delayedSeriesReadKey{}).(bool)
	return v
}

func newDelayedSeriesCompleteThroughInfo(completeThroughMs int64) error {
	if completeThroughMs <= 0 {
		return fmt.Errorf("%w: the query selects delayed series, and none of their samples have been published to blocks yet", annotations.PromQLInfo)
	}
	return fmt.Errorf("%w: the query selects delayed series, which are published through about %s; newer samples are not queryable yet",
		annotations.PromQLInfo, time.UnixMilli(completeThroughMs).UTC().Format(time.RFC3339))
}
