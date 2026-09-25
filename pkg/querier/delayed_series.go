// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/util/annotations"
)

type delayedSeriesReadKey struct{}

type delayedSeriesRead struct {
	// annotate is set when the read can select series of an active rule, whose newest samples are not
	// queryable yet. Series of retired rules are back in ingesters, so their reads aren't annotated.
	annotate bool
}

// withDelayedSeriesRead marks a read that can select series from the tenant's delayed_series limit.
// Ingesters don't hold those series, or don't hold them for the time the rule applied, so the
// store-gateways are queried up to the query end instead of up to now minus -querier.query-store-after.
func withDelayedSeriesRead(ctx context.Context, annotate bool) context.Context {
	return context.WithValue(ctx, delayedSeriesReadKey{}, delayedSeriesRead{annotate: annotate})
}

func isDelayedSeriesRead(ctx context.Context) bool {
	_, ok := ctx.Value(delayedSeriesReadKey{}).(delayedSeriesRead)
	return ok
}

func annotateDelayedSeriesRead(ctx context.Context) bool {
	v, _ := ctx.Value(delayedSeriesReadKey{}).(delayedSeriesRead)
	return v.annotate
}

func newDelayedSeriesCompleteThroughInfo(completeThroughMs int64) error {
	if completeThroughMs <= 0 {
		return fmt.Errorf("%w: the query selects delayed series, and none of their samples have been published to blocks yet", annotations.PromQLInfo)
	}
	return fmt.Errorf("%w: the query selects delayed series, which are published through about %s; newer samples are not queryable yet",
		annotations.PromQLInfo, time.UnixMilli(completeThroughMs).UTC().Format(time.RFC3339))
}
