package promql

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/storage"
)

// TODO: remove duplicate interface from web/api/v1 package
type QueryEngine interface {
	SetQueryLogger(l QueryLogger)
	NewInstantQuery(ctx context.Context, q storage.Queryable, opts QueryOpts, qs string, ts time.Time) (Query, error)
	NewRangeQuery(ctx context.Context, q storage.Queryable, opts QueryOpts, qs string, start, end time.Time, interval time.Duration) (Query, error)
}
