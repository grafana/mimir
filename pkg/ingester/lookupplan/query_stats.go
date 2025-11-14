// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.uber.org/atomic"
)

type queryStatsContextKey int

const queryStatsCtxKey = queryStatsContextKey(1)

// ContextWithQueryStats returns a context with empty QueryStats.
func ContextWithQueryStats(ctx context.Context, stats *QueryStats) context.Context {
	return context.WithValue(ctx, queryStatsCtxKey, stats)
}

// QueryStatsFromContext gets the QueryStats out of the Context. Returns nil if stats have not
// been initialised in the context. Note that QueryStats methods are safe to call with
// a nil receiver.
func QueryStatsFromContext(ctx context.Context) *QueryStats {
	o := ctx.Value(queryStatsCtxKey)
	if o == nil {
		return nil
	}
	return o.(*QueryStats)
}

// QueryStats tracks query execution statistics for lookup planning.
// All fields use atomic types for thread safety.
type QueryStats struct {
	estimatedFinalCardinality atomic.Uint64
	estimatedSelectedPostings atomic.Uint64
	actualSelectedPostings    atomic.Uint64
	actualFinalCardinality    atomic.Uint64
}

// SetEstimatedFinalCardinality sets the estimated final cardinality.
func (q *QueryStats) SetEstimatedFinalCardinality(cardinality uint64) {
	if q == nil {
		return
	}
	q.estimatedFinalCardinality.Store(cardinality)
}

// LoadEstimatedFinalCardinality returns the estimated final cardinality.
func (q *QueryStats) LoadEstimatedFinalCardinality() uint64 {
	if q == nil {
		return 0
	}
	return q.estimatedFinalCardinality.Load()
}

// SetEstimatedSelectedPostings sets the estimated selected postings count.
func (q *QueryStats) SetEstimatedSelectedPostings(postings uint64) {
	if q == nil {
		return
	}
	q.estimatedSelectedPostings.Store(postings)
}

// LoadEstimatedSelectedPostings returns the estimated selected postings count.
func (q *QueryStats) LoadEstimatedSelectedPostings() uint64 {
	if q == nil {
		return 0
	}
	return q.estimatedSelectedPostings.Load()
}

// SetActualSelectedPostings sets the actual selected postings count.
func (q *QueryStats) SetActualSelectedPostings(postings uint64) {
	if q == nil {
		return
	}
	q.actualSelectedPostings.Store(postings)
}

// LoadActualSelectedPostings returns the actual selected postings count.
func (q *QueryStats) LoadActualSelectedPostings() uint64 {
	if q == nil {
		return 0
	}
	return q.actualSelectedPostings.Load()
}

// SetActualFinalCardinality sets the actual final cardinality.
func (q *QueryStats) SetActualFinalCardinality(cardinality uint64) {
	if q == nil {
		return
	}
	q.actualFinalCardinality.Store(cardinality)
}

// LoadActualFinalCardinality returns the actual final cardinality.
func (q *QueryStats) LoadActualFinalCardinality() uint64 {
	if q == nil {
		return 0
	}
	return q.actualFinalCardinality.Load()
}

type ActualSelectedPostingsClonerFactory struct{}

func (p ActualSelectedPostingsClonerFactory) PostingsCloner(_ ulid.ULID, postings index.Postings) tsdb.PostingsCloner {
	return actualSelectedPostingsCloner{index.NewPostingsCloner(postings)}
}

type actualSelectedPostingsCloner struct {
	cloner tsdb.PostingsCloner
}

func (p actualSelectedPostingsCloner) Clone(ctx context.Context) index.Postings {
	stats := QueryStatsFromContext(ctx)
	if stats != nil {
		stats.SetActualSelectedPostings(uint64(p.cloner.NumPostings()))
	}
	return p.cloner.Clone(ctx)
}

func (p actualSelectedPostingsCloner) NumPostings() int {
	return p.cloner.NumPostings()
}
