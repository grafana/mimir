// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"sync/atomic" //lint:ignore faillint we can't use go.uber.org/atomic with a protobuf struct without wrapping it.
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

// IsQueryStatsEnabled returns whether query stats tracking is enabled in the context.
func IsQueryStatsEnabled(ctx context.Context) bool {
	// When query statistics are enabled, the stats object is already initialised
	// within the context, so we can just check it.
	return QueryStatsFromContext(ctx) != nil
}

// QueryStats tracks query execution statistics for lookup planning.
// All fields must be accessed using atomic operations for thread safety.
type QueryStats struct {
	estimatedFinalCardinality uint64
	estimatedSelectedPostings uint64
	actualSelectedPostings    uint64
	actualFinalCardinality    uint64
}

// SetEstimatedFinalCardinality sets the estimated final cardinality.
func (q *QueryStats) SetEstimatedFinalCardinality(cardinality uint64) {
	if q == nil {
		return
	}
	atomic.StoreUint64(&q.estimatedFinalCardinality, cardinality)
}

// LoadEstimatedFinalCardinality returns the estimated final cardinality.
func (q *QueryStats) LoadEstimatedFinalCardinality() uint64 {
	if q == nil {
		return 0
	}
	return atomic.LoadUint64(&q.estimatedFinalCardinality)
}

// SetEstimatedSelectedPostings sets the estimated selected postings count.
func (q *QueryStats) SetEstimatedSelectedPostings(postings uint64) {
	if q == nil {
		return
	}
	atomic.StoreUint64(&q.estimatedSelectedPostings, postings)
}

// LoadEstimatedSelectedPostings returns the estimated selected postings count.
func (q *QueryStats) LoadEstimatedSelectedPostings() uint64 {
	if q == nil {
		return 0
	}
	return atomic.LoadUint64(&q.estimatedSelectedPostings)
}

// SetActualSelectedPostings sets the actual selected postings count.
func (q *QueryStats) SetActualSelectedPostings(postings uint64) {
	if q == nil {
		return
	}
	atomic.StoreUint64(&q.actualSelectedPostings, postings)
}

// LoadActualSelectedPostings returns the actual selected postings count.
func (q *QueryStats) LoadActualSelectedPostings() uint64 {
	if q == nil {
		return 0
	}
	return atomic.LoadUint64(&q.actualSelectedPostings)
}

// SetActualFinalCardinality sets the actual final cardinality.
func (q *QueryStats) SetActualFinalCardinality(cardinality uint64) {
	if q == nil {
		return
	}
	atomic.StoreUint64(&q.actualFinalCardinality, cardinality)
}

// LoadActualFinalCardinality returns the actual final cardinality.
func (q *QueryStats) LoadActualFinalCardinality() uint64 {
	if q == nil {
		return 0
	}
	return atomic.LoadUint64(&q.actualFinalCardinality)
}
