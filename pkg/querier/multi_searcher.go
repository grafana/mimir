// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// mimirSearcher is the cross-source Searcher interface used at the querier
// fan-out layer. It differs from Prometheus's storage.Searcher
// (vendor/github.com/prometheus/prometheus/storage/interface.go) by taking an
// extra *streaminglabelvalues.Params: each leaf (ingester / store-gateway)
// builds its own concurrency-unsafe storage.Filter from these params (spec
// invariant 9), so the params travel separately from the opaque hints.Filter.
//
// distributorQuerier and blocksStoreQuerier (added in PR #2) both implement
// this shape.
type mimirSearcher interface {
	SearchLabelNames(ctx context.Context, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
	SearchLabelValues(ctx context.Context, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
}

// SearchLabelNames fans out across the per-source queriers, merging streamed
// results via storage.MergeSearchResultSets. The merge preserves the requested
// ordering, deduplicates across sources, and stops after the per-tenant-clamped
// limit.
//
// Children that don't implement mimirSearcher are surfaced as
// storage.ErrSearchResultSet so the merge still composes cleanly.
func (mq *multiQuerier) SearchLabelNames(ctx context.Context, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, mq.logger, tracer, "multiQuerier.SearchLabelNames")
	defer spanLog.Finish()

	ctx, queriers, _, _, err := mq.getQueriers(ctx, mq.minT, mq.maxT)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.EmptySearchResultSet()
	}
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	hints, clampWarn := clampSearchHintsLimit(spanLog, hints, mq.limits.MaxLabelNamesLimit(userID), validation.MaxLabelNamesLimitFlag)
	sets := fanOutSearch(ctx, queriers, clampWarn, func(s mimirSearcher) storage.SearchResultSet {
		return s.SearchLabelNames(ctx, params, hints, matchers...)
	})
	return storage.MergeSearchResultSets(sets, hints)
}

// SearchLabelValues mirrors SearchLabelNames; it forwards the label name to
// the children.
func (mq *multiQuerier) SearchLabelValues(ctx context.Context, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, mq.logger, tracer, "multiQuerier.SearchLabelValues")
	defer spanLog.Finish()

	ctx, queriers, _, _, err := mq.getQueriers(ctx, mq.minT, mq.maxT)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.EmptySearchResultSet()
	}
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	hints, clampWarn := clampSearchHintsLimit(spanLog, hints, mq.limits.MaxLabelValuesLimit(userID), validation.MaxLabelValuesLimitFlag)
	sets := fanOutSearch(ctx, queriers, clampWarn, func(s mimirSearcher) storage.SearchResultSet {
		return s.SearchLabelValues(ctx, name, params, hints, matchers...)
	})
	return storage.MergeSearchResultSets(sets, hints)
}

// clampSearchHintsLimit returns a defensively-copied hints with Limit clamped
// to maxLimit per the tenant's per-source ceiling. When the clamp fires it
// also returns a single-element annotations.Annotations carrying the
// MaxLimitError for the merge layer to surface via Warnings(). Mirrors the
// existing LabelNames/LabelValues clamp warning behaviour.
//
// A nil hints becomes a zero hints — no implicit limit is invented.
func clampSearchHintsLimit(spanLog *spanlogger.SpanLogger, hints *storage.SearchHints, maxLimit int, settingName string) (*storage.SearchHints, annotations.Annotations) {
	if hints == nil {
		hints = &storage.SearchHints{}
	} else {
		hintsCopy := *hints
		hints = &hintsCopy
	}
	originalLimit := hints.Limit
	hints.Limit = clampToMaxLimit(spanLog, originalLimit, maxLimit, settingName)
	if hints.Limit > 0 && originalLimit != hints.Limit {
		var warn annotations.Annotations
		warn.Add(NewMaxLimitError(originalLimit, hints.Limit, settingName))
		return hints, warn
	}
	return hints, nil
}

// fanOutSearch collects a SearchResultSet from each child querier by
// type-asserting to mimirSearcher and invoking the caller-supplied closure.
// Children that fail the assertion yield storage.ErrSearchResultSet so the
// merge composes around the error. If clampWarn is non-empty an extra
// warning-only set is appended; the merge primitive merges its Warnings()
// into the final set.
func fanOutSearch(_ context.Context, queriers []storage.Querier, clampWarn annotations.Annotations, call func(mimirSearcher) storage.SearchResultSet) []storage.SearchResultSet {
	sets := make([]storage.SearchResultSet, 0, len(queriers)+1)
	for _, q := range queriers {
		s, ok := q.(mimirSearcher)
		if !ok {
			sets = append(sets, storage.ErrSearchResultSet(fmt.Errorf("querier %T does not implement search", q)))
			continue
		}
		sets = append(sets, call(s))
	}
	if len(clampWarn) > 0 {
		sets = append(sets, storage.NewSearchResultSetFromSlice(nil, clampWarn))
	}
	return sets
}
