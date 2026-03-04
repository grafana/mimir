// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SearchLabelNames implements mimirstorage.Searcher.
// It returns a SearcherValueSet immediately; the distributor call and filter/limit
// application run in the background.
func (q *distributorQuerier) SearchLabelNames(ctx context.Context, hints *mimirstorage.SearchHints, matchers ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		return emptySearcherValueSet(ctx), nil
	}

	minT := clampMinTime(spanlogger.FromContext(ctx, q.logger), q.mint, time.Now().UnixMilli(), -queryIngestersWithin, "query ingesters within")

	ctx, cancel := context.WithCancel(ctx)
	outCh := make(chan mimirstorage.FilteredResult, 256)
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.SearchLabelNames")
		defer spanLog.Finish()

		storeHints := searchHintsToLabelHints(hints)
		names, err := q.distributor.LabelNames(ctx, model.Time(minT), model.Time(q.maxt), storeHints, matchers...)
		if err != nil {
			stream.err = err
			return
		}

		sink := newDedupSink(outCh, hints, stream, cancel, ctx)
		for _, name := range names {
			if !sink.add(name) {
				return
			}
		}
	}()

	return stream, nil
}

// SearchLabelValues implements mimirstorage.Searcher.
// It returns a SearcherValueSet immediately; the distributor call and filter/limit
// application run in the background.
func (q *distributorQuerier) SearchLabelValues(ctx context.Context, name string, hints *mimirstorage.SearchHints, matchers ...*labels.Matcher) (mimirstorage.SearcherValueSet, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		return emptySearcherValueSet(ctx), nil
	}

	minT := clampMinTime(spanlogger.FromContext(ctx, q.logger), q.mint, time.Now().UnixMilli(), -queryIngestersWithin, "query ingesters within")

	ctx, cancel := context.WithCancel(ctx)
	outCh := make(chan mimirstorage.FilteredResult, 256)
	stream := &labelSearchStream{ch: outCh, ctx: ctx, cancel: cancel, hints: hints}

	go func() {
		defer close(outCh)

		spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.SearchLabelValues")
		defer spanLog.Finish()

		storeHints := searchHintsToLabelHints(hints)
		values, err := q.distributor.LabelValuesForLabelName(ctx, model.Time(minT), model.Time(q.maxt), model.LabelName(name), storeHints, matchers...)
		if err != nil {
			stream.err = err
			return
		}

		sink := newDedupSink(outCh, hints, stream, cancel, ctx)
		for _, v := range values {
			if !sink.add(v) {
				return
			}
		}
	}()

	return stream, nil
}
