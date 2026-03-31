// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SearchLabelNames implements mimirstorage.MimirSearcher.
// It delegates directly to the Distributor which performs the streaming merge.
func (q *distributorQuerier) SearchLabelNames(ctx context.Context, hints *mimirstorage.MimirSearchHints, matchers ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return mimirstorage.ErrorSearchResultSet(err), nil
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		return emptySearcherValueSet(ctx), nil
	}

	minT := clampMinTime(spanlogger.FromContext(ctx, q.logger), q.mint, time.Now().UnixMilli(), -queryIngestersWithin, "query ingesters within")

	vs, err := q.distributor.SearchLabelNames(ctx, model.Time(minT), model.Time(q.maxt), hints, matchers...)
	if err != nil {
		return mimirstorage.ErrorSearchResultSet(err), nil
	}
	return vs, nil
}

// SearchLabelValues implements mimirstorage.MimirSearcher.
// It delegates directly to the Distributor which performs the streaming merge.
func (q *distributorQuerier) SearchLabelValues(ctx context.Context, name string, hints *mimirstorage.MimirSearchHints, matchers ...*labels.Matcher) (mimirstorage.SearchResultSet, annotations.Annotations) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return mimirstorage.ErrorSearchResultSet(err), nil
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		return emptySearcherValueSet(ctx), nil
	}

	minT := clampMinTime(spanlogger.FromContext(ctx, q.logger), q.mint, time.Now().UnixMilli(), -queryIngestersWithin, "query ingesters within")

	vs, err := q.distributor.SearchLabelValues(ctx, model.Time(minT), model.Time(q.maxt), model.LabelName(name), hints, matchers...)
	if err != nil {
		return mimirstorage.ErrorSearchResultSet(err), nil
	}
	return vs, nil
}
