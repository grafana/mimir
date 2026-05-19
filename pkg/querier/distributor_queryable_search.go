// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SearchLabelNames forwards to the Distributor with QueryIngestersWithin
// applied: short-circuit to empty if the window ends before retention,
// otherwise clamp minT forward to now-QueryIngestersWithin.
//
// The signature intentionally diverges from storage.Searcher by taking a
// *streaminglabelvalues.Params alongside the SearchHints (the upstream
// interface has no field for fuzzy-algorithm/threshold/case-sensitivity).
// The HTTP wiring in the follow-up PR therefore reaches this method
// through a Mimir-local interface, not via a storage.Searcher type
// assertion.
func (q *distributorQuerier) SearchLabelNames(
	ctx context.Context,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers ...*labels.Matcher,
) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.SearchLabelNames")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		spanLog.DebugLog("msg", "not querying ingesters; query time range ends before the query-ingesters-within limit")
		return storage.EmptySearchResultSet()
	}

	now := time.Now().UnixMilli()
	mint := clampMinTime(spanLog, q.mint, now, -queryIngestersWithin, "query ingesters within")

	return q.distributor.SearchLabelNames(ctx, model.Time(mint), model.Time(q.maxt), params, hints, matchers)
}

// SearchLabelValues mirrors SearchLabelNames with the label name to query.
func (q *distributorQuerier) SearchLabelValues(
	ctx context.Context,
	name string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers ...*labels.Matcher,
) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "distributorQuerier.SearchLabelValues")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	queryIngestersWithin := q.cfgProvider.QueryIngestersWithin(tenantID)

	if !ShouldQueryIngesters(queryIngestersWithin, time.Now(), q.maxt) {
		spanLog.DebugLog("msg", "not querying ingesters; query time range ends before the query-ingesters-within limit")
		return storage.EmptySearchResultSet()
	}

	now := time.Now().UnixMilli()
	mint := clampMinTime(spanLog, q.mint, now, -queryIngestersWithin, "query ingesters within")

	return q.distributor.SearchLabelValues(ctx, model.Time(mint), model.Time(q.maxt), name, params, hints, matchers)
}
