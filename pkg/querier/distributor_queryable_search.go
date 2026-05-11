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

// SearchLabelNames forwards a streaming label-names search to the Distributor,
// applying the QueryIngestersWithin retention guard in the same way as
// distributorQuerier.LabelNames. If the query window ends before the ingester
// retention horizon, the call short-circuits to an empty result set without
// touching the Distributor. Otherwise minT is clamped forward to
// now-QueryIngestersWithin before forwarding.
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
	q.mint = clampMinTime(spanLog, q.mint, now, -queryIngestersWithin, "query ingesters within")

	return q.distributor.SearchLabelNames(ctx, model.Time(q.mint), model.Time(q.maxt), params, hints, matchers)
}

// SearchLabelValues forwards a streaming label-values search to the Distributor,
// applying the QueryIngestersWithin retention guard in the same way as
// distributorQuerier.LabelValues. If the query window ends before the ingester
// retention horizon, the call short-circuits to an empty result set without
// touching the Distributor. Otherwise minT is clamped forward to
// now-QueryIngestersWithin before forwarding.
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
	q.mint = clampMinTime(spanLog, q.mint, now, -queryIngestersWithin, "query ingesters within")

	return q.distributor.SearchLabelValues(ctx, model.Time(q.mint), model.Time(q.maxt), name, params, hints, matchers)
}
