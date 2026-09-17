// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
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

// FetchMetricMetadata fetches metric metadata for the given metric names from
// the ingesters. When a metric has more than one metadata record, the first one
// seen wins.
//
// matcherSets are ignored: the ingester metadata store (MetricsMetadataRequest)
// is keyed by metric name and cannot filter by label matchers. Tenant scoping,
// when federation is involved, is applied above this leaf by the tenant
// federation merge querier selecting which tenants to fetch from.
func (q *distributorQuerier) FetchMetricMetadata(ctx context.Context, names []string, _ [][]*labels.Matcher) (map[string]metadata.Metadata, error) {
	resp, err := q.distributor.MetricsMetadata(ctx, &client.MetricsMetadataRequest{
		MetricNames: names,
		// Bound the response to the number of requested names. With
		// LimitPerMetric=1 that's the exact number of records we can use. It also
		// caps an ingester that predates the MetricNames field (which ignores it)
		// to len(names) records rather than the tenant's whole metadata set. The
		// trade-off is that such an ingester returns an arbitrary subset, so some
		// names may be left un-enriched during a mixed-version rollout; we prefer
		// that bounded degradation over an unbounded response.
		Limit:          int32(len(names)),
		LimitPerMetric: 1,
	})
	if err != nil {
		return nil, err
	}

	out := make(map[string]metadata.Metadata, len(resp))
	for _, m := range resp {
		if _, ok := out[m.MetricFamily]; ok {
			continue
		}
		out[m.MetricFamily] = metadata.Metadata{Type: m.Type, Help: m.Help, Unit: m.Unit}
	}
	return out, nil
}
