// SPDX-License-Identifier: AGPL-3.0-only

package tenantfederation

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NewExemplarQueryable returns an exemplar queryable that makes requests for
// all tenant IDs that are part of the request and aggregates the results from
// each tenant's ExemplarQuerier.
//
// Results contain the series label __tenant_id__ to identify which tenant the
// exemplar is from.
//
// By setting bypassWithSingleQuerier to true, tenant federation logic gets
// bypassed if the request is only for a single tenant. The requests will also
// not contain the pseudo series label __tenant_id__ in this case.
func NewExemplarQueryable(upstream storage.ExemplarQueryable, bypassWithSingleQuerier bool, maxConcurrency int, reg prometheus.Registerer, logger log.Logger) storage.ExemplarQueryable {
	return NewMergeExemplarQueryable(defaultTenantLabel, upstream, bypassWithSingleQuerier, maxConcurrency, reg, logger)
}

// NewMergeExemplarQueryable returns an exemplar queryable that makes requests for
// all tenant IDs that are part of the request and aggregates the results from
// each tenant's ExemplarQuerier.
//
// Results contain the series label `idLabelName` to identify which tenant the
// exemplar is from.
//
// By setting bypassWithSingleQuerier to true, tenant federation logic gets
// bypassed if the request is only for a single tenant. The requests will also
// not contain the pseudo series label `idLabelName` in this case.
func NewMergeExemplarQueryable(idLabelName string, upstream storage.ExemplarQueryable, bypassWithSingleQuerier bool, maxConcurrency int, reg prometheus.Registerer, logger log.Logger) storage.ExemplarQueryable {
	return &mergeExemplarQueryable{
		logger:                  logger,
		idLabelName:             idLabelName,
		bypassWithSingleQuerier: bypassWithSingleQuerier,
		upstream:                upstream,
		maxConcurrency:          maxConcurrency,
		tenantsQueried: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_querier_federation_exemplar_tenants_queried",
			Help:    "Number of tenants queried for a single exemplar query.",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		}),
	}
}

type mergeExemplarQueryable struct {
	logger                  log.Logger
	idLabelName             string
	bypassWithSingleQuerier bool
	upstream                storage.ExemplarQueryable
	maxConcurrency          int
	tenantsQueried          prometheus.Histogram
}

// tenantsAndQueriers returns a list of tenant IDs and corresponding queriers based on the context
func (m *mergeExemplarQueryable) tenantsAndQueriers(ctx context.Context) ([]string, []storage.ExemplarQuerier, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	queriers := make([]storage.ExemplarQuerier, len(tenantIDs))
	for i, tenantID := range tenantIDs {
		q, err := m.upstream.ExemplarQuerier(user.InjectOrgID(ctx, tenantID))
		if err != nil {
			return nil, nil, err
		}

		queriers[i] = q
	}

	return tenantIDs, queriers, nil
}

// ExemplarQuerier returns a new querier that aggregates results from queries run
// across multiple tenants
func (m *mergeExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	ids, queriers, err := m.tenantsAndQueriers(ctx)
	if err != nil {
		return nil, err
	}

	m.tenantsQueried.Observe(float64(len(ids)))
	// If desired and there is only a single querier, just return it directly instead
	// of going through the federation querier. bypassWithSingleQuerier=true allows a
	// bit less overhead when it's not needed while bypassWithSingleQuerier=false will
	// consistently add a __tenant_id__ label to all results.
	if m.bypassWithSingleQuerier && len(queriers) == 1 {
		return queriers[0], nil
	}

	return &mergeExemplarQuerier{
		logger:         m.logger,
		ctx:            ctx,
		idLabelName:    m.idLabelName,
		tenants:        ids,
		queriers:       queriers,
		maxConcurrency: m.maxConcurrency,
	}, nil
}

type exemplarJob struct {
	tenant   string
	matchers [][]*labels.Matcher
	querier  storage.ExemplarQuerier
}

type mergeExemplarQuerier struct {
	logger         log.Logger
	ctx            context.Context
	idLabelName    string
	tenants        []string
	queriers       []storage.ExemplarQuerier
	maxConcurrency int
}

// Select returns the union exemplars within the time range that match each slice of
// matchers, across multiple tenants. The query for each tenant is forwarded to an
// instance of an upstream querier.
func (m *mergeExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	spanlog, ctx := spanlogger.NewWithLogger(m.ctx, m.logger, "mergeExemplarQuerier.Select")
	defer spanlog.Finish()

	// If we have any matchers that are looking for __tenant_id__, use that to filter down the
	// original list of tenants given to this querier and then remove those matchers from the list
	// that will be passed to each querier. We do this because the label isn't an actual label on
	// the series/exemplars, it's a generated label we add when performing the query.
	filteredTenants, filteredMatchers := filterTenantsAndRewriteMatchers(m.idLabelName, m.tenants, matchers)

	// In order to run a query for each tenant in parallel and have them all write to the same
	// structure, we create a slice of jobs and results of the same size. Each job writes to its
	// corresponding slot in the results slice. This way we avoid the need for locks.
	jobs := make([]*exemplarJob, len(filteredTenants))
	results := make([][]exemplar.QueryResult, len(filteredTenants))

	jobIdx := 0
	for idIdx, tenantID := range m.tenants {
		// Upstream queriers are indexed corresponding to the original, non-filtered, IDs
		// given to this querier. Iterate over the original list of tenant IDs but skip if
		// this tenant ID got filtered out. Otherwise, use the index of this tenant ID to
		// pick the corresponding querier.
		if _, matched := filteredTenants[tenantID]; !matched {
			continue
		}

		// Each job gets a copy of the matchers provided to this method so that upstream
		// queriers don't modify our slice of filtered matchers. Otherwise, each querier
		// might modify the slice and end up with an ever-growing list of matchers.
		jobMatchers := make([][]*labels.Matcher, len(filteredMatchers))
		copy(jobMatchers, filteredMatchers)

		jobs[jobIdx] = &exemplarJob{
			tenant:   tenantID,
			matchers: jobMatchers,
			querier:  m.queriers[idIdx],
		}

		jobIdx++
	}

	// Each task grabs a job object from the slice and stores its results in the corresponding
	// index in the results slice. The job handles performing a tenant-specific exemplar query
	// and adding a tenant ID label to each of the results.
	run := func(_ context.Context, idx int) error {
		job := jobs[idx]

		res, err := job.querier.Select(start, end, job.matchers...)
		if err != nil {
			return fmt.Errorf("unable to run federated exemplar query for %s: %w", job.tenant, err)
		}

		for i, e := range res {
			e.SeriesLabels = setLabelsRetainExisting(e.SeriesLabels, labels.Label{
				Name:  m.idLabelName,
				Value: job.tenant,
			})

			res[i] = e
		}

		results[idx] = res
		return nil
	}

	err := concurrency.ForEachJob(ctx, len(jobs), m.maxConcurrency, run)
	if err != nil {
		return nil, err
	}

	var out []exemplar.QueryResult
	for _, exemplars := range results {
		out = append(out, exemplars...)
	}

	return out, nil
}

func filterTenantsAndRewriteMatchers(idLabelName string, ids []string, allMatchers [][]*labels.Matcher) (map[string]struct{}, [][]*labels.Matcher) {
	// If there are no potential matchers by which we could filter down the list of tenants that
	// we're getting exemplars for, just return the full set and provided matchers verbatim.
	if len(allMatchers) == 0 {
		return sliceToSet(ids), allMatchers
	}

	outIDs := make(map[string]struct{})
	outMatchers := make([][]*labels.Matcher, len(allMatchers))

	// The ExemplarQuerier.Select method accepts a slice of slices of matchers. The matchers within
	// a single slice are AND'd together (intersection) and each outer slice is OR'd together (union).
	// In order to support that, we start with a set of 0 tenant IDs and add any tenant IDs that remain
	// after filtering (based on the inner slice of matchers), for each outer slice.
	for i, matchers := range allMatchers {
		filteredIDs, unrelatedMatchers := FilterValuesByMatchers(idLabelName, ids, matchers...)
		for k := range filteredIDs {
			outIDs[k] = struct{}{}
		}

		outMatchers[i] = unrelatedMatchers
	}

	return outIDs, outMatchers
}
