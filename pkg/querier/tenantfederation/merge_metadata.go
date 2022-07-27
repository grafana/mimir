// SPDX-License-Identifier: AGPL-3.0-only

package tenantfederation

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NewMetadataSupplier returns a querier.MetadataSupplier that returns metric
// metadata for all tenant IDs that are part of the request and merges the results.
//
// No deduplication of metadata is done before being returned.
func NewMetadataSupplier(next querier.MetadataSupplier, logger log.Logger) querier.MetadataSupplier {
	return &mergeMetadataSupplier{
		next:     next,
		logger:   logger,
		resolver: tenant.NewMultiResolver(),
	}
}

type mergeMetadataSupplier struct {
	next     querier.MetadataSupplier
	resolver tenant.Resolver
	logger   log.Logger
}

func (m *mergeMetadataSupplier) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeMetadataSupplier.MetricsMetadata")
	defer spanlog.Finish()

	tenantIDs, err := m.resolver.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	if len(tenantIDs) == 1 {
		level.Debug(spanlog).Log("msg", "only a single tenant, bypassing federated metadata supplier")
		return m.next.MetricsMetadata(ctx)
	}

	results := make([][]scrape.MetricMetadata, len(tenantIDs))
	run := func(jobCtx context.Context, idx int) error {
		tenantID := tenantIDs[idx]
		res, err := m.next.MetricsMetadata(user.InjectOrgID(jobCtx, tenantID))
		if err != nil {
			return fmt.Errorf("unable to run federated metadata request for %s: %w", tenantID, err)
		}

		level.Debug(spanlog).Log("msg", "adding results for tenant to merged results", "user", tenantID, "results", len(res))
		results[idx] = res
		return nil
	}

	err = concurrency.ForEachJob(ctx, len(tenantIDs), maxConcurrency, run)
	if err != nil {
		return nil, err
	}

	// Deduplicate results across tenants since the contract for the metadata endpoint
	// requires that each returned metric metadata is unique.
	var out []scrape.MetricMetadata
	unique := make(map[scrape.MetricMetadata]struct{})
	for _, metadata := range results {
		for _, m := range metadata {
			if _, exists := unique[m]; !exists {
				out = append(out, m)
				unique[m] = struct{}{}
			}
		}
	}

	return out, nil
}
