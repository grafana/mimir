// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// fetchMetricMetadataFromDistributor fetches metric metadata for the given
// metric names from the ingesters. When a metric has more than one metadata
// record, the first one seen wins.
//
// Shared by distributorQuerier.FetchMetricMetadata (the include_metadata=true
// enrichment on /search/metric_names) and the /search/metadata endpoint, so
// the two call sites cannot drift apart on this tie-break logic.
func fetchMetricMetadataFromDistributor(ctx context.Context, d Distributor, names []string) (map[string]metadata.Metadata, error) {
	resp, err := d.MetricsMetadata(ctx, &client.MetricsMetadataRequest{
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
