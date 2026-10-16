// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
)

func TestDistributor_SeriesCounts(t *testing.T) {
	fixtures := [][]mimirpb.LabelAdapter{
		labelAdapters(model.MetricNameLabel, "test_1", "status", "200"),
		labelAdapters(model.MetricNameLabel, "test_1", "status", "500"),
		labelAdapters(model.MetricNameLabel, "test_2"),
	}

	// combine adds up counts, or unions hashes, the way the querier does.
	combine := func(res SeriesCountsResult) map[string]int64 {
		out := map[string]int64{}
		sets := map[string]map[uint64]struct{}{}
		for _, resp := range res.Responses {
			for _, g := range resp.Groups {
				if res.Hashed {
					if sets[g.Value] == nil {
						sets[g.Value] = map[uint64]struct{}{}
					}
					for _, h := range g.Hashes {
						sets[g.Value][h] = struct{}{}
					}
					continue
				}
				out[g.Value] += g.Counts[0]
			}
		}
		for v, set := range sets {
			out[v] = int64(len(set))
		}
		return out
	}

	for name, tc := range map[string]struct {
		cfg        prepConfig
		wantHashed bool
	}{
		"classic replication, three zones": {
			cfg: prepConfig{numIngesters: 3, happyIngesters: 3, numDistributors: 1, replicationFactor: 3, ingesterZones: []string{"zone-a", "zone-b", "zone-c"}},
		},
		"classic replication, one zone, replication factor 3": {
			cfg:        prepConfig{numIngesters: 6, happyIngesters: 6, numDistributors: 1, replicationFactor: 3},
			wantHashed: true,
		},
		"ingest storage": {
			cfg: prepConfig{numIngesters: 4, happyIngesters: 4, numDistributors: 1, ingestStorageEnabled: true, limits: prepareDefaultLimits()},
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, minimize := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimize), func(t *testing.T) {
					cfg := tc.cfg
					cfg.configure = func(config *Config) { config.MinimizeIngesterRequests = minimize }
					ds, _, _, _ := prepare(t, cfg)
					ctx := user.InjectOrgID(context.Background(), "series-counts")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)
					for _, lbls := range fixtures {
						_, err := ds[0].Push(ctx, mockWriteRequest(lbls, 1, 100000))
						require.NoError(t, err)
					}

					res, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 200000})
					require.NoError(t, err)
					assert.Equal(t, tc.wantHashed, res.Hashed)
					assert.Equal(t, map[string]int64{"test_1": 2, "test_2": 1}, combine(res))

					byStatus, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 200000, GroupBy: "status"})
					require.NoError(t, err)
					assert.Equal(t, map[string]int64{"200": 1, "500": 1, "": 1}, combine(byStatus))

					if tc.wantHashed {
						_, err := ds[0].SeriesCounts(ctx, nil, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 200000, StepMs: 100000})
						require.ErrorContains(t, err, "per-bucket series counts need ingest storage or zone-aware replication")
					}
				})
			}
		})
	}
}
