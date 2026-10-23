// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// A request whose series carry metadata of their own leaves an ingester on each engine the same series and metric
// families' metadata whether it comes in records, with stand-ins for the families' metadata, or is pushed directly.
func TestRecordsWithSeriesMetadataIngestLikeTheirRequest(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		t.Run(engine, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.MaxGlobalExemplarsPerUser = 100
			cfg := defaultIngesterTestConfig(t)
			cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
			cfg.BlocksStorageConfig.TSDB.Engine = engine
			ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(limits, nil), nil, nil, nil)

			now := time.Now().UnixMilli()
			write, last := compatProducer(t, cfg.IngestStorageConfig.KafkaConfig)
			write("records", legacyMetadataRecordFixture(now))
			offset := last()
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
			waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
			// The exemplar of the series without samples is a soft error, as it is in the records.
			err := ingester.PushToStorageAndReleaseRequest(user.InjectOrgID(context.Background(), "direct"), legacyMetadataRecordFixture(now))
			require.ErrorContains(t, err, "err-mimir-exemplar-series-missing")

			api := serveIngester(t, ingester)
			series := map[string]map[string][]string{}
			metadata := map[string][]string{}
			for _, tenant := range []string{"records", "direct"} {
				ctx := parityContext(tenant, offset)
				series[tenant] = paritySeries(t, api, ctx, parityRequest(math.MinInt64, math.MaxInt64, parityMatcher(client.REGEX_MATCH, "__name__", ".+")))
				response, err := api.MetricsMetadata(ctx, &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
				require.NoError(t, err)
				for _, m := range response.Metadata {
					metadata[tenant] = append(metadata[tenant], fmt.Sprintf("%s|%s|%s|%s", m.MetricFamilyName, m.Type, m.Help, m.Unit))
				}
				slices.Sort(metadata[tenant])
			}
			require.NotEmpty(t, series["direct"])
			require.Equal(t, series["direct"], series["records"])
			require.NotEmpty(t, metadata["direct"])
			require.Equal(t, metadata["direct"], metadata["records"])
		})
	}
}
