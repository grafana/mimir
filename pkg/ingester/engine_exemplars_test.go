// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func TestIngesterExemplarBatchErrors(t *testing.T) {
	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		for _, tooOld := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/too_old=%t", engine, tooOld), func(t *testing.T) {
				cfg := defaultIngesterTestConfig(t)
				cfg.BlocksStorageConfig.TSDB.Engine = engine
				limits := defaultLimitsTestConfig()
				limits.MaxGlobalExemplarsPerUser = 10
				limits.PastGracePeriod = model.Duration(time.Hour)
				i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", nil)
				require.NoError(t, err)
				startAndWaitHealthy(t, i, r)
				ctx := user.InjectOrgID(context.Background(), userID)
				now := time.Now().UnixMilli()
				valid := mimirpb.Exemplar{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "valid"}}, TimestampMs: now, Value: 1}
				invalid := mimirpb.Exemplar{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: strings.Repeat("x", 128)}}, TimestampMs: now, Value: 2}
				exemplars := []mimirpb.Exemplar{valid, invalid, {TimestampMs: now + int64(24*time.Hour/time.Millisecond)}}
				wantErr := "label length"
				if tooOld {
					exemplars = []mimirpb.Exemplar{{TimestampMs: now - int64(2*time.Hour/time.Millisecond)}, invalid, valid}
					wantErr = "too far in the past"
				}
				req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{compatSeries("exemplar_test", mimirpb.Sample{TimestampMs: now, Value: 1})}}
				req.Timeseries[0].Exemplars = exemplars
				_, err = i.Push(ctx, req)
				require.ErrorContains(t, err, wantErr)
				require.Equal(t, float64(1), testutil.ToFloat64(i.metrics.ingestedExemplars))
				require.Equal(t, float64(2), testutil.ToFloat64(i.metrics.ingestedExemplarsFail))
				result, err := i.QueryExemplars(ctx, &client.ExemplarQueryRequest{
					StartTimestampMs: math.MinInt64, EndTimestampMs: math.MaxInt64,
					Matchers: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: "exemplar_test"}}}},
				})
				require.NoError(t, err)
				require.Len(t, result.Timeseries, 1)
				require.Equal(t, []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "valid"}}, TimestampMs: now, Value: 1}}, result.Timeseries[0].Exemplars)
			})
		}
	}
}

func BenchmarkEnginePushExemplars(b *testing.B) {
	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		for _, perSeries := range []int{0, 1, 4} {
			b.Run(fmt.Sprintf("%s/exemplars=%d", engine, perSeries), func(b *testing.B) {
				cfg := defaultIngesterTestConfig(b)
				cfg.BlocksStorageConfig.TSDB.Engine = engine
				limits := defaultLimitsTestConfig()
				limits.MaxGlobalExemplarsPerUser = 100_000
				i, r, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", nil)
				require.NoError(b, err)
				startAndWaitHealthy(b, i, r)
				ctx := user.InjectOrgID(b.Context(), userID)
				allLabels, _ := benchmarkData(1000)
				req := &mimirpb.WriteRequest{Source: mimirpb.API}
				for _, lbls := range allLabels {
					es := make([]mimirpb.Exemplar, perSeries)
					for n := range es {
						es[n] = mimirpb.Exemplar{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: fmt.Sprint(n)}}, Value: float64(n)}
					}
					req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: lbls, Samples: []mimirpb.Sample{{Value: 1}}, Exemplars: es}})
				}
				start := time.Now().UnixMilli()
				b.ReportAllocs()
				b.ResetTimer()
				for iter := 0; iter < b.N; iter++ {
					ts := start + int64(iter*max(1, perSeries))
					for _, series := range req.Timeseries {
						series.Samples[0].TimestampMs = ts
						for n := range series.Exemplars {
							series.Exemplars[n].TimestampMs = ts + int64(n)
						}
					}
					// Keep the request for reuse, excluding request construction from the measurement.
					require.NoError(b, i.PushWithCleanup(ctx, req, func() {}))
				}
				b.StopTimer()
				require.Equal(b, float64(b.N*len(allLabels)*perSeries), testutil.ToFloat64(i.metrics.ingestedExemplars))
				require.Zero(b, testutil.ToFloat64(i.metrics.ingestedExemplarsFail))
			})
		}
	}
}
