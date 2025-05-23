// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/382b6ec8ae40fb5dcdcabd8019f69a4be1cd8869/search/materialize_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/dskit/cancellation"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/parquet/convert"
	"github.com/grafana/mimir/pkg/parquet/schema"
	"github.com/grafana/mimir/pkg/parquet/storage"
)

func TestMaterializeE2E(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := defaultTestConfig()
	data := generateTestData(t, st, ctx, cfg)

	// Convert to Parquet
	shard := convertToParquet(t, ctx, bkt, data, st.Head())

	t.Run("QueryByUniqueLabel", func(t *testing.T) {
		eq := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))
		found := query(t, data.minTime, data.maxTime, shard, eq)
		require.Len(t, found, cfg.totalMetricNames)

		for _, series := range found {
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.seriesHash, series.Labels().Hash())
		}

		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
		sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, nil, matchers...)
		totalFound := 0
		for _, series := range sFound {
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.seriesHash, series.Labels().Hash())
			totalFound++
		}
		require.Equal(t, cfg.totalMetricNames, totalFound)
	})

	t.Run("QueryByMetricName", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			name := fmt.Sprintf("metric_%d", rand.Int()%cfg.totalMetricNames)
			eq := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf(name))

			found := query(t, data.minTime, data.maxTime, shard, eq)
			require.Len(t, found, cfg.metricsPerMetricName, fmt.Sprintf("metric_%d", i))

			for _, series := range found {
				require.Equal(t, series.Labels().Get(labels.MetricName), name)
				require.Contains(t, data.seriesHash, series.Labels().Hash())

				totalSamples := 0
				ci := series.Iterator(nil)
				for ci.Next() {
					si := ci.At().Chunk.Iterator(nil)
					for si.Next() != chunkenc.ValNone {
						totalSamples++
					}
				}
				require.Equal(t, totalSamples, cfg.numberOfSamples)
			}

			matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}
			sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, nil, matchers...)
			totalFound := 0
			for _, series := range sFound {
				totalFound++
				require.Equal(t, series.Labels().Get(labels.MetricName), name)
				require.Contains(t, data.seriesHash, series.Labels().Hash())
			}
			require.Equal(t, cfg.metricsPerMetricName, totalFound)
		}
	})

	t.Run("QueryByTimeRange", func(t *testing.T) {
		colDuration := time.Hour
		c1 := Equal(schema.LabelToColumn(labels.MetricName), parquet.ValueOf("metric_0"))
		c2 := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_0"))

		// Test first column only
		found := query(t, data.minTime, data.minTime+colDuration.Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 1)
		require.Len(t, found[0].(*concreteChunksSeries).chks, 1)

		// Test first two columns
		found = query(t, data.minTime, data.minTime+(2*colDuration).Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 1)
		require.Len(t, found[0].(*concreteChunksSeries).chks, 2)

		// Query outside the range
		found = query(t, data.minTime+(9*colDuration).Milliseconds(), data.minTime+(10*colDuration).Milliseconds()-1, shard, c1, c2)
		require.Len(t, found, 0)
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		s, err := shard.TSDBSchema()
		require.NoError(t, err)
		d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
		m, err := NewMaterializer(s, d, shard)
		require.NoError(t, err)
		rr := []RowRange{{from: int64(0), count: shard.LabelsFile().RowGroups()[0].NumRows()}}
		ctx, cancel := context.WithCancelCause(ctx)
		cancel(cancellation.NewErrorf("test cancellation"))
		_, err = m.Materialize(ctx, 0, data.minTime, data.maxTime, false, rr)
		require.ErrorContains(t, err, "context canceled")
	})
}

type testConfig struct {
	totalMetricNames     int
	metricsPerMetricName int
	numberOfLabels       int
	randomLabels         int
	numberOfSamples      int
}

func defaultTestConfig() testConfig {
	return testConfig{
		totalMetricNames:     1_000,
		metricsPerMetricName: 20,
		numberOfLabels:       5,
		randomLabels:         3,
		numberOfSamples:      250,
	}
}

type testData struct {
	seriesHash map[uint64]*struct{}
	minTime    int64
	maxTime    int64
}

func generateTestData(t *testing.T, st *teststorage.TestStorage, ctx context.Context, cfg testConfig) testData {
	app := st.Appender(ctx)
	seriesHash := make(map[uint64]*struct{})
	builder := labels.NewScratchBuilder(cfg.numberOfLabels)

	for i := 0; i < cfg.totalMetricNames; i++ {
		for n := 0; n < cfg.metricsPerMetricName; n++ {
			builder.Reset()
			builder.Add(labels.MetricName, fmt.Sprintf("metric_%d", i))
			builder.Add("unique", fmt.Sprintf("unique_%d", n))

			for j := 0; j < cfg.numberOfLabels; j++ {
				builder.Add(fmt.Sprintf("label_name_%v", j), fmt.Sprintf("label_value_%v", j))
			}

			firstRandom := rand.Int() % 10
			for k := firstRandom; k < firstRandom+cfg.randomLabels; k++ {
				builder.Add(fmt.Sprintf("random_name_%v", k), fmt.Sprintf("random_value_%v", k))
			}

			builder.Sort()
			lbls := builder.Labels()
			seriesHash[lbls.Hash()] = &struct{}{}
			for s := 0; s < cfg.numberOfSamples; s++ {
				_, err := app.Append(0, lbls, (1 * time.Minute * time.Duration(s)).Milliseconds(), float64(i))
				require.NoError(t, err)
			}
		}
	}

	require.NoError(t, app.Commit())
	h := st.Head()

	return testData{
		seriesHash: seriesHash,
		minTime:    h.MinTime(),
		maxTime:    h.MaxTime(),
	}
}

func convertToParquet(t *testing.T, ctx context.Context, bkt *filesystem.Bucket, data testData, h convert.Convertible) *storage.ParquetShard {
	colDuration := time.Hour
	shards, err := convert.ConvertTSDBBlock(
		ctx,
		bkt,
		data.minTime,
		data.maxTime,
		[]convert.Convertible{h},
		convert.WithName("shard"),
		convert.WithColDuration(colDuration), // let's force more than 1 data col
		convert.WithRowGroupSize(500),
		convert.WithPageBufferSize(300), // force creating multiples pages
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards)

	shard, err := storage.OpenParquetShard(ctx, bkt, "shard", 0)
	require.NoError(t, err)

	return shard
}

func query(t *testing.T, mint, maxt int64, shard *storage.ParquetShard, constraints ...Constraint) []prom_storage.ChunkSeries {
	ctx := context.Background()
	for _, c := range constraints {
		require.NoError(t, c.init(shard.LabelsFile()))
	}

	s, err := shard.TSDBSchema()
	require.NoError(t, err)
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	m, err := NewMaterializer(s, d, shard)
	require.NoError(t, err)

	found := make([]prom_storage.ChunkSeries, 0, 100)
	for i, group := range shard.LabelsFile().RowGroups() {
		rr, err := Filter(context.Background(), group, constraints...)
		total := int64(0)
		for _, r := range rr {
			total += r.count
		}
		require.NoError(t, err)
		series, err := m.Materialize(ctx, i, mint, maxt, false, rr)
		require.NoError(t, err)
		found = append(found, series...)
	}
	return found
}
