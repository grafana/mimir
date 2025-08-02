// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/parquet_queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	seriesset "github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestParquetFieldFunctionality tests that the logic to detect if a block has Parquet files or not works.
func TestParquetFieldFunctionality(t *testing.T) {
	t.Run("Block with Parquet metadata", func(t *testing.T) {
		block := &bucketindex.Block{
			ID:      ulid.MustNew(1, nil),
			Parquet: &bucketindex.ConverterMarkMeta{Version: 1},
		}

		require.NotNil(t, block.Parquet)
		require.Equal(t, 1, block.Parquet.Version)
	})

	t.Run("Block without Parquet metadata", func(t *testing.T) {
		block := &bucketindex.Block{
			ID: ulid.MustNew(2, nil),
		}

		require.Nil(t, block.Parquet)
	})

	t.Run("Index ParquetBlocks method", func(t *testing.T) {
		idx := &bucketindex.Index{
			Blocks: bucketindex.Blocks{
				&bucketindex.Block{ID: ulid.MustNew(1, nil), Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
				&bucketindex.Block{ID: ulid.MustNew(2, nil)},
				&bucketindex.Block{ID: ulid.MustNew(3, nil), Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			},
		}

		parquetBlocks := idx.ParquetBlocks()
		require.Len(t, parquetBlocks, 2)
		require.NotNil(t, parquetBlocks[0].Parquet)
		require.NotNil(t, parquetBlocks[1].Parquet)

		nonParquetBlocks := idx.NonParquetBlocks()
		require.Len(t, nonParquetBlocks, 1)
		require.Nil(t, nonParquetBlocks[0].Parquet)
	})
}

// TestParquetQueryableContextFunctions tests the context functions to add and get storage type from
// context work properly
func TestParquetQueryableContextFunctions(t *testing.T) {
	block1 := &bucketindex.Block{ID: ulid.MustNew(1, nil)}
	block2 := &bucketindex.Block{ID: ulid.MustNew(2, nil)}

	t.Run("InjectBlocksIntoContext and ExtractBlocksFromContext", func(t *testing.T) {
		ctx := context.Background()

		// Test extraction from empty context
		blocks, ok := ExtractBlocksFromContext(ctx)
		require.False(t, ok)
		require.Nil(t, blocks)

		// Test injection and extraction
		ctxWithBlocks := InjectBlocksIntoContext(ctx, block1, block2)
		blocks, ok = ExtractBlocksFromContext(ctxWithBlocks)
		require.True(t, ok)
		require.Len(t, blocks, 2)
		require.Equal(t, block1.ID, blocks[0].ID)
		require.Equal(t, block2.ID, blocks[1].ID)
	})

	t.Run("AddBlockStoreTypeToContext", func(t *testing.T) {
		ctx := context.Background()

		// Test with parquet block store type
		ctxWithParquet := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
		storeType := getBlockStoreType(ctxWithParquet, tsdbBlockStore)
		require.Equal(t, parquetBlockStore, storeType)

		// Test with tsdb block store type  
		ctxWithTSDB := AddBlockStoreTypeToContext(ctx, string(tsdbBlockStore))
		storeType = getBlockStoreType(ctxWithTSDB, parquetBlockStore)
		require.Equal(t, tsdbBlockStore, storeType)

		// Test with invalid type - should return default
		ctxWithInvalid := AddBlockStoreTypeToContext(ctx, "invalid")
		storeType = getBlockStoreType(ctxWithInvalid, parquetBlockStore)
		require.Equal(t, parquetBlockStore, storeType)
	})
}

func defaultOverrides(t *testing.T) *validation.Overrides {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)

	overrides := validation.NewOverrides(limits, nil)
	return overrides
}

type mockParquetQuerier struct {
	queriedBlocks []*bucketindex.Block
	queriedHints  *storage.SelectHints
}

func (m *mockParquetQuerier) Select(ctx context.Context, sortSeries bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if blocks, ok := ExtractBlocksFromContext(ctx); ok {
		m.queriedBlocks = append(m.queriedBlocks, blocks...)
	}
	m.queriedHints = sp
	if sortSeries {
		return seriesset.NewConcreteSeriesSetFromSortedSeries(nil)
	} else {
		return seriesset.NewConcreteSeriesSetFromUnsortedSeries(nil)
	}
}

func (m *mockParquetQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if blocks, ok := ExtractBlocksFromContext(ctx); ok {
		m.queriedBlocks = append(m.queriedBlocks, blocks...)
	}
	return []string{"fromParquet"}, nil, nil
}

func (m *mockParquetQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if blocks, ok := ExtractBlocksFromContext(ctx); ok {
		m.queriedBlocks = append(m.queriedBlocks, blocks...)
	}
	return []string{"fromParquet"}, nil, nil
}

func (m *mockParquetQuerier) Reset() {
	m.queriedBlocks = nil
}

func (mockParquetQuerier) Close() error {
	return nil
}

func TestParquetQueryableFallbackLogic(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	minT := int64(10)
	maxT := util.TimeToMillis(time.Now())

	createStore := func() *blocksStoreSetMock {
		return &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1",
					mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.FromStrings(labels.MetricName, "fromSg"), 1, 1),
						mockHintsResponse(block1, block2),
					},
					mockedLabelNamesResponse: &storepb.LabelNamesResponse{
						Names:    namesFromSeries(labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockNamesHints(block1, block2),
					},
					mockedLabelValuesResponse: &storepb.LabelValuesResponse{
						Values:   valuesFromSeries(labels.MetricName, labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockValuesHints(block1, block2),
					},
				}: {block1, block2}},
		},
		}
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "fromSg"),
	}
	ctx := user.InjectOrgID(context.Background(), "user-1")

	t.Run("should fallback all blocks", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:             &blocksStoreLimitsMock{},
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       time.Hour,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1},
			&bucketindex.Block{ID: block2},
		}, nil)

		t.Run("select", func(t *testing.T) {
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			_, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			_, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})
	})

	t.Run("should fallback partial blocks", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:             &blocksStoreLimitsMock{},
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2},
		}, nil)

		t.Run("select", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 1)
			require.Len(t, mParquetQuerier.queriedBlocks, 1)
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 1)
			require.Len(t, mParquetQuerier.queriedBlocks, 1)
			require.Contains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 1)
			require.Len(t, mParquetQuerier.queriedBlocks, 1)
			require.Contains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})
	})

	t.Run("should query only parquet blocks when possible", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:             &blocksStoreLimitsMock{},
		}

		mParquetQuerier := &mockParquetQuerier{}
		queryStoreAfter := time.Hour
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       queryStoreAfter,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
		}, nil)

		t.Run("select", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			hints := storage.SelectHints{
				Start: minT,
				End:   maxT,
			}
			ss := pq.Select(ctx, true, &hints, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.Equal(t, mParquetQuerier.queriedHints.Start, minT)
			queriedDelta := time.Duration(maxT-mParquetQuerier.queriedHints.End) * time.Millisecond
			require.InDeltaf(t, queriedDelta.Minutes(), queryStoreAfter.Minutes(), 0.1, "query after not set")
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})
	})

	t.Run("Default query TSDB block store even if parquet blocks available. Override with ctx", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:             &blocksStoreLimitsMock{},
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: tsdbBlockStore,
		}

		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
		}, nil)

		t.Run("select", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
		})

		t.Run("select with ctx key override to parquet", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			newCtx := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
			ss := pq.Select(newCtx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})

		t.Run("labelNames", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
			require.Contains(t, r, "fromSg")
			require.NotContains(t, r, "fromParquet")
		})

		t.Run("labelNames with ctx key override to parquet", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			newCtx := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
			r, _, err := pq.LabelNames(newCtx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})

		t.Run("labelValues", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			r, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 2)
			require.Len(t, mParquetQuerier.queriedBlocks, 0)
			require.Contains(t, r, "fromSg")
			require.NotContains(t, r, "fromParquet")
		})

		t.Run("labelValues with ctx key override to parquet", func(t *testing.T) {
			stores.Reset()
			mParquetQuerier.Reset()
			newCtx := AddBlockStoreTypeToContext(ctx, string(parquetBlockStore))
			r, _, err := pq.LabelValues(newCtx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, stores.queriedBlocks, 0)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
			require.NotContains(t, r, "fromSg")
			require.Contains(t, r, "fromParquet")
		})
	})
}

func TestParquetQueryableFallbackDisabled(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	minT := int64(10)
	maxT := util.TimeToMillis(time.Now())

	createStore := func() *blocksStoreSetMock {
		return &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1",
					mockedSeriesResponses: []*storepb.SeriesResponse{
						mockSeriesResponse(labels.FromStrings(labels.MetricName, "fromSg"), 1, 1),
						mockHintsResponse(block1, block2),
					},
					mockedLabelNamesResponse: &storepb.LabelNamesResponse{
						Names:    namesFromSeries(labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockNamesHints(block1, block2),
					},
					mockedLabelValuesResponse: &storepb.LabelValuesResponse{
						Values:   valuesFromSeries(labels.MetricName, labels.FromMap(map[string]string{labels.MetricName: "fromSg", "fromSg": "fromSg"})),
						Warnings: []string{},
						Hints:    mockValuesHints(block1, block2),
					},
				}: {block1, block2}},
		},
		}
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "fromSg"),
	}
	ctx := user.InjectOrgID(context.Background(), "user-1")

	t.Run("should return consistency check errors when fallback disabled and some blocks not available as parquet", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:             &blocksStoreLimitsMock{},
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       time.Hour,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
			fallbackDisabled:      true, // Disable fallback
		}

		// Set up blocks where block1 has parquet metadata but block2 doesn't
		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2},
		}, nil)

		expectedError := fmt.Sprintf("consistency check failed because some blocks were not available as parquet files: %s", block2.String())

		t.Run("select should return consistency check error", func(t *testing.T) {
			ss := pq.Select(ctx, true, nil, matchers...)
			require.Error(t, ss.Err())
			require.Contains(t, ss.Err().Error(), expectedError)
		})

		t.Run("labelNames should return consistency check error", func(t *testing.T) {
			_, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.Error(t, err)
			require.Contains(t, err.Error(), expectedError)
		})

		t.Run("labelValues should return consistency check error", func(t *testing.T) {
			_, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.Error(t, err)
			require.Contains(t, err.Error(), expectedError)
		})
	})

	t.Run("should work normally when all blocks are available as parquet and fallback disabled", func(t *testing.T) {
		finder := &blocksFinderMock{}
		stores := createStore()

		q := &blocksStoreQuerier{
			minT:               minT,
			maxT:               maxT,
			finder:             finder,
			stores:             stores,
			dynamicReplication: newDynamicReplication(),
			consistency:        NewBlocksConsistency(0, nil),
			logger:             log.NewNopLogger(),
			metrics:            newBlocksStoreQueryableMetrics(prometheus.NewPedanticRegistry()),
			limits:             &blocksStoreLimitsMock{},
		}

		mParquetQuerier := &mockParquetQuerier{}
		pq := &parquetQuerierWithFallback{
			minT:                  minT,
			maxT:                  maxT,
			finder:                finder,
			blocksStoreQuerier:    q,
			parquetQuerier:        mParquetQuerier,
			queryStoreAfter:       time.Hour,
			metrics:               newParquetQueryableFallbackMetrics(prometheus.NewRegistry()),
			limits:                defaultOverrides(t),
			logger:                log.NewNopLogger(),
			defaultBlockStoreType: parquetBlockStore,
			fallbackDisabled:      true, // Disable fallback
		}

		// Set up blocks where both blocks have parquet metadata
		finder.On("GetBlocks", mock.Anything, "user-1", minT, mock.Anything).Return(bucketindex.Blocks{
			&bucketindex.Block{ID: block1, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
			&bucketindex.Block{ID: block2, Parquet: &bucketindex.ConverterMarkMeta{Version: 1}},
		}, nil)

		t.Run("select should work without error", func(t *testing.T) {
			mParquetQuerier.Reset()
			ss := pq.Select(ctx, true, nil, matchers...)
			require.NoError(t, ss.Err())
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})

		t.Run("labelNames should work without error", func(t *testing.T) {
			mParquetQuerier.Reset()
			_, _, err := pq.LabelNames(ctx, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})

		t.Run("labelValues should work without error", func(t *testing.T) {
			mParquetQuerier.Reset()
			_, _, err := pq.LabelValues(ctx, labels.MetricName, nil, matchers...)
			require.NoError(t, err)
			require.Len(t, mParquetQuerier.queriedBlocks, 2)
		})
	})
}
