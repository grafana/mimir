// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/testutil/series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"path"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/parquet/convert"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestParquetBlockLabelNames(t *testing.T) {
	const series = 500

	allLabelNames := []string{"i", "n", "j", "p", "q", "r", "s", "t"}
	jFooLabelNames := []string{"i", "j", "n", "p", "t"}
	jNotFooLabelNames := []string{"i", "j", "n", "q", "r", "s"}
	slices.Sort(allLabelNames)
	slices.Sort(jFooLabelNames)
	slices.Sort(jNotFooLabelNames)

	sl := NewLimiter(math.MaxUint64, promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}), func(limit uint64) validation.LimitError {
		return validation.NewLimitError(fmt.Sprintf("exceeded unlimited limit of %v", limit))
	})
	newTestBucketBlock := prepareTestParquetBlock(test.NewTB(t), appendTestSeries(series))

	t.Run("happy case with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		names, err := parquetBlockLabelNames(context.Background(), b, nil, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, allLabelNames, names)
	})

	t.Run("index reader error with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:             b.indexHeaderReader,
			onLabelNamesCalled: func() error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelNames{t: t}
		_, err := parquetBlockLabelNames(context.Background(), b, nil, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.Error(t, err)
	})

	t.Run("happy case cached with no matchers", func(t *testing.T) {
		expectedCalls := 1
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader: b.indexHeaderReader,
			onLabelNamesCalled: func() error {
				expectedCalls--
				if expectedCalls < 0 {
					return fmt.Errorf("didn't expect another index.Reader.LabelNames() call")
				}
				return nil
			},
		}
		b.indexCache = newInMemoryIndexCache(t)

		names, err := parquetBlockLabelNames(context.Background(), b, nil, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, allLabelNames, names)

		// hit the cache now
		names, err = parquetBlockLabelNames(context.Background(), b, nil, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, allLabelNames, names)
	})

	t.Run("error with matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:                     b.indexHeaderReader,
			onLabelValuesOffsetsCalled: func(_ string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelNames{t: t}

		// This test relies on the fact that j!=foo has to call LabelValues(j).
		// We make that call fail in order to make the entire LabelNames(j!=foo) call fail.
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "j", "foo.*bar")}
		_, err := parquetBlockLabelNames(context.Background(), b, matchers, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.Error(t, err)
	})

	t.Run("happy case cached with matchers", func(t *testing.T) {
		expectedCalls := 1
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader: b.indexHeaderReader,
			onLabelNamesCalled: func() error {
				return fmt.Errorf("not expected the LabelNames() calls with matchers")
			},
			onLabelValuesOffsetsCalled: func(string) error {
				expectedCalls--
				if expectedCalls < 0 {
					return fmt.Errorf("didn't expect another index.Reader.LabelValues() call")
				}
				return nil
			},
		}
		b.indexCache = newInMemoryIndexCache(t)

		jFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "j", "foo")}
		_, err := parquetBlockLabelNames(context.Background(), b, jFooMatchers, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		jNotFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")}
		_, err = parquetBlockLabelNames(context.Background(), b, jNotFooMatchers, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)

		// hit the cache now
		names, err := parquetBlockLabelNames(context.Background(), b, jFooMatchers, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, jFooLabelNames, names)
		names, err = parquetBlockLabelNames(context.Background(), b, jNotFooMatchers, sl, 5000, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, jNotFooLabelNames, names)
	})
}

func TestParquetBlockLabelValues(t *testing.T) {
	const series = 100_000

	newTestBucketBlock := prepareTestParquetBlock(test.NewTB(t), appendTestSeries(series))

	t.Run("happy case with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		names, err := parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar", "foo"}, names)
	})

	t.Run("index reader error with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:                     b.indexHeaderReader,
			onLabelValuesOffsetsCalled: func(string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelValues{t: t}

		_, err := parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.Error(t, err)
	})

	t.Run("happy case cached with no matchers", func(t *testing.T) {
		expectedCalls := 1
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader: b.indexHeaderReader,
			onLabelValuesOffsetsCalled: func(string) error {
				expectedCalls--
				if expectedCalls < 0 {
					return fmt.Errorf("didn't expect another index.Reader.LabelValues() call")
				}
				return nil
			},
		}
		b.indexCache = newInMemoryIndexCache(t)

		names, err := parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar", "foo"}, names)

		// hit the cache now
		names, err = parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar", "foo"}, names)
	})

	t.Run("error with matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:                     b.indexHeaderReader,
			onLabelValuesOffsetsCalled: func(_ string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelValues{t: t}

		// This test relies on the fact that p~=foo.* has to call LabelValues(p) when doing ExpandedPostings().
		// We make that call fail in order to make the entire LabelValues(p~=foo.*) call fail.
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "p", "foo.*")}
		_, err := parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", matchers, log.NewNopLogger(), newSafeQueryStats())
		require.Error(t, err)
	})

	t.Run("happy case cached with matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexCache = newInMemoryIndexCache(t)

		pFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "p", "foo")}
		values, err := parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", pFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"foo"}, values)

		qFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "q", "foo")}
		values, err = parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", qFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar"}, values)

		// we break the indexHeaderReader to ensure that results come from a cache
		b.indexHeaderReader = deadlineExceededIndexHeader()

		values, err = parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", pFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"foo"}, values)
		values, err = parquetBlockLabelValues(context.Background(), b, selectAllStrategy{}, 5000, "j", qFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar"}, values)
	})

	t.Run("happy case cached with weak matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexCache = newInMemoryIndexCache(t)

		matchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "p", "foo"),
			labels.MustNewMatcher(labels.MatchRegexp, "i", "1234.+"),
			labels.MustNewMatcher(labels.MatchRegexp, "j", ".+"), // this is too weak and doesn't bring much value, it should be shortcut
		}
		values, err := parquetBlockLabelValues(context.Background(), b, worstCaseFetchedDataStrategy{1.0}, 5000, "j", matchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"foo"}, values)

		// we break the indexHeaderReader to ensure that results come from a cache
		b.indexHeaderReader = deadlineExceededIndexHeader()

		values, err = parquetBlockLabelValues(context.Background(), b, worstCaseFetchedDataStrategy{1.0}, 5000, "j", matchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"foo"}, values)
	})
}

func prepareTestParquetBlock(tb test.TB, dataSetup ...testBlockDataSetup) func() *parquetBucketBlock {
	tmpDir := tb.TempDir()
	bucketDir := filepath.Join(tmpDir, "bkt")

	ubkt, err := filesystem.NewBucket(bucketDir)
	assert.NoError(tb, err)

	bkt := objstore.WithNoopInstr(ubkt)

	tb.Cleanup(func() {
		assert.NoError(tb, ubkt.Close())
		assert.NoError(tb, bkt.Close())
	})

	id, minT, maxT := uploadTestBlock(tb, tmpDir, bkt, dataSetup)
	err = convertBlockToParquet(tb, id, bkt, minT, maxT)
	assert.NoError(tb, err)

	return func() *parquetBucketBlock {
		var chunkObjects []string
		err := bkt.Iter(context.Background(), path.Join(id.String(), "chunks"), func(s string) error {
			chunkObjects = append(chunkObjects, s)
			return nil
		})
		require.NoError(tb, err)

		return &parquetBucketBlock{
			userID:  "tenant",
			logger:  log.NewNopLogger(),
			metrics: NewBucketStoreMetrics(nil),
			// indexHeaderReader: r,
			indexCache:   noopCache{},
			chunkObjs:    chunkObjects,
			bkt:          localBucket{Bucket: ubkt, dir: bucketDir},
			meta:         &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, MinTime: minT, MaxTime: maxT}},
			partitioners: newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		}
	}
}

func convertBlockToParquet(tb test.TB, id ulid.ULID, bkt objstore.Bucket, minT, maxT int64) error {
	// TODO(npazosmendez): we are uploading the TSDB block, then downloading, converting and uploading.
	// This is unnecessarily inneficient for the test, but easier to start with.
	ctx := context.Background()
	bdir := filepath.Join(tb.TempDir(), id.String())
	err := block.Download(ctx, log.NewNopLogger(), bkt, id, bdir, objstore.WithFetchConcurrency(10))
	if err != nil {
		return err
	}

	tsdbBlock, err := tsdb.OpenBlock(&slog.Logger{}, bdir, nil, tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return err
	}

	_, err = convert.ConvertTSDBBlock(
		ctx,
		bkt,
		minT,
		maxT,
		[]convert.Convertible{tsdbBlock},
		convert.WithName(id.String()),
	)
	return err
}
