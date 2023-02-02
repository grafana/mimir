// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/testutil/series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

const (
	// labelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
	labelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
)

func TestBucketBlock_matchLabels(t *testing.T) {
	dir := t.TempDir()

	bkt, err := filesystem.NewBucket(dir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	blockID := ulid.MustNew(1, nil)
	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: blockID},
		Thanos: metadata.Thanos{
			Labels: map[string]string{}, // this is empty in Mimir
		},
	}

	b, err := newBucketBlock(context.Background(), "test", log.NewNopLogger(), NewBucketStoreMetrics(nil), meta, bkt, path.Join(dir, blockID.String()), nil, nil, nil, blockPartitioners{})
	assert.NoError(t, err)

	cases := []struct {
		in    []*labels.Matcher
		match bool
	}{
		{
			in:    []*labels.Matcher{},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "d"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "b"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "e", Value: "f"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: block.BlockIDLabel, Value: blockID.String()},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: block.BlockIDLabel, Value: "xxx"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: block.BlockIDLabel, Value: blockID.String()},
				{Type: labels.MatchEqual, Name: "c", Value: "b"},
			},
			match: false,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "x"},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "d"},
			},
			match: true,
		},
	}
	for _, c := range cases {
		ok := b.matchLabels(c.in)
		assert.Equal(t, c.match, ok)
	}

	// Ensure block's labels in the meta have not been manipulated.
	assert.Equal(t, map[string]string{}, meta.Thanos.Labels)
}

func TestBucketBlockSet_remove(t *testing.T) {
	set := newBucketBlockSet()

	type resBlock struct {
		id         ulid.ULID
		mint, maxt int64
	}
	input := []resBlock{
		{id: ulid.MustNew(1, nil), mint: 0, maxt: 100},
		{id: ulid.MustNew(2, nil), mint: 100, maxt: 200},
		{id: ulid.MustNew(3, nil), mint: 200, maxt: 300},
	}

	for _, in := range input {
		var m metadata.Meta
		m.ULID = in.id
		m.MinTime = in.mint
		m.MaxTime = in.maxt
		assert.NoError(t, set.add(&bucketBlock{meta: &m}))
	}
	set.remove(input[1].id)
	res := set.getFor(0, 300, nil)

	assert.Equal(t, 2, len(res))
	assert.Equal(t, input[0].id, res[0].meta.ULID)
	assert.Equal(t, input[2].id, res[1].meta.ULID)
}

// Regression tests against: https://github.com/thanos-io/thanos/issues/1983.
func TestReadIndexCache_LoadSeries(t *testing.T) {
	bkt := objstore.NewInMemBucket()

	s := NewBucketStoreMetrics(nil)
	b := &bucketBlock{
		meta: &metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ulid.MustNew(1, nil),
			},
		},
		bkt:        bkt,
		logger:     log.NewNopLogger(),
		metrics:    s,
		indexCache: noopCache{},
	}

	buf := encoding.Encbuf{}
	buf.PutByte(0)
	buf.PutByte(0)
	buf.PutUvarint(10)
	buf.PutString("aaaaaaaaaa")
	buf.PutUvarint(10)
	buf.PutString("bbbbbbbbbb")
	buf.PutUvarint(10)
	buf.PutString("cccccccccc")
	assert.NoError(t, bkt.Upload(context.Background(), filepath.Join(b.meta.ULID.String(), block.IndexFilename), bytes.NewReader(buf.Get())))

	r := bucketIndexReader{
		block: b,
	}

	// Success with no refetches.
	loaded := newBucketIndexLoadedSeries()
	assert.NoError(t, r.loadSeries(context.TODO(), []storage.SeriesRef{2, 13, 24}, false, 2, 100, loaded, newSafeQueryStats()))
	assert.Equal(t, map[storage.SeriesRef][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, loaded.series)
	assert.Equal(t, float64(0), promtest.ToFloat64(s.seriesRefetches))

	// Success with 2 refetches.
	loaded = newBucketIndexLoadedSeries()
	assert.NoError(t, r.loadSeries(context.TODO(), []storage.SeriesRef{2, 13, 24}, false, 2, 15, loaded, newSafeQueryStats()))
	assert.Equal(t, map[storage.SeriesRef][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, loaded.series)
	assert.Equal(t, float64(2), promtest.ToFloat64(s.seriesRefetches))

	// Success with refetch on first element.
	loaded = newBucketIndexLoadedSeries()
	assert.NoError(t, r.loadSeries(context.TODO(), []storage.SeriesRef{2}, false, 2, 5, loaded, newSafeQueryStats()))
	assert.Equal(t, map[storage.SeriesRef][]byte{
		2: []byte("aaaaaaaaaa"),
	}, loaded.series)
	assert.Equal(t, float64(3), promtest.ToFloat64(s.seriesRefetches))

	buf.Reset()
	buf.PutByte(0)
	buf.PutByte(0)
	buf.PutUvarint(10)
	buf.PutString("aaaaaaa")
	assert.NoError(t, bkt.Upload(context.Background(), filepath.Join(b.meta.ULID.String(), block.IndexFilename), bytes.NewReader(buf.Get())))

	// Fail, but no recursion at least.
	assert.Error(t, r.loadSeries(context.TODO(), []storage.SeriesRef{2, 13, 24}, false, 1, 15, newBucketIndexLoadedSeries(), newSafeQueryStats()))
}

func TestBlockLabelNames(t *testing.T) {
	const series = 500

	allLabelNames := []string{"i", "n", "j", "p", "q", "r", "s", "t"}
	jFooLabelNames := []string{"i", "j", "n", "p", "t"}
	jNotFooLabelNames := []string{"i", "j", "n", "q", "r", "s"}
	slices.Sort(allLabelNames)
	slices.Sort(jFooLabelNames)
	slices.Sort(jNotFooLabelNames)

	sl := NewLimiter(math.MaxUint64, promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}))
	newTestBucketBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), appendTestSeries(series))

	t.Run("happy case with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		names, err := blockLabelNames(context.Background(), b.indexReader(), nil, sl, log.NewNopLogger())
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

		_, err := blockLabelNames(context.Background(), b.indexReader(), nil, sl, log.NewNopLogger())
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

		names, err := blockLabelNames(context.Background(), b.indexReader(), nil, sl, log.NewNopLogger())
		require.NoError(t, err)
		require.Equal(t, allLabelNames, names)

		// hit the cache now
		names, err = blockLabelNames(context.Background(), b.indexReader(), nil, sl, log.NewNopLogger())
		require.NoError(t, err)
		require.Equal(t, allLabelNames, names)
	})

	t.Run("error with matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:              b.indexHeaderReader,
			onLabelValuesCalled: func(_ string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelNames{t: t}

		// This test relies on the fact that j!=foo has to call LabelValues(j).
		// We make that call fail in order to make the entire LabelNames(j!=foo) call fail.
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "j", "foo.*bar")}
		_, err := blockLabelNames(context.Background(), b.indexReader(), matchers, sl, log.NewNopLogger())
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
			onLabelValuesCalled: func(name string) error {
				expectedCalls--
				if expectedCalls < 0 {
					return fmt.Errorf("didn't expect another index.Reader.LabelValues() call")
				}
				return nil
			},
		}
		b.indexCache = newInMemoryIndexCache(t)

		jFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "j", "foo")}
		_, err := blockLabelNames(context.Background(), b.indexReader(), jFooMatchers, sl, log.NewNopLogger())
		require.NoError(t, err)
		jNotFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")}
		_, err = blockLabelNames(context.Background(), b.indexReader(), jNotFooMatchers, sl, log.NewNopLogger())
		require.NoError(t, err)

		// hit the cache now
		names, err := blockLabelNames(context.Background(), b.indexReader(), jFooMatchers, sl, log.NewNopLogger())
		require.NoError(t, err)
		require.Equal(t, jFooLabelNames, names)
		names, err = blockLabelNames(context.Background(), b.indexReader(), jNotFooMatchers, sl, log.NewNopLogger())
		require.NoError(t, err)
		require.Equal(t, jNotFooLabelNames, names)
	})
}

type cacheNotExpectingToStoreLabelNames struct {
	noopCache
	t *testing.T
}

func (c cacheNotExpectingToStoreLabelNames) StoreLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey indexcache.LabelMatchersKey, v []byte) {
	c.t.Fatalf("StoreLabelNames should not be called")
}

func TestBlockLabelValues(t *testing.T) {
	const series = 500

	newTestBucketBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), appendTestSeries(series))

	t.Run("happy case with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		names, err := blockLabelValues(context.Background(), b.indexReader(), "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar", "foo"}, names)
	})

	t.Run("index reader error with no matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:              b.indexHeaderReader,
			onLabelValuesCalled: func(name string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelValues{t: t}

		_, err := blockLabelValues(context.Background(), b.indexReader(), "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.Error(t, err)
	})

	t.Run("happy case cached with no matchers", func(t *testing.T) {
		expectedCalls := 1
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader: b.indexHeaderReader,
			onLabelValuesCalled: func(name string) error {
				expectedCalls--
				if expectedCalls < 0 {
					return fmt.Errorf("didn't expect another index.Reader.LabelValues() call")
				}
				return nil
			},
		}
		b.indexCache = newInMemoryIndexCache(t)

		names, err := blockLabelValues(context.Background(), b.indexReader(), "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar", "foo"}, names)

		// hit the cache now
		names, err = blockLabelValues(context.Background(), b.indexReader(), "j", nil, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar", "foo"}, names)
	})

	t.Run("error with matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:              b.indexHeaderReader,
			onLabelValuesCalled: func(_ string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreLabelValues{t: t}

		// This test relies on the fact that p~=foo.* has to call LabelValues(p) when doing ExpandedPostings().
		// We make that call fail in order to make the entire LabelValues(p~=foo.*) call fail.
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "p", "foo.*")}
		_, err := blockLabelValues(context.Background(), b.indexReader(), "j", matchers, log.NewNopLogger(), newSafeQueryStats())
		require.Error(t, err)
	})

	t.Run("happy case cached with matchers", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexCache = newInMemoryIndexCache(t)

		pFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "p", "foo")}
		values, err := blockLabelValues(context.Background(), b.indexReader(), "j", pFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"foo"}, values)

		qFooMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "q", "foo")}
		values, err = blockLabelValues(context.Background(), b.indexReader(), "j", qFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar"}, values)

		// we remove the indexHeaderReader to ensure that results come from a cache
		// if this panics, then we know that it's trying to read actual values
		indexrWithoutHeaderReader := b.indexReader()
		indexrWithoutHeaderReader.block.indexHeaderReader = nil

		values, err = blockLabelValues(context.Background(), indexrWithoutHeaderReader, "j", pFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"foo"}, values)
		values, err = blockLabelValues(context.Background(), indexrWithoutHeaderReader, "j", qFooMatchers, log.NewNopLogger(), newSafeQueryStats())
		require.NoError(t, err)
		require.Equal(t, []string{"bar"}, values)
	})
}

type cacheNotExpectingToStoreLabelValues struct {
	noopCache
	t *testing.T
}

func (c cacheNotExpectingToStoreLabelValues) StoreLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey indexcache.LabelMatchersKey, v []byte) {
	c.t.Fatalf("StoreLabelValues should not be called")
}

func TestBucketIndexReader_ExpandedPostings(t *testing.T) {
	tb := test.NewTB(t)
	const series = 500

	bucketBlockFactories := map[string]func() *bucketBlock{
		"binary reader": prepareTestBlockWithBinaryReader(tb, appendTestSeries(series)),
		"stream reader": prepareTestBlockWithStreamReader(tb, appendTestSeries(series)),
	}

	for name, newTestBucketBlock := range bucketBlockFactories {
		t.Run(name, func(t *testing.T) {
			t.Run("happy cases", func(t *testing.T) {
				benchmarkExpandedPostings(test.NewTB(t), newTestBucketBlock, series)
			})

			t.Run("corrupted or undecodable postings cache doesn't fail", func(t *testing.T) {
				b := newTestBucketBlock()
				b.indexCache = corruptedPostingsCache{}

				// cache provides undecodable values
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")}
				refs, err := b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Equal(t, series, len(refs))
			})

			t.Run("promise", func(t *testing.T) {
				expectedErr := fmt.Errorf("failed as expected")

				labelValuesCalls := map[string]*sync.WaitGroup{"i": {}, "n": {}, "fail": {}}
				for _, c := range labelValuesCalls {
					// we expect one call for each label name
					c.Add(1)
				}

				releaseCalls := make(chan struct{})
				onlabelValuesCalled := func(labelName string) error {
					// this will panic if unexpected label is called, or called too many (>1) times
					labelValuesCalls[labelName].Done()
					<-releaseCalls
					if labelName == "fail" {
						return expectedErr
					}
					return nil
				}

				b := newTestBucketBlock()
				b.indexHeaderReader = &interceptedIndexReader{
					Reader:              b.indexHeaderReader,
					onLabelValuesCalled: onlabelValuesCalled,
				}

				// we're building a scenario where:
				// - first three calls (0, 1, 2) will be called concurrently with same matchers
				//   - call 0 will create the promise, but it's expandedPostings call won't return until we have received all calls
				//   - call 1 will wait on the promise
				//   - call 2 will cancel the context once we see it waiting on the promise, so it should stop waiting
				//
				// - call 3 will be called concurrently with the first three, but with different matchers, so we can see that results are not mixed
				//
				// - calls 4 and 5 are called concurrently with a matcher that causes LabelValues to artificially fail, the error should be stored in the promise
				var (
					ress    [6][]storage.SeriesRef
					errs    [6]error
					results sync.WaitGroup
				)
				results.Add(6)

				deduplicatedCallMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")} // all series match this, but we need to call LabelValues("i")
				otherMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "n", "^0_.*$")}          // one fifth of series match this, but we need to call LabelValues("n")
				failingMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "fail", "^.*$")}       // LabelValues() is mocked to fail with "fail" label

				// first call will create the promise
				go func() {
					defer results.Done()
					indexr := b.indexReader()
					defer indexr.Close()

					ress[0], errs[0] = indexr.ExpandedPostings(context.Background(), deduplicatedCallMatchers, newSafeQueryStats())
				}()
				// wait for this call to actually create a promise and call LabelValues
				labelValuesCalls["i"].Wait()

				// second call will wait on the promise
				secondContext := &contextNotifyingOnDoneWaiting{Context: context.Background(), waitingDone: make(chan struct{})}
				go func() {
					defer results.Done()
					indexr := b.indexReader()
					defer indexr.Close()

					ress[1], errs[1] = indexr.ExpandedPostings(secondContext, deduplicatedCallMatchers, newSafeQueryStats())
				}()
				// wait until this is waiting on the promise
				<-secondContext.waitingDone

				// third call will have context canceled before promise returns
				thirdCallInnerContext, thirdContextCancel := context.WithCancel(context.Background())
				thirdContext := &contextNotifyingOnDoneWaiting{Context: thirdCallInnerContext, waitingDone: make(chan struct{})}
				go func() {
					defer results.Done()
					indexr := b.indexReader()
					defer indexr.Close()

					ress[2], errs[2] = indexr.ExpandedPostings(thirdContext, deduplicatedCallMatchers, newSafeQueryStats())
				}()
				// wait until this is waiting on the promise
				<-thirdContext.waitingDone
				// and cancel its context
				thirdContextCancel()

				// fourth call will create its own promise
				go func() {
					defer results.Done()
					indexr := b.indexReader()
					defer indexr.Close()

					ress[3], errs[3] = indexr.ExpandedPostings(context.Background(), otherMatchers, newSafeQueryStats())
				}()
				// wait for this call to actually create a promise and call LabelValues
				labelValuesCalls["n"].Wait()

				// fifth call will create its own promise which will fail
				go func() {
					defer results.Done()
					indexr := b.indexReader()
					defer indexr.Close()

					ress[4], errs[4] = indexr.ExpandedPostings(context.Background(), failingMatchers, newSafeQueryStats())
				}()
				// wait for this call to actually create a promise and call LabelValues
				labelValuesCalls["fail"].Wait()

				// sixth call will wait on the promise to see it fail
				sixthContext := &contextNotifyingOnDoneWaiting{Context: context.Background(), waitingDone: make(chan struct{})}
				go func() {
					defer results.Done()
					indexr := b.indexReader()
					defer indexr.Close()

					ress[5], errs[5] = indexr.ExpandedPostings(sixthContext, failingMatchers, newSafeQueryStats())
				}()
				// wait until this is waiting on the promise
				<-sixthContext.waitingDone

				// let all calls return and wait for the results
				close(releaseCalls)
				results.Wait()

				require.Equal(t, series, len(ress[0]), "First result should have %d series (all of them)", series)
				require.NoError(t, errs[0], "First results should not fail")

				require.Equal(t, series, len(ress[1]), "Second result should have %d series (all of them)", series)
				require.NoError(t, errs[1], "Second results should not fail")

				require.Nil(t, ress[2], "Third result should not have series")
				require.ErrorIs(t, errs[2], context.Canceled, "Third result should have a context.Canceled error")

				require.Equal(t, series/5, len(ress[3]), "Fourth result should have %d series (one fifth of total)", series/5)
				require.NoError(t, errs[3], "Fourth results should not fail")

				require.Nil(t, ress[4], "Fifth result should not have series")
				require.ErrorIs(t, errs[4], expectedErr, "failed", "Fifth result should fail as 'failed'")

				require.Nil(t, ress[5], "Sixth result should not have series")
				require.ErrorIs(t, errs[5], expectedErr, "failed", "Sixth result should fail as 'failed'")
			})

			t.Run("cached", func(t *testing.T) {
				labelValuesCalls := map[string]int{}
				onLabelValuesCalled := func(name string) error {
					labelValuesCalls[name]++
					return nil
				}

				b := newTestBucketBlock()
				b.indexHeaderReader = &interceptedIndexReader{
					Reader:              b.indexHeaderReader,
					onLabelValuesCalled: onLabelValuesCalled,
				}
				b.indexCache = newInMemoryIndexCache(t)

				// first call succeeds and caches value
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")}
				refs, err := b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Equal(t, series, len(refs))
				require.Equal(t, map[string]int{"i": 1}, labelValuesCalls, "Should have called LabelValues once for label 'i'.")

				// second call uses cached value, so it doesn't call LabelValues again
				refs, err = b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Equal(t, series, len(refs))
				require.Equal(t, map[string]int{"i": 1}, labelValuesCalls, "Should have used cached value, so it shouldn't call LabelValues again for label 'i'.")

				// different matcher on same label should not be cached
				differentMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "i", "")}
				refs, err = b.indexReader().ExpandedPostings(context.Background(), differentMatchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Equal(t, series, len(refs))
				require.Equal(t, map[string]int{"i": 2}, labelValuesCalls, "Should have called LabelValues again for label 'i'.")
			})

			t.Run("corrupt cached expanded postings don't make request fail", func(t *testing.T) {
				b := newTestBucketBlock()
				b.indexCache = corruptedExpandedPostingsCache{}

				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")}
				refs, err := b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Equal(t, series, len(refs))
			})

			t.Run("expandedPostings returning error is not cached", func(t *testing.T) {
				b := newTestBucketBlock()
				b.indexHeaderReader = &interceptedIndexReader{
					Reader: b.indexHeaderReader,
					onLabelValuesCalled: func(_ string) error {
						return context.Canceled // alwaysFails
					},
				}
				b.indexCache = cacheNotExpectingToStoreExpandedPostings{t: t}

				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")}
				_, err := b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.Error(t, err)
			})

			t.Run("requesting a label value that doesn't exist doesn't reach the cache or the bucket", func(t *testing.T) {
				b := newTestBucketBlock()
				b.indexCache = forbiddenFetchMultiPostingsIndexCache{t: t, IndexCache: b.indexCache}
				mockBucket := &bucket.ClientMock{}
				b.bkt = mockBucket
				matchers := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$"),
					// With a regular EqualsMatcher we can look up the value of the label in the postings
					// offset table and see if it has any matches. If it matches no series, then
					// we don't need to fetch the rest of the postings lists from the caceh or the bucket.
					labels.MustNewMatcher(labels.MatchEqual, "i", "non-existent-value"),
				}
				postings, err := b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Empty(t, postings)
				mockBucket.Mock.AssertNotCalled(t, "Get")
				mockBucket.Mock.AssertNotCalled(t, "GetRange")
			})

			t.Run("requesting a label value (with regex) that doesn't exist doesn't reach the cache or the bucket", func(t *testing.T) {
				b := newTestBucketBlock()
				b.indexCache = forbiddenFetchMultiPostingsIndexCache{t: t, IndexCache: b.indexCache}
				mockBucket := &bucket.ClientMock{}
				b.bkt = mockBucket
				matchers := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$"),
					// Since prometheus regular expressions are anchored at each end, some regular expressions have a
					// known set of values. For those regular expressions we can short-circuit the cache and bucket lookups too.
					labels.MustNewMatcher(labels.MatchRegexp, "i", "non-existent-value-(1|2)"),
				}
				postings, err := b.indexReader().ExpandedPostings(context.Background(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				require.Empty(t, postings)
				mockBucket.Mock.AssertNotCalled(t, "Get")
				mockBucket.Mock.AssertNotCalled(t, "GetRange")
			})
		})
	}
}

func newInMemoryIndexCache(t testing.TB) indexcache.IndexCache {
	cache, err := indexcache.NewInMemoryIndexCacheWithConfig(log.NewNopLogger(), nil, indexcache.DefaultInMemoryIndexCacheConfig)
	require.NoError(t, err)
	return cache
}

type interceptedIndexReader struct {
	indexheader.Reader
	onLabelNamesCalled  func() error
	onLabelValuesCalled func(name string) error
}

func (iir *interceptedIndexReader) LabelNames() ([]string, error) {
	if iir.onLabelNamesCalled != nil {
		if err := iir.onLabelNamesCalled(); err != nil {
			return nil, err
		}
	}
	return iir.Reader.LabelNames()
}

func (iir *interceptedIndexReader) LabelValues(name string, prefix string, filter func(string) bool) ([]string, error) {
	if iir.onLabelValuesCalled != nil {
		if err := iir.onLabelValuesCalled(name); err != nil {
			return nil, err
		}
	}
	return iir.Reader.LabelValues(name, prefix, filter)
}

type contextNotifyingOnDoneWaiting struct {
	context.Context
	once        sync.Once
	waitingDone chan struct{}
}

func (w *contextNotifyingOnDoneWaiting) Done() <-chan struct{} {
	w.once.Do(func() {
		close(w.waitingDone)
	})
	return w.Context.Done()
}

type corruptedExpandedPostingsCache struct{ noopCache }

func (c corruptedExpandedPostingsCache) FetchExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, key indexcache.LabelMatchersKey) ([]byte, bool) {
	return []byte(codecHeaderSnappy + "corrupted"), true
}

type corruptedPostingsCache struct{ noopCache }

func (c corruptedPostingsCache) FetchMultiPostings(ctx context.Context, userID string, blockID ulid.ULID, keys []labels.Label) (map[labels.Label][]byte, []labels.Label) {
	res := make(map[labels.Label][]byte)
	for _, k := range keys {
		res[k] = []byte("corrupted or unknown")
	}
	return res, nil
}

type cacheNotExpectingToStoreExpandedPostings struct {
	noopCache
	t *testing.T
}

func (c cacheNotExpectingToStoreExpandedPostings) StoreExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, key indexcache.LabelMatchersKey, v []byte) {
	c.t.Fatalf("StoreExpandedPostings should not be called")
}

func BenchmarkBucketIndexReader_ExpandedPostings(b *testing.B) {
	tb := test.NewTB(b)
	const series = 50e5

	bucketBlockFactories := map[string]func() *bucketBlock{
		"binary reader": prepareTestBlockWithBinaryReader(tb, appendTestSeries(series)),
		"stream reader": prepareTestBlockWithStreamReader(tb, appendTestSeries(series)),
	}

	for name, newTestBucketBlock := range bucketBlockFactories {
		b.Run(name, func(b *testing.B) {
			benchmarkExpandedPostings(test.NewTB(b), newTestBucketBlock, series)
		})
	}
}

func prepareTestBucket(tb test.TB, dataSetup ...func(tb testing.TB, appender storage.Appender)) (objstore.BucketReader, string, ulid.ULID, int64, int64) {
	tmpDir := tb.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(tb, err)

	tb.Cleanup(func() {
		assert.NoError(tb, bkt.Close())
	})

	id, minT, maxT := uploadTestBlock(tb, tmpDir, bkt, dataSetup)

	return bkt, tmpDir, id, minT, maxT
}

func prepareTestBlockWithBinaryReader(tb test.TB, dataSetup ...func(tb testing.TB, appender storage.Appender)) func() *bucketBlock {
	bkt, tmpDir, id, minT, maxT := prepareTestBucket(tb, dataSetup...)

	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, mimir_tsdb.DefaultPostingOffsetInMemorySampling, indexheader.Config{})
	require.NoError(tb, err)

	return newBucketBlockFactory(bkt, r, id, minT, maxT)
}

func prepareTestBlockWithStreamReader(tb test.TB, dataSetup ...func(tb testing.TB, appender storage.Appender)) func() *bucketBlock {
	bkt, tmpDir, id, minT, maxT := prepareTestBucket(tb, dataSetup...)

	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)
	r, err := indexheader.NewStreamBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, mimir_tsdb.DefaultPostingOffsetInMemorySampling, metrics, indexheader.Config{})
	require.NoError(tb, err)

	return newBucketBlockFactory(bkt, r, id, minT, maxT)
}

func newBucketBlockFactory(bkt objstore.BucketReader, r indexheader.Reader, id ulid.ULID, minT int64, maxT int64) func() *bucketBlock {
	return func() *bucketBlock {
		return &bucketBlock{
			userID:            "tenant",
			logger:            log.NewNopLogger(),
			metrics:           NewBucketStoreMetrics(nil),
			indexHeaderReader: r,
			indexCache:        noopCache{},
			bkt:               bkt,
			meta:              &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id, MinTime: minT, MaxTime: maxT}},
			partitioners:      newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		}
	}
}

func uploadTestBlock(t testing.TB, tmpDir string, bkt objstore.Bucket, dataSetup []func(tb testing.TB, appender storage.Appender)) (_ ulid.ULID, minT int64, maxT int64) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = tmpDir
	headOpts.ChunkRange = 1000
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, h.Close())
	}()

	logger := log.NewNopLogger()

	for _, setup := range dataSetup {
		setup(t, h.Appender(context.Background()))
	}

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "tmp"), os.ModePerm))
	id := createBlockFromHead(t, filepath.Join(tmpDir, "tmp"), h)

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, "tmp", id.String()), metadata.Thanos{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: metadata.TestSource,
	}, nil)
	assert.NoError(t, err)
	assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, "tmp", id.String()), nil))
	assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, "tmp", id.String()), nil))

	return id, h.MinTime(), h.MaxTime()
}

func appendTestSeries(series int) func(testing.TB, storage.Appender) {
	return func(t testing.TB, app storage.Appender) {
		addSeries := func(l labels.Labels) {
			_, err := app.Append(0, l, 0, 0)
			assert.NoError(t, err)
		}

		series = series / 5
		for n := 0; n < 10; n++ {
			for i := 0; i < series/10; i++ {

				addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", strconv.Itoa(n)+labelLongSuffix, "j", "foo", "p", "foo"))
				// Have some series that won't be matched, to properly test inverted matches.
				addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", strconv.Itoa(n)+labelLongSuffix, "j", "bar", "q", "foo"))
				addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "0_"+strconv.Itoa(n)+labelLongSuffix, "j", "bar", "r", "foo"))
				addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "1_"+strconv.Itoa(n)+labelLongSuffix, "j", "bar", "s", "foo"))
				addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "2_"+strconv.Itoa(n)+labelLongSuffix, "j", "foo", "t", "foo"))
			}
		}
		assert.NoError(t, app.Commit())
	}
}

func createBlockFromHead(t testing.TB, dir string, head *tsdb.Head) ulid.ULID {
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil, nil, true)
	assert.NoError(t, err)

	assert.NoError(t, os.MkdirAll(dir, 0777))

	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	ulid, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	assert.NoError(t, err)
	return ulid
}

// Very similar benchmark to ths: https://github.com/prometheus/prometheus/blob/1d1732bc25cc4b47f513cb98009a4eb91879f175/tsdb/querier_bench_test.go#L82,
// but with postings results check when run as test.
func benchmarkExpandedPostings(
	t test.TB,
	newTestBucketBlock func() *bucketBlock,
	series int,
) {
	ctx := context.Background()
	series = series / 5

	iUniqueValues := series / 10      // The amount of unique values for "i" label prefix. See appendTestSeries.
	iUniqueValue := iUniqueValues / 2 // There will be 50 series matching: 5 per each series, 10 for each n. See appendTestSeries.

	n1 := labels.MustNewMatcher(labels.MatchEqual, "n", "1"+labelLongSuffix)
	nX := labels.MustNewMatcher(labels.MatchEqual, "n", "X"+labelLongSuffix)

	jFoo := labels.MustNewMatcher(labels.MatchEqual, "j", "foo")
	jNotFoo := labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")

	iStar := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.*$")
	iPlus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")
	i1Plus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^1.+$")
	iUniquePrefixPlus := labels.MustNewMatcher(labels.MatchRegexp, "i", fmt.Sprintf("%d.+", iUniqueValue))
	iNotUniquePrefixPlus := labels.MustNewMatcher(labels.MatchNotRegexp, "i", fmt.Sprintf("%d.+", iUniqueValue))
	iEmptyRe := labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")
	iNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "i", "")
	iNot2 := labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+labelLongSuffix)
	iNot2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")
	iNotStar2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^.*2.*$")
	jXXXYYY := labels.MustNewMatcher(labels.MatchRegexp, "j", "XXX|YYY")
	jXplus := labels.MustNewMatcher(labels.MatchRegexp, "j", "X.+")
	iRegexAlternate := labels.MustNewMatcher(labels.MatchRegexp, "i", "0"+labelLongSuffix+"|1"+labelLongSuffix+"|2"+labelLongSuffix)
	iXYZ := labels.MustNewMatcher(labels.MatchRegexp, "i", "X|Y|Z")
	iRegexAlternateSuffix := labels.MustNewMatcher(labels.MatchRegexp, "i", "(0|1|2)"+labelLongSuffix)
	iRegexClass := labels.MustNewMatcher(labels.MatchRegexp, "i", "[0-2]"+labelLongSuffix)
	iRegexNotSetMatches := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "(0|1|2)"+labelLongSuffix)
	pNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "p", "")

	// Just make sure that we're testing what we think we're testing.
	require.NotEmpty(t, iRegexNotSetMatches.SetMatches(), "Should have non empty SetMatches to test the proper path.")

	cases := []struct {
		name     string
		matchers []*labels.Matcher

		expectedLen int
	}{
		{`n="X"`, []*labels.Matcher{nX}, 0},
		{`n="X",j="foo"`, []*labels.Matcher{nX, jFoo}, 0},
		{`n="X",j!="foo"`, []*labels.Matcher{nX, jNotFoo}, 0},
		{`j=~"XXX|YYY"`, []*labels.Matcher{jXXXYYY}, 0},
		{`j=~"X.+"`, []*labels.Matcher{jXplus}, 0},
		{`i=~"X|Y|Z"`, []*labels.Matcher{iXYZ}, 0},
		{`n="1"`, []*labels.Matcher{n1}, int(float64(series) * 0.2)},
		{`n="1",j="foo"`, []*labels.Matcher{n1, jFoo}, int(float64(series) * 0.1)},
		{`j="foo",n="1"`, []*labels.Matcher{jFoo, n1}, int(float64(series) * 0.1)},
		{`n="1",j!="foo"`, []*labels.Matcher{n1, jNotFoo}, int(float64(series) * 0.1)},
		{`i=~".*"`, []*labels.Matcher{iStar}, 5 * series},
		{`i=~".+"`, []*labels.Matcher{iPlus}, 5 * series},
		{`i=~"^.+$",j=~"X.+"`, []*labels.Matcher{iPlus, jXplus}, 0},
		{`i=~""`, []*labels.Matcher{iEmptyRe}, 0},
		{`i!=""`, []*labels.Matcher{iNotEmpty}, 5 * series},
		{`n="1",i=~".*",j="foo"`, []*labels.Matcher{n1, iStar, jFoo}, int(float64(series) * 0.1)},
		{`n="X",i=~"^.+$",j="foo"`, []*labels.Matcher{nX, iStar, jFoo}, 0},
		{`n="1",i=~".*",i!="2",j="foo"`, []*labels.Matcher{n1, iStar, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i!=""`, []*labels.Matcher{n1, iNotEmpty}, int(float64(series) * 0.2)},
		{`n="1",i!="",j="foo"`, []*labels.Matcher{n1, iNotEmpty, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i!="",j=~"X.+"`, []*labels.Matcher{n1, iNotEmpty, jXplus}, 0},
		{`n="1",i!="",j=~"XXX|YYY"`, []*labels.Matcher{n1, iNotEmpty, jXXXYYY}, 0},
		{`n="1",i=~"X|Y|Z",j="foo"`, []*labels.Matcher{n1, iXYZ, jFoo}, 0},
		{`n="1",i=~".+",j="foo"`, []*labels.Matcher{n1, iPlus, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~"1.+",j="foo"`, []*labels.Matcher{n1, i1Plus, jFoo}, int(float64(series) * 0.011111)},
		{`n="1",i=~".+",i!="2",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",i!~"2.*",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2Star, jFoo}, int(1 + float64(series)*0.088888)},
		{`n="X",i=~"^.+$",i!~"^.*2.*$",j="foo"`, []*labels.Matcher{nX, iPlus, iNotStar2Star, jFoo}, 0},
		{`i=~"0xxx|1xxx|2xxx"`, []*labels.Matcher{iRegexAlternate}, 150},                        // 50 series for "1", 50 for "2" and 50 for "3".
		{`i=~"(0|1|2)xxx"`, []*labels.Matcher{iRegexAlternateSuffix}, 150},                      // 50 series for "1", 50 for "2" and 50 for "3".
		{`i=~"[0-2]xxx"`, []*labels.Matcher{iRegexClass}, 150},                                  // 50 series for "1", 50 for "2" and 50 for "3".
		{`i!~[0-2]xxx`, []*labels.Matcher{iRegexNotSetMatches}, 5*series - 150},                 // inverse of iRegexAlternateSuffix
		{`i=~".*", i!~[0-2]xxx`, []*labels.Matcher{iStar, iRegexNotSetMatches}, 5*series - 150}, // inverse of iRegexAlternateSuffix
		{`i=~"<unique_prefix>.+"`, []*labels.Matcher{iUniquePrefixPlus}, 50},
		{`n="1",i=~"<unique_prefix>.+"`, []*labels.Matcher{n1, iUniquePrefixPlus}, 2},
		{`n="1",i!~"<unique_prefix>.+"`, []*labels.Matcher{n1, iNotUniquePrefixPlus}, int(float64(series)*0.2) - 2},
		{`p!=""`, []*labels.Matcher{pNotEmpty}, series},
	}

	for _, c := range cases {
		t.Run(c.name, func(t test.TB) {
			indexr := newBucketIndexReader(newTestBucketBlock())
			indexrStats := newSafeQueryStats()

			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				p, err := indexr.ExpandedPostings(ctx, c.matchers, indexrStats)

				if err != nil {
					t.Fatal(err.Error())
				}
				if c.expectedLen != len(p) {
					t.Fatalf("expected %d postings but got %d", c.expectedLen, len(p))
				}
			}
		})
	}
}

func TestBucketStore_Series(t *testing.T) {
	tb := test.NewTB(t)
	runSeriesInterestingCases(tb, 10000, 10000, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, false, samplesPerSeries, series, 1)
	})
}

func TestBucketStore_Series_WithSkipChunks(t *testing.T) {
	tb := test.NewTB(t)
	runSeriesInterestingCases(tb, 10000, 10000, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, true, samplesPerSeries, series, 1)
	})
}

func BenchmarkBucketStore_Series(b *testing.B) {
	tb := test.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	runSeriesInterestingCases(tb, 10e6, 10e5, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, false, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func BenchmarkBucketStore_Series_WithSkipChunks(b *testing.B) {
	tb := test.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	runSeriesInterestingCases(tb, 10e6, 10e5, func(t test.TB, samplesPerSeries, series int) {
		// Send only requests with 100% ratio because in Mimir we lookup series at block boundaries
		// when skip chunks = true.
		benchBucketSeries(t, true, samplesPerSeries, series, 1)
	})
}

func benchBucketSeries(t test.TB, skipChunk bool, samplesPerSeries, totalSeries int, requestedRatios ...float64) {
	const numOfBlocks = 4

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	var (
		logger                = log.NewNopLogger()
		series                []*storepb.Series
		expectedQueriesBlocks []hintspb.Block
		random                = rand.New(rand.NewSource(120))
	)

	extLset := labels.FromStrings("ext1", "1")
	thanosMeta := metadata.Thanos{
		Labels: extLset.Map(),
		Source: metadata.TestSource,
	}

	blockDir := filepath.Join(tmpDir, "tmp")

	samplesPerSeriesPerBlock := samplesPerSeries / numOfBlocks
	if samplesPerSeriesPerBlock == 0 {
		samplesPerSeriesPerBlock = 1
	}

	seriesPerBlock := totalSeries / numOfBlocks
	if seriesPerBlock == 0 {
		seriesPerBlock = 1
	}

	// Create 4 blocks. Each will have seriesPerBlock number of series that have samplesPerSeriesPerBlock samples.
	// Timestamp will be counted for each new series and new sample, so each series will have unique timestamp.
	// This allows to pick time range that will correspond to number of series picked 1:1.
	for bi := 0; bi < numOfBlocks; bi++ {
		head, bSeries := createHeadWithSeries(t, bi, headGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", bi)),
			SamplesPerSeries: samplesPerSeriesPerBlock,
			Series:           seriesPerBlock,
			PrependLabels:    extLset,
			Random:           random,
			SkipChunks:       t.IsBenchmark() || skipChunk,
		})
		id := createBlockFromHead(t, blockDir, head)
		assert.NoError(t, head.Close())
		series = append(series, bSeries...)
		expectedQueriesBlocks = append(expectedQueriesBlocks, hintspb.Block{Id: id.String()})

		meta, err := metadata.InjectThanos(logger, filepath.Join(blockDir, id.String()), thanosMeta, nil)
		assert.NoError(t, err)

		assert.NoError(t, meta.WriteToDir(logger, filepath.Join(blockDir, id.String())))
		assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), nil))
	}

	ibkt := objstore.WithNoopInstr(bkt)
	f, err := block.NewRawMetaFetcher(logger, ibkt)
	assert.NoError(t, err)

	chunkPool, err := pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 1e9) // 1GB.
	assert.NoError(t, err)

	runTestWithStore := func(t test.TB, st *BucketStore, reg prometheus.Gatherer) {
		if !t.IsBenchmark() {
			st.chunkPool = &trackedBytesPool{parent: st.chunkPool}

			// Reset the memory pool tracker (only if streaming store-gateway is enabled).
			if st.maxSeriesPerBatch > 0 {
				seriesEntrySlicePool.(*pool.TrackedPool).Reset()
				seriesChunksSlicePool.(*pool.TrackedPool).Reset()
			}
		}

		assert.NoError(t, st.SyncBlocks(context.Background()))

		var bCases []*seriesCase
		for _, p := range requestedRatios {
			expectedSamples := int(p * float64(totalSeries*samplesPerSeries))
			if expectedSamples == 0 {
				expectedSamples = 1
			}
			seriesCut := int(p * float64(numOfBlocks*seriesPerBlock))
			if seriesCut == 0 {
				seriesCut = 1
			} else if seriesCut == 1 {
				seriesCut = expectedSamples / samplesPerSeriesPerBlock
			}

			bCases = append(bCases, &seriesCase{
				Name: fmt.Sprintf("%dof%d", expectedSamples, totalSeries*samplesPerSeries),
				Req: &storepb.SeriesRequest{
					MinTime: 0,
					MaxTime: int64(expectedSamples) - 1,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					},
					SkipChunks: skipChunk,
				},
				ExpectedHints: hintspb.SeriesResponseHints{
					QueriedBlocks: expectedQueriesBlocks,
				},
				// This does not cut chunks properly, but those are assured against for non benchmarks only, where we use 100% case only.
				ExpectedSeries: series[:seriesCut],
			})
		}
		runTestServerSeries(t, st, bCases...)

		if !t.IsBenchmark() {
			if !skipChunk {
				assert.Zero(t, st.chunkPool.(*trackedBytesPool).balance.Load())
				st.chunkPool.(*trackedBytesPool).gets.Store(0)

				// Only if streaming store-gateway is enabled.
				if st.maxSeriesPerBatch > 0 {
					assert.Zero(t, seriesEntrySlicePool.(*pool.TrackedPool).Balance.Load())
					assert.Zero(t, seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load())

					assert.Greater(t, int(seriesEntrySlicePool.(*pool.TrackedPool).Gets.Load()), 0)
					assert.Greater(t, int(seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load()), 0)
				}
			}

			for _, b := range st.blocks {
				// NOTE(bwplotka): It is 4 x 1.0 for 100mln samples. Kind of make sense: long series.
				assert.Equal(t, 0.0, promtest.ToFloat64(b.metrics.seriesRefetches))
			}

			// Check exposed metrics. Ensure that streaming store-gateway metrics are tracked only when
			// streaming store-gateway is enabled.
			assertHistograms := map[string]bool{
				"cortex_bucket_store_series_request_stage_duration_seconds":         st.maxSeriesPerBatch > 0,
				"cortex_bucket_store_series_batch_preloading_load_duration_seconds": st.maxSeriesPerBatch > 0 && st.maxSeriesPerBatch < totalSeries, // Tracked only when a request is split in multiple batches.
				"cortex_bucket_store_series_batch_preloading_wait_duration_seconds": st.maxSeriesPerBatch > 0 && st.maxSeriesPerBatch < totalSeries, // Tracked only when a request is split in multiple batches.
				"cortex_bucket_store_series_refs_fetch_duration_seconds":            st.maxSeriesPerBatch > 0,
			}

			metrics, err := util.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)

			for metricName, expected := range assertHistograms {
				if count := metrics.SumHistograms(metricName).Count(); expected {
					assert.Greater(t, count, uint64(0), "metric name: %s", metricName)
				} else {
					assert.Equal(t, uint64(0), count, "metric name: %s", metricName)
				}
			}
		}
	}

	for testName, bucketStoreOpts := range map[string][]BucketStoreOption{
		"with default options":                                 {WithLogger(logger), WithChunkPool(chunkPool)},
		"with series streaming (1K per batch)":                 {WithLogger(logger), WithChunkPool(chunkPool), WithStreamingSeriesPerBatch(1000)},
		"with series streaming (10K per batch)":                {WithLogger(logger), WithChunkPool(chunkPool), WithStreamingSeriesPerBatch(10000)},
		"with series streaming and index cache (1K per batch)": {WithLogger(logger), WithChunkPool(chunkPool), WithStreamingSeriesPerBatch(1000), WithIndexCache(newInMemoryIndexCache(t))},
	} {
		reg := prometheus.NewPedanticRegistry()
		st, err := NewBucketStore(
			"test",
			ibkt,
			f,
			tmpDir,
			newStaticChunksLimiterFactory(0),
			newStaticSeriesLimiterFactory(0),
			newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
			1,
			mimir_tsdb.DefaultPostingOffsetInMemorySampling,
			indexheader.Config{},
			false,
			0,
			hashcache.NewSeriesHashCache(1024*1024),
			NewBucketStoreMetrics(reg),
			bucketStoreOpts...,
		)
		assert.NoError(t, err)

		t.Run(testName, func(t test.TB) {
			runTestWithStore(t, st, reg)
		})
	}
}

func TestBucketStore_Series_Concurrency(t *testing.T) {
	const (
		numWorkers           = 10
		numRequestsPerWorker = 100
		numBlocks            = 4
		numSeriesPerBlock    = 100
		numSamplesPerSeries  = 200
	)

	var (
		ctx              = context.Background()
		logger           = log.NewNopLogger()
		expectedSeries   []*storepb.Series
		expectedBlockIDs []string
		random           = rand.New(rand.NewSource(120))
		tmpDir           = t.TempDir()
	)

	test.VerifyNoLeak(t)

	// Create a filesystem-based bucket.
	bucket, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bucket.Close()) }()
	instrumentedBucket := objstore.WithNoopInstr(bucket)

	// Generate some blocks.
	t.Log("generating test blocks")
	blockDir := filepath.Join(tmpDir, "tmp")
	for b := 0; b < numBlocks; b++ {
		head, blockSeries := createHeadWithSeries(t, b, headGenOptions{
			TSDBDir:          filepath.Join(tmpDir, fmt.Sprintf("%d", b)),
			SamplesPerSeries: numSamplesPerSeries,
			Series:           numSeriesPerBlock,
			PrependLabels:    labels.FromStrings(labels.MetricName, "test_metric", "zzz_block_id", strconv.Itoa(b)),
			Random:           random,
		})

		blockID := createBlockFromHead(t, blockDir, head)
		assert.NoError(t, head.Close())

		expectedSeries = append(expectedSeries, blockSeries...)
		expectedBlockIDs = append(expectedBlockIDs, blockID.String())

		require.NoError(t, block.Upload(ctx, logger, bucket, filepath.Join(blockDir, blockID.String()), nil))
	}
	t.Log("generated test blocks")

	// Prepare a request to query all series.
	hints := &hintspb.SeriesRequestHints{
		BlockMatchers: []storepb.LabelMatcher{
			{
				Type:  storepb.LabelMatcher_RE,
				Name:  block.BlockIDLabel,
				Value: strings.Join(expectedBlockIDs, "|"),
			},
		},
	}

	marshalledHints, err := types.MarshalAny(hints)
	require.NoError(t, err)

	req := &storepb.SeriesRequest{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: labels.MetricName, Value: "test_metric"},
		},
		Hints: marshalledHints,
	}

	runRequest := func(t *testing.T, store *BucketStore) {
		srv := newBucketStoreSeriesServer(ctx)
		require.NoError(t, store.Series(req, srv))
		require.Equal(t, 0, len(srv.Warnings), "%v", srv.Warnings)
		require.Equal(t, len(expectedSeries), len(srv.SeriesSet))

		// Huge responses can produce unreadable diffs - make it more human readable.
		for j := range expectedSeries {
			require.Equal(t, expectedSeries[j].Labels, srv.SeriesSet[j].Labels, "series labels mismatch at position %d", j)
			require.Equal(t, expectedSeries[j].Chunks, srv.SeriesSet[j].Chunks, "series chunks mismatch at position %d", j)
		}
	}

	// Run the test with different batch sizes.
	for _, batchSize := range []int{len(expectedSeries) / 100, len(expectedSeries) * 2} {
		t.Run(fmt.Sprintf("batch size: %d", batchSize), func(t *testing.T) {
			// Reset the memory pool tracker.
			seriesChunkRefsSetPool.(*pool.TrackedPool).Reset()

			metaFetcher, err := block.NewRawMetaFetcher(logger, instrumentedBucket)
			assert.NoError(t, err)

			chunkPool, err := pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 1e9) // 1GB.
			assert.NoError(t, err)
			trackedChunkPool := &trackedBytesPool{parent: chunkPool}

			// Create the bucket store.
			store, err := NewBucketStore(
				"test-user",
				instrumentedBucket,
				metaFetcher,
				tmpDir,
				newStaticChunksLimiterFactory(0),
				newStaticSeriesLimiterFactory(0),
				newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
				1,
				mimir_tsdb.DefaultPostingOffsetInMemorySampling,
				indexheader.Config{},
				false, // Lazy index-header loading disabled.
				0,
				hashcache.NewSeriesHashCache(1024*1024),
				NewBucketStoreMetrics(nil),
				WithLogger(logger),
				WithChunkPool(trackedChunkPool),
				WithStreamingSeriesPerBatch(batchSize),
			)
			require.NoError(t, err)
			require.NoError(t, store.SyncBlocks(ctx))

			// Run workers.
			wg := sync.WaitGroup{}
			wg.Add(numWorkers)

			for c := 0; c < numWorkers; c++ {
				go func() {
					defer wg.Done()

					for r := 0; r < numRequestsPerWorker; r++ {
						runRequest(t, store)
					}
				}()
			}

			// Wait until all workers have done.
			wg.Wait()

			// Ensure all chunks have been released to the pool.
			require.Equal(t, 0, int(trackedChunkPool.balance.Load()))

			// Ensure the seriesChunkRefsSet memory pool has been used and all slices pulled from
			// pool have put back.
			assert.Greater(t, seriesChunkRefsSetPool.(*pool.TrackedPool).Gets.Load(), int64(0))
			assert.Equal(t, int64(0), seriesChunkRefsSetPool.(*pool.TrackedPool).Balance.Load())
		})
	}
}

type trackedBytesPool struct {
	parent  pool.Bytes
	balance atomic.Uint64
	gets    atomic.Uint64
}

func (m *trackedBytesPool) Get(sz int) (*[]byte, error) {
	b, err := m.parent.Get(sz)
	if err != nil {
		return nil, err
	}
	m.balance.Add(uint64(cap(*b)))
	m.gets.Add(uint64(1))
	return b, nil
}

func (m *trackedBytesPool) Put(b *[]byte) {
	m.balance.Sub(uint64(cap(*b)))
	m.parent.Put(b)
}

// Regression test against: https://github.com/thanos-io/thanos/issues/2147.
func TestBucketStore_Series_OneBlock_InMemIndexCacheSegfault(t *testing.T) {
	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	thanosMeta := metadata.Thanos{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: metadata.TestSource,
	}

	chunkPool, err := pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 100e7)
	assert.NoError(t, err)

	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(logger, nil, indexcache.InMemoryIndexCacheConfig{
		MaxItemSize: 3000,
		// This is the exact size of cache needed for our *single request*.
		// This is limited in order to make sure we test evictions.
		MaxSize: 8889,
	})
	assert.NoError(t, err)

	var b1 *bucketBlock

	const numSeries = 100
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = tmpDir
	headOpts.ChunkRange = 1

	// Create 4 blocks. Each will have numSeriesPerBlock number of series that have 1 sample only.
	// Timestamp will be counted for each new series, so each series will have unique timestamp.
	// This allows to pick time range that will correspond to number of series picked 1:1.
	{
		// Block 1.
		h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, h.Close()) }()

		app := h.Appender(context.Background())

		for i := 0; i < numSeries; i++ {
			ts := int64(i)
			lbls := labels.FromStrings("foo", "bar", "b", "1", "i", fmt.Sprintf("%07d%s", ts, labelLongSuffix))

			_, err := app.Append(0, lbls, ts, 0)
			assert.NoError(t, err)
		}
		assert.NoError(t, app.Commit())

		blockDir := filepath.Join(tmpDir, "tmp")
		id := createBlockFromHead(t, blockDir, h)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		assert.NoError(t, err)
		assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), nil))

		b1 = &bucketBlock{
			indexCache:   indexCache,
			logger:       logger,
			metrics:      NewBucketStoreMetrics(nil),
			bkt:          bkt,
			meta:         meta,
			partitioners: newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
			chunkObjs:    []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:    chunkPool,
		}
		b1.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b1.meta.ULID, mimir_tsdb.DefaultPostingOffsetInMemorySampling, indexheader.Config{})
		assert.NoError(t, err)
	}

	var b2 *bucketBlock
	{
		// Block 2, do not load this block yet.
		h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, h.Close()) }()

		app := h.Appender(context.Background())

		for i := 0; i < numSeries; i++ {
			ts := int64(i)
			lbls := labels.FromStrings("foo", "bar", "b", "2", "i", fmt.Sprintf("%07d%s", ts, labelLongSuffix))

			_, err := app.Append(0, lbls, ts, 0)
			assert.NoError(t, err)
		}
		assert.NoError(t, app.Commit())

		blockDir := filepath.Join(tmpDir, "tmp2")
		id := createBlockFromHead(t, blockDir, h)

		meta, err := metadata.InjectThanos(log.NewNopLogger(), filepath.Join(blockDir, id.String()), thanosMeta, nil)
		assert.NoError(t, err)
		assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), nil))

		b2 = &bucketBlock{
			indexCache:   indexCache,
			logger:       logger,
			metrics:      NewBucketStoreMetrics(nil),
			bkt:          bkt,
			meta:         meta,
			partitioners: newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
			chunkObjs:    []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:    chunkPool,
		}
		b2.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b2.meta.ULID, mimir_tsdb.DefaultPostingOffsetInMemorySampling, indexheader.Config{})
		assert.NoError(t, err)
	}

	store := &BucketStore{
		userID:          "test",
		bkt:             objstore.WithNoopInstr(bkt),
		logger:          logger,
		indexCache:      indexCache,
		indexReaderPool: indexheader.NewReaderPool(log.NewNopLogger(), false, 0, indexheader.NewReaderPoolMetrics(nil)),
		metrics:         NewBucketStoreMetrics(nil),
		blockSet:        &bucketBlockSet{blocks: []*bucketBlock{b1, b2}},
		blocks: map[ulid.ULID]*bucketBlock{
			b1.meta.ULID: b1,
			b2.meta.ULID: b2,
		},
		queryGate:            gate.NewNoop(),
		chunksLimiterFactory: newStaticChunksLimiterFactory(0),
		seriesLimiterFactory: newStaticSeriesLimiterFactory(0),
		maxSeriesPerBatch:    65536,
		chunkPool:            chunkPool,
	}

	t.Run("invoke series for one block. Fill the cache on the way.", func(t *testing.T) {
		srv := newBucketStoreSeriesServer(context.Background())
		assert.NoError(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: int64(numSeries) - 1,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				// This bug shows only when we use lot's of symbols for matching.
				{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
			},
		}, srv))
		assert.Equal(t, 0, len(srv.Warnings))
		assert.Equal(t, numSeries, len(srv.SeriesSet))
	})
	t.Run("invoke series for second block. This should revoke previous cache.", func(t *testing.T) {
		srv := newBucketStoreSeriesServer(context.Background())
		assert.NoError(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: int64(numSeries) - 1,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
				// This bug shows only when we use lot's of symbols for matching.
				{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
			},
		}, srv))
		assert.Equal(t, 0, len(srv.Warnings))
		assert.Equal(t, numSeries, len(srv.SeriesSet))
	})
	t.Run("remove second block. Cache stays. Ask for first again.", func(t *testing.T) {
		assert.NoError(t, store.removeBlock(b2.meta.ULID))

		srv := newBucketStoreSeriesServer(context.Background())
		assert.NoError(t, store.Series(&storepb.SeriesRequest{
			MinTime: 0,
			MaxTime: int64(numSeries) - 1,
			Matchers: []storepb.LabelMatcher{
				{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "1"},
				// This bug shows only when we use lot's of symbols for matching.
				{Type: storepb.LabelMatcher_NEQ, Name: "i", Value: ""},
			},
		}, srv))
		assert.Equal(t, 0, len(srv.Warnings))
		assert.Equal(t, numSeries, len(srv.SeriesSet))
	})
}

func TestBucketStore_Series_RequestAndResponseHints(t *testing.T) {
	newTestCases := func(seriesSet1 []*storepb.Series, seriesSet2 []*storepb.Series, block1 ulid.ULID, block2 ulid.ULID) []*seriesCase {
		return []*seriesCase{
			{
				Name: "querying a range containing 1 block should return 1 block in the response hints",
				Req: &storepb.SeriesRequest{
					MinTime: 0,
					MaxTime: 1,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					},
				},
				ExpectedSeries: seriesSet1,
				ExpectedHints: hintspb.SeriesResponseHints{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
					},
				},
			}, {
				Name: "querying a range containing multiple blocks should return multiple blocks in the response hints",
				Req: &storepb.SeriesRequest{
					MinTime: 0,
					MaxTime: 3,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					},
				},
				ExpectedSeries: append(append([]*storepb.Series{}, seriesSet1...), seriesSet2...),
				ExpectedHints: hintspb.SeriesResponseHints{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
						{Id: block2.String()},
					},
				},
			}, {
				Name: "querying a range containing multiple blocks but filtering a specific block should query only the requested block",
				Req: &storepb.SeriesRequest{
					MinTime: 0,
					MaxTime: 3,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
					},
					Hints: mustMarshalAny(&hintspb.SeriesRequestHints{
						BlockMatchers: []storepb.LabelMatcher{
							{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: block1.String()},
						},
					}),
				},
				ExpectedSeries: seriesSet1,
				ExpectedHints: hintspb.SeriesResponseHints{
					QueriedBlocks: []hintspb.Block{
						{Id: block1.String()},
					},
				},
			},
		}
	}

	t.Run("with default options", func(t *testing.T) {
		tb, store, seriesSet1, seriesSet2, block1, block2, close := setupStoreForHintsTest(t)
		tb.Cleanup(close)
		runTestServerSeries(tb, store, newTestCases(seriesSet1, seriesSet2, block1, block2)...)
	})

	t.Run("with series streaming", func(t *testing.T) {
		tb, store, seriesSet1, seriesSet2, block1, block2, close := setupStoreForHintsTest(t, WithStreamingSeriesPerBatch(5000))
		tb.Cleanup(close)
		runTestServerSeries(tb, store, newTestCases(seriesSet1, seriesSet2, block1, block2)...)
	})
}

func TestBucketStore_Series_ErrorUnmarshallingRequestHints(t *testing.T) {
	tmpDir := t.TempDir()

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
	)

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil)
	assert.NoError(t, err)

	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(logger, nil, indexcache.InMemoryIndexCacheConfig{})
	assert.NoError(t, err)

	store, err := NewBucketStore(
		"test",
		instrBkt,
		fetcher,
		tmpDir,
		newStaticChunksLimiterFactory(10000/MaxSamplesPerChunk),
		newStaticSeriesLimiterFactory(0),
		newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.Config{},
		false,
		0,
		hashcache.NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, store.RemoveBlocksAndClose()) }()

	assert.NoError(t, store.SyncBlocks(context.Background()))

	// Create a request with invalid hints (uses response hints instead of request hints).
	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 3,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
		},
		Hints: mustMarshalAny(&hintspb.SeriesResponseHints{}),
	}

	srv := newBucketStoreSeriesServer(context.Background())
	err = store.Series(req, srv)
	assert.Error(t, err)
	assert.Equal(t, true, regexp.MustCompile(".*unmarshal series request hints.*").MatchString(err.Error()))
}

func TestBucketStore_Series_CanceledRequest(t *testing.T) {
	tmpDir := t.TempDir()
	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	instrBkt := objstore.WithNoopInstr(bkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil)
	assert.NoError(t, err)

	store, err := NewBucketStore(
		"test",
		instrBkt,
		fetcher,
		tmpDir,
		newStaticChunksLimiterFactory(10000/MaxSamplesPerChunk),
		newStaticSeriesLimiterFactory(0),
		newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.Config{},
		false,
		0,
		hashcache.NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
		WithQueryGate(gate.NewBlocking(0)),
	)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, store.RemoveBlocksAndClose()) }()

	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 3,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	srv := newBucketStoreSeriesServer(ctx)
	err = store.Series(req, srv)
	assert.Error(t, err)
	s, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.Canceled, s.Code())
}

func TestBucketStore_Series_InvalidRequest(t *testing.T) {
	tmpDir := t.TempDir()
	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	instrBkt := objstore.WithNoopInstr(bkt)
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil)
	assert.NoError(t, err)

	store, err := NewBucketStore(
		"test",
		instrBkt,
		fetcher,
		tmpDir,
		newStaticChunksLimiterFactory(10000/MaxSamplesPerChunk),
		newStaticSeriesLimiterFactory(0),
		newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.Config{},
		false,
		0,
		hashcache.NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
	)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, store.RemoveBlocksAndClose()) }()

	// Use an invalid matcher regex to trigger an error.
	req := &storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 3,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "foo", Value: "("},
		},
	}

	srv := newBucketStoreSeriesServer(context.Background())
	err = store.Series(req, srv)
	assert.Error(t, err)
	s, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, s.Code())
	assert.ErrorContains(t, s.Err(), "error parsing regexp: missing closing )")
}

func TestBucketStore_Series_BlockWithMultipleChunks(t *testing.T) {
	appendF := func(app storage.Appender, lset labels.Labels, ts int64) error {
		_, err := app.Append(0, lset, ts, float64(ts))
		return err
	}
	testBucketStoreSeriesBlockWithMultipleChunks(t, appendF, chunkenc.EncXOR)
}

func TestBucketStore_Series_BlockWithMultipleHistogramChunks(t *testing.T) {
	histograms := tsdb.GenerateTestHistograms(10000)
	appendF := func(app storage.Appender, lset labels.Labels, ts int64) error {
		_, err := app.AppendHistogram(0, lset, ts, histograms[ts], nil)
		return err
	}
	testBucketStoreSeriesBlockWithMultipleChunks(t, appendF, chunkenc.EncHistogram)
}

func TestBucketStore_Series_BlockWithMultipleFloatHistogramChunks(t *testing.T) {
	histograms := tsdb.GenerateTestFloatHistograms(10000)
	appendF := func(app storage.Appender, lset labels.Labels, ts int64) error {
		_, err := app.AppendHistogram(0, lset, ts, nil, histograms[ts])
		return err
	}
	testBucketStoreSeriesBlockWithMultipleChunks(t, appendF, chunkenc.EncFloatHistogram)
}

func testBucketStoreSeriesBlockWithMultipleChunks(
	t *testing.T,
	appendF func(storage.Appender, labels.Labels, int64) error,
	encoding chunkenc.Encoding) {
	tmpDir := t.TempDir()

	// Create a block with 1 series but an high number of samples,
	// so that they will span across multiple chunks.
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.EnableNativeHistograms.Store(true)
	headOpts.ChunkDirRoot = filepath.Join(tmpDir, "block")
	headOpts.ChunkRange = math.MaxInt64

	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, h.Close()) }()

	series := labels.FromStrings("__name__", "test")
	for ts := int64(0); ts < 10000; ts++ {
		// Appending a single sample is very unoptimised, but guarantees each chunk is always MaxSamplesPerChunk
		// (except the last one, which could be smaller).
		app := h.Appender(context.Background())
		err := appendF(app, series, ts)
		assert.NoError(t, err)
		assert.NoError(t, app.Commit())
	}

	blk := createBlockFromHead(t, headOpts.ChunkDirRoot, h)

	thanosMeta := metadata.Thanos{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: metadata.TestSource,
	}

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(headOpts.ChunkDirRoot, blk.String()), thanosMeta, nil)
	assert.NoError(t, err)

	// Create a bucket and upload the block there.
	bktDir := filepath.Join(tmpDir, "bucket")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	instrBkt := objstore.WithNoopInstr(bkt)
	logger := log.NewNopLogger()
	assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(headOpts.ChunkDirRoot, blk.String()), nil))

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil)
	assert.NoError(t, err)

	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(logger, nil, indexcache.InMemoryIndexCacheConfig{})
	assert.NoError(t, err)

	store, err := NewBucketStore(
		"tenant",
		instrBkt,
		fetcher,
		tmpDir,
		newStaticChunksLimiterFactory(100000/MaxSamplesPerChunk),
		newStaticSeriesLimiterFactory(0),
		newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.Config{},
		false,
		0,
		hashcache.NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	assert.NoError(t, err)
	assert.NoError(t, store.SyncBlocks(context.Background()))

	tests := map[string]struct {
		reqMinTime      int64
		reqMaxTime      int64
		expectedSamples int
	}{
		"query the entire block": {
			reqMinTime:      math.MinInt64,
			reqMaxTime:      math.MaxInt64,
			expectedSamples: 10000,
		},
		"query the beginning of the block": {
			reqMinTime:      0,
			reqMaxTime:      100,
			expectedSamples: MaxSamplesPerChunk,
		},
		"query the middle of the block": {
			reqMinTime:      4000,
			reqMaxTime:      4050,
			expectedSamples: MaxSamplesPerChunk,
		},
		"query the end of the block": {
			reqMinTime:      9800,
			reqMaxTime:      10000,
			expectedSamples: (MaxSamplesPerChunk * 2) + (10000 % MaxSamplesPerChunk),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &storepb.SeriesRequest{
				MinTime: testData.reqMinTime,
				MaxTime: testData.reqMaxTime,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "test"},
				},
			}

			srv := newBucketStoreSeriesServer(context.Background())
			err = store.Series(req, srv)
			assert.NoError(t, err)
			assert.True(t, len(srv.SeriesSet) == 1)

			// Count the number of samples in the returned chunks.
			numSamples := 0
			for _, rawChunk := range srv.SeriesSet[0].Chunks {
				decodedChunk, err := chunkenc.FromData(encoding, rawChunk.Raw.Data)
				assert.NoError(t, err)

				numSamples += decodedChunk.NumSamples()
			}

			assert.True(t, testData.expectedSamples == numSamples, "expected: %d, actual: %d", testData.expectedSamples, numSamples)
		})
	}
}

func TestBucketStore_Series_LimitsWithStreamingEnabled(t *testing.T) {
	var (
		ctx    = context.Background()
		tmpDir = t.TempDir()
		bktDir = filepath.Join(tmpDir, "bucket")
		logger = log.NewNopLogger()
	)

	const (
		numSamplesPerSeries = 10 // A low number so that all samples fit in a single chunk.
		minTime             = 0
		maxTime             = 1000
	)

	// Create two blocks. Some series exists in both blocks, some don't.
	// Samples for the overlapping series are equal between the two blocks
	// (simulate the case of uncompacted blocks from ingesters).
	_, err := testhelper.CreateBlock(ctx, bktDir, []labels.Labels{
		labels.FromStrings(labels.MetricName, "series_1"),
		labels.FromStrings(labels.MetricName, "series_2"),
		labels.FromStrings(labels.MetricName, "series_3"),
	}, numSamplesPerSeries, minTime, maxTime, nil)
	require.NoError(t, err)

	_, err = testhelper.CreateBlock(ctx, bktDir, []labels.Labels{
		labels.FromStrings(labels.MetricName, "series_1"),
		labels.FromStrings(labels.MetricName, "series_2"),
		labels.FromStrings(labels.MetricName, "series_3"),
	}, numSamplesPerSeries, minTime, maxTime, nil)
	require.NoError(t, err)

	// Create a bucket and upload the block there.
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	instrBkt := objstore.WithNoopInstr(bkt)

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil)
	assert.NoError(t, err)

	tests := map[string]struct {
		reqMatchers    []storepb.LabelMatcher
		seriesLimit    uint64
		chunksLimit    uint64
		expectedErr    string
		expectedSeries int
	}{
		"should fail if the number of unique series queried is greater than the configured series limit": {
			reqMatchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: "series_[123]"}},
			seriesLimit: 1,
			expectedErr: ErrSeriesLimitMessage,
		},
		"should pass if the number of unique series queried is equal or less than the configured series limit": {
			reqMatchers:    []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: "series_[123]"}},
			seriesLimit:    3,
			expectedSeries: 3,
		},
		"should fail if the number of chunks queried is greater than the configured chunks limit": {
			reqMatchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: "series_[123]"}},
			chunksLimit: 3,
			expectedErr: ErrChunksLimitMessage,
		},
		"should pass if the number of chunks queried is equal or less than the configured chunks limit": {
			reqMatchers:    []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: "series_[123]"}},
			chunksLimit:    6,
			expectedSeries: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, batchSize := range []int{1, 2} {
				t.Run(fmt.Sprintf("batch size: %d", batchSize), func(t *testing.T) {
					store, err := NewBucketStore(
						"tenant",
						instrBkt,
						fetcher,
						tmpDir,
						newStaticChunksLimiterFactory(testData.chunksLimit),
						newStaticSeriesLimiterFactory(testData.seriesLimit),
						newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
						10,
						mimir_tsdb.DefaultPostingOffsetInMemorySampling,
						indexheader.Config{},
						false,
						0,
						hashcache.NewSeriesHashCache(1024*1024),
						NewBucketStoreMetrics(nil),
						WithStreamingSeriesPerBatch(batchSize),
					)
					assert.NoError(t, err)
					assert.NoError(t, store.SyncBlocks(ctx))

					req := &storepb.SeriesRequest{
						MinTime:  minTime,
						MaxTime:  maxTime,
						Matchers: testData.reqMatchers,
					}

					srv := newBucketStoreSeriesServer(ctx)
					err = store.Series(req, srv)

					if testData.expectedErr != "" {
						require.Error(t, err)
						assert.ErrorContains(t, err, testData.expectedErr)
					} else {
						require.NoError(t, err)
						assert.Len(t, srv.SeriesSet, testData.expectedSeries)
					}
				})
			}
		})
	}
}

func mustMarshalAny(pb proto.Message) *types.Any {
	out, err := types.MarshalAny(pb)
	if err != nil {
		panic(err)
	}
	return out
}

func createBlockWithOneSeriesWithStep(t test.TB, dir string, lbls labels.Labels, blockIndex, totalSamples int, random *rand.Rand, step int64) ulid.ULID {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = dir
	headOpts.ChunkRange = int64(totalSamples) * step
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, h.Close()) }()

	app := h.Appender(context.Background())

	ts := int64(blockIndex * totalSamples)
	ref, err := app.Append(0, lbls, ts, random.Float64())
	assert.NoError(t, err)
	for i := 1; i < totalSamples; i++ {
		_, err := app.Append(ref, nil, ts+step*int64(i), random.Float64())
		assert.NoError(t, err)
	}
	assert.NoError(t, app.Commit())

	return createBlockFromHead(t, dir, h)
}

func setupStoreForHintsTest(t *testing.T, opts ...BucketStoreOption) (test.TB, *BucketStore, []*storepb.Series, []*storepb.Series, ulid.ULID, ulid.ULID, func()) {
	tb := test.NewTB(t)

	closers := []func(){}

	tmpDir := t.TempDir()

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	closers = append(closers, func() { assert.NoError(t, bkt.Close()) })

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
		random   = rand.New(rand.NewSource(120))
	)

	prependLabels := labels.FromStrings("ext1", "1")
	// Inject the Thanos meta to each block in the storage.
	thanosMeta := metadata.Thanos{
		Labels: prependLabels.Map(),
		Source: metadata.TestSource,
	}

	// Create TSDB blocks.
	head, seriesSet1 := createHeadWithSeries(t, 0, headGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "0"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    prependLabels,
		Random:           random,
	})
	block1 := createBlockFromHead(t, bktDir, head)
	assert.NoError(t, head.Close())
	head2, seriesSet2 := createHeadWithSeries(t, 1, headGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "1"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    prependLabels,
		Random:           random,
	})
	block2 := createBlockFromHead(t, bktDir, head2)
	assert.NoError(t, head2.Close())

	for _, blockID := range []ulid.ULID{block1, block2} {
		_, err := metadata.InjectThanos(logger, filepath.Join(bktDir, blockID.String()), thanosMeta, nil)
		assert.NoError(t, err)
	}

	// Instance a real bucket store we'll use to query back the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil)
	assert.NoError(tb, err)

	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(logger, nil, indexcache.InMemoryIndexCacheConfig{})
	assert.NoError(tb, err)

	opts = append([]BucketStoreOption{WithLogger(logger), WithIndexCache(indexCache)}, opts...)
	store, err := NewBucketStore(
		"tenant",
		instrBkt,
		fetcher,
		tmpDir,
		newStaticChunksLimiterFactory(10000/MaxSamplesPerChunk),
		newStaticSeriesLimiterFactory(0),
		newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.Config{},
		false,
		0,
		hashcache.NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		opts...,
	)
	assert.NoError(tb, err)
	assert.NoError(tb, store.SyncBlocks(context.Background()))

	closers = append(closers, func() { assert.NoError(t, store.RemoveBlocksAndClose()) })

	return tb, store, seriesSet1, seriesSet2, block1, block2, func() {
		for _, close := range closers {
			close()
		}
	}
}

func TestLabelNamesAndValuesHints(t *testing.T) {
	_, store, seriesSet1, seriesSet2, block1, block2, close := setupStoreForHintsTest(t)
	defer close()

	type labelNamesValuesCase struct {
		name string

		labelNamesReq      *storepb.LabelNamesRequest
		expectedNames      []string
		expectedNamesHints hintspb.LabelNamesResponseHints

		labelValuesReq      *storepb.LabelValuesRequest
		expectedValues      []string
		expectedValuesHints hintspb.LabelValuesResponseHints
	}

	testCases := []labelNamesValuesCase{
		{
			name: "querying a range containing 1 block should return 1 block in the labels hints",

			labelNamesReq: &storepb.LabelNamesRequest{
				Start: 0,
				End:   1,
			},
			expectedNames: labelNamesFromSeriesSet(seriesSet1),
			expectedNamesHints: hintspb.LabelNamesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},

			labelValuesReq: &storepb.LabelValuesRequest{
				Label: "ext1",
				Start: 0,
				End:   1,
			},
			expectedValues: []string{"1"},
			expectedValuesHints: hintspb.LabelValuesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},
		},
		{
			name: "querying a range containing multiple blocks should return multiple blocks in the response hints",

			labelNamesReq: &storepb.LabelNamesRequest{
				Start: 0,
				End:   3,
			},
			expectedNames: labelNamesFromSeriesSet(
				append(append([]*storepb.Series{}, seriesSet1...), seriesSet2...),
			),
			expectedNamesHints: hintspb.LabelNamesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
					{Id: block2.String()},
				},
			},

			labelValuesReq: &storepb.LabelValuesRequest{
				Label: "ext1",
				Start: 0,
				End:   3,
			},
			expectedValues: []string{"1"},
			expectedValuesHints: hintspb.LabelValuesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
					{Id: block2.String()},
				},
			},
		},
		{
			name: "querying a range containing multiple blocks but filtering a specific block should query only the requested block",

			labelNamesReq: &storepb.LabelNamesRequest{
				Start: 0,
				End:   3,
				Hints: mustMarshalAny(&hintspb.LabelNamesRequestHints{
					BlockMatchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: block1.String()},
					},
				}),
			},
			expectedNames: labelNamesFromSeriesSet(seriesSet1),
			expectedNamesHints: hintspb.LabelNamesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},

			labelValuesReq: &storepb.LabelValuesRequest{
				Label: "ext1",
				Start: 0,
				End:   3,
				Hints: mustMarshalAny(&hintspb.LabelValuesRequestHints{
					BlockMatchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_EQ, Name: block.BlockIDLabel, Value: block1.String()},
					},
				}),
			},
			expectedValues: []string{"1"},
			expectedValuesHints: hintspb.LabelValuesResponseHints{
				QueriedBlocks: []hintspb.Block{
					{Id: block1.String()},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			namesResp, err := store.LabelNames(context.Background(), tc.labelNamesReq)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedNames, namesResp.Names)

			var namesHints hintspb.LabelNamesResponseHints
			assert.NoError(t, types.UnmarshalAny(namesResp.Hints, &namesHints))
			// The order is not determinate, so we are sorting them.
			sort.Slice(namesHints.QueriedBlocks, func(i, j int) bool {
				return namesHints.QueriedBlocks[i].Id < namesHints.QueriedBlocks[j].Id
			})
			assert.Equal(t, tc.expectedNamesHints, namesHints)

			valuesResp, err := store.LabelValues(context.Background(), tc.labelValuesReq)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedValues, valuesResp.Values)

			var valuesHints hintspb.LabelValuesResponseHints
			assert.NoError(t, types.UnmarshalAny(valuesResp.Hints, &valuesHints))
			// The order is not determinate, so we are sorting them.
			sort.Slice(valuesHints.QueriedBlocks, func(i, j int) bool {
				return valuesHints.QueriedBlocks[i].Id < valuesHints.QueriedBlocks[j].Id
			})
			assert.Equal(t, tc.expectedValuesHints, valuesHints)
		})
	}
}

func labelNamesFromSeriesSet(series []*storepb.Series) []string {
	labelsMap := map[string]struct{}{}

	for _, s := range series {
		for _, label := range s.Labels {
			labelsMap[label.Name] = struct{}{}
		}
	}

	labels := make([]string, 0, len(labelsMap))
	for k := range labelsMap {
		labels = append(labels, k)
	}

	slices.Sort(labels)
	return labels
}

func BenchmarkBucketBlock_readChunkRange(b *testing.B) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()

		// Read chunks of different length. We're not using random to make the benchmark repeatable.
		readLengths = []int64{300, 500, 1000, 5000, 10000, 30000, 50000, 100000, 300000, 1500000}
	)

	tmpDir := b.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, bkt.Close())
	})

	// Create a block.
	blockID := createBlockWithOneSeriesWithStep(test.NewTB(b), tmpDir, labels.FromStrings("__name__", "test"), 0, 100000, rand.New(rand.NewSource(0)), 5000)

	// Upload the block to the bucket.
	thanosMeta := metadata.Thanos{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: metadata.TestSource,
	}

	blockMeta, err := metadata.InjectThanos(logger, filepath.Join(tmpDir, blockID.String()), thanosMeta, nil)
	assert.NoError(b, err)

	assert.NoError(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), nil))

	// Create a chunk pool with buckets between 8B and 32KB.
	chunkPool, err := pool.NewBucketedBytes(8, 32*1024, 2, 1e10)
	assert.NoError(b, err)

	// Create a bucket block with only the dependencies we need for the benchmark.
	blk, err := newBucketBlock(context.Background(), "tenant", logger, NewBucketStoreMetrics(nil), blockMeta, bkt, tmpDir, nil, chunkPool, nil, blockPartitioners{})
	assert.NoError(b, err)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		offset := int64(0)
		length := readLengths[n%len(readLengths)]

		_, err := blk.readChunkRange(ctx, 0, offset, length, byteRanges{{offset: 0, length: int(length)}})
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

func BenchmarkBlockSeries(b *testing.B) {
	blk, blockMeta := prepareBucket(b)

	for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
		for _, queryShardingEnabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("concurrency: %d, query sharding enabled: %v", concurrency, queryShardingEnabled), func(b *testing.B) {
				benchmarkBlockSeriesWithConcurrency(b, concurrency, blockMeta, blk, queryShardingEnabled)
			})
		}
	}
}

func prepareBucket(b *testing.B) (*bucketBlock, *metadata.Meta) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	tmpDir := b.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, bkt.Close())
	})

	// Create a block.
	head, _ := createHeadWithSeries(b, 0, headGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "head"),
		SamplesPerSeries: 86400 / 15, // Simulate 1 day block with 15s scrape interval.
		ScrapeInterval:   15 * time.Second,
		Series:           1000,
		PrependLabels:    nil,
		Random:           rand.New(rand.NewSource(120)),
		SkipChunks:       true,
	})
	blockID := createBlockFromHead(b, tmpDir, head)

	// Upload the block to the bucket.
	thanosMeta := metadata.Thanos{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: metadata.TestSource,
	}

	blockMeta, err := metadata.InjectThanos(logger, filepath.Join(tmpDir, blockID.String()), thanosMeta, nil)
	assert.NoError(b, err)

	assert.NoError(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), nil))
	assert.NoError(b, head.Close())

	// Create chunk pool and partitioner using the same production settings.
	chunkPool, err := NewDefaultChunkBytesPool(64 * 1024 * 1024 * 1024)
	assert.NoError(b, err)

	partitioner := newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil)

	// Create an index header reader.
	indexHeaderReader, err := indexheader.NewBinaryReader(ctx, logger, bkt, tmpDir, blockMeta.ULID, mimir_tsdb.DefaultPostingOffsetInMemorySampling, indexheader.Config{})
	assert.NoError(b, err)
	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(logger, nil, indexcache.DefaultInMemoryIndexCacheConfig)
	assert.NoError(b, err)

	// Create a bucket block with only the dependencies we need for the benchmark.
	blk, err := newBucketBlock(context.Background(), "tenant", logger, NewBucketStoreMetrics(nil), blockMeta, bkt, tmpDir, indexCache, chunkPool, indexHeaderReader, partitioner)
	assert.NoError(b, err)
	return blk, blockMeta
}

func benchmarkBlockSeriesWithConcurrency(b *testing.B, concurrency int, blockMeta *metadata.Meta, blk *bucketBlock, queryShardingEnabled bool) {
	ctx := context.Background()

	// Run the same number of queries per goroutine.
	queriesPerWorker := b.N / concurrency

	// No limits.
	chunksLimiter := newStaticChunksLimiterFactory(0)(nil)
	seriesLimiter := newStaticSeriesLimiterFactory(0)(nil)

	// Create the series hash cached used when query sharding is enabled.
	seriesHashCache := hashcache.NewSeriesHashCache(1024 * 1024 * 1024).GetBlockCache(blockMeta.ULID.String())

	// Run multiple workers to execute the queries.
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()

			for n := 0; n < queriesPerWorker; n++ {
				var reqMatchers []storepb.LabelMatcher
				var shardSelector *sharding.ShardSelector

				if queryShardingEnabled {
					// Each query touches the same series but a different shard.
					reqMatchers = []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: ".+"},
					}

					shardSelector = &sharding.ShardSelector{
						ShardIndex: uint64(n) % 20,
						ShardCount: 20,
					}
				} else {
					// Each query touches a subset of series. To make it reproducible and make sure
					// we just don't query consecutive series (as is in the real world), we do create
					// a label matcher which looks for a short integer within the label value.
					reqMatchers = []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: fmt.Sprintf(".*%d.*", n%20)},
					}
				}

				req := &storepb.SeriesRequest{
					MinTime:    blockMeta.MinTime,
					MaxTime:    blockMeta.MaxTime,
					Matchers:   reqMatchers,
					SkipChunks: false,
				}

				matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
				// TODO FIXME! require.NoError calls b.Fatalf under the hood, which
				// must be called only from the goroutine running the Benchmark function.
				require.NoError(b, err)

				indexReader := blk.indexReader()
				chunkReader := blk.chunkReader(ctx)
				chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, chunkBytesSlabSize)

				seriesSet, _, err := blockSeries(context.Background(), indexReader, chunkReader, chunksPool, matchers, shardSelector, cachedSeriesHasher{seriesHashCache}, chunksLimiter, seriesLimiter, req.SkipChunks, req.MinTime, req.MaxTime, log.NewNopLogger())
				require.NoError(b, err)

				// Ensure at least 1 series has been returned (as expected).
				require.Equal(b, true, seriesSet.Next())

				require.NoError(b, indexReader.Close())
				require.NoError(b, chunkReader.Close())
			}
		}()
	}

	wg.Wait()
}

func TestBlockSeries_skipChunks_ignoresMintMaxt(t *testing.T) {
	const series = 100
	newTestBucketBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), appendTestSeries(series))
	b := newTestBucketBlock()

	mint, maxt := int64(0), int64(0)
	skipChunks := true

	sl := NewLimiter(math.MaxUint64, promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}))
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "i", "")}
	ss, _, err := blockSeries(context.Background(), b.indexReader(), nil, nil, matchers, nil, nil, nil, sl, skipChunks, mint, maxt, log.NewNopLogger())
	require.NoError(t, err)
	require.True(t, ss.Next(), "Result set should have series because when skipChunks=true, mint/maxt should be ignored")
}

func TestBlockSeries_Cache(t *testing.T) {
	newTestBucketBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), appendTestSeries(100))

	t.Run("does not update cache on error", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:              b.indexHeaderReader,
			onLabelValuesCalled: func(_ string) error { return context.DeadlineExceeded },
		}
		b.indexCache = cacheNotExpectingToStoreSeries{t: t}

		sl := NewLimiter(math.MaxUint64, promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}))

		// This test relies on the fact that p~=foo.* has to call LabelValues(p) when doing ExpandedPostings().
		// We make that call fail in order to make the entire LabelValues(p~=foo.*) call fail.
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "p", "foo.*")}
		_, _, err := blockSeries(context.Background(), b.indexReader(), nil, nil, matchers, nil, nil, nil, sl, true, b.meta.MinTime, b.meta.MaxTime, log.NewNopLogger())
		require.Error(t, err)
	})

	t.Run("caches series", func(t *testing.T) {
		b := newTestBucketBlock()
		b.indexCache = newInMemoryIndexCache(t)

		sl := NewLimiter(math.MaxUint64, promauto.With(nil).NewCounter(prometheus.CounterOpts{Name: "test"}))
		shc := hashcache.NewSeriesHashCache(1 << 20).GetBlockCache(b.meta.ULID.String())

		testCases := []struct {
			matchers         []*labels.Matcher
			shard            *sharding.ShardSelector
			expectedLabelSet []labels.Labels
		}{
			// no shard
			{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "i", "0"+labelLongSuffix),
					labels.MustNewMatcher(labels.MatchRegexp, "n", "0.*"),
					labels.MustNewMatcher(labels.MatchRegexp, "p", "foo.*"),
				},
				shard: nil,
				expectedLabelSet: []labels.Labels{
					labels.FromStrings("i", "0"+labelLongSuffix, "n", "0"+labelLongSuffix, "j", "foo", "p", "foo"),
				},
			},
			// shard 1_of_2
			{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "i", "0"+labelLongSuffix),
					labels.MustNewMatcher(labels.MatchRegexp, "n", "0a.*"),
				},
				shard: &sharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
				expectedLabelSet: []labels.Labels{
					labels.FromStrings("i", "0"+labelLongSuffix, "n", "0"+labelLongSuffix, "j", "bar", "q", "foo"),
				},
			},
			// shard 2_of_2
			{
				matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "i", "0"+labelLongSuffix),
					labels.MustNewMatcher(labels.MatchRegexp, "n", "0a.*"),
				},
				shard: &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
				expectedLabelSet: []labels.Labels{
					labels.FromStrings("i", "0"+labelLongSuffix, "n", "0"+labelLongSuffix, "j", "foo", "p", "foo"),
				},
			},
		}

		indexr := b.indexReader()
		for i, tc := range testCases {
			ss, _, err := blockSeries(context.Background(), indexr, nil, nil, tc.matchers, tc.shard, cachedSeriesHasher{shc}, nil, sl, true, b.meta.MinTime, b.meta.MaxTime, log.NewNopLogger())
			require.NoError(t, err, "Unexpected error for test case %d", i)
			lset := lsetFromSeriesSet(t, ss)
			require.Equalf(t, tc.expectedLabelSet, lset, "Wrong label set for test case %d", i)
		}

		// Cache should be filled by now.
		// We break the index cache to not allow looking up series, so we know we don't look up series.
		indexr.block.indexCache = forbiddenFetchMultiSeriesForRefsIndexCache{b.indexCache, t}
		for i, tc := range testCases {
			ss, _, err := blockSeries(context.Background(), indexr, nil, nil, tc.matchers, tc.shard, cachedSeriesHasher{shc}, nil, sl, true, b.meta.MinTime, b.meta.MaxTime, log.NewNopLogger())
			require.NoError(t, err, "Unexpected error for test case %d", i)
			lset := lsetFromSeriesSet(t, ss)
			require.Equalf(t, tc.expectedLabelSet, lset, "Wrong label set for test case %d", i)
		}
	})
}

func lsetFromSeriesSet(t *testing.T, ss storepb.SeriesSet) []labels.Labels {
	var lset []labels.Labels
	for ss.Next() {
		ls, _ := ss.At()
		lset = append(lset, ls)
	}
	require.NoError(t, ss.Err())
	return lset
}

type cacheNotExpectingToStoreSeries struct {
	noopCache
	t *testing.T
}

func (c cacheNotExpectingToStoreSeries) StoreSeries(ctx context.Context, userID string, blockID ulid.ULID, matchersKey indexcache.LabelMatchersKey, shard *sharding.ShardSelector, v []byte) {
	c.t.Fatalf("StoreSeries should not be called")
}

func (c cacheNotExpectingToStoreSeries) StoreSeriesForPostings(ctx context.Context, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey indexcache.PostingsKey, v []byte) {
	c.t.Fatalf("StoreSeriesForPostings should not be called")
}

type headGenOptions struct {
	TSDBDir                  string
	SamplesPerSeries, Series int
	ScrapeInterval           time.Duration

	WithWAL       bool
	PrependLabels labels.Labels
	SkipChunks    bool // Skips chunks in returned slice (not in generated head!).

	Random *rand.Rand
}

// createHeadWithSeries returns head filled with given samples and same series returned in separate list for assertion purposes.
// Each series looks as follows:
// {foo=bar,i=000001aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd} <random value> where number indicate sample number from 0.
// Returned series are framed in the same way as remote read would frame them.
func createHeadWithSeries(t testing.TB, j int, opts headGenOptions) (*tsdb.Head, []*storepb.Series) {
	if opts.SamplesPerSeries < 1 || opts.Series < 1 {
		t.Fatal("samples and series has to be 1 or more")
	}
	if opts.ScrapeInterval == 0 {
		opts.ScrapeInterval = 1 * time.Millisecond
	}

	t.Logf(
		"Creating %d %d-sample series with %s interval in %s\n",
		opts.Series,
		opts.SamplesPerSeries,
		opts.ScrapeInterval.String(),
		opts.TSDBDir,
	)

	var w *wlog.WL
	var err error
	if opts.WithWAL {
		w, err = wlog.New(nil, nil, filepath.Join(opts.TSDBDir, "wal"), true)
		assert.NoError(t, err)
	} else {
		assert.NoError(t, os.MkdirAll(filepath.Join(opts.TSDBDir, "wal"), os.ModePerm))
	}

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = opts.TSDBDir
	h, err := tsdb.NewHead(nil, nil, w, nil, headOpts, nil)
	assert.NoError(t, err)

	app := h.Appender(context.Background())
	for i := 0; i < opts.Series; i++ {
		tsLabel := j*opts.Series*opts.SamplesPerSeries + i*opts.SamplesPerSeries

		// Add "PrependLabels" to real series labels.
		lbls := labels.NewBuilder(opts.PrependLabels)
		lbls.Set("foo", "bar")
		lbls.Set("i", fmt.Sprintf("%07d%s", tsLabel, labelLongSuffix))
		ref, err := app.Append(
			0,
			lbls.Labels(nil),
			int64(tsLabel)*opts.ScrapeInterval.Milliseconds(),
			opts.Random.Float64(),
		)
		assert.NoError(t, err)

		for is := 1; is < opts.SamplesPerSeries; is++ {
			_, err := app.Append(ref, nil, int64(tsLabel+is)*opts.ScrapeInterval.Milliseconds(), opts.Random.Float64())
			assert.NoError(t, err)
		}
	}
	assert.NoError(t, app.Commit())

	// Use TSDB and get all series for assertion.
	chks, err := h.Chunks()
	assert.NoError(t, err)
	defer func() { assert.NoError(t, chks.Close()) }()

	ir, err := h.Index()
	assert.NoError(t, err)
	defer func() { assert.NoError(t, ir.Close()) }()

	var (
		chunkMetas []chunks.Meta
		expected   = make([]*storepb.Series, 0, opts.Series)
	)

	var builder labels.ScratchBuilder
	all := allPostings(t, ir)
	for all.Next() {
		assert.NoError(t, ir.Series(all.At(), &builder, &chunkMetas))
		expected = append(expected, &storepb.Series{Labels: mimirpb.FromLabelsToLabelAdapters(builder.Labels())})

		if opts.SkipChunks {
			continue
		}

		for _, c := range chunkMetas {
			chEnc, err := chks.Chunk(c)
			assert.NoError(t, err)

			// Open Chunk.
			if c.MaxTime == math.MaxInt64 {
				c.MaxTime = c.MinTime + int64(chEnc.NumSamples()) - 1
			}

			expected[len(expected)-1].Chunks = append(expected[len(expected)-1].Chunks, storepb.AggrChunk{
				MinTime: c.MinTime,
				MaxTime: c.MaxTime,
				Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chEnc.Bytes()},
			})
		}
	}
	assert.NoError(t, all.Err())
	return h, expected
}

func runSeriesInterestingCases(t test.TB, maxSamples, maxSeries int, f func(t test.TB, samplesPerSeries, series int)) {
	for _, tc := range []struct {
		samplesPerSeries int
		series           int
	}{
		{
			samplesPerSeries: 1,
			series:           maxSeries,
		},
		{
			samplesPerSeries: maxSamples / (maxSeries / 10),
			series:           maxSeries / 10,
		},
		{
			samplesPerSeries: maxSamples,
			series:           1,
		},
	} {
		if ok := t.Run(fmt.Sprintf("%dSeriesWith%dSamples", tc.series, tc.samplesPerSeries), func(t test.TB) {
			f(t, tc.samplesPerSeries, tc.series)
		}); !ok {
			return
		}
		runtime.GC()
	}
}

// seriesCase represents single test/benchmark case for testing storepb series.
type seriesCase struct {
	Name string
	Req  *storepb.SeriesRequest

	// Exact expectations are checked only for tests. For benchmarks only length is assured.
	ExpectedSeries   []*storepb.Series
	ExpectedWarnings []string
	ExpectedHints    hintspb.SeriesResponseHints
}

// runTestServerSeries runs tests against given cases.
func runTestServerSeries(t test.TB, store *BucketStore, cases ...*seriesCase) {
	for _, c := range cases {
		t.Run(c.Name, func(t test.TB) {
			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				srv := newBucketStoreSeriesServer(context.Background())
				require.NoError(t, store.Series(c.Req, srv))
				require.Equal(t, len(c.ExpectedWarnings), len(srv.Warnings), "%v", srv.Warnings)
				require.Equal(t, len(c.ExpectedSeries), len(srv.SeriesSet), "Matchers: %v Min time: %d Max time: %d", c.Req.Matchers, c.Req.MinTime, c.Req.MaxTime)

				if !t.IsBenchmark() {
					if len(c.ExpectedSeries) == 1 {
						// For bucketStoreAPI chunks are not sorted within response. TODO: Investigate: Is this fine?
						sort.Slice(srv.SeriesSet[0].Chunks, func(i, j int) bool {
							return srv.SeriesSet[0].Chunks[i].MinTime < srv.SeriesSet[0].Chunks[j].MinTime
						})
					}

					// Huge responses can produce unreadable diffs - make it more human readable.
					if len(c.ExpectedSeries) > 4 {
						for j := range c.ExpectedSeries {
							assert.Equal(t, c.ExpectedSeries[j].Labels, srv.SeriesSet[j].Labels, "%v series chunks mismatch", j)

							// Check chunks when it is not a skip chunk query
							if !c.Req.SkipChunks {
								if len(c.ExpectedSeries[j].Chunks) > 20 {
									assert.Equal(t, len(c.ExpectedSeries[j].Chunks), len(srv.SeriesSet[j].Chunks), "%v series chunks number mismatch", j)
								}
								assert.Equal(t, c.ExpectedSeries[j].Chunks, srv.SeriesSet[j].Chunks, "%v series chunks mismatch", j)
							}
						}
					} else {
						assert.Equal(t, c.ExpectedSeries, srv.SeriesSet)
					}

					assert.Equal(t, c.ExpectedHints, srv.Hints)
				}
			}
		})
	}
}

func TestFilterPostingsByCachedShardHash(t *testing.T) {
	tests := map[string]struct {
		inputPostings    []storage.SeriesRef
		shard            *sharding.ShardSelector
		cacheEntries     [][2]uint64 // List of cache entries where each entry is the pair [seriesID, hash]
		expectedPostings []storage.SeriesRef
	}{
		"should be a noop if the cache is empty": {
			inputPostings:    []storage.SeriesRef{0, 1, 2, 3, 4, 5},
			shard:            &sharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{},
			expectedPostings: []storage.SeriesRef{0, 1, 2, 3, 4, 5},
		},
		"should filter postings at the beginning of the slice": {
			inputPostings:    []storage.SeriesRef{0, 1, 2, 3, 4, 5},
			shard:            &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			cacheEntries:     [][2]uint64{{0, 0}, {1, 1}},
			expectedPostings: []storage.SeriesRef{1, 2, 3, 4, 5},
		},
		"should filter postings in the middle of the slice": {
			inputPostings:    []storage.SeriesRef{0, 1, 2, 3, 4, 5},
			shard:            &sharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{{0, 0}, {1, 1}},
			expectedPostings: []storage.SeriesRef{0, 2, 3, 4, 5},
		},
		"should filter postings at the end of the slice": {
			inputPostings:    []storage.SeriesRef{0, 1, 2, 3, 4, 5},
			shard:            &sharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{{4, 4}, {5, 5}},
			expectedPostings: []storage.SeriesRef{0, 1, 2, 3, 4},
		},
		"should filter postings when all postings are in the cache": {
			inputPostings:    []storage.SeriesRef{0, 1, 2, 3, 4, 5},
			shard:            &sharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			expectedPostings: []storage.SeriesRef{0, 2, 4},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			hasher := mockSeriesHasher{cached: make(map[storage.SeriesRef]uint64)}
			for _, pair := range testData.cacheEntries {
				hasher.cached[storage.SeriesRef(pair[0])] = pair[1]
			}

			actualPostings := filterPostingsByCachedShardHash(testData.inputPostings, testData.shard, hasher, nil)
			assert.Equal(t, testData.expectedPostings, actualPostings)
		})
	}
}

func TestFilterPostingsByCachedShardHash_NoAllocations(t *testing.T) {
	inputPostings := []storage.SeriesRef{0, 1, 2, 3, 4, 5}
	shard := &sharding.ShardSelector{ShardIndex: 0, ShardCount: 2}
	cacheEntries := [][2]uint64{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}

	cache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	for _, pair := range cacheEntries {
		cache.Store(storage.SeriesRef(pair[0]), pair[1])
	}
	stats := &queryStats{}

	assert.Equal(t, float64(0), testing.AllocsPerRun(1, func() {
		filterPostingsByCachedShardHash(inputPostings, shard, cachedSeriesHasher{cache}, stats)
	}))
}

func BenchmarkFilterPostingsByCachedShardHash_AllPostingsShifted(b *testing.B) {
	// This benchmark tests the case only the 1st posting is removed
	// and so all subsequent postings will be shifted.
	cache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	cache.Store(0, 0)
	shard := &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2}

	// Create a long list of postings.
	const numPostings = 10000
	originalPostings := make([]storage.SeriesRef, numPostings)
	for i := 0; i < numPostings; i++ {
		originalPostings[i] = storage.SeriesRef(i)
	}

	inputPostings := make([]storage.SeriesRef, numPostings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Copy the original postings into the input ones, since they will be overwritten.
		inputPostings = inputPostings[0:numPostings]
		copy(inputPostings, originalPostings)

		filterPostingsByCachedShardHash(inputPostings, shard, cachedSeriesHasher{cache}, nil)
	}
}

func BenchmarkFilterPostingsByCachedShardHash_NoPostingsShifted(b *testing.B) {
	// This benchmark tests the case the output postings is equal to the input one.
	cache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	shard := &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2}

	// Create a long list of postings.
	const numPostings = 10000
	ps := make([]storage.SeriesRef, numPostings)
	for i := 0; i < numPostings; i++ {
		ps[i] = storage.SeriesRef(i)
	}

	for n := 0; n < b.N; n++ {
		// We reuse the same postings slice because we expect this test to not
		// modify it (cache is empty).
		filterPostingsByCachedShardHash(ps, shard, cachedSeriesHasher{cache}, nil)
	}
}
