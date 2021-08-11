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
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/oklog/ulid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/filesystem"
	"github.com/thanos-io/thanos/pkg/pool"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/querier/querysharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/test"
)

const (
	// labelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
	labelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
)

var emptyRelabelConfig = make([]*relabel.Config, 0)

func TestBucketBlock_Property(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(2000)
	parameters.MinSuccessfulTests = 20000
	properties := gopter.NewProperties(parameters)

	set := newBucketBlockSet(labels.Labels{})

	type resBlock struct {
		mint, maxt int64
		window     int64
	}
	// This input resembles a typical production-level block layout
	// in remote object storage.
	input := []resBlock{
		{window: downsample.ResLevel0, mint: 0, maxt: 100},
		{window: downsample.ResLevel0, mint: 100, maxt: 200},
		// Compaction level 2 begins but not downsampling (8 hour block length).
		{window: downsample.ResLevel0, mint: 200, maxt: 600},
		{window: downsample.ResLevel0, mint: 600, maxt: 1000},
		// Compaction level 3 begins, Some of it is downsampled but still retained (48 hour block length).
		{window: downsample.ResLevel0, mint: 1000, maxt: 1750},
		{window: downsample.ResLevel1, mint: 1000, maxt: 1750},
		// Compaction level 4 begins, different downsampling levels cover the same (336 hour block length).
		{window: downsample.ResLevel0, mint: 1750, maxt: 7000},
		{window: downsample.ResLevel1, mint: 1750, maxt: 7000},
		{window: downsample.ResLevel2, mint: 1750, maxt: 7000},
		// Compaction level 4 already happened, raw samples have been deleted.
		{window: downsample.ResLevel0, mint: 7000, maxt: 14000},
		{window: downsample.ResLevel1, mint: 7000, maxt: 14000},
		// Compaction level 4 already happened, raw and downsample res level 1 samples have been deleted.
		{window: downsample.ResLevel2, mint: 14000, maxt: 21000},
	}

	for _, in := range input {
		var m metadata.Meta
		m.Thanos.Downsample.Resolution = in.window
		m.MinTime = in.mint
		m.MaxTime = in.maxt

		assert.NoError(t, set.add(&bucketBlock{meta: &m}))
	}

	properties.Property("getFor always gets at least some data in range", prop.ForAllNoShrink(
		func(low, high, maxResolution int64) bool {
			// Bogus case.
			if low >= high {
				return true
			}

			res := set.getFor(low, high, maxResolution, nil)

			// The data that we get must all encompass our requested range.
			if len(res) == 1 && (res[0].meta.Thanos.Downsample.Resolution > maxResolution ||
				res[0].meta.MinTime > low) {
				return false
			} else if len(res) > 1 {
				mint := int64(21001)
				for i := 0; i < len(res)-1; i++ {
					if res[i].meta.Thanos.Downsample.Resolution > maxResolution {
						return false
					}
					if res[i+1].meta.MinTime != res[i].meta.MaxTime {
						return false
					}
					if res[i].meta.MinTime < mint {
						mint = res[i].meta.MinTime
					}
				}
				if res[len(res)-1].meta.MinTime < mint {
					mint = res[len(res)-1].meta.MinTime
				}
				if low < mint {
					return false
				}

			}
			return true
		}, gen.Int64Range(0, 21000), gen.Int64Range(0, 21000), gen.Int64Range(0, 60*60*1000)),
	)

	properties.Property("getFor always gets all data in range", prop.ForAllNoShrink(
		func(low, high int64) bool {
			// Bogus case.
			if low >= high {
				return true
			}

			maxResolution := downsample.ResLevel2
			res := set.getFor(low, high, maxResolution, nil)

			// The data that we get must all encompass our requested range.
			if len(res) == 1 && (res[0].meta.Thanos.Downsample.Resolution > maxResolution ||
				res[0].meta.MinTime > low || res[0].meta.MaxTime < high) {
				return false
			} else if len(res) > 1 {
				mint := int64(21001)
				maxt := int64(0)
				for i := 0; i < len(res)-1; i++ {
					if res[i+1].meta.MinTime != res[i].meta.MaxTime {
						return false
					}
					if res[i].meta.MinTime < mint {
						mint = res[i].meta.MinTime
					}
					if res[i].meta.MaxTime > maxt {
						maxt = res[i].meta.MaxTime
					}
				}
				if res[len(res)-1].meta.MinTime < mint {
					mint = res[len(res)-1].meta.MinTime
				}
				if res[len(res)-1].meta.MaxTime > maxt {
					maxt = res[len(res)-1].meta.MaxTime
				}
				if low < mint {
					return false
				}
				if high > maxt {
					return false
				}

			}
			return true
		}, gen.Int64Range(0, 21000), gen.Int64Range(0, 21000)),
	)

	properties.TestingRun(t)
}

func TestBucketBlock_matchLabels(t *testing.T) {
	dir, err := ioutil.TempDir("", "bucketblock-test")
	assert.NoError(t, err)
	defer assert.NoError(t, os.RemoveAll(dir))

	bkt, err := filesystem.NewBucket(dir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	blockID := ulid.MustNew(1, nil)
	meta := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: blockID},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				"a": "b",
				"c": "d",
			},
		},
	}

	b, err := newBucketBlock(context.Background(), log.NewNopLogger(), NewBucketStoreMetrics(nil), meta, bkt, path.Join(dir, blockID.String()), nil, nil, nil, nil)
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
			match: true,
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
		ok := b.matchRelabelLabels(c.in)
		assert.Equal(t, c.match, ok)
	}

	// Ensure block's labels in the meta have not been manipulated.
	assert.Equal(t, map[string]string{
		"a": "b",
		"c": "d",
	}, meta.Thanos.Labels)
}

func TestBucketBlockSet_addGet(t *testing.T) {
	set := newBucketBlockSet(labels.Labels{})

	type resBlock struct {
		mint, maxt int64
		window     int64
	}
	// Input is expected to be sorted. It is sorted in addBlock.
	input := []resBlock{
		// Blocks from 0 to 100 with raw resolution.
		{window: downsample.ResLevel0, mint: 0, maxt: 100},
		{window: downsample.ResLevel0, mint: 100, maxt: 200},
		{window: downsample.ResLevel0, mint: 100, maxt: 200}, // Same overlap.
		{window: downsample.ResLevel0, mint: 200, maxt: 299}, // Short overlap.
		{window: downsample.ResLevel0, mint: 200, maxt: 300},
		{window: downsample.ResLevel0, mint: 300, maxt: 400},
		{window: downsample.ResLevel0, mint: 300, maxt: 600}, // Long overlap.
		{window: downsample.ResLevel0, mint: 400, maxt: 500},
		// Lower resolution data not covering last block.
		{window: downsample.ResLevel1, mint: 0, maxt: 100},
		{window: downsample.ResLevel1, mint: 100, maxt: 200},
		{window: downsample.ResLevel1, mint: 200, maxt: 300},
		{window: downsample.ResLevel1, mint: 300, maxt: 400},
		// Lower resolution data only covering middle blocks.
		{window: downsample.ResLevel2, mint: 100, maxt: 200},
		{window: downsample.ResLevel2, mint: 200, maxt: 300},
	}

	for _, in := range input {
		var m metadata.Meta
		m.Thanos.Downsample.Resolution = in.window
		m.MinTime = in.mint
		m.MaxTime = in.maxt

		assert.NoError(t, set.add(&bucketBlock{meta: &m}))
	}

	for _, c := range []struct {
		mint, maxt    int64
		maxResolution int64
		res           []resBlock
	}{
		{
			mint:          -100,
			maxt:          1000,
			maxResolution: 0,
			res: []resBlock{
				{window: downsample.ResLevel0, mint: 0, maxt: 100},
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 299},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          100,
			maxt:          400,
			maxResolution: downsample.ResLevel1 - 1,
			res: []resBlock{
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 100, maxt: 200},
				{window: downsample.ResLevel0, mint: 200, maxt: 299},
				{window: downsample.ResLevel0, mint: 200, maxt: 300},
				{window: downsample.ResLevel0, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				// Block intervals are half-open: [b.MinTime, b.MaxTime), so 400-500 contains single sample.
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          100,
			maxt:          500,
			maxResolution: downsample.ResLevel1,
			res: []resBlock{
				{window: downsample.ResLevel1, mint: 100, maxt: 200},
				{window: downsample.ResLevel1, mint: 200, maxt: 300},
				{window: downsample.ResLevel1, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		}, {
			mint:          0,
			maxt:          500,
			maxResolution: downsample.ResLevel2,
			res: []resBlock{
				{window: downsample.ResLevel1, mint: 0, maxt: 100},
				{window: downsample.ResLevel2, mint: 100, maxt: 200},
				{window: downsample.ResLevel2, mint: 200, maxt: 300},
				{window: downsample.ResLevel1, mint: 300, maxt: 400},
				{window: downsample.ResLevel0, mint: 300, maxt: 600},
				{window: downsample.ResLevel0, mint: 400, maxt: 500},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			var exp []*bucketBlock
			for _, b := range c.res {
				var m metadata.Meta
				m.Thanos.Downsample.Resolution = b.window
				m.MinTime = b.mint
				m.MaxTime = b.maxt
				exp = append(exp, &bucketBlock{meta: &m})
			}
			assert.Equal(t, exp, set.getFor(c.mint, c.maxt, c.maxResolution, nil))
		})
	}
}

func TestBucketBlockSet_remove(t *testing.T) {
	set := newBucketBlockSet(labels.Labels{})

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
	res := set.getFor(0, 300, 0, nil)

	assert.Equal(t, 2, len(res))
	assert.Equal(t, input[0].id, res[0].meta.ULID)
	assert.Equal(t, input[2].id, res[1].meta.ULID)
}

func TestBucketBlockSet_labelMatchers(t *testing.T) {
	set := newBucketBlockSet(labels.FromStrings("a", "b", "c", "d"))

	cases := []struct {
		in    []*labels.Matcher
		res   []*labels.Matcher
		match bool
	}{
		{
			in:    []*labels.Matcher{},
			res:   []*labels.Matcher{},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "a", Value: "b"},
				{Type: labels.MatchEqual, Name: "c", Value: "d"},
			},
			res:   []*labels.Matcher{},
			match: true,
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
			res: []*labels.Matcher{
				{Type: labels.MatchEqual, Name: "e", Value: "f"},
			},
			match: true,
		},
		// Those are matchers mentioned here: https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
		// We want to provide explicit tests that says when Thanos supports its and when not. We don't support it here in
		// external labelset level.
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "x"},
			},
			res: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "x"},
			},
			match: true,
		},
		{
			in: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "d"},
			},
			res: []*labels.Matcher{
				{Type: labels.MatchNotEqual, Name: "", Value: "d"},
			},
			match: true,
		},
	}
	for _, c := range cases {
		res, ok := set.labelMatchers(c.in...)
		assert.Equal(t, c.match, ok)
		assert.Equal(t, c.res, res)
	}
}

func TestBucketStore_Info(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := ioutil.TempDir("", "bucketstore-test")
	assert.NoError(t, err)

	defer assert.NoError(t, os.RemoveAll(dir))

	chunkPool, err := NewDefaultChunkBytesPool(2e5)
	assert.NoError(t, err)

	bucketStore, err := NewBucketStore(
		nil,
		nil,
		dir,
		NewChunksLimiterFactory(0),
		NewSeriesLimiterFactory(0),
		newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		20,
		true,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
		NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithChunkPool(chunkPool),
		WithFilterConfig(allowAllFilterConf),
	)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bucketStore.Close()) }()

	resp, err := bucketStore.Info(ctx, &storepb.InfoRequest{})
	assert.NoError(t, err)

	assert.Equal(t, storepb.StoreType_STORE, resp.StoreType)
	assert.Equal(t, int64(math.MaxInt64), resp.MinTime)
	assert.Equal(t, int64(math.MinInt64), resp.MaxTime)
	assert.Equal(t, []labelpb.ZLabelSet(nil), resp.LabelSets)
	assert.Equal(t, []labelpb.ZLabel(nil), resp.Labels)
}

type recorder struct {
	mtx sync.Mutex
	objstore.Bucket

	getRangeTouched []string
	getTouched      []string
}

func (r *recorder) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.getTouched = append(r.getTouched, name)
	return r.Bucket.Get(ctx, name)
}

func (r *recorder) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.getRangeTouched = append(r.getRangeTouched, name)
	return r.Bucket.GetRange(ctx, name, off, length)
}

func TestBucketStore_Sharding(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	dir, err := ioutil.TempDir("", "test-sharding-prepare")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(dir)) }()

	bkt := objstore.NewInMemBucket()
	series := []labels.Labels{labels.FromStrings("a", "1", "b", "1")}

	id1, err := CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r1"}}, 0, metadata.NoneFunc)
	assert.NoError(t, err)
	assert.NoError(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id1.String()), metadata.NoneFunc))

	id2, err := CreateBlock(ctx, dir, series, 10, 1000, 2000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r1"}}, 0, metadata.NoneFunc)
	assert.NoError(t, err)
	assert.NoError(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id2.String()), metadata.NoneFunc))

	id3, err := CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "b"}, {Name: "region", Value: "r1"}}, 0, metadata.NoneFunc)
	assert.NoError(t, err)
	assert.NoError(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id3.String()), metadata.NoneFunc))

	id4, err := CreateBlock(ctx, dir, series, 10, 0, 1000, labels.Labels{{Name: "cluster", Value: "a"}, {Name: "region", Value: "r2"}}, 0, metadata.NoneFunc)
	assert.NoError(t, err)
	assert.NoError(t, block.Upload(ctx, logger, bkt, filepath.Join(dir, id4.String()), metadata.NoneFunc))

	if ok := t.Run("new_runs", func(t *testing.T) {
		testSharding(t, "", bkt, id1, id2, id3, id4)
	}); !ok {
		return
	}

	dir2, err := ioutil.TempDir("", "test-sharding2")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(dir2)) }()

	t.Run("reuse_disk", func(t *testing.T) {
		testSharding(t, dir2, bkt, id1, id2, id3, id4)
	})
}

func testSharding(t *testing.T, reuseDisk string, bkt objstore.Bucket, all ...ulid.ULID) {
	var cached []ulid.ULID

	logger := log.NewNopLogger()
	for _, sc := range []struct {
		name              string
		relabel           string
		expectedIDs       []ulid.ULID
		expectedAdvLabels []labelpb.ZLabelSet
	}{
		{
			name:        "no sharding",
			expectedIDs: all,
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r2"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "b"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "drop cluster=a sources",
			relabel: `
            - action: drop
              regex: "a"
              source_labels:
              - cluster
            `,
			expectedIDs: []ulid.ULID{all[2]},
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "b"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "keep only cluster=a sources",
			relabel: `
            - action: keep
              regex: "a"
              source_labels:
              - cluster
            `,
			expectedIDs: []ulid.ULID{all[0], all[1], all[3]},
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r2"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "keep only cluster=a without .*2 region sources",
			relabel: `
            - action: keep
              regex: "a"
              source_labels:
              - cluster
            - action: drop
              regex: ".*2"
              source_labels:
              - region
            `,
			expectedIDs: []ulid.ULID{all[0], all[1]},
			expectedAdvLabels: []labelpb.ZLabelSet{
				{
					Labels: []labelpb.ZLabel{
						{Name: "cluster", Value: "a"},
						{Name: "region", Value: "r1"},
					},
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: CompatibilityTypeLabelName, Value: "store"},
					},
				},
			},
		},
		{
			name: "drop all",
			relabel: `
            - action: drop
              regex: "a"
              source_labels:
              - cluster
            - action: drop
              regex: "r1"
              source_labels:
              - region
            `,
			expectedIDs:       []ulid.ULID{},
			expectedAdvLabels: []labelpb.ZLabelSet{},
		},
	} {
		t.Run(sc.name, func(t *testing.T) {
			dir := reuseDisk

			if dir == "" {
				var err error
				dir, err = ioutil.TempDir("", "test-sharding")
				assert.NoError(t, err)
				defer func() { assert.NoError(t, os.RemoveAll(dir)) }()
			}
			relabelConf, err := block.ParseRelabelConfig([]byte(sc.relabel), block.SelectorSupportedRelabelActions)
			assert.NoError(t, err)

			rec := &recorder{Bucket: bkt}
			metaFetcher, err := block.NewMetaFetcher(logger, 20, objstore.WithNoopInstr(bkt), dir, nil, []block.MetadataFilter{
				block.NewTimePartitionMetaFilter(allowAllFilterConf.MinTime, allowAllFilterConf.MaxTime),
				block.NewLabelShardedMetaFilter(relabelConf),
			}, nil)
			assert.NoError(t, err)

			bucketStore, err := NewBucketStore(
				objstore.WithNoopInstr(rec),
				metaFetcher,
				dir,
				NewChunksLimiterFactory(0),
				NewSeriesLimiterFactory(0),
				newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
				20,
				true,
				mimir_tsdb.DefaultPostingOffsetInMemorySampling,
				false,
				false,
				0,
				NewSeriesHashCache(1024*1024),
				NewBucketStoreMetrics(nil),
				WithLogger(logger),
				WithFilterConfig(allowAllFilterConf),
			)
			assert.NoError(t, err)
			defer func() { assert.NoError(t, bucketStore.Close()) }()

			assert.NoError(t, bucketStore.InitialSync(context.Background()))

			// Check "stored" blocks.
			ids := make([]ulid.ULID, 0, len(bucketStore.blocks))
			for id := range bucketStore.blocks {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool {
				return ids[i].Compare(ids[j]) < 0
			})
			assert.Equal(t, sc.expectedIDs, ids)

			// Check Info endpoint.
			resp, err := bucketStore.Info(context.Background(), &storepb.InfoRequest{})
			assert.NoError(t, err)

			assert.Equal(t, storepb.StoreType_STORE, resp.StoreType)
			assert.Equal(t, []labelpb.ZLabel(nil), resp.Labels)
			assert.Equal(t, sc.expectedAdvLabels, resp.LabelSets)

			// Make sure we don't download files we did not expect to.
			// Regression test: https://github.com/thanos-io/thanos/issues/1664

			// Sort records. We load blocks concurrently so operations might be not ordered.
			sort.Strings(rec.getRangeTouched)

			// With binary header nothing should be downloaded fully.
			assert.Equal(t, []string(nil), rec.getTouched)
			if reuseDisk != "" {
				assert.Equal(t, expectedTouchedBlockOps(all, sc.expectedIDs, cached), rec.getRangeTouched)
				cached = sc.expectedIDs
				return
			}

			assert.Equal(t, expectedTouchedBlockOps(all, sc.expectedIDs, nil), rec.getRangeTouched)
		})
	}
}

func expectedTouchedBlockOps(all, expected, cached []ulid.ULID) []string {
	var ops []string
	for _, id := range all {
		blockCached := false
		for _, fid := range cached {
			if id.Compare(fid) == 0 {
				blockCached = true
				break
			}
		}
		if blockCached {
			continue
		}

		found := false
		for _, fid := range expected {
			if id.Compare(fid) == 0 {
				found = true
				break
			}
		}

		if found {
			ops = append(ops,
				// To create binary header we touch part of index few times.
				path.Join(id.String(), block.IndexFilename), // Version.
				path.Join(id.String(), block.IndexFilename), // TOC.
				path.Join(id.String(), block.IndexFilename), // Symbols.
				path.Join(id.String(), block.IndexFilename), // PostingOffsets.
			)
		}
	}
	sort.Strings(ops)
	return ops
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
		block:        b,
		stats:        &queryStats{},
		loadedSeries: map[uint64][]byte{},
	}

	// Success with no refetches.
	assert.NoError(t, r.loadSeries(context.TODO(), []uint64{2, 13, 24}, false, 2, 100))
	assert.Equal(t, map[uint64][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, r.loadedSeries)
	assert.Equal(t, float64(0), promtest.ToFloat64(s.seriesRefetches))

	// Success with 2 refetches.
	r.loadedSeries = map[uint64][]byte{}
	assert.NoError(t, r.loadSeries(context.TODO(), []uint64{2, 13, 24}, false, 2, 15))
	assert.Equal(t, map[uint64][]byte{
		2:  []byte("aaaaaaaaaa"),
		13: []byte("bbbbbbbbbb"),
		24: []byte("cccccccccc"),
	}, r.loadedSeries)
	assert.Equal(t, float64(2), promtest.ToFloat64(s.seriesRefetches))

	// Success with refetch on first element.
	r.loadedSeries = map[uint64][]byte{}
	assert.NoError(t, r.loadSeries(context.TODO(), []uint64{2}, false, 2, 5))
	assert.Equal(t, map[uint64][]byte{
		2: []byte("aaaaaaaaaa"),
	}, r.loadedSeries)
	assert.Equal(t, float64(3), promtest.ToFloat64(s.seriesRefetches))

	buf.Reset()
	buf.PutByte(0)
	buf.PutByte(0)
	buf.PutUvarint(10)
	buf.PutString("aaaaaaa")
	assert.NoError(t, bkt.Upload(context.Background(), filepath.Join(b.meta.ULID.String(), block.IndexFilename), bytes.NewReader(buf.Get())))

	// Fail, but no recursion at least.
	assert.Error(t, r.loadSeries(context.TODO(), []uint64{2, 13, 24}, false, 1, 15))
}

func TestBucketIndexReader_ExpandedPostings(t *testing.T) {
	tb := test.NewTB(t)

	tmpDir, err := ioutil.TempDir("", "test-expanded-postings")
	assert.NoError(tb, err)
	defer func() { assert.NoError(tb, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(tb, err)
	defer func() { assert.NoError(tb, bkt.Close()) }()

	id := uploadTestBlock(tb, tmpDir, bkt, 500)

	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, mimir_tsdb.DefaultPostingOffsetInMemorySampling)
	assert.NoError(tb, err)

	benchmarkExpandedPostings(tb, bkt, id, r, 500)
}

func BenchmarkBucketIndexReader_ExpandedPostings(b *testing.B) {
	tb := test.NewTB(b)

	tmpDir, err := ioutil.TempDir("", "bench-expanded-postings")
	assert.NoError(tb, err)
	defer func() { assert.NoError(tb, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(tb, err)
	defer func() { assert.NoError(tb, bkt.Close()) }()

	id := uploadTestBlock(tb, tmpDir, bkt, 50e5)
	r, err := indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, id, mimir_tsdb.DefaultPostingOffsetInMemorySampling)
	assert.NoError(tb, err)

	benchmarkExpandedPostings(tb, bkt, id, r, 50e5)
}

func uploadTestBlock(t testing.TB, tmpDir string, bkt objstore.Bucket, series int) ulid.ULID {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = tmpDir
	headOpts.ChunkRange = 1000
	h, err := tsdb.NewHead(nil, nil, nil, headOpts, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, h.Close())
	}()

	logger := log.NewNopLogger()

	appendTestData(t, h.Appender(context.Background()), series)

	assert.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "tmp"), os.ModePerm))
	id := createBlockFromHead(t, filepath.Join(tmpDir, "tmp"), h)

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, "tmp", id.String()), metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, nil)
	assert.NoError(t, err)
	assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, "tmp", id.String()), metadata.NoneFunc))
	assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, "tmp", id.String()), metadata.NoneFunc))

	return id
}

func appendTestData(t testing.TB, app storage.Appender, series int) {
	addSeries := func(l labels.Labels) {
		_, err := app.Append(0, l, 0, 0)
		assert.NoError(t, err)
	}

	series = series / 5
	for n := 0; n < 10; n++ {
		for i := 0; i < series/10; i++ {
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", strconv.Itoa(n)+labelLongSuffix, "j", "foo"))
			// Have some series that won't be matched, to properly test inverted matches.
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", strconv.Itoa(n)+labelLongSuffix, "j", "bar"))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "0_"+strconv.Itoa(n)+labelLongSuffix, "j", "bar"))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "1_"+strconv.Itoa(n)+labelLongSuffix, "j", "bar"))
			addSeries(labels.FromStrings("i", strconv.Itoa(i)+labelLongSuffix, "n", "2_"+strconv.Itoa(n)+labelLongSuffix, "j", "foo"))
		}
	}
	assert.NoError(t, app.Commit())
}

func createBlockFromHead(t testing.TB, dir string, head *tsdb.Head) ulid.ULID {
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, log.NewNopLogger(), []int64{1000000}, nil, nil)
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
	bkt objstore.BucketReader,
	id ulid.ULID,
	r indexheader.Reader,
	series int,
) {
	n1 := labels.MustNewMatcher(labels.MatchEqual, "n", "1"+labelLongSuffix)

	jFoo := labels.MustNewMatcher(labels.MatchEqual, "j", "foo")
	jNotFoo := labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")

	iStar := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.*$")
	iPlus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")
	i1Plus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^1.+$")
	iEmptyRe := labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")
	iNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "i", "")
	iNot2 := labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+labelLongSuffix)
	iNot2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")
	iRegexSet := labels.MustNewMatcher(labels.MatchRegexp, "i", "0"+labelLongSuffix+"|1"+labelLongSuffix+"|2"+labelLongSuffix)

	series = series / 5
	cases := []struct {
		name     string
		matchers []*labels.Matcher

		expectedLen int
	}{
		{`n="1"`, []*labels.Matcher{n1}, int(float64(series) * 0.2)},
		{`n="1",j="foo"`, []*labels.Matcher{n1, jFoo}, int(float64(series) * 0.1)},
		{`j="foo",n="1"`, []*labels.Matcher{jFoo, n1}, int(float64(series) * 0.1)},
		{`n="1",j!="foo"`, []*labels.Matcher{n1, jNotFoo}, int(float64(series) * 0.1)},
		{`i=~".*"`, []*labels.Matcher{iStar}, 5 * series},
		{`i=~".+"`, []*labels.Matcher{iPlus}, 5 * series},
		{`i=~""`, []*labels.Matcher{iEmptyRe}, 0},
		{`i!=""`, []*labels.Matcher{iNotEmpty}, 5 * series},
		{`n="1",i=~".*",j="foo"`, []*labels.Matcher{n1, iStar, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".*",i!="2",j="foo"`, []*labels.Matcher{n1, iStar, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i!=""`, []*labels.Matcher{n1, iNotEmpty}, int(float64(series) * 0.2)},
		{`n="1",i!="",j="foo"`, []*labels.Matcher{n1, iNotEmpty, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",j="foo"`, []*labels.Matcher{n1, iPlus, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~"1.+",j="foo"`, []*labels.Matcher{n1, i1Plus, jFoo}, int(float64(series) * 0.011111)},
		{`n="1",i=~".+",i!="2",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",i!~"2.*",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2Star, jFoo}, int(1 + float64(series)*0.088888)},
		{`i=~"0|1|2"`, []*labels.Matcher{iRegexSet}, 150}, // 50 series for "1", 50 for "2" and 50 for "3".
	}

	for _, c := range cases {
		t.Run(c.name, func(t test.TB) {
			b := &bucketBlock{
				logger:            log.NewNopLogger(),
				metrics:           NewBucketStoreMetrics(nil),
				indexHeaderReader: r,
				indexCache:        noopCache{},
				bkt:               bkt,
				meta:              &metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}},
				partitioner:       newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
			}

			indexr := newBucketIndexReader(context.Background(), b)

			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				p, err := indexr.ExpandedPostings(c.matchers)
				assert.NoError(t, err)
				assert.Equal(t, c.expectedLen, len(p))
			}
		})
	}
}

func TestBucketSeries(t *testing.T) {
	tb := test.NewTB(t)
	runSeriesInterestingCases(tb, 10000, 10000, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, false, samplesPerSeries, series, 1)
	})
}

func TestBucketSkipChunksSeries(t *testing.T) {
	tb := test.NewTB(t)
	runSeriesInterestingCases(tb, 10000, 10000, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, true, samplesPerSeries, series, 1)
	})
}

func BenchmarkBucketSeries(b *testing.B) {
	tb := test.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	runSeriesInterestingCases(tb, 10e6, 10e5, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, false, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func BenchmarkBucketSkipChunksSeries(b *testing.B) {
	tb := test.NewTB(b)
	// 10e6 samples = ~1736 days with 15s scrape
	runSeriesInterestingCases(tb, 10e6, 10e5, func(t test.TB, samplesPerSeries, series int) {
		benchBucketSeries(t, true, samplesPerSeries, series, 1/100e6, 1/10e4, 1)
	})
}

func benchBucketSeries(t test.TB, skipChunk bool, samplesPerSeries, totalSeries int, requestedRatios ...float64) {
	const numOfBlocks = 4

	tmpDir, err := ioutil.TempDir("", "testorbench-bucketseries")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	var (
		logger = log.NewNopLogger()
		series []*storepb.Series
		random = rand.New(rand.NewSource(120))
	)

	extLset := labels.Labels{{Name: "ext1", Value: "1"}}
	thanosMeta := metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
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
	// Timestamp will be counted for each new series and new sample, so each each series will have unique timestamp.
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

		meta, err := metadata.InjectThanos(logger, filepath.Join(blockDir, id.String()), thanosMeta, nil)
		assert.NoError(t, err)

		assert.NoError(t, meta.WriteToDir(logger, filepath.Join(blockDir, id.String())))
		assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), metadata.NoneFunc))
	}

	ibkt := objstore.WithNoopInstr(bkt)
	f, err := block.NewRawMetaFetcher(logger, ibkt)
	assert.NoError(t, err)

	chunkPool, err := pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 1e9) // 1GB.
	assert.NoError(t, err)

	st, err := NewBucketStore(
		ibkt,
		f,
		tmpDir,
		NewChunksLimiterFactory(0),
		NewSeriesLimiterFactory(0),
		newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		1,
		false,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		false,
		false,
		0,
		NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
		WithChunkPool(chunkPool),
	)
	assert.NoError(t, err)

	if !t.IsBenchmark() {
		st.chunkPool = &mockedPool{parent: st.chunkPool}
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
			// This does not cut chunks properly, but those are assured against for non benchmarks only, where we use 100% case only.
			ExpectedSeries: series[:seriesCut],
		})
	}
	runTestServerSeries(t, st, bCases...)

	if !t.IsBenchmark() {
		if !skipChunk {
			// TODO(bwplotka): This is wrong negative for large number of samples (1mln). Investigate.
			assert.Equal(t, 0, int(st.chunkPool.(*mockedPool).balance.Load()))
			st.chunkPool.(*mockedPool).gets.Store(0)
		}

		for _, b := range st.blocks {
			// NOTE(bwplotka): It is 4 x 1.0 for 100mln samples. Kind of make sense: long series.
			assert.Equal(t, 0.0, promtest.ToFloat64(b.metrics.seriesRefetches))
		}
	}
}

var _ = fakePool{}

type fakePool struct{}

func (m fakePool) Get(sz int) (*[]byte, error) {
	b := make([]byte, 0, sz)
	return &b, nil
}

func (m fakePool) Put(_ *[]byte) {}

type mockedPool struct {
	parent  pool.Bytes
	balance atomic.Uint64
	gets    atomic.Uint64
}

func (m *mockedPool) Get(sz int) (*[]byte, error) {
	b, err := m.parent.Get(sz)
	if err != nil {
		return nil, err
	}
	m.balance.Add(uint64(cap(*b)))
	m.gets.Add(uint64(1))
	return b, nil
}

func (m *mockedPool) Put(b *[]byte) {
	m.balance.Sub(uint64(cap(*b)))
	m.parent.Put(b)
}

// Regression test against: https://github.com/thanos-io/thanos/issues/2147.
func TestBucketSeries_OneBlock_InMemIndexCacheSegfault(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "segfault-series")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	logger := log.NewNopLogger()
	thanosMeta := metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	chunkPool, err := pool.NewBucketedBytes(chunkBytesPoolMinSize, chunkBytesPoolMaxSize, 2, 100e7)
	assert.NoError(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{
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
		h, err := tsdb.NewHead(nil, nil, nil, headOpts, nil)
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
		assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), metadata.NoneFunc))

		b1 = &bucketBlock{
			indexCache:  indexCache,
			logger:      logger,
			metrics:     NewBucketStoreMetrics(nil),
			bkt:         bkt,
			meta:        meta,
			partitioner: newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
			chunkObjs:   []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:   chunkPool,
		}
		b1.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b1.meta.ULID, mimir_tsdb.DefaultPostingOffsetInMemorySampling)
		assert.NoError(t, err)
	}

	var b2 *bucketBlock
	{
		// Block 2, do not load this block yet.
		h, err := tsdb.NewHead(nil, nil, nil, headOpts, nil)
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
		assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(blockDir, id.String()), metadata.NoneFunc))

		b2 = &bucketBlock{
			indexCache:  indexCache,
			logger:      logger,
			metrics:     NewBucketStoreMetrics(nil),
			bkt:         bkt,
			meta:        meta,
			partitioner: newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
			chunkObjs:   []string{filepath.Join(id.String(), "chunks", "000001")},
			chunkPool:   chunkPool,
		}
		b2.indexHeaderReader, err = indexheader.NewBinaryReader(context.Background(), log.NewNopLogger(), bkt, tmpDir, b2.meta.ULID, mimir_tsdb.DefaultPostingOffsetInMemorySampling)
		assert.NoError(t, err)
	}

	store := &BucketStore{
		bkt:             objstore.WithNoopInstr(bkt),
		logger:          logger,
		indexCache:      indexCache,
		indexReaderPool: indexheader.NewReaderPool(log.NewNopLogger(), false, 0, nil),
		metrics:         NewBucketStoreMetrics(nil),
		blockSets: map[uint64]*bucketBlockSet{
			labels.Labels{{Name: "ext1", Value: "1"}}.Hash(): {blocks: [][]*bucketBlock{{b1, b2}}},
		},
		blocks: map[ulid.ULID]*bucketBlock{
			b1.meta.ULID: b1,
			b2.meta.ULID: b2,
		},
		queryGate:            gate.NewNoop(),
		chunksLimiterFactory: NewChunksLimiterFactory(0),
		seriesLimiterFactory: NewSeriesLimiterFactory(0),
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

func TestSeries_RequestAndResponseHints(t *testing.T) {
	tb, store, seriesSet1, seriesSet2, block1, block2, close := setupStoreForHintsTest(t)
	defer close()

	testCases := []*seriesCase{
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

	runTestServerSeries(tb, store, testCases...)
}

func TestSeries_ErrorUnmarshallingRequestHints(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-series-hints-enabled")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(tmpDir)) }()

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, bkt.Close()) }()

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
	)

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	assert.NoError(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	assert.NoError(t, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		false,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, store.Close()) }()

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

func TestSeries_BlockWithMultipleChunks(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "test-block-with-multiple-chunks")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(tmpDir)) }()

	// Create a block with 1 series but an high number of samples,
	// so that they will span across multiple chunks.
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(tmpDir, "block")
	headOpts.ChunkRange = 10000000000

	h, err := tsdb.NewHead(nil, nil, nil, headOpts, nil)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, h.Close()) }()

	series := labels.FromStrings("__name__", "test")
	for ts := int64(0); ts < 10000; ts++ {
		// Appending a single sample is very unoptimised, but guarantees each chunk is always MaxSamplesPerChunk
		// (except the last one, which could be smaller).
		app := h.Appender(context.Background())
		_, err := app.Append(0, series, ts, float64(ts))
		assert.NoError(t, err)
		assert.NoError(t, app.Commit())
	}

	blk := createBlockFromHead(t, headOpts.ChunkDirRoot, h)

	thanosMeta := metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
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
	assert.NoError(t, block.Upload(context.Background(), logger, bkt, filepath.Join(headOpts.ChunkDirRoot, blk.String()), metadata.NoneFunc))

	// Instance a real bucket store we'll use to query the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	assert.NoError(t, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	assert.NoError(t, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(100000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		false,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		NewSeriesHashCache(1024*1024),
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
				decodedChunk, err := chunkenc.FromData(chunkenc.EncXOR, rawChunk.Raw.Data)
				assert.NoError(t, err)

				numSamples += decodedChunk.NumSamples()
			}

			assert.True(t, testData.expectedSamples == numSamples, "expected: %d, actual: %d", testData.expectedSamples, numSamples)
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

func TestBigEndianPostingsCount(t *testing.T) {
	const count = 1000
	raw := make([]byte, count*4)

	for ix := 0; ix < count; ix++ {
		binary.BigEndian.PutUint32(raw[4*ix:], rand.Uint32())
	}

	p := newBigEndianPostings(raw)
	assert.Equal(t, count, p.length())

	c := 0
	for p.Next() {
		c++
	}
	assert.Equal(t, count, c)
}

func createBlockWithOneSeriesWithStep(t test.TB, dir string, lbls labels.Labels, blockIndex, totalSamples int, random *rand.Rand, step int64) ulid.ULID {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = dir
	headOpts.ChunkRange = int64(totalSamples) * step
	h, err := tsdb.NewHead(nil, nil, nil, headOpts, nil)
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

func setupStoreForHintsTest(t *testing.T) (test.TB, *BucketStore, []*storepb.Series, []*storepb.Series, ulid.ULID, ulid.ULID, func()) {
	tb := test.NewTB(t)

	closers := []func(){}

	tmpDir, err := ioutil.TempDir("", "test-hints")
	assert.NoError(t, err)
	closers = append(closers, func() { assert.NoError(t, os.RemoveAll(tmpDir)) })

	bktDir := filepath.Join(tmpDir, "bkt")
	bkt, err := filesystem.NewBucket(bktDir)
	assert.NoError(t, err)
	closers = append(closers, func() { assert.NoError(t, bkt.Close()) })

	var (
		logger   = log.NewNopLogger()
		instrBkt = objstore.WithNoopInstr(bkt)
		random   = rand.New(rand.NewSource(120))
	)

	extLset := labels.Labels{{Name: "ext1", Value: "1"}}
	// Inject the Thanos meta to each block in the storage.
	thanosMeta := metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	// Create TSDB blocks.
	head, seriesSet1 := createHeadWithSeries(t, 0, headGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "0"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    extLset,
		Random:           random,
	})
	block1 := createBlockFromHead(t, bktDir, head)
	assert.NoError(t, head.Close())
	head2, seriesSet2 := createHeadWithSeries(t, 1, headGenOptions{
		TSDBDir:          filepath.Join(tmpDir, "1"),
		SamplesPerSeries: 1,
		Series:           2,
		PrependLabels:    extLset,
		Random:           random,
	})
	block2 := createBlockFromHead(t, bktDir, head2)
	assert.NoError(t, head2.Close())

	for _, blockID := range []ulid.ULID{block1, block2} {
		_, err := metadata.InjectThanos(logger, filepath.Join(bktDir, blockID.String()), thanosMeta, nil)
		assert.NoError(t, err)
	}

	// Instance a real bucket store we'll use to query back the series.
	fetcher, err := block.NewMetaFetcher(logger, 10, instrBkt, tmpDir, nil, nil, nil)
	assert.NoError(tb, err)

	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.InMemoryIndexCacheConfig{})
	assert.NoError(tb, err)

	store, err := NewBucketStore(
		instrBkt,
		fetcher,
		tmpDir,
		NewChunksLimiterFactory(10000/MaxSamplesPerChunk),
		NewSeriesLimiterFactory(0),
		newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		10,
		false,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		true,
		false,
		0,
		NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(nil),
		WithLogger(logger),
		WithIndexCache(indexCache),
	)
	assert.NoError(tb, err)
	assert.NoError(tb, store.SyncBlocks(context.Background()))

	closers = append(closers, func() { assert.NoError(t, store.Close()) })

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
		}, {
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

	sort.Strings(labels)
	return labels
}

func BenchmarkBucketBlock_readChunkRange(b *testing.B) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()

		// Read chunks of different length. We're not using random to make the benchmark repeatable.
		readLengths = []int64{300, 500, 1000, 5000, 10000, 30000, 50000, 100000, 300000, 1500000}
	)

	tmpDir, err := ioutil.TempDir("", "benchmark")
	assert.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, os.RemoveAll(tmpDir))
	})

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	assert.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, bkt.Close())
	})

	// Create a block.
	blockID := createBlockWithOneSeriesWithStep(test.NewTB(b), tmpDir, labels.FromStrings("__name__", "test"), 0, 100000, rand.New(rand.NewSource(0)), 5000)

	// Upload the block to the bucket.
	thanosMeta := metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	blockMeta, err := metadata.InjectThanos(logger, filepath.Join(tmpDir, blockID.String()), thanosMeta, nil)
	assert.NoError(b, err)

	assert.NoError(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	// Create a chunk pool with buckets between 8B and 32KB.
	chunkPool, err := pool.NewBucketedBytes(8, 32*1024, 2, 1e10)
	assert.NoError(b, err)

	// Create a bucket block with only the dependencies we need for the benchmark.
	blk, err := newBucketBlock(context.Background(), logger, NewBucketStoreMetrics(nil), blockMeta, bkt, tmpDir, nil, chunkPool, nil, nil)
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
	blk, blockMeta := prepareBucket(b, compact.ResolutionLevelRaw)

	aggrs := []storepb.Aggr{storepb.Aggr_RAW}
	for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
		for _, queryShardingEnabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("concurrency: %d, query sharding enabled: %v", concurrency, queryShardingEnabled), func(b *testing.B) {
				benchmarkBlockSeriesWithConcurrency(b, concurrency, blockMeta, blk, aggrs, queryShardingEnabled)
			})
		}
	}
}

func prepareBucket(b *testing.B, resolutionLevel compact.ResolutionLevel) (*bucketBlock, *metadata.Meta) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	tmpDir, err := ioutil.TempDir("", "benchmark")
	assert.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, os.RemoveAll(tmpDir))
	})

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
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}

	blockMeta, err := metadata.InjectThanos(logger, filepath.Join(tmpDir, blockID.String()), thanosMeta, nil)
	assert.NoError(b, err)

	assert.NoError(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	if resolutionLevel > 0 {
		// Downsample newly-created block.
		blockID, err = downsample.Downsample(logger, blockMeta, head, tmpDir, int64(resolutionLevel))
		assert.NoError(b, err)
		blockMeta, err = metadata.ReadFromDir(filepath.Join(tmpDir, blockID.String()))
		assert.NoError(b, err)

		assert.NoError(b, block.Upload(context.Background(), logger, bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))
	}
	assert.NoError(b, head.Close())

	// Create chunk pool and partitioner using the same production settings.
	chunkPool, err := NewDefaultChunkBytesPool(64 * 1024 * 1024 * 1024)
	assert.NoError(b, err)

	partitioner := newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil)

	// Create an index header reader.
	indexHeaderReader, err := indexheader.NewBinaryReader(ctx, logger, bkt, tmpDir, blockMeta.ULID, mimir_tsdb.DefaultPostingOffsetInMemorySampling)
	assert.NoError(b, err)
	indexCache, err := storecache.NewInMemoryIndexCacheWithConfig(logger, nil, storecache.DefaultInMemoryIndexCacheConfig)
	assert.NoError(b, err)

	// Create a bucket block with only the dependencies we need for the benchmark.
	blk, err := newBucketBlock(context.Background(), logger, NewBucketStoreMetrics(nil), blockMeta, bkt, tmpDir, indexCache, chunkPool, indexHeaderReader, partitioner)
	assert.NoError(b, err)
	return blk, blockMeta
}

func benchmarkBlockSeriesWithConcurrency(b *testing.B, concurrency int, blockMeta *metadata.Meta, blk *bucketBlock, aggrs []storepb.Aggr, queryShardingEnabled bool) {
	ctx := context.Background()

	// Run the same number of queries per goroutine.
	queriesPerWorker := b.N / concurrency

	// No limits.
	chunksLimiter := NewChunksLimiterFactory(0)(nil)
	seriesLimiter := NewSeriesLimiterFactory(0)(nil)

	// Create the series hash cached used when query sharding is enabled.
	seriesHashCache := NewSeriesHashCache(1024 * 1024 * 1024).GetBlockCache(blockMeta.ULID.String())

	// Run multiple workers to execute the queries.
	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		go func() {
			defer wg.Done()

			for n := 0; n < queriesPerWorker; n++ {
				var reqMatchers []storepb.LabelMatcher
				var shardSelector *querysharding.ShardSelector

				if queryShardingEnabled {
					// Each query touches the same series but a different shard.
					reqMatchers = []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "i", Value: ".+"},
					}

					shardSelector = &querysharding.ShardSelector{
						ShardIndex: n % 20,
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
					Aggregates: aggrs,
				}

				matchers, err := storepb.MatchersToPromMatchers(req.Matchers...)
				// TODO FIXME! require.NoError calls b.Fatalf under the hood, which
				// must be called only from the goroutine running the Benchmark function.
				require.NoError(b, err)

				indexReader := blk.indexReader(ctx)
				chunkReader := blk.chunkReader(ctx)

				seriesSet, _, err := blockSeries(nil, indexReader, chunkReader, matchers, shardSelector, seriesHashCache, chunksLimiter, seriesLimiter, req.SkipChunks, req.MinTime, req.MaxTime, req.Aggregates)
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

func BenchmarkDownsampledBlockSeries(b *testing.B) {
	blk, blockMeta := prepareBucket(b, compact.ResolutionLevel5m)
	aggrs := []storepb.Aggr{}
	for i := 1; i < int(storepb.Aggr_COUNTER); i++ {
		aggrs = append(aggrs, storepb.Aggr(i))
		for _, concurrency := range []int{1, 2, 4, 8, 16, 32} {
			b.Run(fmt.Sprintf("aggregates: %v, concurrency: %d", aggrs, concurrency), func(b *testing.B) {
				benchmarkBlockSeriesWithConcurrency(b, concurrency, blockMeta, blk, aggrs, false)
			})
		}
	}
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
// Returned series list has "ext1"="1" prepended. Each series looks as follows:
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

	var w *wal.WAL
	var err error
	if opts.WithWAL {
		w, err = wal.New(nil, nil, filepath.Join(opts.TSDBDir, "wal"), true)
		assert.NoError(t, err)
	} else {
		assert.NoError(t, os.MkdirAll(filepath.Join(opts.TSDBDir, "wal"), os.ModePerm))
	}

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = opts.TSDBDir
	h, err := tsdb.NewHead(nil, nil, w, headOpts, nil)
	assert.NoError(t, err)

	app := h.Appender(context.Background())
	for i := 0; i < opts.Series; i++ {
		tsLabel := j*opts.Series*opts.SamplesPerSeries + i*opts.SamplesPerSeries
		ref, err := app.Append(
			0,
			labels.FromStrings("foo", "bar", "i", fmt.Sprintf("%07d%s", tsLabel, labelLongSuffix)),
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
		lset       labels.Labels
		chunkMetas []chunks.Meta
		expected   = make([]*storepb.Series, 0, opts.Series)
	)

	all := allPostings(t, ir)
	for all.Next() {
		assert.NoError(t, ir.Series(all.At(), &lset, &chunkMetas))
		expected = append(expected, &storepb.Series{Labels: labelpb.ZLabelsFromPromLabels(append(opts.PrependLabels.Copy(), lset...))})

		if opts.SkipChunks {
			continue
		}

		for _, c := range chunkMetas {
			chEnc, err := chks.Chunk(c.Ref)
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
func runTestServerSeries(t test.TB, store storepb.StoreServer, cases ...*seriesCase) {
	for _, c := range cases {
		t.Run(c.Name, func(t test.TB) {
			t.ResetTimer()
			for i := 0; i < t.N(); i++ {
				srv := newBucketStoreSeriesServer(context.Background())
				assert.NoError(t, store.Series(c.Req, srv))
				assert.Equal(t, len(c.ExpectedWarnings), len(srv.Warnings), "%v", srv.Warnings)
				assert.Equal(t, len(c.ExpectedSeries), len(srv.SeriesSet))

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
		inputPostings    []uint64
		shard            *querysharding.ShardSelector
		cacheEntries     [][2]uint64 // List of cache entries where each entry is the pair [seriesID, hash]
		expectedPostings []uint64
	}{
		"should be a noop if the cache is empty": {
			inputPostings:    []uint64{0, 1, 2, 3, 4, 5},
			shard:            &querysharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{},
			expectedPostings: []uint64{0, 1, 2, 3, 4, 5},
		},
		"should filter postings at the beginning of the slice": {
			inputPostings:    []uint64{0, 1, 2, 3, 4, 5},
			shard:            &querysharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			cacheEntries:     [][2]uint64{{0, 0}, {1, 1}},
			expectedPostings: []uint64{1, 2, 3, 4, 5},
		},
		"should filter postings in the middle of the slice": {
			inputPostings:    []uint64{0, 1, 2, 3, 4, 5},
			shard:            &querysharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{{0, 0}, {1, 1}},
			expectedPostings: []uint64{0, 2, 3, 4, 5},
		},
		"should filter postings at the end of the slice": {
			inputPostings:    []uint64{0, 1, 2, 3, 4, 5},
			shard:            &querysharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{{4, 4}, {5, 5}},
			expectedPostings: []uint64{0, 1, 2, 3, 4},
		},
		"should filter postings when all postings are in the cache": {
			inputPostings:    []uint64{0, 1, 2, 3, 4, 5},
			shard:            &querysharding.ShardSelector{ShardIndex: 0, ShardCount: 2},
			cacheEntries:     [][2]uint64{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
			expectedPostings: []uint64{0, 2, 4},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cache := NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
			for _, pair := range testData.cacheEntries {
				cache.Store(pair[0], pair[1])
			}

			actualPostings, _ := filterPostingsByCachedShardHash(testData.inputPostings, testData.shard, cache)
			assert.Equal(t, testData.expectedPostings, actualPostings)
		})
	}
}

func TestFilterPostingsByCachedShardHash_NoAllocations(t *testing.T) {
	inputPostings := []uint64{0, 1, 2, 3, 4, 5}
	shard := &querysharding.ShardSelector{ShardIndex: 0, ShardCount: 2}
	cacheEntries := [][2]uint64{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}

	cache := NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	for _, pair := range cacheEntries {
		cache.Store(pair[0], pair[1])
	}

	assert.Equal(t, float64(0), testing.AllocsPerRun(1, func() {
		filterPostingsByCachedShardHash(inputPostings, shard, cache)
	}))
}

func BenchmarkFilterPostingsByCachedShardHash_AllPostingsShifted(b *testing.B) {
	// This benchmark tests the case only the 1st posting is removed
	// and so all subsequent postings will be shifted.
	cache := NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	cache.Store(0, 0)
	shard := &querysharding.ShardSelector{ShardIndex: 1, ShardCount: 2}

	// Create a long list of postings.
	const numPostings = 10000
	originalPostings := make([]uint64, numPostings)
	for i := 0; i < numPostings; i++ {
		originalPostings[i] = uint64(i)
	}

	inputPostings := make([]uint64, numPostings)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Copy the original postings into the input ones, since they will be overwritten.
		inputPostings = inputPostings[0:numPostings]
		copy(inputPostings, originalPostings)

		filterPostingsByCachedShardHash(inputPostings, shard, cache)
	}
}

func BenchmarkFilterPostingsByCachedShardHash_NoPostingsShifted(b *testing.B) {
	// This benchmark tests the case the output postings is equal to the input one.
	cache := NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	shard := &querysharding.ShardSelector{ShardIndex: 1, ShardCount: 2}

	// Create a long list of postings.
	const numPostings = 10000
	ps := make([]uint64, numPostings)
	for i := 0; i < numPostings; i++ {
		ps[i] = uint64(i)
	}

	for n := 0; n < b.N; n++ {
		// We reuse the same postings slice because we expect this test to not
		// modify it (cache is empty).
		filterPostingsByCachedShardHash(ps, shard, cache)
	}
}
