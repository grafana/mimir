package storegateway

import (
	"context"
	"path"
	"sync"
	"testing"

	"github.com/go-kit/log"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

func TestLoad_IgnoreNativeHistogramChunks(t *testing.T) {
	testCases := []struct {
		name                 string
		chunks               []chunks.Meta
		expectedNumNilChunks int
	}{
		{
			name: "some chunks are histogram chunks",
			chunks: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(10, 10, nil, nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(20, 20, nil, nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(30, 30, nil, nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(40, 0, tsdb.GenerateTestHistogram(1), nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(50, 50, nil, nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(60, 0, tsdb.GenerateTestHistogram(2), nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(70, 0, tsdb.GenerateTestHistogram(3), nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(80, 80, nil, nil),
				}),
			},
			expectedNumNilChunks: 3,
		},
		{
			name: "all chunks are histogram chunks",
			chunks: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(10, 0, tsdb.GenerateTestHistogram(1), nil),
				}),
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					newSample(20, 0, tsdb.GenerateTestHistogram(2), nil),
				}),
			},
			expectedNumNilChunks: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := t.TempDir()

			bkt, err := filesystem.NewBucket(d)
			require.NoError(t, err)
			defer func() { require.NoError(t, bkt.Close()) }()

			spec := testutil.BlockSeriesSpec{
				Labels: labels.FromStrings("foo", "bar"),
				Chunks: tc.chunks,
			}

			meta, err := testutil.GenerateBlockFromSpec("userID", d, []*testutil.BlockSeriesSpec{&spec})
			require.NoError(t, err)

			bb, err := newBucketBlock(
				context.Background(),
				"test",
				log.NewNopLogger(),
				NewBucketStoreMetrics(nil),
				meta,
				bkt,
				path.Join(d, meta.ULID.String()),
				nil,
				nil,
				nil,
				newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
				func() bool { return true },
			)
			require.NoError(t, err)

			bktChkReader := bb.chunkReader(context.Background())
			defer func() { require.NoError(t, bktChkReader.Close()) }()

			res := []seriesEntry{
				{
					lset: spec.Labels,
					refs: []chunks.ChunkRef{},
					chks: make([]storepb.AggrChunk, len(spec.Chunks)),
				},
			}
			for i, chkMeta := range spec.Chunks {
				require.NoError(t, bktChkReader.addLoad(chkMeta.Ref, 0, i))
				res[0].refs = append(res[0].refs, chkMeta.Ref)
			}

			chksPool := pool.NewSafeSlabPool[byte](pool.Interface(&sync.Pool{
				New: nil,
			}), 16384)

			require.NoError(t, bktChkReader.load(res, chksPool, newSafeQueryStats()))

			numNilChunks := 0
			for _, chk := range res[0].chks {
				if chk.Raw == nil {
					numNilChunks++
				}
			}
			require.Equal(t, tc.expectedNumNilChunks, numNilChunks)
		})
	}
}

func newSample(t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) tsdbutil.Sample {
	return sample{t, v, h, fh}
}
