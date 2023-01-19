// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/iterators/chunk_merge_iterator_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package iterators

import (
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

var (
	generateTestHistogram      = e2e.GenerateTestHistogram
	generateTestFloatHistogram = e2e.GenerateTestFloatHistogram
)

func TestChunkMergeIterator(t *testing.T) {
	for i, tc := range []struct {
		chunks     []chunk.Chunk
		mint, maxt int64
		enc        chunkenc.ValueType
	}{
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 100,
			enc:  chunkenc.ValFloat,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 100,
			enc:  chunkenc.ValFloat,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValFloat,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValFloat,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
			},
			maxt: 100,
			enc:  chunkenc.ValHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
			},
			maxt: 100,
			enc:  chunkenc.ValHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
			},
			maxt: 100,
			enc:  chunkenc.ValFloatHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
			},
			maxt: 100,
			enc:  chunkenc.ValFloatHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValFloatHistogram,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValFloatHistogram,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			iter := NewChunkMergeIterator(tc.chunks, 0, 0)
			for i := tc.mint; i < tc.maxt; i++ {
				require.Equal(t, tc.enc, iter.Next())
				switch tc.enc {
				case chunkenc.ValFloat:
					ts, s := iter.At()
					assert.Equal(t, i, ts)
					assert.Equal(t, float64(i), s)
					assert.NoError(t, iter.Err())
				case chunkenc.ValHistogram:
					ts, h := iter.AtHistogram()
					assert.Equal(t, i, ts)
					assert.Equal(t, *generateTestHistogram(int(i)), *h)
					assert.NoError(t, iter.Err())
				case chunkenc.ValFloatHistogram:
					ts, h := iter.AtFloatHistogram()
					assert.Equal(t, i, ts)
					assert.Equal(t, *generateTestFloatHistogram(int(i)), *h)
					assert.NoError(t, iter.Err())
				default:
					require.FailNowf(t, "Unexpected encoding", "%v", tc.enc)
				}
			}
			assert.Equal(t, chunkenc.ValNone, iter.Next())
		})
	}
}

func TestChunkMergeIteratorMixed(t *testing.T) {
	iter := NewChunkMergeIterator([]chunk.Chunk{
		mkChunk(t, 0, 75, 1*time.Millisecond, chunk.PrometheusXorChunk),
		mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
		mkChunk(t, 125, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
	}, 0, 0)

	for i := int64(0); i < 200; i++ {
		valType := iter.Next()
		switch valType {
		case chunkenc.ValFloat:
			ts, s := iter.At()
			assert.Equal(t, i, ts)
			assert.Equal(t, float64(i), s)
			assert.NoError(t, iter.Err())
		case chunkenc.ValHistogram:
			ts, h := iter.AtHistogram()
			assert.Equal(t, i, ts)
			assert.Equal(t, *generateTestHistogram(int(i)), *h)
			assert.NoError(t, iter.Err())
		case chunkenc.ValFloatHistogram:
			ts, h := iter.AtFloatHistogram()
			assert.Equal(t, i, ts)
			assert.Equal(t, *generateTestFloatHistogram(int(i)), *h)
			assert.NoError(t, iter.Err())
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", valType)
		}
	}
}

func TestChunkMergeIteratorSeek(t *testing.T) {
	for i, tc := range []struct {
		chunks     []chunk.Chunk
		mint, maxt int64
		enc        chunkenc.ValueType
	}{
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValFloat,
		},
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValHistogram,
		},
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
			},
			maxt: 200,
			enc:  chunkenc.ValFloatHistogram,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			for i := int64(tc.mint); i < tc.maxt; i += 20 {
				iter := NewChunkMergeIterator(tc.chunks, 0, 0)

				require.Equal(t, tc.enc, iter.Seek(i))
				switch tc.enc {
				case chunkenc.ValFloat:
					ts, s := iter.At()
					assert.Equal(t, i, ts)
					assert.Equal(t, float64(i), s)
					assert.NoError(t, iter.Err())
				case chunkenc.ValHistogram:
					ts, h := iter.AtHistogram()
					assert.Equal(t, i, ts)
					assert.Equal(t, *generateTestHistogram(int(i)), *h)
					assert.NoError(t, iter.Err())
				case chunkenc.ValFloatHistogram:
					ts, h := iter.AtFloatHistogram()
					assert.Equal(t, i, ts)
					assert.Equal(t, *generateTestFloatHistogram(int(i)), *h)
					assert.NoError(t, iter.Err())
				default:
					require.FailNowf(t, "Unexpected encoding", "%v", tc.enc)
				}

				for j := i + 1; j < tc.maxt; j++ {
					require.Equal(t, tc.enc, iter.Next())
					switch tc.enc {
					case chunkenc.ValFloat:
						ts, s := iter.At()
						assert.Equal(t, j, ts)
						assert.Equal(t, float64(j), s)
						assert.NoError(t, iter.Err())
					case chunkenc.ValHistogram:
						ts, h := iter.AtHistogram()
						assert.Equal(t, j, ts)
						assert.Equal(t, *generateTestHistogram(int(j)), *h)
						assert.NoError(t, iter.Err())
					case chunkenc.ValFloatHistogram:
						ts, h := iter.AtFloatHistogram()
						assert.Equal(t, j, ts)
						assert.Equal(t, *generateTestFloatHistogram(int(j)), *h)
						assert.NoError(t, iter.Err())
					default:
						require.FailNowf(t, "Unexpected encoding", "%v", tc.enc)
					}
				}
				assert.Equal(t, chunkenc.ValNone, iter.Next())
			}
		})
	}
}

func TestChunkMergeIteratorSeekMixed(t *testing.T) {
	for i, tc := range []struct {
		mint, maxt int64
		enc        chunkenc.ValueType
	}{
		{
			mint: 0,
			maxt: 50,
			enc:  chunkenc.ValFloat,
		},
		{
			mint: 50,
			maxt: 125,
			enc:  chunkenc.ValHistogram,
		},
		{
			mint: 125,
			maxt: 200,
			enc:  chunkenc.ValFloatHistogram,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			for i := int64(tc.mint); i < tc.maxt; i += 10 {
				iter := NewChunkMergeIterator([]chunk.Chunk{
					mkChunk(t, 0, 75, 1*time.Millisecond, chunk.PrometheusXorChunk),
					mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
					mkChunk(t, 125, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
				}, 0, 0)

				require.Equal(t, tc.enc, iter.Seek(i))
				switch tc.enc {
				case chunkenc.ValFloat:
					ts, s := iter.At()
					assert.Equal(t, i, ts)
					assert.Equal(t, float64(i), s)
					assert.NoError(t, iter.Err())
				case chunkenc.ValHistogram:
					ts, h := iter.AtHistogram()
					assert.Equal(t, i, ts)
					assert.Equal(t, *generateTestHistogram(int(i)), *h)
					assert.NoError(t, iter.Err())
				case chunkenc.ValFloatHistogram:
					ts, h := iter.AtFloatHistogram()
					assert.Equal(t, i, ts)
					assert.Equal(t, *generateTestFloatHistogram(int(i)), *h)
					assert.NoError(t, iter.Err())
				default:
					require.FailNowf(t, "Unexpected encoding", "%v", tc.enc)
				}

				for j := i + 1; j < tc.maxt; j++ {
					require.Equal(t, tc.enc, iter.Next())
					switch tc.enc {
					case chunkenc.ValFloat:
						ts, s := iter.At()
						assert.Equal(t, j, ts)
						assert.Equal(t, float64(j), s)
						assert.NoError(t, iter.Err())
					case chunkenc.ValHistogram:
						ts, h := iter.AtHistogram()
						assert.Equal(t, j, ts)
						assert.Equal(t, *generateTestHistogram(int(j)), *h)
						assert.NoError(t, iter.Err())
					case chunkenc.ValFloatHistogram:
						ts, h := iter.AtFloatHistogram()
						assert.Equal(t, j, ts)
						assert.Equal(t, *generateTestFloatHistogram(int(j)), *h)
						assert.NoError(t, iter.Err())
					default:
						require.FailNowf(t, "Unexpected encoding", "%v", tc.enc)
					}
				}
			}
		})
	}
}

func mkChunk(t require.TestingT, mint, maxt model.Time, step time.Duration, encoding chunk.Encoding) chunk.Chunk {
	metric := labels.FromStrings(model.MetricNameLabel, "foo")
	pc, err := chunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		var npc chunk.EncodedChunk
		switch encoding {
		case chunk.PrometheusXorChunk:
			npc, err = pc.Add(model.SamplePair{
				Timestamp: i,
				Value:     model.SampleValue(float64(i)),
			})
		case chunk.PrometheusHistogramChunk:
			npc, err = pc.AddHistogram(int64(i), generateTestHistogram(int(i)))
		case chunk.PrometheusFloatHistogramChunk:
			npc, err = pc.AddFloatHistogram(int64(i), generateTestFloatHistogram(int(i)))
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", encoding)
		}
		require.NoError(t, err)
		require.Nil(t, npc)
	}
	return chunk.NewChunk(metric, pc, mint, maxt)
}
