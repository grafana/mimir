// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"path"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestTSDBChunks(t *testing.T) {
	tmpDir := t.TempDir()

	spec := block.SeriesSpec{
		Labels: labels.FromStrings(model.MetricNameLabel, "asdf"),
		Chunks: []chunks.Meta{
			must(chunks.ChunkFromSamples([]chunks.Sample{
				test.Sample{TS: 10, Val: 11},
				test.Sample{TS: 20, Val: 12},
				test.Sample{TS: 30, Val: 13},
			})),
			must(chunks.ChunkFromSamples([]chunks.Sample{
				test.Sample{TS: 40, Hist: test.GenerateTestHistogram(1)},
				test.Sample{TS: 50, Hist: test.GenerateTestHistogram(2)},
				test.Sample{TS: 60, Hist: test.GenerateTestHistogram(3)},
			})),
		},
	}

	meta, err := block.GenerateBlockFromSpec(tmpDir, []*block.SeriesSpec{&spec})
	require.NoError(t, err)

	co := test.CaptureOutput(t)

	chunkFilename := path.Join(tmpDir, meta.ULID.String(), "chunks", "000001")
	require.NoError(t, printChunksFile(chunkFilename, true))

	sout, _ := co.Done()

	expected := `%s
Chunk #0: position: 8 length: 15 encoding: XOR, crc32: 3d73a06f, samples: 3
Chunk #0, sample #0: st: 0 (1970-01-01T00:00:00Z), ts: 10 (1970-01-01T00:00:00.01Z), val: 11
Chunk #0, sample #1: st: 0 (1970-01-01T00:00:00Z), ts: 20 (1970-01-01T00:00:00.02Z), val: 12
Chunk #0, sample #2: st: 0 (1970-01-01T00:00:00Z), ts: 30 (1970-01-01T00:00:00.03Z), val: 13
Chunk #0: minTS=10 (1970-01-01T00:00:00.01Z), maxTS=30 (1970-01-01T00:00:00.03Z)
Chunk #1: position: 29 length: 56 encoding: histogram, crc32: 2232509d, samples: 3
Chunk #1, sample #0: st: 0 (1970-01-01T00:00:00Z), ts: 40 (1970-01-01T00:00:00.04Z), val: {count:21, sum:36.8, [-4,-2.82842712474619):2, [-2.82842712474619,-2):2, [-1.414213562373095,-1):3, [-1,-0.7071067811865475):2, [-0.001,0.001]:3, (0.7071067811865475,1]:2, (1,1.414213562373095]:3, (2,2.82842712474619]:2, (2.82842712474619,4]:2}
Chunk #1, sample #1: st: 0 (1970-01-01T00:00:00Z), ts: 50 (1970-01-01T00:00:00.05Z), val: {count:30, sum:55.199999999999996, [-4,-2.82842712474619):3, [-2.82842712474619,-2):3, [-1.414213562373095,-1):4, [-1,-0.7071067811865475):3, [-0.001,0.001]:4, (0.7071067811865475,1]:3, (1,1.414213562373095]:4, (2,2.82842712474619]:3, (2.82842712474619,4]:3}
Chunk #1, sample #2: st: 0 (1970-01-01T00:00:00Z), ts: 60 (1970-01-01T00:00:00.06Z), val: {count:39, sum:73.6, [-4,-2.82842712474619):4, [-2.82842712474619,-2):4, [-1.414213562373095,-1):5, [-1,-0.7071067811865475):4, [-0.001,0.001]:5, (0.7071067811865475,1]:4, (1,1.414213562373095]:5, (2,2.82842712474619]:4, (2.82842712474619,4]:4}
Chunk #1: minTS=40 (1970-01-01T00:00:00.04Z), maxTS=60 (1970-01-01T00:00:00.06Z)
`
	expected = fmt.Sprintf(expected, chunkFilename)

	require.Equal(t, expected, sout)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
