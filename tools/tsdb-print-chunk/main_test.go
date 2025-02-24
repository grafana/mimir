// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"path"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestTSDBPrintChunk(t *testing.T) {
	tmpDir := t.TempDir()

	spec := block.SeriesSpec{
		Labels: labels.FromStrings(labels.MetricName, "asdf"),
		Chunks: []chunks.Meta{
			must(chunks.ChunkFromSamples([]chunks.Sample{
				sample{10, 11, nil, nil},
				sample{20, 12, nil, nil},
				sample{30, 13, nil, nil},
			})),
			must(chunks.ChunkFromSamples([]chunks.Sample{
				sample{40, 0, test.GenerateTestHistogram(1), nil},
				sample{50, 0, test.GenerateTestHistogram(2), nil},
				sample{60, 0, test.GenerateTestHistogram(3), nil},
			})),
		},
	}

	meta, err := block.GenerateBlockFromSpec(tmpDir, []*block.SeriesSpec{&spec})
	require.NoError(t, err)

	blockDir := path.Join(tmpDir, meta.ULID.String())

	var chunkRefs []string
	for _, chkMeta := range spec.Chunks {
		chunkRefs = append(chunkRefs, strconv.Itoa(int(chkMeta.Ref)))
	}

	co := test.CaptureOutput(t)
	printChunks(blockDir, chunkRefs)
	sout, _ := co.Done()

	expected := `Chunk ref: 8 samples: 3 bytes: 15
11	10 (1970-01-01T00:00:00.01Z)
12	20 (1970-01-01T00:00:00.02Z)
13	30 (1970-01-01T00:00:00.03Z)
Chunk ref: 29 samples: 3 bytes: 56
{count:21, sum:36.8, [-4,-2.82842712474619):2, [-2.82842712474619,-2):2, [-1.414213562373095,-1):3, [-1,-0.7071067811865475):2, [-0.001,0.001]:3, (0.7071067811865475,1]:2, (1,1.414213562373095]:3, (2,2.82842712474619]:2, (2.82842712474619,4]:2}	40 (1970-01-01T00:00:00.04Z) H UnknownCounterReset
{count:30, sum:55.199999999999996, [-4,-2.82842712474619):3, [-2.82842712474619,-2):3, [-1.414213562373095,-1):4, [-1,-0.7071067811865475):3, [-0.001,0.001]:4, (0.7071067811865475,1]:3, (1,1.414213562373095]:4, (2,2.82842712474619]:3, (2.82842712474619,4]:3}	50 (1970-01-01T00:00:00.05Z) H NotCounterReset
{count:39, sum:73.6, [-4,-2.82842712474619):4, [-2.82842712474619,-2):4, [-1.414213562373095,-1):5, [-1,-0.7071067811865475):4, [-0.001,0.001]:5, (0.7071067811865475,1]:4, (1,1.414213562373095]:5, (2,2.82842712474619]:4, (2.82842712474619,4]:4}	60 (1970-01-01T00:00:00.06Z) H NotCounterReset
`

	require.Equal(t, expected, sout)
}

type sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s sample) T() int64                      { return s.t }
func (s sample) F() float64                    { return s.v }
func (s sample) H() *histogram.Histogram       { return s.h }
func (s sample) FH() *histogram.FloatHistogram { return s.fh }

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s sample) Copy() chunks.Sample {
	c := sample{t: s.t, v: s.v}
	if s.h != nil {
		c.h = s.h.Copy()
	}
	if s.fh != nil {
		c.fh = s.fh.Copy()
	}
	return c
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
