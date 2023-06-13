// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestBlockStreamingQuerierSeriesSet(t *testing.T) {

	cases := map[string]struct {
		input     []testSeries
		expResult []testSeries
	}{
		"simple case of one series": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar"),
					values: []testSample{{1, 1}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar"),
					values: []testSample{{1, 1}},
				},
			},
		},
		"multiple unique series": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar2"),
					values: []testSample{{2, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar2"),
					values: []testSample{{2, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
		},
		"multiple entries of the same series": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}, {6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
			},
		},
		"multiple entries of the same series again": {
			input: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{4, 3}, {5, 3}, {6, 3}},
				},
			},
			expResult: []testSeries{
				{
					lbls:   labels.FromStrings("foo", "bar1"),
					values: []testSample{{1, 1}, {2, 1}, {5, 10}, {6, 2}, {9, 2}},
				},
				{
					lbls:   labels.FromStrings("foo", "bar3"),
					values: []testSample{{3, 3}, {4, 3}, {5, 3}, {6, 3}},
				},
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ss := &blockStreamingQuerierSeriesSet{streamReader: &mockChunkStreamer{series: c.input}}
			for _, s := range c.input {
				ss.series = append(ss.series, &storepb.StreamingSeries{
					Labels: mimirpb.FromLabelsToLabelAdapters(s.lbls),
				})
			}
			idx := 0
			for ss.Next() {
				s := ss.At()
				require.Equal(t, c.expResult[idx].lbls, s.Labels())
				it := s.Iterator(nil)
				var actSamples []testSample
				for it.Next() != chunkenc.ValNone {
					ts, val := it.At()
					actSamples = append(actSamples, testSample{t: ts, v: val})
				}
				require.Equal(t, c.expResult[idx].values, actSamples)
				require.NoError(t, it.Err())
				idx++
			}
			require.NoError(t, ss.Err())
			require.Equal(t, len(c.expResult), idx)
		})
	}
}

type testSeries struct {
	lbls   labels.Labels
	values []testSample
}

type testSample struct {
	t int64
	v float64
}

type mockChunkStreamer struct {
	series []testSeries
	next   int
}

func (m *mockChunkStreamer) GetChunks(seriesIndex uint64) ([]storepb.AggrChunk, error) {
	if m.next >= len(m.series) {
		return nil, fmt.Errorf("out of chunks")
	}

	if uint64(m.next) != seriesIndex {
		return nil, fmt.Errorf("asked for the wrong series, exp: %d, got %d", m.next, seriesIndex)
	}

	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	if err != nil {
		return nil, err
	}

	samples := m.series[m.next].values
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
	for _, s := range samples {
		app.Append(s.t, s.v)
		if s.t < mint {
			mint = s.t
		}
		if s.t > maxt {
			maxt = s.t
		}
	}

	m.next++

	return []storepb.AggrChunk{{
		MinTime: mint,
		MaxTime: maxt,
		Raw:     &storepb.Chunk{Data: chk.Bytes()},
	}}, nil
}
