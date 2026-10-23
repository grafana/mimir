// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestMetricMetadataDictionaryKeepsValuesWhileReferenced(t *testing.T) {
	d := newMetricMetadataDictionary()
	help := strings.Repeat("h", 10)
	requests := metadata.Metadata{Type: model.MetricTypeCounter, Help: help[:9], Unit: "requests"}
	id := d.intern(requests)
	require.Equal(t, id, d.intern(requests))
	require.Equal(t, requests, d.value(id))
	require.NotSame(t, unsafe.StringData(help), unsafe.StringData(d.value(id).Help), "the value is copied in")
	require.EqualValues(t, 1, d.values.Load())
	require.EqualValues(t, len("counter")+9+len("requests"), d.bytes.Load())

	d.release(id)
	require.Zero(t, d.reclaim(), "a reference is left")
	d.release(id)
	require.Equal(t, 1, d.reclaim())
	require.Zero(t, d.values.Load())
	require.Zero(t, d.bytes.Load())

	// The id goes to the next new value.
	errors := metadata.Metadata{Type: model.MetricTypeCounter, Help: "Errors."}
	require.Equal(t, id, d.intern(errors))
	require.Equal(t, errors, d.value(id))
	require.NotEqual(t, id, d.intern(requests))
}

func TestMetricMetadataDictionaryGrowsInPages(t *testing.T) {
	d := newMetricMetadataDictionary()
	ids := map[uint32]metadata.Metadata{}
	for n := range 3 * metricMetadataPageSize {
		m := metadata.Metadata{Type: model.MetricTypeGauge, Help: fmt.Sprint(n)}
		ids[d.intern(m)] = m
	}
	for id, m := range ids {
		require.Equal(t, m, d.value(id))
	}
}

// metadataModel is a series' runs as the Prometheus TSDB's native metric metadata merges observations into its
// versions: the observation wins at an equal time, and of equal neighbours, the earliest is kept.
type metadataModel []metadataRun

func (m *metadataModel) observe(t int64, id uint32) {
	merged := slices.DeleteFunc(slices.Clone(*m), func(r metadataRun) bool { return r.from == t })
	at, _ := slices.BinarySearchFunc(merged, t, func(r metadataRun, t int64) int { return int(r.from - t) })
	merged = slices.Insert(merged, at, metadataRun{from: t, id: id})
	var runs metadataModel
	for _, run := range merged {
		if len(runs) > 0 && runs[len(runs)-1].id == run.id {
			continue
		}
		runs = append(runs, run)
	}
	*m = runs
}

// trim drops the runs that end at or before minTime: the next one starts by then.
func (m *metadataModel) trim(minTime int64) {
	for len(*m) > 1 && (*m)[1].from <= minTime {
		*m = (*m)[1:]
	}
}

func TestShardMetadataObservationsLikeAModel(t *testing.T) {
	for seed := range int64(50) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))
			d := newMetricMetadataDictionary()
			stats := &metricMetadataStats{}
			m := newShardMetadata(d, stats)
			values := make([]uint32, 4)
			for i := range values {
				values[i] = d.intern(metadata.Metadata{Type: model.MetricTypeCounter, Help: fmt.Sprint(i)})
			}
			series := make([]Series, 3)
			models := make([]metadataModel, len(series))
			for i := range series {
				series[i].ref = uint64(i + 1)
			}
			now := int64(1_000_000)
			for range 300 {
				i := r.Intn(len(series))
				switch op := r.Intn(20); {
				case op == 0:
					minTime := now - int64(r.Intn(20_000))
					for i := range series {
						m.trim(&series[i], minTime)
						models[i].trim(minTime)
					}
				case op == 1:
					m.drop(&series[i])
					models[i] = nil
				default:
					// Mostly in order, sometimes at the same time as before, or older.
					now += int64(r.Intn(3)) * 1000
					at := now
					if r.Intn(4) == 0 {
						at -= int64(r.Intn(10)) * 1000
					}
					id := values[r.Intn(len(values))]
					m.observe(&series[i], id, at, 0)
					models[i].observe(at, id)
				}
				for i := range series {
					require.Equal(t, []metadataRun(models[i]), m.runsOf(&series[i], nil), "series %d", i)
				}
			}
			// Every run holds a reference, and the interned ones one more.
			held := map[uint32]int64{}
			runs := 0
			for i := range series {
				for _, run := range m.runsOf(&series[i], nil) {
					held[run.id]++
					runs++
				}
			}
			for _, id := range values {
				require.Equal(t, held[id]+1, d.entry(id).refs.Load(), "id %d", id)
			}
			require.Equal(t, runs, m.series+m.runs)
			for i := range series {
				m.drop(&series[i])
			}
			require.Zero(t, m.series)
			require.Zero(t, m.runs)
			require.Empty(t, m.older)
			for _, id := range values {
				d.release(id)
			}
			require.Equal(t, len(values), d.reclaim())
			require.Zero(t, stats.clamped.Load())
		})
	}
}

func TestShardMetadataClampsObservationsBeforeTheBase(t *testing.T) {
	d := newMetricMetadataDictionary()
	stats := &metricMetadataStats{}
	m := newShardMetadata(d, stats)
	id := d.intern(metadata.Metadata{Type: model.MetricTypeGauge})
	series := Series{ref: 1}
	m.observe(&series, id, chunkRangeMs, 0)
	require.Equal(t, chunkRangeMs/2, m.base)
	other := Series{ref: 2}
	m.observe(&other, id, 0, 0)
	require.Equal(t, chunkRangeMs/2, m.start(&other))
	require.EqualValues(t, 1, stats.clamped.Load())
}

func TestMetricMetadataOf(t *testing.T) {
	stats := &metricMetadataStats{}
	_, ok := metricMetadataOf(mimirpb.SeriesMetadata{}, stats)
	require.False(t, ok, "empty metadata is none")
	m, ok := metricMetadataOf(mimirpb.SeriesMetadata{Help: "Help."}, stats)
	require.True(t, ok)
	require.Equal(t, metadata.Metadata{Type: model.MetricTypeUnknown, Help: "Help."}, m, "an empty type is unknown")
	_, ok = metricMetadataOf(mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: strings.Repeat("h", maxMetricMetadataBytes)}, stats)
	require.False(t, ok)
	require.EqualValues(t, 1, stats.oversized.Load())
}
